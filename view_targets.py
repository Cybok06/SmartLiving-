from flask import Blueprint, render_template, request, redirect, url_for, flash
from bson.objectid import ObjectId
from datetime import datetime, timedelta
from db import db

view_targets_bp = Blueprint('view_targets', __name__)

targets_collection = db["targets"]
users_collection = db["users"]
customers_collection = db["customers"]
payments_collection = db["payments"]

def get_date_range(duration):
    today = datetime.utcnow().date()
    if duration == "daily":
        return today, today
    elif duration == "weekly":
        start = today - timedelta(days=today.weekday())
        return start, start + timedelta(days=6)
    elif duration == "monthly":
        start = today.replace(day=1)
        end = (start.replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)
        return start, end
    elif duration == "yearly":
        start = today.replace(month=1, day=1)
        end = today.replace(month=12, day=31)
        return start, end
    return today, today

@view_targets_bp.route('/view_targets')
def view_targets():
    selected_duration = request.args.get("duration")
    selected_branch = request.args.get("branch")

    query = {}
    if selected_duration:
        query["duration_type"] = selected_duration

    targets = list(targets_collection.find(query))
    results = []
    branches = set()

    # Preload all manager data to populate branch filter and optionally default to first branch
    for target in targets:
        manager_id = target.get("manager_id")
        if not manager_id:
            continue
        manager = users_collection.find_one({"_id": ObjectId(manager_id)}, {"branch": 1})
        if manager and manager.get("branch"):
            branches.add(manager["branch"])

    if not selected_branch and branches:
        selected_branch = sorted(branches)[0]

    for target in targets:
        manager_id = target.get("manager_id")
        if not manager_id:
            continue

        manager = users_collection.find_one(
            {"_id": ObjectId(manager_id)},
            {"name": 1, "branch": 1}
        )
        if not manager:
            continue

        branch = manager.get("branch", "Unknown")
        if branch not in branches:
            branches.add(branch)

        if selected_branch and selected_branch != branch:
            continue

        duration = target.get("duration_type", "daily")
        start_date, end_date = get_date_range(duration)
        start_str, end_str = str(start_date), str(end_date)

        product_sales_result = list(customers_collection.aggregate([
            {
                "$match": {
                    "manager_id": ObjectId(manager_id),
                    "purchases.purchase_date": {"$gte": start_str, "$lte": end_str}
                }
            },
            {"$unwind": "$purchases"},
            {
                "$match": {
                    "purchases.purchase_date": {"$gte": start_str, "$lte": end_str}
                }
            },
            {
                "$group": {
                    "_id": None,
                    "total": {"$sum": "$purchases.product.total"}
                }
            }
        ]))
        product_sales_amount = product_sales_result[0]["total"] if product_sales_result else 0

        customer_count = customers_collection.count_documents({
            "manager_id": ObjectId(manager_id),
            "purchases": {
                "$elemMatch": {
                    "purchase_date": {"$gte": start_str, "$lte": end_str}
                }
            }
        })

        agent_ids = users_collection.find(
            {"role": "agent", "manager_id": ObjectId(manager_id)},
            {"_id": 1}
        )
        agent_id_list = [str(agent["_id"]) for agent in agent_ids]

        payment_result = list(payments_collection.aggregate([
            {
                "$match": {
                    "agent_id": {"$in": agent_id_list},
                    "payment_type": {"$in": ["PAYMENT", "PRODUCT", "SUSU"]},
                    "date": {"$gte": start_str, "$lte": end_str}
                }
            },
            {"$group": {"_id": None, "total": {"$sum": "$amount"}}}
        ]))
        total_payment = payment_result[0]["total"] if payment_result else 0

        pt = target.get("product_target", 1)
        ct = target.get("cash_target", 1)
        cut = target.get("customer_target", 1)

        product_pct = round((product_sales_amount / pt) * 100 if pt else 0, 2)
        payment_pct = round((total_payment / ct) * 100 if ct else 0, 2)
        customer_pct = round((customer_count / cut) * 100 if cut else 0, 2)
        overall = round((product_pct + payment_pct + customer_pct) / 3, 2)

        results.append({
            "_id": str(target["_id"]),
            "title": target.get("title"),
            "duration": duration,
            "manager": manager.get("name"),
            "branch": branch,

            "product_target": pt,
            "product_achieved": product_sales_amount,
            "product_pct": product_pct,

            "cash_target": ct,
            "cash_achieved": total_payment,
            "payment_pct": payment_pct,

            "customer_target": cut,
            "customer_achieved": customer_count,
            "customer_pct": customer_pct,

            "overall": overall
        })

    return render_template("view_targets.html",
                           results=results,
                           branches=sorted(branches),
                           request=request,
                           selected_branch=selected_branch)

@view_targets_bp.route('/update-target/<target_id>', methods=["POST"])
def update_target(target_id):
    title = request.form.get("title")
    duration = request.form.get("duration")
    product_target = request.form.get("product_target", type=int)
    cash_target = request.form.get("cash_target", type=int)
    customer_target = request.form.get("customer_target", type=int)

    update_data = {
        "title": title,
        "duration_type": duration,
        "product_target": product_target,
        "cash_target": cash_target,
        "customer_target": customer_target
    }

    targets_collection.update_one({"_id": ObjectId(target_id)}, {"$set": update_data})
    flash("Target updated successfully", "success")

    duration_filter = request.args.get("duration", "")
    branch_filter = request.args.get("branch", "")
    return redirect(url_for('view_targets.view_targets', duration=duration_filter, branch=branch_filter))

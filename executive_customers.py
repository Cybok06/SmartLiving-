from flask import Blueprint, render_template, request, redirect, url_for, flash
from bson.objectid import ObjectId
from datetime import datetime
from collections import defaultdict
from db import db

executive_customers_bp = Blueprint("executive_customers", __name__)

# Collections
customers_col = db["customers"]
users_col = db["users"]
payments_col = db["payments"]

# ================================
# 1. Summary: Customers per Branch
# ================================
@executive_customers_bp.route("/executive/customers")
def executive_customers():
    managers = users_col.find({"role": "manager"}, {"_id": 1, "branch": 1})
    manager_branch_map = {str(m["_id"]): m.get("branch", "Unknown") for m in managers}

    agents = users_col.find({"role": "agent"}, {"_id": 1, "manager_id": 1})
    agent_to_manager_map = {str(agent["_id"]): str(agent.get("manager_id", "")) for agent in agents}

    branch_counts = defaultdict(int)
    for customer in customers_col.find({}, {"agent_id": 1}):
        agent_id = str(customer.get("agent_id"))
        manager_id = agent_to_manager_map.get(agent_id)
        if manager_id:
            branch = manager_branch_map.get(manager_id, "Unknown")
            branch_counts[branch] += 1

    data = [{"branch": branch, "count": count} for branch, count in branch_counts.items()]
    return render_template("executive_customers.html", data=data)

# ====================================
# 2. Searchable Executive Customer List + Pagination
# ====================================
@executive_customers_bp.route("/executive/customers/list")
def executive_customers_list():
    search_term = request.args.get('search', '').strip()
    page = int(request.args.get('page', 1))
    per_page = 20
    skip = (page - 1) * per_page

    query = {}
    if search_term:
        query["$or"] = [
            {"name": {"$regex": search_term, "$options": "i"}},
            {"phone_number": {"$regex": search_term, "$options": "i"}}
        ]

    total_customers = customers_col.count_documents(query)
    customers_cursor = customers_col.find(query, {
        "_id": 1,
        "name": 1,
        "phone_number": 1,
        "image_url": 1
    }).skip(skip).limit(per_page)

    customers_data = [{
        "id": str(c["_id"]),
        "name": c.get("name", "N/A"),
        "phone": c.get("phone_number", "N/A"),
        "image_url": c.get("image_url", "https://via.placeholder.com/80")
    } for c in customers_cursor]

    total_pages = (total_customers + per_page - 1) // per_page

    return render_template(
        "executive_customer_list.html",
        customers=customers_data,
        search_term=search_term,
        current_page=page,
        total_pages=total_pages
    )

# ==========================
# 3. Executive Customer View
# ==========================
@executive_customers_bp.route("/executive/customer/<customer_id>")
def executive_customer_profile(customer_id):
    try:
        customer_object_id = ObjectId(customer_id)
    except:
        flash("Invalid customer ID format.", "danger")
        return redirect(url_for('executive_customers.executive_customers_list'))

    customer = customers_col.find_one({'_id': customer_object_id})
    if not customer:
        return "Customer not found", 404

    purchases = customer.get('purchases', [])
    total_debt = sum(int(p['product'].get('total', 0)) for p in purchases if 'product' in p)

    all_payments = list(payments_col.find({"customer_id": customer_object_id}))
    deposits = sum(p.get("amount", 0) for p in all_payments if p.get("payment_type") != "WITHDRAWAL")
    withdrawals = [p for p in all_payments if p.get("payment_type") == "WITHDRAWAL"]
    withdrawn_amount = sum(p.get("amount", 0) for p in withdrawals)

    total_paid = round(deposits - withdrawn_amount, 2)
    withdrawn_amount = round(withdrawn_amount, 2)
    amount_left = round(total_debt - total_paid, 2)

    penalties = customer.get('penalties', [])
    total_penalty = round(sum(p.get("amount", 0) for p in penalties), 2)

    steps = ["payment_ongoing", "completed", "approved", "packaging", "delivering", "delivered"]
    current_status = customer.get("status", "payment_ongoing")

    for p in purchases:
        try:
            start = datetime.strptime(p.get("purchase_date", "")[:10], "%Y-%m-%d")
            end = datetime.strptime(p.get("end_date", "")[:10], "%Y-%m-%d")
            total_days = (end - start).days
            elapsed_days = (datetime.today() - start).days
            progress = int((elapsed_days / total_days) * 100) if total_days > 0 else 100
            p["product"]["progress"] = max(0, min(progress, 100))
        except:
            p["product"]["progress"] = 0

    return render_template(
        "executive_customer_profile.html",
        customer=customer,
        total_debt=total_debt,
        total_paid=total_paid,
        amount_left=amount_left,
        withdrawn_amount=withdrawn_amount,
        withdrawals=withdrawals,
        penalties=penalties,
        total_penalty=total_penalty,
        steps=steps,
        current_status=current_status
    )

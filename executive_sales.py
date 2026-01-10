from flask import Blueprint, render_template, request, jsonify
from db import db
from datetime import datetime, timedelta

executive_sales_bp = Blueprint("executive_sales", __name__)
payments_col = db["payments"]
users_col = db["users"]

# Load manager branches as a map
def get_manager_branch_map():
    return {
        str(m["_id"]): m.get("branch", "Unknown")
        for m in users_col.find({"role": "manager"}, {"_id": 1, "branch": 1})
    }

# Optimized aggregation using MongoDB
def aggregate_sales_by_branch(start_date, end_date):
    pipeline = [
        {
            "$match": {
                "payment_type": {"$nin": ["SUSU", "WITHDRAWAL"]},
                "date": {"$gte": start_date.strftime("%Y-%m-%d"), "$lte": end_date.strftime("%Y-%m-%d")}
            }
        },
        {
            "$group": {
                "_id": "$manager_id",
                "total": {"$sum": "$amount"}
            }
        }
    ]

    result = payments_col.aggregate(pipeline)
    manager_map = get_manager_branch_map()

    branch_sales = {}
    for doc in result:
        manager_id = str(doc["_id"])
        branch = manager_map.get(manager_id, "Unknown")
        branch_sales[branch] = branch_sales.get(branch, 0) + float(doc["total"])
    return branch_sales

@executive_sales_bp.route("/executive/sales")
def executive_sales():
    now = datetime.utcnow()
    range_param = request.args.get("range", "daily")
    start_date_str = request.args.get("start")
    end_date_str = request.args.get("end")
    history_type = request.args.get("history", "weekly")

    # Parse start/end date
    try:
        if start_date_str and end_date_str:
            start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
            end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
        elif range_param == "weekly":
            start_date = now - timedelta(days=now.weekday())
            end_date = now
        elif range_param == "monthly":
            start_date = datetime(now.year, now.month, 1)
            end_date = now
        else:
            start_date = datetime(now.year, now.month, now.day)
            end_date = now
    except ValueError:
        start_date = datetime(now.year, now.month, now.day)
        end_date = now

    # 🔥 FAST MAIN SALES
    sales = aggregate_sales_by_branch(start_date, end_date)

    # 🔥 FAST HISTORY (last 6 units)
    history = []
    for i in range(1, 7):
        if history_type == "monthly":
            ref = datetime(now.year, now.month, 1) - timedelta(days=30 * i)
            next_month = datetime(ref.year + (ref.month // 12), ((ref.month % 12) + 1), 1)
            start, end = ref, next_month
            label = start.strftime("%B %Y")
        elif history_type == "weekly":
            start = (now - timedelta(days=now.weekday())) - timedelta(weeks=i)
            end = start + timedelta(days=7)
            label = f"{start.strftime('%b %d')} - {end.strftime('%b %d')}"
        else:
            start = now - timedelta(days=i)
            end = start + timedelta(days=1)
            label = start.strftime("%Y-%m-%d")

        period_sales = aggregate_sales_by_branch(start, end)
        history.append({
            "label": label,
            "range": f"{start.strftime('%Y-%m-%d')} to {end.strftime('%Y-%m-%d')}",
            "sales": period_sales
        })

    # AJAX only
    if request.headers.get("X-Requested-With") == "XMLHttpRequest":
        return jsonify({
            "sales": sales,
            "start": start_date.strftime("%Y-%m-%d"),
            "end": end_date.strftime("%Y-%m-%d")
        })

    # Standard full page render
    return render_template("executive_sales.html",
                           sales=sales,
                           active_range=range_param,
                           start_date=start_date.strftime("%Y-%m-%d"),
                           end_date=end_date.strftime("%Y-%m-%d"),
                           history_type=history_type,
                           history=history)

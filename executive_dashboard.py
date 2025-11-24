from flask import Blueprint, render_template, jsonify
from db import db
from datetime import datetime, timedelta

executive_bp = Blueprint('executive_dashboard', __name__)

# Collections
customers_col = db["customers"]
payments_col = db["payments"]
leads_col = db["leads"]
users_col = db["users"]


@executive_bp.route("/executive/dashboard")
def executive_dashboard():
    """
    Lightweight initial render:
    - Summary metrics only
    - Heavy chart data (top_products, top_managers) is loaded via AJAX
    """
    now = datetime.utcnow()

    # Time ranges
    start_of_day = datetime(now.year, now.month, now.day)
    start_of_next_day = start_of_day + timedelta(days=1)
    start_of_week = start_of_day - timedelta(days=now.weekday())
    start_of_next_week = start_of_week + timedelta(days=7)
    start_of_month = datetime(now.year, now.month, 1)
    start_of_next_month = (
        datetime(now.year + 1, 1, 1)
        if now.month == 12
        else datetime(now.year, now.month + 1, 1)
    )

    # Total customers (fast, approximate but OK for dashboard)
    total_customers = customers_col.estimated_document_count()

    # Total leads
    total_leads = leads_col.estimated_document_count()

    # Total products sold
    sold_result = customers_col.aggregate([
        {"$match": {"purchases": {"$exists": True, "$ne": []}}},
        {"$group": {"_id": None, "total": {"$sum": {"$size": "$purchases"}}}}
    ])
    total_products_sold = next(sold_result, {}).get("total", 0)

    # Customers earned (today/week/month)
    earned_pipeline = customers_col.aggregate([
        {"$match": {"purchases": {"$exists": True, "$ne": []}}},
        {"$project": {
            "converted_dates": {
                "$map": {
                    "input": "$purchases",
                    "as": "p",
                    "in": {
                        "$cond": [
                            {"$eq": [{"$type": "$$p.purchase_date"}, "string"]},
                            {
                                "$dateFromString": {
                                    "dateString": "$$p.purchase_date",
                                    "format": "%Y-%m-%d"
                                }
                            },
                            "$$p.purchase_date"
                        ]
                    }
                }
            }
        }},
        {"$project": {
            "first_purchase_date": {"$min": "$converted_dates"}
        }},
        {"$facet": {
            "today": [
                {"$match": {"first_purchase_date": {"$gte": start_of_day, "$lt": start_of_next_day}}},
                {"$count": "count"}
            ],
            "week": [
                {"$match": {"first_purchase_date": {"$gte": start_of_week, "$lt": start_of_next_week}}},
                {"$count": "count"}
            ],
            "month": [
                {"$match": {"first_purchase_date": {"$gte": start_of_month, "$lt": start_of_next_month}}},
                {"$count": "count"}
            ]
        }}
    ])
    earned = next(earned_pipeline, {})

    def safe_count(result):
        return result[0]['count'] if result and isinstance(result[0], dict) else 0

    customers_today = safe_count(earned.get("today", []))
    customers_week = safe_count(earned.get("week", []))
    customers_month = safe_count(earned.get("month", []))

    # Total earnings & withdrawals
    payments_summary = payments_col.aggregate([
        {
            "$facet": {
                "total": [
                    {"$match": {"payment_type": {"$ne": "WITHDRAWAL"}}},
                    {"$group": {"_id": None, "amount": {"$sum": "$amount"}}}
                ],
                "withdrawal": [
                    {"$match": {"payment_type": "WITHDRAWAL"}},
                    {"$group": {"_id": None, "amount": {"$sum": "$amount"}}}
                ]
            }
        }
    ])
    e = next(payments_summary, {})

    def get_amount(key):
        values = e.get(key, [])
        if values and isinstance(values[0], dict):
            return round(values[0].get("amount", 0), 2)
        return 0.00

    total_earnings = get_amount("total")
    total_withdrawals = get_amount("withdrawal")

    # NOTE: charts (top_products/top_managers) are loaded via /executive/dashboard/charts

    return render_template("executive_dashboard.html", data={
        "total_earnings": total_earnings,
        "total_withdrawals": total_withdrawals,
        "total_customers": total_customers,
        "total_products_sold": total_products_sold,
        "total_leads": total_leads,
        "customers_today": customers_today,
        "customers_week": customers_week,
        "customers_month": customers_month
    })


@executive_bp.route("/executive/dashboard/charts")
def dashboard_charts():
    """
    Heavier aggregations for charts.
    Called asynchronously from the frontend so the main page loads faster.
    """
    # Top Products Sold
    top_products_cursor = customers_col.aggregate([
        {"$unwind": "$purchases"},
        {"$group": {"_id": "$purchases.product.name", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 5}
    ])
    top_products = {"labels": [], "values": []}
    for item in top_products_cursor:
        top_products["labels"].append(item["_id"] or "Unnamed")
        top_products["values"].append(item["count"])

    # Top Managers by Payments
    top_managers_cursor = payments_col.aggregate([
        {"$match": {"payment_type": {"$ne": "WITHDRAWAL"}}},
        {"$lookup": {
            "from": "users",
            "localField": "manager_id",
            "foreignField": "_id",
            "as": "manager"
        }},
        {"$unwind": "$manager"},
        {"$group": {
            "_id": {
                "manager_id": "$manager._id",
                "name": "$manager.name",
                "branch": "$manager.branch"
            },
            "total": {"$sum": "$amount"}
        }},
        {"$sort": {"total": -1}},
        {"$limit": 5}
    ])
    top_managers = {"labels": [], "values": []}
    for item in top_managers_cursor:
        label = f"{item['_id']['name']} ({item['_id']['branch']})"
        top_managers["labels"].append(label)
        top_managers["values"].append(round(item["total"], 2))

    return jsonify({
        "ok": True,
        "top_products": top_products,
        "top_managers": top_managers
    })

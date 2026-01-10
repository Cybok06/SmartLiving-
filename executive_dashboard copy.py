from flask import Blueprint, render_template, request
from db import db
from datetime import datetime, timedelta
from bson import ObjectId

executive_bp = Blueprint('executive_dashboard', __name__)

customers_col = db["customers"]
payments_col = db["payments"]
leads_col = db["leads"]
users_col = db["users"]

@executive_bp.route("/executive/dashboard")
def executive_dashboard():
    now = datetime.utcnow()
    start_of_day = datetime(now.year, now.month, now.day)
    start_of_week = start_of_day - timedelta(days=start_of_day.weekday())
    start_of_month = datetime(now.year, now.month, 1)
    tomorrow = start_of_day + timedelta(days=1)

    # Total counts
    total_customers = customers_col.estimated_document_count()
    total_leads = leads_col.estimated_document_count()

    # Total products sold
    sold_result = customers_col.aggregate([
        {"$match": {"purchases": {"$exists": True, "$ne": []}}},
        {"$group": {"_id": None, "total": {"$sum": {"$size": "$purchases"}}}}
    ])
    total_products_sold = next(sold_result, {}).get("total", 0)

    # 🟩 Customers earned by purchase date
    earned_data = {"today": 0, "week": 0, "month": 0}
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
                            {"$dateFromString": {
                                "dateString": "$$p.purchase_date",
                                "format": "%Y-%m-%d"
                            }},
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
                {"$match": {"first_purchase_date": {"$gte": start_of_day, "$lt": tomorrow}}},
                {"$count": "count"}
            ],
            "week": [
                {"$match": {"first_purchase_date": {"$gte": start_of_week, "$lt": tomorrow}}},
                {"$count": "count"}
            ],
            "month": [
                {"$match": {"first_purchase_date": {"$gte": start_of_month, "$lt": tomorrow}}},
                {"$count": "count"}
            ]
        }}
    ])
    result = next(earned_pipeline, {})
    earned_data["today"] = result.get("today", [{}])[0].get("count", 0)
    earned_data["week"] = result.get("week", [{}])[0].get("count", 0)
    earned_data["month"] = result.get("month", [{}])[0].get("count", 0)

    # 🟩 Total earnings (excluding WITHDRAWAL)
    earnings_pipeline = payments_col.aggregate([
        {"$match": {"payment_type": {"$ne": "WITHDRAWAL"}}},
        {"$group": {"_id": None, "amount": {"$sum": "$amount"}}}
    ])
    total_earnings = next(earnings_pipeline, {}).get("amount", 0)

    # 🟩 Total withdrawals (all-time)
    withdrawal_pipeline = payments_col.aggregate([
        {"$match": {"payment_type": "WITHDRAWAL"}},
        {"$group": {"_id": None, "amount": {"$sum": "$amount"}}}
    ])
    total_withdrawals = next(withdrawal_pipeline, {}).get("amount", 0)

    # 🟩 Top Products in selected range
    top_products_cursor = customers_col.aggregate([
        {"$match": {"purchases": {"$exists": True, "$ne": []}}},
        {"$unwind": "$purchases"},
        {"$addFields": {
            "parsed_date": {
                "$cond": [
                    {"$eq": [{"$type": "$purchases.purchase_date"}, "string"]},
                    {"$dateFromString": {"dateString": "$purchases.purchase_date", "format": "%Y-%m-%d"}},
                    "$purchases.purchase_date"
                ]
            }
        }},
        {"$match": {"parsed_date": {"$gte": start_of_month, "$lt": tomorrow}}},
        {"$group": {"_id": "$purchases.product.name", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 5}
    ])
    top_products = {"labels": [], "values": []}
    for item in top_products_cursor:
        top_products["labels"].append(item["_id"] or "Unnamed")
        top_products["values"].append(item["count"])

    # 🟩 Top Managers by total payments (all-time)
    manager_docs = users_col.find({"role": "manager"}, {"_id": 1, "name": 1, "branch": 1})
    manager_map = {str(m["_id"]): f"{m['name']} ({m.get('branch', 'Unknown')})" for m in manager_docs}

    top_managers_raw = payments_col.aggregate([
        {"$match": {"payment_type": {"$ne": "WITHDRAWAL"}, "manager_id": {"$ne": None}}},
        {"$group": {"_id": "$manager_id", "total": {"$sum": "$amount"}}},
        {"$sort": {"total": -1}},
        {"$limit": 5}
    ])
    top_managers = {"labels": [], "values": []}
    for entry in top_managers_raw:
        manager_id = str(entry["_id"])
        label = manager_map.get(manager_id, "Unknown")
        top_managers["labels"].append(label)
        top_managers["values"].append(round(entry["total"], 2))

    return render_template("executive_dashboard.html", data={
        "total_earnings": round(total_earnings, 2),
        "total_withdrawals": round(total_withdrawals, 2),
        "total_customers": total_customers,
        "total_products_sold": total_products_sold,
        "total_leads": total_leads,
        "customers_today": earned_data["today"],
        "customers_week": earned_data["week"],
        "customers_month": earned_data["month"],
        "top_products": top_products,
        "top_managers": top_managers
    })

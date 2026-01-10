from flask import Blueprint, render_template, session, request
from pymongo import MongoClient
from pymongo.server_api import ServerApi
from bson.objectid import ObjectId
from datetime import datetime
from db import db

admin_close_account_bp = Blueprint('admin_close_account', __name__)


account_confirmed_col = db.account_confirmed
users_col = db.users

@admin_close_account_bp.route('/account_close_summary', methods=['GET'])
def account_close_summary():
    admin_id = session.get('admin_id')
    if not admin_id:
        return "Unauthorized", 401

    manager_filter = request.args.get('manager')
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')

    query = {}
    if start_date and end_date:
        try:
            start = datetime.strptime(start_date, "%Y-%m-%d")
            end = datetime.strptime(end_date, "%Y-%m-%d")
            query["date"] = {"$gte": start, "$lte": end}
        except:
            pass

    all_records = list(account_confirmed_col.find(query))

    # Get manager and admin names
    manager_ids = {rec['manager_id'] for rec in all_records}
    admin_ids = {rec['confirmed_by'] for rec in all_records if isinstance(rec['confirmed_by'], ObjectId)}

    managers = users_col.find({"_id": {"$in": list(manager_ids)}})
    admins = users_col.find({"_id": {"$in": list(admin_ids)}})

    manager_map = {m['_id']: m.get('username', 'Unknown') for m in managers}
    admin_map = {a['_id']: a.get('username', 'Unknown Admin') for a in admins}

    # Group by (manager_id, date)
    grouped = {}
    for rec in all_records:
        mid = rec['manager_id']
        date_str = rec['date'].strftime('%Y-%m-%d')
        key = (mid, date_str)

        if key not in grouped:
            grouped[key] = {
                "manager_name": manager_map.get(mid, "Unknown"),
                "date": date_str,
                "total_payment": 0,
                "surplus": 0,
                "shortage": 0,
                "confirmed_by": rec.get('confirmed_by'),
                "is_mine": rec.get('confirmed_by') == ObjectId(admin_id)
            }

        if grouped[key]["is_mine"]:
            grouped[key]["total_payment"] += rec.get("total_payment", 0)
            grouped[key]["surplus"] += rec.get("surplus", 0)
            grouped[key]["shortage"] += rec.get("shortage", 0)

    # Prepare for display
    display_records = []
    total_payment = total_surplus = total_shortage = total_confirmed = 0

    for data in grouped.values():
        is_mine = data["is_mine"]
        if is_mine:
            total_payment += data["total_payment"]
            total_surplus += data["surplus"]
            total_shortage += data["shortage"]
            total_confirmed += 1

        display_records.append({
            "date": data["date"],
            "manager_name": data["manager_name"],
            "total_payment": data["total_payment"] if is_mine else "",
            "surplus": data["surplus"] if is_mine else "",
            "shortage": data["shortage"] if is_mine else "",
            "closed_by": "You" if is_mine else admin_map.get(data["confirmed_by"], "Unknown Admin")
        })

    return render_template('admin_account_summary.html',
                           total_confirmed=total_confirmed,
                           total_payment=total_payment,
                           total_surplus=total_surplus,
                           total_shortage=total_shortage,
                           records=display_records,
                           selected_manager=manager_filter,
                           start_date=start_date,
                           end_date=end_date)

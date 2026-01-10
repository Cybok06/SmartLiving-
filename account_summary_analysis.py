from flask import Blueprint, render_template, session, jsonify
from pymongo import MongoClient
from pymongo.server_api import ServerApi
from bson.objectid import ObjectId
from datetime import datetime, timedelta
from collections import defaultdict
from db import db

account_summary_analysis_bp = Blueprint('account_summary_analysis', __name__)

# DB connection

account_confirmed_col = db.account_confirmed

@account_summary_analysis_bp.route('/account_summary_analysis')
def account_summary_analysis():
    return render_template('account_summary_analysis.html')

@account_summary_analysis_bp.route('/account_summary_data')
def account_summary_data():
    current_admin_id = session.get('admin_id')
    if not current_admin_id:
        return jsonify({"error": "Unauthorized"}), 401

    # Prepare last 30 days
    today = datetime.utcnow()
    start_date = today - timedelta(days=29)

    # Fetch only records by the current admin
    records = list(account_confirmed_col.find({
        "confirmed_by": ObjectId(current_admin_id),
        "date": {"$gte": start_date}
    }))

    # Organize data by date
    stats = defaultdict(lambda: {"payment": 0, "surplus": 0, "shortage": 0})
    for rec in records:
        date_str = rec["date"].strftime("%Y-%m-%d")
        stats[date_str]["payment"] += rec.get("total_payment", 0)
        stats[date_str]["surplus"] += rec.get("surplus", 0)
        stats[date_str]["shortage"] += rec.get("shortage", 0)

    # Generate full date range
    labels = [(today - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(29, -1, -1)]
    data = {
        "labels": labels,
        "payment": [stats[d]["payment"] for d in labels],
        "surplus": [stats[d]["surplus"] for d in labels],
        "shortage": [stats[d]["shortage"] for d in labels]
    }

    return jsonify(data)

from flask import Blueprint, render_template, session, redirect, url_for, flash
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from bson.objectid import ObjectId
from datetime import datetime, timedelta
import json
from db import db
manager_analysis_bp = Blueprint('manager_analysis', __name__)


users_col = db.users
customers_col = db.customers
payments_col = db.payments

def serialize_agent(agent):
    return {
        '_id': str(agent.get('_id')),
        'name': agent.get('name'),
        'customer_count': agent.get('customer_count', 0),
        'retention_count': agent.get('retention_count', 0)  # Add retention to agent data
    }

@manager_analysis_bp.route('/manager_analysis')
def manager_analysis():
    if 'manager_id' not in session:
        return redirect(url_for('login.login'))

    try:
        manager_oid = ObjectId(session['manager_id'])
    except Exception:
        flash("Invalid manager session.", "error")
        return redirect(url_for('login.logout'))

    # Get agents under manager
    agents = list(users_col.find({"manager_id": manager_oid, "role": "agent"}))
    agent_ids = [str(agent['_id']) for agent in agents]

    # 1. Top customer locations (Pie Chart)
    location_data = list(customers_col.aggregate([
        {"$match": {"agent_id": {"$in": agent_ids}}},
        {"$group": {"_id": "$location", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 10}
    ]))

    # 2. Most purchased products (Bar Chart)
    pipeline_products = [
        {"$match": {"agent_id": {"$in": agent_ids}}},
        {"$unwind": "$purchases"},
        {"$group": {
            "_id": "$purchases.product.name",
            "total_quantity": {"$sum": "$purchases.product.quantity"}
        }},
        {"$sort": {"total_quantity": -1}},
        {"$limit": 10}
    ]
    product_ranking = list(customers_col.aggregate(pipeline_products))

    # 3. Customer Retention / Repeat Purchases per agent
    retention_pipeline = [
        {"$match": {"agent_id": {"$in": agent_ids}}},
        {"$project": {
            "agent_id": 1,
            "purchase_count": {"$size": {"$ifNull": ["$purchases", []]}}
        }},
        {"$match": {"purchase_count": {"$gt": 1}}},
        {"$group": {
            "_id": "$agent_id",
            "retention_count": {"$sum": 1}
        }}
    ]
    retention_data = list(customers_col.aggregate(retention_pipeline))
    retention_map = {r["_id"]: r["retention_count"] for r in retention_data}

    # 4. Agents ranked by total customers (Bar Chart)
    customer_counts = customers_col.aggregate([
        {"$match": {"agent_id": {"$in": agent_ids}}},
        {"$group": {"_id": "$agent_id", "count": {"$sum": 1}}}
    ])
    customer_count_map = {c['_id']: c['count'] for c in customer_counts}

    for agent in agents:
        aid_str = str(agent['_id'])
        agent['customer_count'] = customer_count_map.get(aid_str, 0)
        agent['retention_count'] = retention_map.get(aid_str, 0)

    agents_serialized = [serialize_agent(agent) for agent in agents]

    return render_template(
        'manager_analysis.html',
        location_data=json.dumps(location_data),
        product_ranking=json.dumps(product_ranking),
        agents=json.dumps(agents_serialized)
    )

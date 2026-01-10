from flask import Blueprint, render_template, jsonify
from flask_login import login_required, current_user
from pymongo import MongoClient
from collections import defaultdict
from datetime import datetime, timedelta
from db import db

analysis_bp = Blueprint('analysis', __name__)

# MongoDB connection

payments_collection = db["payments"]
customers_collection = db["customers"]

@analysis_bp.route('/')
@login_required
def analysis_dashboard():
    return render_template('analysis.html')

@analysis_bp.route('/report_data')
@login_required
def report_data():
    now = datetime.now()
    agent_id = str(current_user.id)

    # 1. Daily Payments (Last 30 Days) for current agent
    payment_data = []
    for i in range(30):
        day = (now - timedelta(days=29 - i)).strftime('%Y-%m-%d')
        total_payment = sum(
            p['amount'] for p in payments_collection.find({
                'date': day,
                'agent_id': agent_id
            })
        )
        payment_data.append({"date": day, "total_payment": total_payment})

    # 2. Total Debt and Total Collected (only for this agent's customers)
    total_debt = 0
    total_collected = 0

    customers = customers_collection.find({'agent_id': agent_id})
    for customer in customers:
        for purchase in customer.get('purchases', []):
            total = purchase.get('product', {}).get('total', 0)
            total_debt += float(total)

    total_collected = sum(
        p.get('amount', 0) for p in payments_collection.find({'agent_id': agent_id})
    )

    # 3. Most Purchased Products
    product_sales = defaultdict(int)
    customers = customers_collection.find({'agent_id': agent_id})
    for customer in customers:
        for purchase in customer.get('purchases', []):
            product_name = purchase.get('product', {}).get('name')
            if product_name:
                product_sales[product_name] += 1

    most_purchased_products = [
        {"product": name, "count": count}
        for name, count in sorted(product_sales.items(), key=lambda x: x[1], reverse=True)
    ]

    # 4. Most Occurring Locations
    location_count = defaultdict(int)
    customers = customers_collection.find({'agent_id': agent_id}, {"location": 1})
    for customer in customers:
        location = customer.get("location", "Unknown")
        location_count[location] += 1

    most_occurring_locations = [
        {"location": loc, "count": count}
        for loc, count in sorted(location_count.items(), key=lambda x: x[1], reverse=True)
    ]

    return jsonify({
        "payment_data": payment_data,
        "total_debt": total_debt,
        "total_collected": total_collected,
        "most_purchased_products": most_purchased_products,
        "most_occurring_locations": most_occurring_locations
    })

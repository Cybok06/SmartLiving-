from flask import Blueprint, render_template, session, redirect, url_for
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from bson.objectid import ObjectId
from db import db

sales_summary_bp = Blueprint('sales_summary', __name__)

# MongoDB connection

users_col = db.users
customers_col = db.customers
payments_col = db.payments  # New collection reference

@sales_summary_bp.route('/sales-summary')
def sales_summary():
    if 'manager_id' not in session:
        return redirect(url_for('login.login'))

    try:
        manager_id = ObjectId(session['manager_id'])
    except Exception:
        return redirect(url_for('login.logout'))

    agents = list(users_col.find({'manager_id': manager_id, 'role': 'agent'}))
    sales_data = []
    ranking_data = []

    total_all_agents_sales = 0
    total_all_agents_collected = 0

    for agent in agents:
        agent_id_str = str(agent['_id'])

        # === 1. Calculate Total Sales from Customers ===
        customers = list(customers_col.find({'agent_id': agent_id_str}))
        total_sales = 0
        for customer in customers:
            for purchase in customer.get('purchases', []):
                if purchase.get('agent_id') == agent_id_str:
                    product = purchase.get('product', {})
                    total_sales += int(product.get('total', 0))

        # === 2. Calculate Total Amount Collected by Agent ===
        payments = payments_col.find({'agent_id': agent_id_str})
        total_collected = 0
        for payment in payments:
            total_collected += float(payment.get('amount', 0.0))

        # === 3. Calculate Balance ===
        balance = total_sales - total_collected

        # Add to global totals
        total_all_agents_sales += total_sales
        total_all_agents_collected += total_collected

        # === 4. Add to Sales Data ===
        sales_data.append({
            'agent_name': agent['name'],
            'agent_id': agent_id_str,
            'total_sales': total_sales,
            'total_collected': total_collected,
            'balance': balance
        })

        # === 5. Add to Ranking Data ===
        ranking_data.append({
            'name': agent['name'],
            'phone': agent.get('phone', 'N/A'),
            'image_url': agent.get('image_url', 'https://via.placeholder.com/80'),
            'total_sales': total_sales,
            'customers': len(customers)
        })

    # Sort agents by sales and customer count for ranking
    ranking_data.sort(key=lambda x: (x['total_sales'], x['customers']), reverse=True)

    return render_template('sales_summary.html',
                           sales_data=sales_data,
                           total_all_agents_sales=total_all_agents_sales,
                           total_all_agents_collected=total_all_agents_collected,
                           ranking_data=ranking_data)

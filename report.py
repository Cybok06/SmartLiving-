from flask import Blueprint, render_template, Response, session
from pymongo import MongoClient
from datetime import datetime, timedelta
from collections import defaultdict
import csv
import io
import logging
from db import db
# Setup logging
logging.basicConfig(level=logging.INFO)

report_bp = Blueprint('report', __name__)

# MongoDB setup

customers_collection = db["customers"]
payments_collection = db["payments"]
products_collection = db["products"]

@report_bp.route('/reports')
def reports():
    agent_id = session.get("agent_id")
    if not agent_id:
        return "Unauthorized", 401

    now = datetime.now()
    today_str = now.strftime('%Y-%m-%d')
    last_month = now - timedelta(days=30)
    two_weeks_ago = now - timedelta(days=14)

    # 1. Customer Overview
    customer_query = {"agent_id": agent_id}
    total_customers = customers_collection.count_documents(customer_query)
    new_customers_count = customers_collection.count_documents({
        **customer_query,
        'created_at': {'$gte': last_month}
    })

    customers = list(customers_collection.find(customer_query, {'name': 1, 'phone_number': 1, 'purchases': 1}))

    # 2. Payment Summary
    payments = list(payments_collection.find({"agent_id": agent_id}))
    total_payments = sum(payment.get('amount', 0) for payment in payments)

    payments_this_month = sum(
        payment.get('amount', 0)
        for payment in payments
        if 'date' in payment and is_date_within(payment['date'], last_month)
    )

    # 3. Product Sales Report
    product_sales = defaultdict(int)
    total_sales = 0
    today_sales = 0
    for customer in customers:
        for p in customer.get('purchases', []):
            product = p.get('product', {})
            product_name = product.get('name')
            product_price = product.get('total', 0)
            purchase_date = p.get('date')  # Expecting 'date' in each purchase

            if product_name and isinstance(product_price, (int, float)):
                product_sales[product_name] += 1
                total_sales += product_price

                if purchase_date == today_str:
                    today_sales += product_price
            else:
                logging.warning(f"Missing or invalid product for customer {customer['_id']}")

    product_report = [{'name': name, 'count': count} for name, count in product_sales.items()]

    # 4. Debt Analysis
    total_debt = 0
    for customer in customers:
        for purchase in customer.get('purchases', []):
            total_debt += purchase.get('product', {}).get('total', 0)

    average_debt = round(total_debt / total_customers, 2) if total_customers else 0

    # 5. Activity Metrics
    active_count = 0
    inactive_count = 0
    for customer in customers:
        latest_payment = payments_collection.find_one(
            {'customer_id': customer['_id'], 'agent_id': agent_id},
            sort=[('date', -1)]
        )
        if latest_payment and 'date' in latest_payment:
            try:
                last_payment_date = datetime.strptime(latest_payment['date'], '%Y-%m-%d')
                if last_payment_date >= two_weeks_ago:
                    active_count += 1
                else:
                    inactive_count += 1
            except Exception as e:
                logging.warning(f"Error parsing date for customer {customer['_id']}: {e}")
                inactive_count += 1
        else:
            inactive_count += 1

    # 6. Today's Metrics
    today_payments = sum(
        p.get('amount', 0)
        for p in payments
        if p.get('date') == today_str
    )

    # Build the report dictionary
    report = {
        'customer_count': total_customers,
        'new_customers_count': new_customers_count,
        'total_payments': round(total_payments, 2),
        'payments_this_month': round(payments_this_month, 2),
        'product_report': product_report,
        'total_sales': round(total_sales, 2),
        'total_debt': round(total_debt, 2),
        'average_debt': average_debt,
        'active_count': active_count,
        'inactive_count': inactive_count,
        'today_sales': round(today_sales, 2),
        'today_payments': round(today_payments, 2)
    }

    return render_template('report.html', report=report)

def is_date_within(date_str, since_date):
    try:
        parsed = datetime.strptime(date_str, '%Y-%m-%d')
        return parsed >= since_date
    except Exception as e:
        logging.error(f"Error parsing date: {date_str}. Error: {str(e)}")
        return False

@report_bp.route('/download_report')
def download_report():
    agent_id = session.get("agent_id")
    if not agent_id:
        return "Unauthorized", 401

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(['Report Title', 'Value'])

    customer_query = {"agent_id": agent_id}
    writer.writerow(['Total Customers', customers_collection.count_documents(customer_query)])
    writer.writerow(['Total Payments', sum(p.get('amount', 0) for p in payments_collection.find({"agent_id": agent_id}))])
    writer.writerow(['Total Debt', sum(
        purchase.get('product', {}).get('total', 0)
        for c in customers_collection.find(customer_query)
        for purchase in c.get('purchases', []))
    ])

    output.seek(0)
    return Response(
        output,
        mimetype='text/csv',
        headers={"Content-Disposition": "attachment;filename=crm_report.csv"}
    )

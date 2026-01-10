from flask import Blueprint, render_template, request, session, redirect, url_for, flash
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from bson.objectid import ObjectId
from datetime import datetime
from db import db

payments_bp = Blueprint('payments', __name__)

# MongoDB connection

users_col = db.users
payments_col = db.payments
customers_col = db.customers

@payments_bp.route('/payments', methods=['GET', 'POST'])
def payments_list():
    if 'manager_id' not in session:
        return redirect(url_for('login.login'))

    try:
        manager_id = ObjectId(session['manager_id'])
    except Exception:
        flash("Invalid session.", "error")
        return redirect(url_for('login.logout'))

    # Fetch all agents for this manager for filtering dropdown
    agents = list(users_col.find({'manager_id': manager_id, 'role': 'agent'}))
    for agent in agents:
        agent['_id_str'] = str(agent['_id'])

    # Fetch all customers of the manager's agents for filtering dropdown
    agent_ids = [agent['_id_str'] for agent in agents]
    customers = list(customers_col.find({'agent_id': {'$in': agent_ids}}))
    for customer in customers:
        customer['_id_str'] = str(customer['_id'])

    # Get filters from query parameters
    filter_agent_id = request.args.get('agent_id', '')
    filter_customer_id = request.args.get('customer_id', '')
    start_date_str = request.args.get('start_date', '')
    end_date_str = request.args.get('end_date', '')

    # Build the query
    query = {}

    # Filter payments by agent
    if filter_agent_id:
        query['agent_id'] = filter_agent_id
    else:
        query['agent_id'] = {'$in': agent_ids}  # Only agents under this manager

    # Filter payments by customer
    if filter_customer_id:
        try:
            query['customer_id'] = ObjectId(filter_customer_id)
        except Exception:
            flash("Invalid customer filter ID.", "error")

    # Date range filtering
    if start_date_str:
        try:
            start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
            query['date'] = query.get('date', {})
            query['date']['$gte'] = start_date
        except ValueError:
            flash('Invalid start date format. Use YYYY-MM-DD.', 'error')

    if end_date_str:
        try:
            end_date = datetime.strptime(end_date_str, '%Y-%m-%d')
            query['date'] = query.get('date', {})
            query['date']['$lte'] = end_date
        except ValueError:
            flash('Invalid end date format. Use YYYY-MM-DD.', 'error')

    # Fetch payments based on query
    payments = list(payments_col.find(query).sort('date', -1))

    # Enrich payments with agent and customer names for display
    agent_map = {agent['_id_str']: agent['name'] for agent in agents}
    customer_map = {customer['_id_str']: customer['name'] for customer in customers}

    for payment in payments:
        # Convert payment date string to datetime object if needed
        if 'date' in payment and isinstance(payment['date'], str):
            try:
                payment['date'] = datetime.strptime(payment['date'], '%Y-%m-%d')
            except Exception:
                payment['date'] = None
        else:
            payment['date'] = None

        payment['agent_name'] = agent_map.get(payment['agent_id'], 'Unknown Agent')
        cust_id_str = str(payment['customer_id'])
        payment['customer_name'] = customer_map.get(cust_id_str, 'Unknown Customer')

    return render_template('payments.html',
                           payments=payments,
                           agents=agents,
                           customers=customers,
                           filter_agent_id=filter_agent_id,
                           filter_customer_id=filter_customer_id,
                           start_date=start_date_str,
                           end_date=end_date_str)

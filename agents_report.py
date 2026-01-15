from flask import Blueprint, render_template, session, redirect, url_for, flash
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from bson.objectid import ObjectId
from datetime import datetime, timedelta, timezone
from db import db

agents_report_bp = Blueprint('agents_report', __name__)

users_col = db.users
customers_col = db.customers
payments_col = db.payments

def make_aware_if_naive(dt):
    if dt and dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt

@agents_report_bp.route('/agents-report')
def agents_report():
    if 'manager_id' not in session:
        return redirect(url_for('login.login'))

    try:
        manager_id = ObjectId(session['manager_id'])
    except Exception:
        flash("Invalid session, please log in again.", "error")
        return redirect(url_for('login.logout'))

    agents = list(users_col.find({'manager_id': manager_id, 'role': 'agent'}))

    total_agents = len(agents)
    active_agents = sum(1 for a in agents if a.get('status', '').lower() == 'active')
    not_active_agents = total_agents - active_agents

    now = datetime.utcnow().replace(tzinfo=timezone.utc)
    first_day_of_month = datetime(now.year, now.month, 1, tzinfo=timezone.utc)
    last_day_of_month = (first_day_of_month + timedelta(days=32)).replace(day=1) - timedelta(seconds=1)

    agent_ids_str = [str(a['_id']) for a in agents]
    customers = list(customers_col.find({'agent_id': {'$in': agent_ids_str}}))

    total_customers = len(customers)
    active_customers = 0
    not_active_customers = 0

    active_customer_ids = set()
    recent_cutoff = now - timedelta(days=30)

    recent_payments_cursor = payments_col.find({
        'agent_id': {'$in': agent_ids_str},
        'date': {'$gte': recent_cutoff.strftime('%Y-%m-%d')}
    })

    recent_payments_agent_map = {}
    for payment in recent_payments_cursor:
        agent_id = payment.get('agent_id')
        recent_payments_agent_map[agent_id] = recent_payments_agent_map.get(agent_id, 0) + 1
        active_customer_ids.add(str(payment.get('customer_id')))

    for customer in customers:
        last_purchase_date = None
        for purchase in customer.get('purchases', []):
            try:
                pd = datetime.fromisoformat(purchase.get('purchase_date').replace('Z', '+00:00'))
                pd = make_aware_if_naive(pd)
                if (last_purchase_date is None) or (pd > last_purchase_date):
                    last_purchase_date = pd
            except Exception:
                continue

        last_purchase_date = make_aware_if_naive(last_purchase_date)
        customer_id_str = str(customer['_id'])
        if (last_purchase_date and last_purchase_date >= recent_cutoff) or (customer_id_str in active_customer_ids):
            active_customers += 1
        else:
            not_active_customers += 1

    new_customers_this_month = 0
    for c in customers:
        dt_registered = None
        date_registered = c.get('date_registered')

        if isinstance(date_registered, dict):
            try:
                ts = int(date_registered.get('$date', {}).get('$numberLong', 0)) / 1000
                dt_registered = datetime.fromtimestamp(ts, tz=timezone.utc)
            except Exception:
                dt_registered = None
        elif isinstance(date_registered, str):
            try:
                dt_registered = datetime.fromisoformat(date_registered)
                dt_registered = make_aware_if_naive(dt_registered)
            except Exception:
                dt_registered = None
        elif isinstance(date_registered, datetime):
            dt_registered = make_aware_if_naive(date_registered)

        if dt_registered is None:
            purchase_dates = []
            for p in c.get('purchases', []):
                try:
                    pd = datetime.fromisoformat(p.get('purchase_date').replace('Z', '+00:00'))
                    purchase_dates.append(make_aware_if_naive(pd))
                except Exception:
                    continue
            if purchase_dates:
                dt_registered = min(purchase_dates)

        if dt_registered and first_day_of_month <= dt_registered <= last_day_of_month:
            new_customers_this_month += 1

    total_payments_collected = 0
    total_sales = 0

    for agent in agents:
        agent_id_str = str(agent['_id'])
        payments = payments_col.find({'agent_id': agent_id_str})
        for p in payments:
            try:
                total_payments_collected += float(p.get('amount', 0))
            except Exception:
                pass

        agent_customers = [c for c in customers if c.get('agent_id') == agent_id_str]
        for customer in agent_customers:
            for purchase in customer.get('purchases', []):
                product = purchase.get('product', {})
                try:
                    total_sales += int(product.get('total', 0))
                except Exception:
                    pass

    payment_collection_efficiency = (total_payments_collected / total_sales * 100) if total_sales > 0 else 0

    agent_activity = []
    for agent in agents:
        agent_id_str = str(agent['_id'])
        agent_customers_count = sum(1 for c in customers if c.get('agent_id') == agent_id_str)
        agent_active_customers_count = 0

        for c in customers:
            if c.get('agent_id') != agent_id_str:
                continue
            last_purchase_date = None
            for purchase in c.get('purchases', []):
                try:
                    pd = datetime.fromisoformat(purchase.get('purchase_date').replace('Z', '+00:00'))
                    pd = make_aware_if_naive(pd)
                    if (last_purchase_date is None) or (pd > last_purchase_date):
                        last_purchase_date = pd
                except Exception:
                    continue
            cust_id_str = str(c['_id'])
            last_purchase_date = make_aware_if_naive(last_purchase_date)
            if (last_purchase_date and last_purchase_date >= recent_cutoff) or (cust_id_str in active_customer_ids):
                agent_active_customers_count += 1

        recent_pay_count = recent_payments_agent_map.get(agent_id_str, 0)

        agent_activity.append({
            'agent_name': agent['name'],
            'agent_id': agent_id_str,
            'total_customers': agent_customers_count,
            'active_customers': agent_active_customers_count,
            'recent_payments': recent_pay_count,
            'status': agent.get('status', 'Unknown')
        })

    top_agents = []
    for agent in agents:
        agent_id_str = str(agent['_id'])
        agent_customers = [c for c in customers if c.get('agent_id') == agent_id_str]

        total_agent_sales = 0
        for customer in agent_customers:
            for purchase in customer.get('purchases', []):
                product = purchase.get('product', {})
                try:
                    total_agent_sales += int(product.get('total', 0))
                except Exception:
                    pass

        top_agents.append({
            'agent_name': agent['name'],
            'agent_id': agent_id_str,
            'total_sales': total_agent_sales,
            'customer_count': len(agent_customers),
            'status': agent.get('status', 'Unknown')
        })

    top_agents.sort(key=lambda x: x['total_sales'], reverse=True)

    return render_template('agent_report.html',
                           total_agents=total_agents,
                           active_agents=active_agents,
                           not_active_agents=not_active_agents,
                           total_customers=total_customers,
                           active_customers=active_customers,
                           not_active_customers=not_active_customers,
                           new_customers_this_month=new_customers_this_month,
                           payment_collection_efficiency=payment_collection_efficiency,
                           agent_activity=agent_activity,
                           top_agents=top_agents)

from flask import Blueprint, render_template, request, redirect, url_for, session, flash
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from bson.objectid import ObjectId
from datetime import datetime
from db import db

account_bp = Blueprint('account', __name__)

users_col = db.users
payments_col = db.payments
customers_col = db.customers
accountability_col = db.accountability

@account_bp.route('/account', methods=['GET', 'POST'])
def account_view():
    if 'manager_id' not in session:
        return redirect(url_for('login.login'))

    manager_id = ObjectId(session['manager_id'])
    today = datetime.now().date()
    today_str = today.strftime('%Y-%m-%d')

    existing_record = accountability_col.find_one({
        'manager_id': manager_id,
        'date': {
            '$gte': datetime.combine(today, datetime.min.time()),
            '$lt': datetime.combine(today, datetime.max.time())
        }
    })

    agents = list(users_col.find({'manager_id': manager_id, 'role': 'agent'}))
    data = []

    for agent in agents:
        agent_id = str(agent['_id'])
        agent_payments = list(payments_col.find({
            'agent_id': agent_id,
            'date': today_str
        }))

        total_payment = sum(float(p.get('amount', 0)) for p in agent_payments)

        susu_payments = []
        for p in agent_payments:
            if p.get('payment_type') == 'SUSU':
                customer = customers_col.find_one({'_id': p['customer_id']})
                customer_name = customer['name'] if customer else "Unknown"
                susu_payments.append({
                    'amount': float(p['amount']),
                    'customer': customer_name,
                    'payment_id': str(p['_id'])
                })

        data.append({
            'agent_id': agent_id,
            'name': agent['name'],
            'image_url': agent.get('image_url', 'https://via.placeholder.com/80'),
            'total_payment': total_payment,
            'susu_payments': susu_payments
        })

    if request.method == 'POST':
        if existing_record:
            flash("Payments submitted already, Contact the admin for further payments.", "danger")
            return redirect(url_for('account.account_view'))

        accountability_records = []

        for agent in agents:
            agent_id = str(agent['_id'])

            shortage = float(request.form.get(f'shortage_{agent_id}', 0) or 0)
            surplus = float(request.form.get(f'surplus_{agent_id}', 0) or 0)

            agent_payments = list(payments_col.find({
                'agent_id': agent_id,
                'date': today_str
            }))
            total_payment = sum(float(p.get('amount', 0)) for p in agent_payments)

            final_total = total_payment - shortage + surplus  # removed withdrawn_total subtraction

            expense_amounts = request.form.getlist(f'expense_amount_{agent_id}[]')
            expense_descriptions = request.form.getlist(f'expense_description_{agent_id}[]')

            expenses = []
            for amt, desc in zip(expense_amounts, expense_descriptions):
                try:
                    amount = float(amt)
                    description = desc.strip()
                    if amount > 0 and description:
                        expenses.append({
                            'amount': amount,
                            'description': description
                        })
                except ValueError:
                    continue

            record = {
                'manager_id': manager_id,
                'agent_id': agent_id,
                'date': datetime.utcnow(),
                'shortage': shortage,
                'surplus': surplus,
                'total_payment': total_payment,
                'final_total': final_total,
                'expenses': expenses,
                'recorded_at': datetime.utcnow()
            }
            accountability_records.append(record)

        if accountability_records:
            accountability_col.insert_many(accountability_records)
            flash("Accountability records submitted successfully.", "success")
        return redirect(url_for('account.account_view'))

    return render_template('account.html', data=data, submission_done=bool(existing_record))

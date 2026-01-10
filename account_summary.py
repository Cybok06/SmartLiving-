from flask import Blueprint, render_template, session, redirect, url_for, request
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from bson.objectid import ObjectId
from datetime import datetime, timedelta
from collections import defaultdict
from db import db
account_summary_bp = Blueprint('account_summary', __name__)


accountability_col = db.accountability
users_col = db.users  # Needed for agent names

@account_summary_bp.route('/account-summary', methods=['GET'])
def account_summary():
    if 'manager_id' not in session:
        return redirect(url_for('login.login'))

    try:
        manager_id = ObjectId(session['manager_id'])
    except Exception:
        return redirect(url_for('login.logout'))

    # Get optional filter date
    filter_date_str = request.args.get('filter_date')
    filter_date = None
    if filter_date_str:
        try:
            filter_date = datetime.strptime(filter_date_str, '%Y-%m-%d').date()
        except ValueError:
            pass

    today = datetime.utcnow().date()
    selected_date = filter_date or today
    show_today_summary = (filter_date is None) or (filter_date == today)

    # === Today's Summary ===
    submission_done = False
    total_payment = final_payment = total_expense = settled_payment = total_shortage = total_surplus = 0.0
    expenses = []

    if show_today_summary:
        start = datetime.combine(today, datetime.min.time())
        end = start + timedelta(days=1)

        today_records = list(accountability_col.find({
            'manager_id': manager_id,
            'date': {'$gte': start, '$lt': end}
        }))
        submission_done = len(today_records) > 0

        for record in today_records:
            total_payment += record.get('total_payment', 0.0)
            final_payment += record.get('final_total', 0.0)
            total_shortage += record.get('shortage', 0.0)
            total_surplus += record.get('surplus', 0.0)

            for exp in record.get('expenses', []):
                expenses.append(exp)
                total_expense += exp.get('amount', 0.0)

        settled_payment = final_payment - total_expense

    # === Past Summaries ===
    query = {'manager_id': manager_id}
    if filter_date:
        start = datetime.combine(filter_date, datetime.min.time())
        end = start + timedelta(days=1)
        query['date'] = {'$gte': start, '$lt': end}
    else:
        start = datetime.min
        end = datetime.combine(today, datetime.min.time())
        query['date'] = {'$gte': start, '$lt': end}

    past_records_cursor = accountability_col.find(query).sort('date', -1)
    grouped = defaultdict(list)

    agents_cursor = users_col.find({'manager_id': manager_id, 'role': 'agent'})
    agent_map = {str(agent['_id']): agent['name'] for agent in agents_cursor}

    grand_total_settled = 0.0

    for rec in past_records_cursor:
        dt = rec.get('date')
        if not dt:
            continue

        dt_str = dt.strftime('%Y-%m-%d')
        agent_id_str = str(rec.get('agent_id', 'Unknown'))
        agent_name = agent_map.get(agent_id_str, 'Unknown Agent')

        expenses_list = rec.get('expenses', [])
        total_exp = sum(exp.get('amount', 0.0) for exp in expenses_list)
        settled = rec.get('final_total', 0.0) - total_exp
        grand_total_settled += settled

        rec['agent_name'] = agent_name
        rec['total_expense'] = total_exp
        rec['settled_payment'] = settled

        grouped[dt_str].append(rec)

    past_records = [{'date': dt, 'records': recs} for dt, recs in grouped.items()]
    past_records.sort(key=lambda x: x['date'], reverse=True)

    return render_template('account_summary.html',
                           submission_done=submission_done,
                           total_payment=total_payment,
                           final_payment=final_payment,
                           expenses=expenses,
                           total_expense=total_expense,
                           settled_payment=settled_payment,
                           total_shortage=total_shortage,
                           total_surplus=total_surplus,
                           selected_date=selected_date,
                           past_records=past_records,
                           grand_total_settled=grand_total_settled)


@account_summary_bp.route('/account-summary/aggregate', methods=['GET'])
def account_summary_aggregate():
    if 'manager_id' not in session:
        return redirect(url_for('login.login'))

    try:
        manager_id = ObjectId(session['manager_id'])
    except Exception:
        return redirect(url_for('login.logout'))

    records = list(accountability_col.find({'manager_id': manager_id}))

    total_payment = final_payment = total_expense = total_shortage = total_surplus = 0.0
    expenses = []

    for record in records:
        total_payment += record.get('total_payment', 0.0)
        final_payment += record.get('final_total', 0.0)
        total_shortage += record.get('shortage', 0.0)
        total_surplus += record.get('surplus', 0.0)

        for exp in record.get('expenses', []):
            expenses.append(exp)
            total_expense += exp.get('amount', 0.0)

    settled_payment = final_payment - total_expense

    return render_template('aggregate_summary.html',
                           total_payment=total_payment,
                           final_payment=final_payment,
                           total_shortage=total_shortage,
                           total_surplus=total_surplus,
                           expenses=expenses,
                           total_expense=total_expense,
                           settled_payment=settled_payment)

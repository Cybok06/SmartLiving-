from flask import Blueprint, render_template, redirect, url_for, session, request, flash
from pymongo import MongoClient
from bson.objectid import ObjectId
from datetime import datetime
from collections import defaultdict
from db import db

admin_account_bp = Blueprint('admin_account', __name__)


accountability_col = db.accountability
confirmed_col = db.account_confirmed
users_col = db.users

@admin_account_bp.route('/admin/accountability')
def admin_accountability():
    if 'admin_id' not in session:
        return redirect(url_for('login.login'))

    today = datetime.utcnow().date()
    start = datetime.combine(today, datetime.min.time())
    end = datetime.combine(today, datetime.max.time())

    today_records = list(accountability_col.find({
        'date': {'$gte': start, '$lt': end}
    }))

    # Fetch all managers once
    managers = {str(u['_id']): u['name'] for u in users_col.find({'role': 'manager'})}

    grouped_managers = defaultdict(lambda: {
        'total_payment': 0,
        'final_total': 0,
        'shortage': 0,
        'surplus': 0,
        'total_expense': 0,
        'settled_payment': 0,
        'records': [],
        'manager_name': ''
    })

    for record in today_records:
        mid = str(record['manager_id'])
        m = grouped_managers[mid]

        m['manager_name'] = managers.get(mid, 'Unknown Manager')
        m['total_payment'] += record.get('total_payment', 0)
        m['final_total'] += record.get('final_total', 0)
        m['shortage'] += record.get('shortage', 0)
        m['surplus'] += record.get('surplus', 0)

        total_exp = sum(exp.get('amount', 0) for exp in record.get('expenses', []))
        m['total_expense'] += total_exp
        m['settled_payment'] += record.get('final_total', 0) - total_exp
        m['records'].append(record)

    return render_template('admin_accountability.html', grouped=grouped_managers.values())


@admin_account_bp.route('/admin/accountability/confirm-manager/<manager_id>', methods=['POST'])
def confirm_manager_accountability(manager_id):
    if 'admin_id' not in session:
        return redirect(url_for('login.login'))

    admin_id = session['admin_id']
    today = datetime.utcnow().date()
    start = datetime.combine(today, datetime.min.time())
    end = datetime.combine(today, datetime.max.time())

    records = list(accountability_col.find({
        'manager_id': ObjectId(manager_id),
        'date': {'$gte': start, '$lt': end}
    }))

    for record in records:
        record['confirmed_by'] = ObjectId(admin_id)  # ✅ Store admin ID instead of name
        record['confirmed_at'] = datetime.utcnow()
        confirmed_col.insert_one(record)
        accountability_col.delete_one({'_id': record['_id']})

    flash("✅ All records under manager confirmed and archived.", "success")
    return redirect(url_for('admin_account.admin_accountability'))

from flask import Blueprint, render_template, request, redirect, url_for, flash, session
from bson.objectid import ObjectId
from datetime import datetime, timedelta
from db import db

target_bp = Blueprint('target', __name__, url_prefix='/executive/target')

users_col = db.users
targets_col = db.targets

# Helper to get date range
def calculate_dates(duration):
    today = datetime.utcnow().date()
    if duration == 'daily':
        return today, today
    elif duration == 'weekly':
        return today, today + timedelta(days=6)
    elif duration == 'monthly':
        start = today.replace(day=1)
        end = (start + timedelta(days=32)).replace(day=1) - timedelta(days=1)
        return start, end
    elif duration == 'yearly':
        start = today.replace(month=1, day=1)
        end = today.replace(month=12, day=31)
        return start, end
    return today, today

@target_bp.route('/set', methods=['GET', 'POST'])
def set_target():
    if 'executive_id' not in session:
        flash("Unauthorized access.", "error")
        return redirect(url_for('login.login'))

    if request.method == 'POST':
        manager_ids = request.form.getlist('manager_ids')  # Supports multiple managers
        title = request.form.get('title', '').strip()
        duration = request.form.get('duration')
        product_target = int(request.form.get('product_target') or 0)
        cash_target = int(request.form.get('cash_target') or 0)
        customer_target = int(request.form.get('customer_target') or 0)

        if not manager_ids or not title or (product_target == 0 and cash_target == 0 and customer_target == 0):
            flash("Please fill all required fields including title and targets.", "error")
            return redirect(url_for('target.set_target'))

        start_date, end_date = calculate_dates(duration)

        for manager_id in manager_ids:
            manager = users_col.find_one({'_id': ObjectId(manager_id), 'role': 'manager'})
            if not manager:
                continue

            agents = list(users_col.find({'manager_id': ObjectId(manager_id), 'role': 'agent'}))
            num_agents = len(agents)

            if num_agents == 0:
                flash(f"Manager {manager.get('name', manager_id)} has no agents. Skipped.", "warning")
                continue

            agent_targets = []
            for agent in agents:
                agent_targets.append({
                    "agent_id": str(agent['_id']),
                    "agent_name": agent.get('name', ''),
                    "product_target": product_target // num_agents,
                    "cash_target": cash_target // num_agents,
                    "customer_target": customer_target // num_agents,
                    "status": "in_progress"
                })

            targets_col.insert_one({
                "executive_id": session['executive_id'],
                "manager_id": manager_id,
                "manager_name": manager.get('name', ''),
                "branch": manager.get('branch', ''),
                "title": title,
                "duration_type": duration,
                "start_date": str(start_date),
                "end_date": str(end_date),
                "product_target": product_target,
                "cash_target": cash_target,
                "customer_target": customer_target,
                "agents_distribution": agent_targets,
                "created_at": datetime.utcnow(),
                "status": "in_progress"
            })

        flash("Targets successfully set and distributed to selected managers.", "success")
        return redirect(url_for('target.set_target'))

    managers = list(users_col.find({'role': 'manager'}))
    return render_template('set_target.html', managers=managers)

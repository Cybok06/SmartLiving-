from flask import Blueprint, render_template, request, redirect, url_for, session, flash, jsonify
from bson.objectid import ObjectId
from datetime import datetime
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from db import db
# MongoDB connection

users_col = db.users
customers_col = db.customers
tasks_col = db.tasks

task_messages_bp = Blueprint('task_messages', __name__)

@task_messages_bp.route('/tasks', methods=['GET', 'POST'])
def manager_tasks():
    if 'manager_id' not in session:
        return redirect(url_for('login.login'))

    manager_id = ObjectId(session['manager_id'])
    agents = list(users_col.find({'manager_id': manager_id, 'role': 'agent'}))

    # Build agent-customer mapping
    agents_customers = {}
    for agent in agents:
        agent_id_str = str(agent['_id'])
        customers = list(customers_col.find({'agent_id': agent_id_str}))
        agents_customers[agent_id_str] = [{'id': str(c['_id']), 'name': c['name']} for c in customers]

    if request.method == 'POST':
        agent_id = request.form.get('agent_id')
        customer_id = request.form.get('customer_id')
        message = request.form.get('message')
        image_url = request.form.get('image_url')

        if not message or not agent_id:
            flash('Agent and message are required.', 'danger')
            return redirect(url_for('task_messages.manager_tasks'))

        task = {
            'manager_id': manager_id,
            'agent_id': 'all' if agent_id == 'all' else ObjectId(agent_id),
            'customer_id': None if agent_id == 'all' else (ObjectId(customer_id) if customer_id else None),
            'message': message,
            'image_url': image_url if image_url else None,
            'timestamp': datetime.utcnow(),
            'status': 'pending'
        }

        tasks_col.insert_one(task)
        flash('Task sent successfully.', 'success')
        return redirect(url_for('task_messages.manager_tasks'))

    # Filter tasks by status, and only include tasks with 'agent_id' field (i.e., sent by manager)
    filter_status = request.args.get('status', 'pending')

    tasks = list(tasks_col.find({
        'manager_id': manager_id,
        'status': filter_status,
        'agent_id': {'$exists': True}
    }).sort('timestamp', -1))

    # Attach names safely
    for task in tasks:
        if 'agent_id' not in task:
            task['agent_name'] = '—'
        elif task['agent_id'] == 'all':
            task['agent_name'] = 'All Agents'
        else:
            agent = users_col.find_one({'_id': task['agent_id']})
            task['agent_name'] = agent['name'] if agent else 'Unknown'

        if task.get('customer_id'):
            customer = customers_col.find_one({'_id': task['customer_id']})
            task['customer_name'] = customer['name'] if customer else 'Unknown'
        else:
            task['customer_name'] = '—'

    return render_template(
        'task_messages.html',
        agents=agents,
        agents_customers=agents_customers,
        tasks=tasks,
        current_status=filter_status
    )

@task_messages_bp.route('/tasks/customers/<agent_id>')
def get_customers_for_agent(agent_id):
    if agent_id == 'all':
        return jsonify([])
    customers = list(customers_col.find({'agent_id': agent_id}))
    return jsonify([{'id': str(c['_id']), 'name': c['name']} for c in customers])

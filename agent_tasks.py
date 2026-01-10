from flask import Blueprint, render_template, request, redirect, url_for, flash
from flask_login import login_required, current_user
from bson.objectid import ObjectId
from datetime import datetime
from db import db

# Collections
tasks_collection = db.tasks
customers_collection = db.customers
users_collection = db.users  # Needed to find admin/manager who sent task

agent_tasks_bp = Blueprint('agent_tasks', __name__)

@agent_tasks_bp.route('/agent/tasks')
@login_required
def view_tasks():
    if current_user.role != 'agent':
        return "Unauthorized", 403

    agent_id = ObjectId(current_user.id)

    # Fetch tasks sent to this agent specifically or to all agents
    tasks = list(tasks_collection.find({
        'target_type': 'agent',
        '$or': [
            {'user_id': agent_id},
            {'user_id': 'all'}
        ]
    }).sort('timestamp', -1))

    for task in tasks:
        # Attach customer name
        if task.get('customer_id'):
            customer = customers_collection.find_one({'_id': task['customer_id']})
            task['customer_name'] = customer['name'] if customer else 'Unknown'
        else:
            task['customer_name'] = '—'

        # Determine sender (admin or manager)
        if task.get('admin_id'):
            admin = users_collection.find_one({'_id': task['admin_id']})
            task['sent_by'] = f"Admin {admin['username']}" if admin else 'Admin'
        elif task.get('manager_id'):
            manager = users_collection.find_one({'_id': task['manager_id']})
            task['sent_by'] = f"Manager {manager['username']}" if manager else 'Manager'
        else:
            task['sent_by'] = 'Unknown'

    return render_template('task.html', tasks=tasks)

@agent_tasks_bp.route('/agent/tasks/<task_id>/complete', methods=['POST'])
@login_required
def mark_task_complete(task_id):
    if current_user.role != 'agent':
        return "Unauthorized", 403

    task = tasks_collection.find_one({'_id': ObjectId(task_id)})

    if not task:
        flash("Task not found.", "danger")
        return redirect(url_for('agent_tasks.view_tasks'))

    # Ensure the agent is authorized to complete this task
    if task['target_type'] != 'agent':
        flash("This task was not assigned to an agent.", "danger")
        return redirect(url_for('agent_tasks.view_tasks'))

    if task['user_id'] != 'all' and str(task['user_id']) != current_user.id:
        flash("You are not authorized to complete this task.", "danger")
        return redirect(url_for('agent_tasks.view_tasks'))

    # Mark task as completed
    tasks_collection.update_one(
        {'_id': ObjectId(task_id)},
        {'$set': {'status': 'completed', 'completed_at': datetime.utcnow()}}
    )

    flash("Task marked as completed.", "success")
    return redirect(url_for('agent_tasks.view_tasks'))

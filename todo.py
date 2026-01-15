from flask import Blueprint, render_template, request, redirect, url_for, flash, session
from flask_login import current_user
from datetime import datetime
from bson import ObjectId
from db import db

todo_bp = Blueprint('todo', __name__)
todo_collection = db['todos']

@todo_bp.route('/todo', methods=['GET', 'POST'])
def todo_list():
    # Determine logged in user from current_user (agent) or session (manager/admin)
    user_id = None
    role = None

    if hasattr(current_user, 'is_authenticated') and current_user.is_authenticated:
        user_id = str(current_user.id)
        role = current_user.role
    elif 'manager_id' in session:
        user_id = session['manager_id']
        role = 'manager'
    elif 'admin_id' in session:
        user_id = session['admin_id']
        role = 'admin'
    else:
        return redirect(url_for('login.login'))

    previous_url = request.referrer or url_for(f'login.{role}_dashboard')

    # Handle task submission
    if request.method == 'POST':
        task_text = request.form.get('task')
        due_date = request.form.get('due_date')
        priority = request.form.get('priority')

        if task_text and due_date and priority:
            todo_collection.insert_one({
                'user_id': user_id,
                'role': role,
                'task': task_text,
                'due_date': due_date,
                'priority': priority,
                'completed': False,
                'timestamp': datetime.utcnow()
            })
            flash("Task added successfully", "success")
        else:
            flash("All fields are required.", "danger")
        return redirect(url_for('todo.todo_list'))

    tasks = list(todo_collection.find({'user_id': user_id}).sort('due_date', 1))
    return render_template('todo.html', tasks=tasks, role=role, previous_url=previous_url)

@todo_bp.route('/todo/complete/<task_id>', methods=['POST'])
def complete_task(task_id):
    todo_collection.update_one(
        {'_id': ObjectId(task_id)},
        {'$set': {'completed': True}}
    )
    return redirect(url_for('todo.todo_list'))

@todo_bp.route('/todo/delete/<task_id>', methods=['POST'])
def delete_task(task_id):
    todo_collection.delete_one({'_id': ObjectId(task_id)})
    return redirect(url_for('todo.todo_list'))

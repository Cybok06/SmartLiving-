from flask import Blueprint, render_template, session, redirect, url_for, request, flash
from pymongo import MongoClient
from bson.objectid import ObjectId
from datetime import datetime
from db import db
# MongoDB Setup

admin_task_view_bp = Blueprint('admin_task_view', __name__)

tasks_col = db.tasks
users_col = db.users

@admin_task_view_bp.route('/admin_tasks', methods=['GET', 'POST'])
def view_admin_tasks():
    if 'manager_id' not in session:
        return redirect(url_for('login.login'))

    manager_id = ObjectId(session['manager_id'])

    if request.method == 'POST':
        task_id = request.form.get('task_id')
        tasks_col.update_one(
            {'_id': ObjectId(task_id), 'manager_id': manager_id},
            {'$set': {'status': 'completed'}}
        )
        flash("Task marked as completed", "success")
        return redirect(url_for('admin_task_view.view_admin_tasks'))

    # Only show tasks where 'manager_id' matches and were sent by admin
    tasks = list(tasks_col.find({'manager_id': manager_id}).sort('timestamp', -1))

    for task in tasks:
        task['_id'] = str(task['_id'])
        task['time_str'] = task['timestamp'].strftime('%Y-%m-%d %H:%M')
        task['image_url'] = task.get('image_url')

    return render_template('view_admin_task.html', tasks=tasks)

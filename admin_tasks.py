from flask import Blueprint, render_template, request, redirect, url_for, session, flash, jsonify
from bson.objectid import ObjectId
from datetime import datetime
from db import db

users_col = db.users
tasks_col = db.tasks

admin_task_bp = Blueprint('admin_task', __name__)

@admin_task_bp.route('/admin/tasks', methods=['GET', 'POST'])
def admin_tasks():
    if 'admin_id' not in session:
        return redirect(url_for('login.login'))

    if request.method == 'POST':
        target_type = request.form.get('target_type')
        user_id = request.form.get('user_id')
        branch_name = request.form.get('branch_name')
        message = request.form.get('message')
        image_url = request.form.get('image_url')

        if not message or not user_id or not target_type:
            flash('Target, role, and message are required.', 'danger')
            return redirect(url_for('admin_task.admin_tasks'))

        task = {
            'admin_id': ObjectId(session['admin_id']),
            'target_type': target_type,
            'user_id': 'all' if user_id == 'all' else ObjectId(user_id),
            'branch_name': None if user_id == 'all' else branch_name,
            'message': message,
            'image_url': image_url if image_url else None,
            'timestamp': datetime.utcnow(),
            'status': 'pending'
        }

        tasks_col.insert_one(task)
        flash('Task sent successfully.', 'success')
        return redirect(url_for('admin_task.admin_tasks'))

    # Filtering tasks by status
    filter_status = request.args.get('status', 'pending')
    tasks = list(tasks_col.find({
        'admin_id': ObjectId(session['admin_id']),
        'status': filter_status
    }).sort('timestamp', -1))

    for task in tasks:
        if task['user_id'] == 'all':
            task['target_name'] = 'All ' + ('Managers' if task['target_type'] == 'manager' else 'Agents')
        else:
            target = users_col.find_one({'_id': task['user_id']})
            task['target_name'] = target['username'] if target else 'Unknown'
        task['branch_display'] = task.get('branch_name') or '—'

    return render_template('admin_tasks.html', tasks=tasks, current_status=filter_status)

@admin_task_bp.route('/admin/tasks/get_users/<role>')
def get_users_by_role(role):
    role = role.lower()
    if role not in ['agent', 'manager']:
        return jsonify([])

    users = list(users_col.find({'role': role}, {'_id': 1, 'username': 1, 'branch': 1}))
    result = []
    for user in users:
        result.append({
            '_id': str(user['_id']),
            'username': user.get('username', ''),
            'branch': user.get('branch', '')
        })
    return jsonify(result)

@admin_task_bp.route('/admin/tasks/branches/<user_id>')
def get_branch_for_user(user_id):
    if user_id == 'all':
        return jsonify({'branch': ''})
    try:
        user = users_col.find_one({'_id': ObjectId(user_id)})
        return jsonify({'branch': user.get('branch', '') if user else ''})
    except:
        return jsonify({'branch': ''})

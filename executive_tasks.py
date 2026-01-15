from flask import Blueprint, render_template, request, redirect, url_for, session, flash, jsonify
from bson.objectid import ObjectId
from datetime import datetime
from db import db
from werkzeug.utils import secure_filename
import uuid
import os

executive_task_bp = Blueprint('executive_task', __name__)

users_col = db.users
tasks_col = db.tasks

# Upload config
UPLOAD_FOLDER = os.path.join(os.getcwd(), 'uploads')
ALLOWED_EXTENSIONS = {'png', 'jpg', 'jpeg', 'gif'}

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

@executive_task_bp.route('/executive/tasks', methods=['GET', 'POST'])
def executive_tasks():
    if 'executive_id' not in session:
        return redirect(url_for('login.login'))

    if request.method == 'POST':
        target_type = request.form.get('target_type')
        user_id = request.form.get('user_id')
        branch_name = request.form.get('branch_name')
        message = request.form.get('message')
        image_url = request.form.get('image_url')

        if not message or not user_id or not target_type:
            flash('Target role, user, and message are required.', 'danger')
            return redirect(url_for('executive_task.executive_tasks'))

        task = {
            'executive_id': ObjectId(session['executive_id']),
            'target_type': target_type,
            'user_id': 'all' if user_id == 'all' else ObjectId(user_id),
            'branch_name': None if user_id == 'all' else branch_name,
            'message': message,
            'image_url': image_url if image_url else None,
            'timestamp': datetime.utcnow(),
            'status': 'pending'
        }

        tasks_col.insert_one(task)
        flash('✅ Task sent successfully.', 'success')
        return redirect(url_for('executive_task.executive_tasks'))

    filter_status = request.args.get('status', 'pending')
    tasks = list(tasks_col.find({
        'executive_id': ObjectId(session['executive_id']),
        'status': filter_status
    }).sort('timestamp', -1))

    for task in tasks:
        if task['user_id'] == 'all':
            task['target_name'] = 'All ' + task['target_type'].capitalize() + 's'
        else:
            target = users_col.find_one({'_id': task['user_id']})
            task['target_name'] = target['username'] if target else 'Unknown'
        task['branch_display'] = task.get('branch_name') or '—'

    return render_template('executive_tasks.html', tasks=tasks, current_status=filter_status)

@executive_task_bp.route('/executive/tasks/get_users/<role>')
def executive_get_users(role):
    if role not in ['agent', 'manager', 'admin']:
        return jsonify([])

    users = list(users_col.find({'role': role}, {'_id': 1, 'username': 1, 'branch': 1}))
    return jsonify([
        {
            '_id': str(user['_id']),
            'username': user.get('username', ''),
            'branch': user.get('branch', 'Admin') if role == 'admin' else user.get('branch', '')
        }
        for user in users
    ])

@executive_task_bp.route('/executive/tasks/branches/<user_id>')
def executive_get_branch(user_id):
    if user_id == 'all':
        return jsonify({'branch': ''})
    try:
        user = users_col.find_one({'_id': ObjectId(user_id)})
        return jsonify({'branch': user.get('branch', 'Admin') if user else ''})
    except:
        return jsonify({'branch': ''})

@executive_task_bp.route('/executive/tasks/upload_image', methods=['POST'])
def upload_executive_image():
    try:
        image = request.files.get('image')
        if not image or not allowed_file(image.filename):
            return jsonify({'success': False, 'error': 'Invalid or missing image'}), 400

        os.makedirs(UPLOAD_FOLDER, exist_ok=True)

        filename = f"{uuid.uuid4().hex}_{secure_filename(image.filename)}"
        filepath = os.path.join(UPLOAD_FOLDER, filename)
        image.save(filepath)

        image_url = f"/uploads/{filename}"
        return jsonify({'success': True, 'image_url': image_url})
    except Exception as e:
        print("Image upload error:", e)
        return jsonify({'success': False, 'error': str(e)}), 500

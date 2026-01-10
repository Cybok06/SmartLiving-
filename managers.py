from flask import Blueprint, render_template, request, redirect, url_for, flash
from flask_bcrypt import Bcrypt
from bson.objectid import ObjectId
from db import db

# Blueprint setup
managers_bp = Blueprint('managers', __name__)
bcrypt = Bcrypt()

users_col = db.users

# Ensure bcrypt is initialized with the app
@managers_bp.record_once
def on_load(state):
    bcrypt.init_app(state.app)

# List managers with optional filters
@managers_bp.route('/managers', methods=['GET'])
def manager_list():
    query = request.args.get('username', '').lower()
    branch_filter = request.args.get('branch', '').lower()

    managers = list(users_col.find({'role': 'manager'}))
    filtered = [
        m for m in managers
        if (not query or query in m.get('username', '').lower()) and
           (not branch_filter or branch_filter in m.get('branch', '').lower())
    ]

    return render_template('manager_list.html', managers=filtered)

# View manager profile
@managers_bp.route('/manager/<manager_id>', methods=['GET'])
def view_manager_profile(manager_id):
    manager = users_col.find_one({'_id': ObjectId(manager_id)})
    if not manager:
        flash("Manager not found.", "danger")
        return redirect(url_for('managers.manager_list'))

    return render_template('admin_manager_profile.html', manager=manager)

# Update manager profile
@managers_bp.route('/manager/<manager_id>', methods=['POST'])
def update_manager_profile(manager_id):
    manager = users_col.find_one({'_id': ObjectId(manager_id)})
    if not manager:
        flash("Manager not found.", "danger")
        return redirect(url_for('managers.manager_list'))

    username = request.form.get('username', '').strip()
    email = request.form.get('email', '').strip()
    phone = request.form.get('phone', '').strip()
    branch = request.form.get('branch', '').strip()
    gender = request.form.get('gender', '').strip()
    position = request.form.get('position', '').strip()
    location = request.form.get('location', '').strip()
    start_date = request.form.get('start_date', '').strip()
    status = request.form.get('status', 'Active').strip()
    assets = [a.strip() for a in request.form.get('assets', '').split(',') if a.strip()]

    update_data = {
        'username': username,
        'email': email,
        'phone': phone,
        'branch': branch,
        'gender': gender,
        'position': position,
        'location': location,
        'start_date': start_date,
        'status': status,
        'assets': assets
    }

    # Optional password update using bcrypt
    new_password = request.form.get('password', '').strip()
    if new_password:
        hashed_password = bcrypt.generate_password_hash(new_password).decode('utf-8')
        update_data['password'] = hashed_password

    users_col.update_one({'_id': ObjectId(manager_id)}, {'$set': update_data})
    flash("Manager profile updated successfully.", "success")

    return redirect(url_for('managers.view_manager_profile', manager_id=manager_id))

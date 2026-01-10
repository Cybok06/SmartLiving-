from flask import Blueprint, render_template, request, redirect, url_for, flash, current_app
from flask_bcrypt import Bcrypt
from datetime import datetime
from bson.objectid import ObjectId
from db import db

register_manager_bp = Blueprint('register_manager', __name__)
bcrypt = Bcrypt()

users_col = db.users

# Ensure bcrypt is initialized with the app
@register_manager_bp.record_once
def on_load(state):
    bcrypt.init_app(state.app)

@register_manager_bp.route('/register_manager', methods=['GET', 'POST'])
def register_manager():
    if request.method == 'POST':
        data = request.form

        # Check if username already exists
        if users_col.find_one({'username': data['username']}):
            flash("Username already exists.", "danger")
            return render_template('register_manager.html')

        # Hash password using properly initialized bcrypt
        hashed_password = bcrypt.generate_password_hash(data['password']).decode('utf-8')

        manager = {
            'username': data['username'],
            'password': hashed_password,
            'role': 'manager',
            'name': data['name'],
            'phone': data['phone'],
            'email': data['email'],
            'gender': data['gender'],
            'branch': data['branch'],
            'position': data['position'],
            'location': data['location'],
            'start_date': data['start_date'],
            'image_url': data['image_url'],
            'status': 'Active',
            'assets': [item.strip() for item in data.get('assets', '').split(',')],
            'date_registered': datetime.utcnow()
        }

        users_col.insert_one(manager)
        flash("Manager registered successfully.", "success")
        return redirect(url_for('login.admin_dashboard'))  # Optional: update this path to wherever admin goes

    return render_template('register_manager.html')

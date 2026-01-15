from flask import Blueprint, request, render_template, redirect, url_for, session, jsonify
from flask_bcrypt import Bcrypt
from bson.objectid import ObjectId
from datetime import datetime
from db import db
from werkzeug.utils import secure_filename
import uuid
import os

register_bp = Blueprint('register', __name__)
bcrypt = Bcrypt()

# MongoDB collection
users_collection = db.users

# Upload config
UPLOAD_FOLDER = '/uploads'
ALLOWED_EXTENSIONS = {'png', 'jpg', 'jpeg'}

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

@register_bp.record_once
def on_load(state):
    bcrypt.init_app(state.app)

@register_bp.route('/register', methods=['GET', 'POST'])
def register():
    if request.method == 'POST':
        if 'manager_id' not in session:
            return redirect(url_for('login.login'))

        data = request.form

        # Check if the username already exists
        if users_collection.find_one({'username': data['username']}):
            return "Username already exists."

        hashed_password = bcrypt.generate_password_hash(data['password']).decode('utf-8')

        user = {
            'username': data['username'],
            'password': hashed_password,
            'role': 'agent',
            'name': data['name'],
            'phone': data['phone'],
            'email': data['email'],
            'gender': data['gender'],
            'branch': data['branch'],
            'position': data['position'],
            'location': data['location'],
            'start_date': data['start_date'],
            'image_url': data['image_url'],
            'status': data.get('status', 'Active'),
            'assets': [item.strip() for item in data.get('assets', '').split(',')],
            'date_registered': datetime.utcnow(),
            'manager_id': ObjectId(session['manager_id'])
        }

        users_collection.insert_one(user)
        return redirect(url_for('login.manager_dashboard'))

    return render_template('register.html')

@register_bp.route('/register/upload_image', methods=['POST'])
def upload_agent_image():
    try:
        image = request.files.get('image')
        if not image or not allowed_file(image.filename):
            return jsonify({'error': 'Invalid or missing image'}), 400

        filename = f"{uuid.uuid4().hex}_{secure_filename(image.filename)}"
        image_path = os.path.join(UPLOAD_FOLDER, filename)
        image.save(image_path)

        image_url = f"/uploads/{filename}"
        return jsonify({'success': True, 'image_url': image_url})

    except Exception as e:
        print("Image upload error:", e)
        return jsonify({'error': str(e)}), 500

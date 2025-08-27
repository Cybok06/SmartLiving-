from bson import ObjectId
from flask import Blueprint, render_template, request, jsonify, send_from_directory
from flask_login import login_required, current_user
from werkzeug.utils import secure_filename
from db import db
import os
import uuid

customer_bp = Blueprint('customer', __name__)
customers_collection = db["customers"]
users_collection = db["users"]

# Upload folder and allowed extensions
UPLOAD_FOLDER = '/uploads'
ALLOWED_EXTENSIONS = {'png', 'jpg', 'jpeg'}

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

# Show registration page
@customer_bp.route('/register', methods=['GET'])
@login_required
def register_customer():
    return render_template('register_customer.html', agent_id=current_user.id)

# Serve uploaded images (if needed elsewhere)
@customer_bp.route('/uploads/<filename>')
def uploaded_file(filename):
    return send_from_directory(UPLOAD_FOLDER, filename)

# Add a new customer
@customer_bp.route('/add', methods=['POST'])
@login_required
def add_customer():
    try:
        name = request.form.get('name')
        location = request.form.get('location')
        occupation = request.form.get('occupation')
        phone_number = request.form.get('phone_number')
        comment = request.form.get('comment')
        agent_id = request.form.get('agent_id') or str(current_user.id)
        latitude = request.form.get('latitude')
        longitude = request.form.get('longitude')
        image_file = request.files.get('image')

        if not agent_id:
            return jsonify({'error': 'Agent ID is required'}), 400

        # Fetch agent to get manager_id
        agent = users_collection.find_one({"_id": ObjectId(agent_id), "role": "agent"})
        if not agent or "manager_id" not in agent:
            return jsonify({'error': 'Manager ID not found for this agent'}), 400

        manager_id = agent["manager_id"]

        # Handle image upload
        image_url = None
        if image_file and allowed_file(image_file.filename):
            filename = f"{uuid.uuid4().hex}_{secure_filename(image_file.filename)}"
            image_path = os.path.join(UPLOAD_FOLDER, filename)
            image_file.save(image_path)
            image_url = f"/uploads/{filename}"
        else:
            return jsonify({'error': 'Invalid or missing image file'}), 400

        # Build customer object
        customer = {
            'name': name,
            'image_url': image_url,
            'location': location,
            'occupation': occupation,
            'phone_number': phone_number,
            'comment': comment,
            'agent_id': agent_id,
            'manager_id': manager_id
        }

        # Coordinates
        if latitude and longitude:
            try:
                customer['coordinates'] = {
                    'latitude': float(latitude),
                    'longitude': float(longitude)
                }
            except ValueError:
                print("Invalid latitude or longitude format")

        # Insert into DB
        customers_collection.insert_one(customer)
        return jsonify({'message': 'Customer registered successfully!'}), 200

    except Exception as e:
        print("Error:", str(e))
        return jsonify({'error': 'Failed to register customer'}), 500

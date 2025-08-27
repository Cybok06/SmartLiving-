import os
import json
import traceback
from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify, send_from_directory
from werkzeug.utils import secure_filename
from bson.objectid import ObjectId
from db import db

add_inventory_bp = Blueprint('add_inventory', __name__)
UPLOAD_FOLDER = os.path.join(os.getcwd(), 'uploads')
ALLOWED_EXTENSIONS = {'png', 'jpg', 'jpeg', 'gif'}

# Ensure uploads folder exists
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

# DB collections
inventory_col = db.inventory
users_col = db.users

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

# ✅ Image upload endpoint
@add_inventory_bp.route('/add_inventory/upload_image', methods=['POST'])
def upload_inventory_image():
    if 'image' not in request.files:
        return jsonify({'success': False, 'error': 'No image in request'})

    image = request.files['image']
    if image.filename == '':
        return jsonify({'success': False, 'error': 'No file selected'})

    if image and allowed_file(image.filename):
        filename = secure_filename(image.filename)
        save_path = os.path.join(UPLOAD_FOLDER, filename)

        # Avoid overwriting files
        counter = 1
        base, ext = os.path.splitext(filename)
        while os.path.exists(save_path):
            filename = f"{base}_{counter}{ext}"
            save_path = os.path.join(UPLOAD_FOLDER, filename)
            counter += 1

        image.save(save_path)
        return jsonify({'success': True, 'image_url': f'/uploads/{filename}'})

    return jsonify({'success': False, 'error': 'Invalid file type'})


# ✅ Serve uploaded images
@add_inventory_bp.route('/uploads/<filename>')
def uploaded_inventory_file(filename):
    return send_from_directory(UPLOAD_FOLDER, filename)


# ✅ Main add_inventory route
@add_inventory_bp.route('/add_inventory', methods=['GET', 'POST'])
def add_inventory():
    if request.method == 'POST':
        try:
            name = request.form.get('name', '').strip()
            initial_price = request.form.get('initial_price', '').strip()
            profit_input = request.form.get('profit', '').strip()
            description = request.form.get('description', '').strip()
            image_url = request.form.get('image_url', '').strip()
            qty = request.form.get('qty', '').strip()
            selected_managers = request.form.getlist('manager_ids')

            # Validate fields
            if not all([name, initial_price, profit_input, description, image_url, qty]):
                flash("❌ All fields are required.", "danger")
                return redirect(url_for('add_inventory.add_inventory'))

            if not selected_managers:
                flash("❌ Please select at least one manager.", "danger")
                return redirect(url_for('add_inventory.add_inventory'))

            initial_price = float(initial_price)
            qty = int(qty)
            if qty <= 0:
                flash("❌ Quantity must be greater than 0.", "danger")
                return redirect(url_for('add_inventory.add_inventory'))

            # Calculate final price
            if profit_input.endswith('%'):
                percent = float(profit_input.rstrip('%'))
                price = initial_price + (initial_price * percent / 100)
            else:
                price = initial_price + float(profit_input)

            # Save inventory for each selected manager
            for manager_id in selected_managers:
                inventory_col.insert_one({
                    'name': name,
                    'initial_price': initial_price,
                    'profit_input': profit_input,
                    'price': round(price, 2),
                    'qty': qty,
                    'description': description,
                    'image_url': image_url,
                    'manager_id': ObjectId(manager_id)
                })

            flash("✅ Inventory item added for selected manager(s).", "success")
            return redirect(url_for('add_inventory.add_inventory'))

        except Exception as e:
            print("❌ Inventory add error:", str(e))
            traceback.print_exc()
            flash("❌ Something went wrong. Please try again.", "danger")
            return redirect(url_for('add_inventory.add_inventory'))

    managers = list(users_col.find({'role': 'manager'}))
    return render_template('add_inventory.html', managers=managers)

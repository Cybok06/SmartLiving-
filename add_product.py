from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify, send_from_directory
from bson.objectid import ObjectId
from urllib.parse import unquote
from werkzeug.utils import secure_filename
import json
import os
import traceback
from db import db

add_product_bp = Blueprint('add_product', __name__)
UPLOAD_FOLDER = os.path.join(os.getcwd(), 'uploads')
ALLOWED_EXTENSIONS = {'png', 'jpg', 'jpeg', 'gif'}

# Ensure upload folder exists
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

inventory_col = db.inventory
products_col = db.products
users_col = db.users

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

# ✅ Image Upload Endpoint
@add_product_bp.route('/products/upload_image', methods=['POST'])
def upload_image():
    if 'image' not in request.files:
        return jsonify({'success': False, 'error': 'No file part in request'})

    image = request.files['image']
    if image.filename == '':
        return jsonify({'success': False, 'error': 'No selected file'})

    if image and allowed_file(image.filename):
        filename = secure_filename(image.filename)
        save_path = os.path.join(UPLOAD_FOLDER, filename)

        # If file already exists, add a number to avoid overwrite
        counter = 1
        base, ext = os.path.splitext(filename)
        while os.path.exists(save_path):
            filename = f"{base}_{counter}{ext}"
            save_path = os.path.join(UPLOAD_FOLDER, filename)
            counter += 1

        image.save(save_path)
        return jsonify({'success': True, 'image_url': f'/uploads/{filename}'})

    return jsonify({'success': False, 'error': 'File type not allowed'})

# ✅ Serve images
@add_product_bp.route('/uploads/<filename>')
def uploaded_file(filename):
    return send_from_directory(UPLOAD_FOLDER, filename)


# ✅ Main product add route
@add_product_bp.route('/add_product', methods=['GET', 'POST'])
def add_product():
    managers = list(users_col.find({'role': 'manager'}))
    raw_inventory = list(inventory_col.find({}))

    # Group inventory items by shared attributes (excluding manager_id)
    grouped_inventory = {}
    for item in raw_inventory:
        key_data = {
            "name": item['name'],
            "image_url": item['image_url'],
            "price": item['price'],
            "description": item['description']
        }
        key = json.dumps(key_data, separators=(',', ':'))
        if key not in grouped_inventory:
            grouped_inventory[key] = item

    # Distinct types and categories
    product_types = sorted(set(products_col.distinct("product_type")))
    categories = sorted(set(products_col.distinct("category")))

    if request.method == 'POST':
        try:
            required_fields = ['name', 'price', 'cash_price', 'description', 'image_url']
            for field in required_fields:
                if not request.form.get(field):
                    flash(f"❌ '{field}' is required.", "danger")
                    return redirect(url_for('add_product.add_product'))

            name = request.form['name']
            price = float(request.form['price'])
            cash_price = float(request.form['cash_price'])
            description = request.form['description']
            image_url = request.form['image_url']

            # Use custom field if present
            product_type = request.form.get('custom_product_type') or request.form.get('product_type') or ''
            category = request.form.get('custom_category') or request.form.get('category') or ''
            package_name = request.form.get('package_name', '')

            selected_manager_ids = request.form.getlist('managers')
            component_keys = request.form.getlist('components')

            components_by_key = {}
            for key in component_keys:
                qty_key = f"qty_{key}"
                try:
                    quantity = int(request.form.get(qty_key, 1))
                    if quantity <= 0:
                        flash("❌ Quantity must be at least 1.", "danger")
                        return redirect(url_for('add_product.add_product'))

                    decoded_key = unquote(key)
                    components_by_key[decoded_key] = quantity
                except (ValueError, TypeError):
                    flash("❌ Invalid quantity for a component.", "danger")
                    return redirect(url_for('add_product.add_product'))

            if not selected_manager_ids:
                flash("❌ Please select at least one manager.", "danger")
                return redirect(url_for('add_product.add_product'))

            valid_count = 0

            for manager_id in selected_manager_ids:
                manager_oid = ObjectId(manager_id)
                missing = []
                resolved_components = []

                for key_str, qty in components_by_key.items():
                    try:
                        comp_data = json.loads(key_str)
                    except json.JSONDecodeError:
                        flash("❌ Error reading component data.", "danger")
                        return redirect(url_for('add_product.add_product'))

                    match = inventory_col.find_one({
                        "name": comp_data['name'],
                        "image_url": comp_data['image_url'],
                        "price": comp_data['price'],
                        "description": comp_data['description'],
                        "manager_id": manager_oid
                    })

                    if match:
                        resolved_components.append({
                            "_id": match["_id"],
                            "quantity": qty
                        })
                    else:
                        missing.append(comp_data['name'])

                if missing:
                    manager = users_col.find_one({'_id': manager_oid})
                    flash(f"❌ {manager['name']} is missing: {', '.join(missing)}", 'danger')
                    continue

                if resolved_components:
                    product = {
                        'name': name,
                        'price': price,
                        'cash_price': cash_price,
                        'description': description,
                        'image_url': image_url,
                        'product_type': product_type,
                        'category': category,
                        'package_name': package_name,
                        'components': resolved_components,
                        'manager_id': manager_oid
                    }
                    products_col.insert_one(product)
                    valid_count += 1

            if valid_count:
                flash(f"✅ Product added for {valid_count} manager(s).", "success")
            else:
                flash("❌ No product was added. All selected managers had missing components.", "danger")

        except Exception as e:
            print("❌ Error while adding product:", str(e))
            traceback.print_exc()
            flash("❌ An unexpected error occurred. Please check your inputs.", "danger")

        return redirect(url_for('add_product.add_product'))

    return render_template(
        'add_product.html',
        inventory=grouped_inventory,
        managers=managers,
        product_types=product_types,
        categories=categories
    )

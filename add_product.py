from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify, send_from_directory
from bson.objectid import ObjectId
from urllib.parse import unquote
from werkzeug.utils import secure_filename
from datetime import datetime
import json
import os
import traceback
import requests

from db import db

add_product_bp = Blueprint('add_product', __name__)
UPLOAD_FOLDER = os.path.join(os.getcwd(), 'uploads')
ALLOWED_EXTENSIONS = {'png', 'jpg', 'jpeg', 'gif'}

# ===== Cloudflare (hardcoded as requested) =====
CF_ACCOUNT_ID   = "63e6f91eec9591f77699c4b434ab44c6"
CF_IMAGES_TOKEN = "Brz0BEfl_GqEUjEghS2UEmLZhK39EUmMbZgu_hIo"
CF_HASH         = "h9fmMoa1o2c2P55TcWJGOg"
DEFAULT_VARIANT = "public"  # make sure this variant exists in Cloudflare Images

# Ensure local upload folder exists (kept for legacy route below)
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

inventory_col = db.inventory
products_col  = db.products
users_col     = db.users
images_col    = db.images   # NEW: store CF image logs/metadata here

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

# =============== NEW: Upload directly to Cloudflare ===============
@add_product_bp.route('/products/upload_image', methods=['POST'])
def upload_image():
    """
    Receives 'image' file, requests a one-time Cloudflare direct upload URL,
    uploads the file to Cloudflare, stores a record in DB, and returns JSON:
      { success, image_url, image_id, variant }
    """
    try:
        if 'image' not in request.files:
            return jsonify({'success': False, 'error': 'No file part in request'}), 400

        image = request.files['image']
        if image.filename == '':
            return jsonify({'success': False, 'error': 'No selected file'}), 400

        if not (image and allowed_file(image.filename)):
            return jsonify({'success': False, 'error': 'File type not allowed'}), 400

        # Step 1: get one-time direct upload URL from Cloudflare
        direct_url = f"https://api.cloudflare.com/client/v4/accounts/{CF_ACCOUNT_ID}/images/v2/direct_upload"
        headers    = {"Authorization": f"Bearer {CF_IMAGES_TOKEN}"}

        # Optional: pass metadata; you can include anything (won’t be exposed to end users)
        # data = {"metadata": json.dumps({"source": "add_product", "filename": image.filename})}
        data = {}  # keeping minimal; add requireSignedURLs or metadata if needed

        res = requests.post(direct_url, headers=headers, data=data, timeout=20)
        try:
            j = res.json()
        except Exception:
            return jsonify({'success': False, 'error': 'Cloudflare (direct_upload) returned non-JSON'}), 502

        if not j.get('success'):
            return jsonify({'success': False, 'error': 'Cloudflare direct_upload failed', 'details': j}), 400

        upload_url = j['result']['uploadURL']
        image_id   = j['result']['id']

        # Step 2: upload the actual file to Cloudflare
        up = requests.post(
            upload_url,
            files={'file': (secure_filename(image.filename), image.stream, image.mimetype or 'application/octet-stream')},
            timeout=60
        )
        try:
            uj = up.json()
        except Exception:
            return jsonify({'success': False, 'error': 'Cloudflare (upload) returned non-JSON'}), 502

        if not uj.get('success'):
            return jsonify({'success': False, 'error': 'Cloudflare upload failed', 'details': uj}), 400

        # Step 3: build delivery URL & save in DB
        variant   = request.args.get('variant', DEFAULT_VARIANT)
        image_url = f"https://imagedelivery.net/{CF_HASH}/{image_id}/{variant}"

        # Save a record in DB for traceability
        images_col.insert_one({
            'provider': 'cloudflare_images',
            'image_id': image_id,
            'variant': variant,
            'url': image_url,
            'original_filename': secure_filename(image.filename),
            'mimetype': image.mimetype,
            'size_bytes': request.content_length,  # not exact file size, but fine for quick trace
            'created_at': datetime.utcnow()
        })

        return jsonify({'success': True, 'image_url': image_url, 'image_id': image_id, 'variant': variant})

    except Exception as e:
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 500

# =============== (Legacy) Serve local files if ever needed ===============
@add_product_bp.route('/uploads/<filename>')
def uploaded_file(filename):
    return send_from_directory(UPLOAD_FOLDER, filename)

# =============== Main product add route (unchanged logic) ===============
@add_product_bp.route('/add_product', methods=['GET', 'POST'])
def add_product():
    managers = list(users_col.find({'role': 'manager'}))
    raw_inventory = list(inventory_col.find({}))

    # Group inventory items by shared attributes (excluding manager_id)
    grouped_inventory = {}
    for item in raw_inventory:
        key_data = {
            "name": item.get('name'),
            "image_url": item.get('image_url'),
            "price": item.get('price'),
            "description": item.get('description')
        }
        key = json.dumps(key_data, separators=(',', ':'))
        if key not in grouped_inventory:
            grouped_inventory[key] = item

    # Distinct types and categories
    product_types = sorted(set(products_col.distinct("product_type")))
    categories    = sorted(set(products_col.distinct("category")))

    if request.method == 'POST':
        try:
            required_fields = ['name', 'price', 'cash_price', 'description', 'image_url']
            for field in required_fields:
                if not request.form.get(field):
                    flash(f"❌ '{field}' is required.", "danger")
                    return redirect(url_for('add_product.add_product'))

            name        = request.form['name']
            price       = float(request.form['price'])
            cash_price  = float(request.form['cash_price'])
            description = request.form['description']
            image_url   = request.form['image_url']  # should be the Cloudflare URL returned by /products/upload_image

            # Use custom field if present
            product_type = request.form.get('custom_product_type') or request.form.get('product_type') or ''
            category     = request.form.get('custom_category') or request.form.get('category') or ''
            package_name = request.form.get('package_name', '')

            selected_manager_ids = request.form.getlist('managers')
            component_keys       = request.form.getlist('components')

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
                        "name": comp_data.get('name'),
                        "image_url": comp_data.get('image_url'),
                        "price": comp_data.get('price'),
                        "description": comp_data.get('description'),
                        "manager_id": manager_oid
                    })

                    if match:
                        resolved_components.append({
                            "_id": match["_id"],
                            "quantity": qty
                        })
                    else:
                        missing.append(comp_data.get('name'))

                if missing:
                    manager = users_col.find_one({'_id': manager_oid})
                    flash(f"❌ {manager.get('name', 'Manager')} is missing: {', '.join(missing)}", 'danger')
                    continue

                if resolved_components:
                    product = {
                        'name': name,
                        'price': price,
                        'cash_price': cash_price,
                        'description': description,
                        'image_url': image_url,  # Cloudflare delivery URL
                        'product_type': product_type,
                        'category': category,
                        'package_name': package_name,
                        'components': resolved_components,
                        'manager_id': manager_oid,
                        'created_at': datetime.utcnow()
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

from flask import Blueprint, render_template, request, redirect, url_for, flash, session, jsonify, current_app
from bson.objectid import ObjectId
import re
from datetime import datetime
import os
from werkzeug.utils import secure_filename
from db import db

# Initialize Blueprint
inventory_products_bp = Blueprint('inventory_products', __name__)

# Database Collections
inventory_col = db.inventory
users_col = db.users
inventory_logs_col = db.inventory_logs
deleted_col = db.deleted

# File Upload Config
UPLOAD_FOLDER = os.path.join(os.getcwd(), 'uploads')
ALLOWED_EXTENSIONS = {'png', 'jpg', 'jpeg', 'gif'}
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

# -----------------------------
# Route: Change Image for Product
# -----------------------------
@inventory_products_bp.route('/inventory_products/change_image/<item_id>', methods=['POST'])
def change_inventory_image(item_id):
    admin_username = session.get('username', 'Unknown')
    file = request.files.get('image')

    if not file or not allowed_file(file.filename):
        flash("❌ Invalid image file. Only PNG, JPG, JPEG, and GIF allowed.", "danger")
        return redirect(url_for('inventory_products.inventory_products'))

    item = inventory_col.find_one({"_id": ObjectId(item_id)})
    if not item:
        flash("❌ Item not found.", "danger")
        return redirect(url_for('inventory_products.inventory_products'))

    filename = secure_filename(file.filename)
    save_path = os.path.join(UPLOAD_FOLDER, filename)
    file.save(save_path)

    image_url = f"/uploads/{filename}"
    name_to_match = item['name']
    price_to_match = item['price']

    matched_items = list(inventory_col.find({
        "name": name_to_match,
        "price": price_to_match
    }))

    for product in matched_items:
        inventory_col.update_one({'_id': product['_id']}, {'$set': {'image_url': image_url}})
        inventory_logs_col.insert_one({
            'product_id': product['_id'],
            'product_name': product.get('name'),
            'action': 'image_update',
            'old_image_url': product.get('image_url'),
            'new_image_url': image_url,
            'updated_by': admin_username,
            'updated_at': datetime.now()
        })

    flash("✅ Image updated across all matching products.", "success")
    return redirect(url_for('inventory_products.inventory_products'))


# -----------------------------
# Route: Inventory Management
# -----------------------------
@inventory_products_bp.route('/inventory_products', methods=['GET', 'POST'])
def inventory_products():
    if request.method == 'POST':
        action = request.form.get('action')
        item_id = request.form.get('item_id')
        admin_username = session.get('username', 'Unknown')
        selected_branches = request.form.getlist('branches')

        if not selected_branches:
            flash("❌ Please select at least one branch.", "danger")
            return redirect(url_for('inventory_products.inventory_products'))

        item = inventory_col.aggregate([
            {"$lookup": {
                "from": "users", "localField": "manager_id", "foreignField": "_id", "as": "manager"
            }},
            {"$unwind": "$manager"},
            {"$match": {"_id": ObjectId(item_id)}}
        ])
        item = next(item, None)

        if not item:
            flash("❌ Item not found.", "danger")
            return redirect(url_for('inventory_products.inventory_products'))

        name_to_match = item['name']

        matched_items = list(inventory_col.aggregate([
            {"$lookup": {
                "from": "users", "localField": "manager_id", "foreignField": "_id", "as": "manager"
            }},
            {"$unwind": "$manager"},
            {"$match": {
                "name": name_to_match,
                "manager.branch": {"$in": selected_branches}
            }}
        ]))

        if action == 'update':
            try:
                new_name = request.form.get('name').strip()
                new_price = float(request.form.get('price'))
                new_qty = int(request.form.get('qty'))

                for product in matched_items:
                    inventory_col.update_one(
                        {'_id': product['_id']},
                        {'$set': {'name': new_name, 'price': new_price, 'qty': new_qty}}
                    )
                    inventory_logs_col.insert_one({
                        'product_id': product['_id'],
                        'product_name': new_name,
                        'old_name': product.get('name'),
                        'new_name': new_name,
                        'old_price': product.get('price'),
                        'new_price': new_price,
                        'old_qty': product.get('qty'),
                        'new_qty': new_qty,
                        'updated_by': admin_username,
                        'action': 'update',
                        'updated_at': datetime.now()
                    })

                flash("✅ Product updated across selected branches.", "success")
            except Exception as e:
                flash(f"❌ Error: {str(e)}", "danger")

        elif action == 'delete':
            for product in matched_items:
                inventory_logs_col.insert_one({
                    'product_id': product['_id'],
                    'product_name': product.get('name'),
                    'price': product.get('price'),
                    'qty': product.get('qty'),
                    'deleted_by': admin_username,
                    'action': 'delete',
                    'deleted_at': datetime.now()
                })
                deleted_col.insert_one({
                    'deleted_item': product,
                    'deleted_by': admin_username,
                    'deleted_at': datetime.now()
                })
                inventory_col.delete_one({'_id': product['_id']})

            flash("🗑️ Product deleted across selected branches.", "success")

        return redirect(url_for('inventory_products.inventory_products'))

    # GET logic
    manager_query = request.args.get('manager', '').strip()
    branch_query = request.args.get('branch', '').strip()
    product_query = request.args.get('product', '').strip()
    limit = int(request.args.get('limit', 50))
    offset = int(request.args.get('offset', 0))
    current_page = (offset // limit) + 1

    pipeline = [
        {"$lookup": {
            "from": "users", "localField": "manager_id", "foreignField": "_id", "as": "manager"
        }},
        {"$unwind": "$manager"},
    ]

    filters = {}
    if manager_query:
        filters["manager.name"] = {"$regex": re.escape(manager_query), "$options": "i"}
    if branch_query:
        filters["manager.branch"] = {"$regex": re.escape(branch_query), "$options": "i"}
    if product_query:
        filters["name"] = {"$regex": re.escape(product_query), "$options": "i"}

    if filters:
        pipeline.append({"$match": filters})

    total_count = inventory_col.aggregate(pipeline + [{"$count": "count"}])
    total_count = next(total_count, {}).get("count", 0)

    pipeline += [{"$sort": {"manager.branch": 1, "name": 1}}, {"$skip": offset}, {"$limit": limit}]
    raw_inventory = list(inventory_col.aggregate(pipeline))

    group_products = not manager_query and not branch_query
    if group_products:
        grouped = {}
        for item in raw_inventory:
            key = (item.get("name"), item.get("price"), item.get("description"), item.get("image_url"))
            if key not in grouped:
                grouped[key] = {
                    "_id": item["_id"],
                    "name": item["name"],
                    "price": item["price"],
                    "description": item["description"],
                    "image_url": item["image_url"],
                    "qty": item["qty"],
                    "manager": {"name": "Multiple", "branch": "All Branches"}
                }
            else:
                grouped[key]["qty"] += item["qty"]
        inventory = list(grouped.values())
    else:
        inventory = raw_inventory

    manager_names = sorted(users_col.distinct("name", {"role": "manager"}))
    branch_names = sorted(users_col.distinct("branch", {"role": "manager"}))

    return render_template(
        'inventory_products.html',
        inventory=inventory,
        managers=manager_names,
        branches=branch_names,
        total_count=total_count,
        offset=offset,
        limit=limit,
        current_page=current_page
    )


# -----------------------------
# Route: Inventory Logs History
# -----------------------------
@inventory_products_bp.route('/inventory_history/<item_id>')
def inventory_history(item_id):
    logs = list(inventory_logs_col.find({'product_id': ObjectId(item_id)}).sort('updated_at', -1))
    for log in logs:
        log['_id'] = str(log['_id'])
        log['product_id'] = str(log['product_id'])
        log['updated_at'] = log.get('updated_at') or log.get('deleted_at')
        if log['updated_at']:
            log['updated_at'] = log['updated_at'].strftime('%Y-%m-%d %H:%M:%S')
    return jsonify(logs)

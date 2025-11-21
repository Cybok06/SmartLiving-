from flask import Blueprint, render_template, request, redirect, url_for, flash, session, jsonify
from bson.objectid import ObjectId, InvalidId
import re, os
from datetime import datetime
from werkzeug.utils import secure_filename
from db import db

# Initialize Blueprint
inventory_products_bp = Blueprint('inventory_products', __name__)

# Database Collections
inventory_col       = db.inventory
users_col           = db.users
inventory_logs_col  = db.inventory_logs
deleted_col         = db.deleted

# File Upload Config
UPLOAD_FOLDER = os.path.join(os.getcwd(), 'uploads')
ALLOWED_EXTENSIONS = {'png', 'jpg', 'jpeg', 'gif'}
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

# -----------------------------
# Helpers
# -----------------------------
def allowed_file(filename: str) -> bool:
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


def safe_float(val):
    if val is None:
        return None
    s = str(val).strip()
    if s == "":
        return None
    try:
        return float(s)
    except Exception:
        return None


def safe_int(val):
    if val is None:
        return None
    s = str(val).strip()
    if s == "":
        return None
    try:
        return int(s)
    except Exception:
        return None


def money2(v):
    if v is None:
        return None
    try:
        return round(float(v), 2)
    except Exception:
        return None


def to_oid(val):
    try:
        return ObjectId(val)
    except (InvalidId, TypeError):
        return None


# -----------------------------
# Route: Change Image for Product
# -----------------------------
@inventory_products_bp.route('/inventory_products/change_image/<item_id>', methods=['POST'])
def change_inventory_image(item_id):
    admin_username = session.get('username', 'Unknown')
    oid = to_oid(item_id)
    if not oid:
        flash("❌ Invalid item ID.", "danger")
        return redirect(url_for('inventory_products.inventory_products'))

    file = request.files.get('image')

    if not file or not allowed_file(file.filename):
        flash("❌ Invalid image file. Only PNG, JPG, JPEG, and GIF allowed.", "danger")
        return redirect(url_for('inventory_products.inventory_products'))

    item = inventory_col.find_one({"_id": oid})
    if not item:
        flash("❌ Item not found.", "danger")
        return redirect(url_for('inventory_products.inventory_products'))

    # Save with collision-safe filename
    filename = secure_filename(file.filename)
    base, ext = os.path.splitext(filename)
    save_path = os.path.join(UPLOAD_FOLDER, filename)
    counter = 1
    while os.path.exists(save_path):
        filename = f"{base}_{counter}{ext}"
        save_path = os.path.join(UPLOAD_FOLDER, filename)
        counter += 1
    file.save(save_path)

    image_url = f"/uploads/{filename}"

    # Match by name + legacy price; if selling_price present, also match those with same name+selling_price
    name_to_match = item.get('name')
    legacy_price  = item.get('price')
    selling_price = item.get('selling_price')

    # Build query: same name AND (same legacy price OR same selling_price)
    or_terms = []
    if legacy_price is not None:
        or_terms.append({"price": legacy_price})
    if selling_price is not None:
        or_terms.append({"selling_price": selling_price})

    q = {"name": name_to_match}
    if or_terms:
        q["$or"] = or_terms

    matched_items = list(inventory_col.find(q))

    now = datetime.utcnow()
    for product in matched_items:
        inventory_col.update_one(
            {'_id': product['_id']},
            {'$set': {'image_url': image_url, 'updated_at': now}}
        )
        inventory_logs_col.insert_one({
            'product_id': product['_id'],
            'product_name': product.get('name'),
            'action': 'image_update',
            'log_type': 'image_update',
            'old_image_url': product.get('image_url'),
            'new_image_url': image_url,
            'updated_by': admin_username,
            'updated_at': now
        })

    flash(f"✅ Image updated for {len(matched_items)} matching products.", "success")
    return redirect(url_for('inventory_products.inventory_products'))


# -----------------------------
# Route: Inventory Management (Update / Delete / Transfer)
# -----------------------------
@inventory_products_bp.route('/inventory_products', methods=['GET', 'POST'])
def inventory_products():
    if request.method == 'POST':
        action         = request.form.get('action')
        item_id        = request.form.get('item_id')
        admin_username = session.get('username', 'Unknown')

        # ---------- TRANSFER ACTION ----------
        if action == 'transfer':
            # Transfer a quantity from this item (source branch) to another branch
            transfer_qty = safe_int(request.form.get('transfer_qty'))
            to_branch    = (request.form.get('to_branch') or '').strip()

            src_oid = to_oid(item_id)
            if not src_oid:
                flash("❌ Invalid item ID for transfer.", "danger")
                return redirect(url_for('inventory_products.inventory_products'))

            if not transfer_qty or transfer_qty <= 0:
                flash("❌ Enter a valid transfer quantity (> 0).", "danger")
                return redirect(url_for('inventory_products.inventory_products'))

            if not to_branch:
                flash("❌ Please select a destination branch.", "danger")
                return redirect(url_for('inventory_products.inventory_products'))

            # Load source item with manager info (to get branch)
            src_item = next(inventory_col.aggregate([
                {
                    "$lookup": {
                        "from": "users",
                        "localField": "manager_id",
                        "foreignField": "_id",
                        "as": "manager"
                    }
                },
                {"$unwind": "$manager"},
                {"$match": {"_id": src_oid}}
            ]), None)

            if not src_item:
                flash("❌ Source item not found for transfer.", "danger")
                return redirect(url_for('inventory_products.inventory_products'))

            from_branch = src_item["manager"]["branch"]
            if to_branch == from_branch:
                flash("❌ Destination branch cannot be the same as source branch.", "danger")
                return redirect(url_for('inventory_products.inventory_products'))

            src_current_qty = src_item.get("qty") or 0
            if transfer_qty > src_current_qty:
                flash(f"❌ Cannot transfer {transfer_qty}. Only {src_current_qty} available in {from_branch}.", "danger")
                return redirect(url_for('inventory_products.inventory_products'))

            # Find destination item (same product name) in destination branch
            dest_item = next(inventory_col.aggregate([
                {
                    "$lookup": {
                        "from": "users",
                        "localField": "manager_id",
                        "foreignField": "_id",
                        "as": "manager"
                    }
                },
                {"$unwind": "$manager"},
                {
                    "$match": {
                        "name": src_item["name"],
                        "manager.branch": to_branch
                    }
                }
            ]), None)

            if not dest_item:
                flash(f"❌ No matching product found in destination branch '{to_branch}'.", "danger")
                return redirect(url_for('inventory_products.inventory_products'))

            dest_current_qty = dest_item.get("qty") or 0

            # Apply transfer
            now = datetime.utcnow()
            new_src_qty  = src_current_qty - transfer_qty
            new_dest_qty = dest_current_qty + transfer_qty

            inventory_col.update_one(
                {"_id": src_item["_id"]},
                {"$set": {"qty": new_src_qty, "updated_at": now}}
            )
            inventory_col.update_one(
                {"_id": dest_item["_id"]},
                {"$set": {"qty": new_dest_qty, "updated_at": now}}
            )

            # Logs: one for source (transfer_out), one for destination (transfer_in)
            inventory_logs_col.insert_one({
                "product_id": src_item["_id"],
                "product_name": src_item.get("name"),
                "action": "transfer_out",
                "log_type": "transfer",
                "from_branch": from_branch,
                "to_branch": to_branch,
                "qty_moved": transfer_qty,
                "old_qty": src_current_qty,
                "new_qty": new_src_qty,
                "updated_by": admin_username,
                "updated_at": now
            })

            inventory_logs_col.insert_one({
                "product_id": dest_item["_id"],
                "product_name": dest_item.get("name"),
                "action": "transfer_in",
                "log_type": "transfer",
                "from_branch": from_branch,
                "to_branch": to_branch,
                "qty_moved": transfer_qty,
                "old_qty": dest_current_qty,
                "new_qty": new_dest_qty,
                "updated_by": admin_username,
                "updated_at": now
            })

            flash(
                f"🔁 Transferred {transfer_qty} unit(s) of '{src_item['name']}' from {from_branch} to {to_branch}.",
                "success"
            )
            return redirect(url_for('inventory_products.inventory_products'))

        # ---------- UPDATE / DELETE ACTIONS ----------
        # Here we still use branch selection (multi-branch mass update/delete)
        selected_branches = request.form.getlist('branches')
        if not selected_branches:
            flash("❌ Please select at least one branch.", "danger")
            return redirect(url_for('inventory_products.inventory_products'))

        anchor_oid = to_oid(item_id)
        if not anchor_oid:
            flash("❌ Invalid item ID.", "danger")
            return redirect(url_for('inventory_products.inventory_products'))

        # Load the anchor item with manager info
        anchor_item = next(inventory_col.aggregate([
            {
                "$lookup": {
                    "from": "users",
                    "localField": "manager_id",
                    "foreignField": "_id",
                    "as": "manager"
                }
            },
            {"$unwind": "$manager"},
            {"$match": {"_id": anchor_oid}}
        ]), None)

        if not anchor_item:
            flash("❌ Item not found.", "danger")
            return redirect(url_for('inventory_products.inventory_products'))

        name_to_match = anchor_item['name']

        # Find items with same name within selected branches (by manager.branch)
        matched_items = list(inventory_col.aggregate([
            {
                "$lookup": {
                    "from": "users",
                    "localField": "manager_id",
                    "foreignField": "_id",
                    "as": "manager"
                }
            },
            {"$unwind": "$manager"},
            {
                "$match": {
                    "name": name_to_match,
                    "manager.branch": {"$in": selected_branches}
                }
            }
        ]))

        if action == 'update':
            try:
                new_name  = (request.form.get('name') or "").strip()
                new_price = safe_float(request.form.get('price'))
                new_qty   = safe_int(request.form.get('qty'))

                # NEW pricing fields
                new_cost_price    = safe_float(request.form.get('cost_price'))
                new_selling_price = safe_float(request.form.get('selling_price'))

                if not new_name or new_price is None or new_qty is None:
                    flash("❌ Provide valid name, legacy price, and quantity.", "danger")
                    return redirect(url_for('inventory_products.inventory_products'))

                updated_count = 0
                now = datetime.utcnow()

                for product in matched_items:
                    updates = {
                        'name': new_name,
                        'price': money2(new_price),
                        'qty': new_qty,
                        'updated_at': now
                    }

                    # read old values for logging
                    old_cost   = product.get('cost_price')
                    old_sell   = product.get('selling_price')
                    old_margin = product.get('margin')

                    # apply new modern pricing if provided (partial updates allowed)
                    if new_cost_price is not None:
                        updates['cost_price'] = money2(new_cost_price)
                    if new_selling_price is not None:
                        updates['selling_price'] = money2(new_selling_price)

                    # compute margin consistently if either cost or selling changed
                    if ('cost_price' in updates) or ('selling_price' in updates):
                        c = updates.get('cost_price', old_cost)
                        s = updates.get('selling_price', old_sell)
                        if c is not None and s is not None:
                            updates['margin'] = money2(s - c)

                    inventory_col.update_one({'_id': product['_id']}, {'$set': updates})
                    updated_count += 1

                    log_doc = {
                        'product_id': product['_id'],
                        'product_name': new_name,
                        'log_type': 'update',
                        'old_name': product.get('name'),
                        'new_name': new_name,
                        'old_price': product.get('price'),
                        'new_price': money2(new_price),
                        'old_qty': product.get('qty'),
                        'new_qty': new_qty,

                        # new pricing logs
                        'old_cost_price': old_cost,
                        'new_cost_price': updates.get('cost_price', old_cost),
                        'old_selling_price': old_sell,
                        'new_selling_price': updates.get('selling_price', old_sell),
                        'old_margin': old_margin,
                        'new_margin': updates.get('margin', old_margin),

                        'updated_by': admin_username,
                        'action': 'update',
                        'updated_at': now
                    }
                    inventory_logs_col.insert_one(log_doc)

                flash(f"✅ Product updated across {updated_count} selected branch item(s).", "success")

            except Exception as e:
                flash(f"❌ Error: {str(e)}", "danger")

        elif action == 'delete':
            deleted_count = 0
            now = datetime.utcnow()
            for product in matched_items:
                inventory_logs_col.insert_one({
                    'product_id': product['_id'],
                    'product_name': product.get('name'),
                    'price': product.get('price'),
                    'qty': product.get('qty'),
                    'cost_price': product.get('cost_price'),
                    'selling_price': product.get('selling_price'),
                    'margin': product.get('margin'),
                    'deleted_by': admin_username,
                    'action': 'delete',
                    'log_type': 'delete',
                    'deleted_at': now,
                    'updated_at': now  # keep sorting consistent
                })
                deleted_col.insert_one({
                    'deleted_item': product,
                    'deleted_by': admin_username,
                    'deleted_at': now
                })
                inventory_col.delete_one({'_id': product['_id']})
                deleted_count += 1

            flash(f"🗑️ Product deleted across {deleted_count} selected branch item(s).", "success")

        else:
            flash("❌ Unknown action.", "danger")

        return redirect(url_for('inventory_products.inventory_products'))

    # -----------------------------
    # GET logic
    # -----------------------------
    manager_query = (request.args.get('manager') or '').strip()
    branch_query  = (request.args.get('branch') or '').strip()
    product_query = (request.args.get('product') or '').strip()
    limit         = safe_int(request.args.get('limit')) or 50
    offset        = safe_int(request.args.get('offset')) or 0
    if limit <= 0:
        limit = 50
    if offset < 0:
        offset = 0
    current_page  = (offset // limit) + 1

    pipeline = [
        {
            "$lookup": {
                "from": "users",
                "localField": "manager_id",
                "foreignField": "_id",
                "as": "manager"
            }
        },
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

    # total count
    total_cursor = inventory_col.aggregate(pipeline + [{"$count": "count"}])
    total_count = next(total_cursor, {}).get("count", 0)

    # fetch page
    page_pipeline = pipeline + [
        {"$sort": {"manager.branch": 1, "name": 1}},
        {"$skip": offset},
        {"$limit": limit}
    ]
    raw_inventory = list(inventory_col.aggregate(page_pipeline))

    # Grouping when no manager/branch filter
    group_products = not manager_query and not branch_query
    if group_products:
        grouped = {}
        for item in raw_inventory:
            # Use a composite key that includes modern pricing to avoid mixing different S/C prices
            key = (
                item.get("name"),
                item.get("price"),          # legacy price
                item.get("selling_price"),
                item.get("cost_price"),
                item.get("description"),
                item.get("image_url")
            )
            if key not in grouped:
                grouped[key] = {
                    "_id": item["_id"],
                    "name": item.get("name"),
                    "price": item.get("price"),
                    "selling_price": item.get("selling_price"),
                    "cost_price": item.get("cost_price"),
                    "margin": item.get("margin"),
                    "description": item.get("description"),
                    "image_url": item.get("image_url"),
                    "qty": item.get("qty") or 0,
                    "manager": {"name": "Multiple", "branch": "All Branches"}
                }
            else:
                # Sum quantities across branches
                grouped[key]["qty"] += (item.get("qty") or 0)
        inventory = list(grouped.values())
    else:
        inventory = raw_inventory

    # Provide manager/branch options
    manager_names = sorted(users_col.distinct("name", {"role": "manager"}))
    branch_names  = sorted(users_col.distinct("branch", {"role": "manager"}))

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
    oid = to_oid(item_id)
    if not oid:
        return jsonify([])

    # Optional filter: view = transfer | updates | deletes | images | all/empty
    view = (request.args.get('view') or '').strip().lower()

    query = {'product_id': oid}
    if view == 'transfer':
        query['log_type'] = 'transfer'
    elif view == 'updates':
        query['log_type'] = 'update'
    elif view == 'deletes':
        query['log_type'] = 'delete'
    elif view == 'images':
        query['log_type'] = 'image_update'
    # else: no additional filter (all logs for this product)

    logs = list(inventory_logs_col.find(query).sort('updated_at', -1))
    for log in logs:
        log['_id'] = str(log['_id'])
        log['product_id'] = str(log.get('product_id', ''))
        ts = log.get('updated_at') or log.get('deleted_at')
        if ts:
            if isinstance(ts, datetime):
                log['updated_at'] = ts.strftime('%Y-%m-%d %H:%M:%S')
            else:
                log['updated_at'] = str(ts)
        else:
            log['updated_at'] = ""
    return jsonify(logs)

from flask import Blueprint, render_template, request, redirect, url_for, flash, session, jsonify
from bson.objectid import ObjectId, InvalidId
import re, os
from datetime import datetime, timedelta
from werkzeug.utils import secure_filename
from db import db

inventory_products_bp = Blueprint('inventory_products', __name__)

inventory_col       = db.inventory
users_col           = db.users
inventory_logs_col  = db.inventory_logs
deleted_col         = db.deleted

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

def parse_date_yyyy_mm_dd(val: str):
    """Parse YYYY-MM-DD to datetime at midnight. Return None if empty/invalid."""
    if val is None:
        return None
    s = str(val).strip()
    if s == "":
        return None
    try:
        return datetime.strptime(s, "%Y-%m-%d")
    except Exception:
        return None

def fmt_date_yyyy_mm_dd(dtval):
    if not dtval:
        return ""
    if isinstance(dtval, datetime):
        return dtval.strftime("%Y-%m-%d")
    try:
        return str(dtval)[:10]
    except Exception:
        return ""

def expiry_meta(expiry_dt: datetime | None):
    """Return flags + days_to_expiry based on UTC day boundary."""
    today0 = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    if not expiry_dt:
        return {
            "expiry_date": None,
            "expiry_date_str": None,
            "days_to_expiry": None,
            "is_expired": False,
            "expiring_soon_7": False,
            "expiring_soon_30": False,
            "expiry_priority": 4,  # no expiry -> last
        }

    exp0 = expiry_dt.replace(hour=0, minute=0, second=0, microsecond=0)
    days = (exp0 - today0).days

    is_expired = days < 0
    exp7  = (0 <= days <= 7)
    exp30 = (0 <= days <= 30)

    # priority order:
    # 0 expired, 1 expiring <=7, 2 expiring <=30, 3 valid, 4 no expiry
    if is_expired:
        pr = 0
    elif exp7:
        pr = 1
    elif exp30:
        pr = 2
    else:
        pr = 3

    return {
        "expiry_date": expiry_dt,
        "expiry_date_str": fmt_date_yyyy_mm_dd(expiry_dt),
        "days_to_expiry": days,
        "is_expired": is_expired,
        "expiring_soon_7": exp7,
        "expiring_soon_30": exp30,
        "expiry_priority": pr,
    }

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

    name_to_match = item.get('name')
    legacy_price  = item.get('price')
    selling_price = item.get('selling_price')

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
# Route: Inventory Management
# -----------------------------
@inventory_products_bp.route('/inventory_products', methods=['GET', 'POST'])
def inventory_products():
    if request.method == 'POST':
        action         = request.form.get('action')
        item_id        = request.form.get('item_id')
        admin_username = session.get('username', 'Unknown')

        # ---------- TRANSFER ----------
        if action == 'transfer':
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

            src_item = next(inventory_col.aggregate([
                {"$lookup": {"from":"users","localField":"manager_id","foreignField":"_id","as":"manager"}},
                {"$unwind":"$manager"},
                {"$match":{"_id":src_oid}}
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

            dest_item = next(inventory_col.aggregate([
                {"$lookup": {"from":"users","localField":"manager_id","foreignField":"_id","as":"manager"}},
                {"$unwind":"$manager"},
                {"$match":{"name":src_item["name"], "manager.branch":to_branch}}
            ]), None)

            if not dest_item:
                flash(f"❌ No matching product found in destination branch '{to_branch}'.", "danger")
                return redirect(url_for('inventory_products.inventory_products'))

            dest_current_qty = dest_item.get("qty") or 0

            now = datetime.utcnow()
            new_src_qty  = src_current_qty - transfer_qty
            new_dest_qty = dest_current_qty + transfer_qty

            inventory_col.update_one({"_id": src_item["_id"]}, {"$set": {"qty": new_src_qty, "updated_at": now}})
            inventory_col.update_one({"_id": dest_item["_id"]}, {"$set": {"qty": new_dest_qty, "updated_at": now}})

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

            flash(f"🔁 Transferred {transfer_qty} unit(s) of '{src_item['name']}' from {from_branch} to {to_branch}.", "success")
            return redirect(url_for('inventory_products.inventory_products'))

        # ---------- UPDATE / DELETE ----------
        selected_branches = request.form.getlist('branches')
        if not selected_branches:
            flash("❌ Please select at least one branch.", "danger")
            return redirect(url_for('inventory_products.inventory_products'))

        anchor_oid = to_oid(item_id)
        if not anchor_oid:
            flash("❌ Invalid item ID.", "danger")
            return redirect(url_for('inventory_products.inventory_products'))

        anchor_item = next(inventory_col.aggregate([
            {"$lookup": {"from":"users","localField":"manager_id","foreignField":"_id","as":"manager"}},
            {"$unwind":"$manager"},
            {"$match":{"_id":anchor_oid}}
        ]), None)

        if not anchor_item:
            flash("❌ Item not found.", "danger")
            return redirect(url_for('inventory_products.inventory_products'))

        name_to_match = anchor_item['name']

        matched_items = list(inventory_col.aggregate([
            {"$lookup": {"from":"users","localField":"manager_id","foreignField":"_id","as":"manager"}},
            {"$unwind":"$manager"},
            {"$match":{"name":name_to_match, "manager.branch":{"$in": selected_branches}}}
        ]))

        if action == 'update':
            try:
                new_name  = (request.form.get('name') or "").strip()
                new_price = safe_float(request.form.get('price'))
                new_qty   = safe_int(request.form.get('qty'))

                new_cost_price    = safe_float(request.form.get('cost_price'))
                new_selling_price = safe_float(request.form.get('selling_price'))

                # ✅ NEW: expiry from edit modal
                expiry_str = (request.form.get('expiry_date') or '').strip()
                expiry_dt  = parse_date_yyyy_mm_dd(expiry_str)

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

                    old_cost   = product.get('cost_price')
                    old_sell   = product.get('selling_price')
                    old_margin = product.get('margin')

                    # old expiry
                    old_exp_dt  = product.get('expiry_date')
                    old_exp_str = product.get('expiry_date_str') or fmt_date_yyyy_mm_dd(old_exp_dt)

                    # modern pricing partial updates
                    if new_cost_price is not None:
                        updates['cost_price'] = money2(new_cost_price)
                    if new_selling_price is not None:
                        updates['selling_price'] = money2(new_selling_price)

                    if ('cost_price' in updates) or ('selling_price' in updates):
                        c = updates.get('cost_price', old_cost)
                        s = updates.get('selling_price', old_sell)
                        if c is not None and s is not None:
                            updates['margin'] = money2(s - c)

                    # ✅ expiry updates (we always allow setting/clearing)
                    # If empty input, clear expiry
                    if expiry_str == "":
                        updates['expiry_date'] = None
                        updates['expiry_date_str'] = None
                        updates['is_expired'] = False
                        updates['expiring_soon_7'] = False
                        updates['expiring_soon_30'] = False
                    else:
                        if not expiry_dt:
                            flash("❌ Expiry date is invalid. Use YYYY-MM-DD.", "danger")
                            return redirect(url_for('inventory_products.inventory_products'))

                        meta = expiry_meta(expiry_dt)
                        updates['expiry_date'] = meta['expiry_date']
                        updates['expiry_date_str'] = meta['expiry_date_str']
                        updates['is_expired'] = meta['is_expired']
                        updates['expiring_soon_7'] = meta['expiring_soon_7']
                        updates['expiring_soon_30'] = meta['expiring_soon_30']

                    inventory_col.update_one({'_id': product['_id']}, {'$set': updates})
                    updated_count += 1

                    # logs
                    new_exp_str = updates.get('expiry_date_str', None)
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

                        'old_cost_price': old_cost,
                        'new_cost_price': updates.get('cost_price', old_cost),
                        'old_selling_price': old_sell,
                        'new_selling_price': updates.get('selling_price', old_sell),
                        'old_margin': old_margin,
                        'new_margin': updates.get('margin', old_margin),

                        # ✅ expiry log fields
                        'old_expiry_date': old_exp_str or None,
                        'new_expiry_date': new_exp_str,

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

                    # ✅ keep expiry in delete log
                    'expiry_date': product.get('expiry_date_str') or fmt_date_yyyy_mm_dd(product.get('expiry_date')) or None,

                    'deleted_by': admin_username,
                    'action': 'delete',
                    'log_type': 'delete',
                    'deleted_at': now,
                    'updated_at': now
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
    # GET logic (Sorting: expiry first)
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
        {"$lookup": {"from":"users","localField":"manager_id","foreignField":"_id","as":"manager"}},
        {"$unwind":"$manager"},
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

    # ✅ Expiry sorting via stored flags (fast), fallback if missing by computing in python
    # priority: is_expired desc, expiring_soon_7 desc, expiring_soon_30 desc, then expiry_date asc, then branch/name
    total_cursor = inventory_col.aggregate(pipeline + [{"$count":"count"}])
    total_count = next(total_cursor, {}).get("count", 0)

    page_pipeline = pipeline + [
        {"$sort": {
            "is_expired": -1,
            "expiring_soon_7": -1,
            "expiring_soon_30": -1,
            "expiry_date": 1,
            "manager.branch": 1,
            "name": 1
        }},
        {"$skip": offset},
        {"$limit": limit}
    ]
    raw_inventory = list(inventory_col.aggregate(page_pipeline))

    # normalize expiry fields + ensure flags exist (for old records)
    for it in raw_inventory:
        exp_dt = it.get("expiry_date")
        exp_str = it.get("expiry_date_str") or fmt_date_yyyy_mm_dd(exp_dt) or None
        exp_dt2 = parse_date_yyyy_mm_dd(exp_str) if (not exp_dt and exp_str) else exp_dt
        meta = expiry_meta(exp_dt2)

        it["expiry_date"] = meta["expiry_date"]
        it["expiry_date_str"] = meta["expiry_date_str"]
        it["days_to_expiry"] = meta["days_to_expiry"]
        it["is_expired"] = meta["is_expired"]
        it["expiring_soon_7"] = meta["expiring_soon_7"]
        it["expiring_soon_30"] = meta["expiring_soon_30"]
        it["expiry_priority"] = meta["expiry_priority"]

    # Grouping when no manager/branch filter
    group_products = not manager_query and not branch_query
    if group_products:
        grouped = {}
        for item in raw_inventory:
            key = (
                item.get("name"),
                item.get("price"),
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
                    "manager": {"name": "Multiple", "branch": "All Branches"},

                    # ✅ take earliest expiry across branches for grouped display
                    "expiry_date": item.get("expiry_date"),
                    "expiry_date_str": item.get("expiry_date_str"),
                    "days_to_expiry": item.get("days_to_expiry"),
                    "is_expired": item.get("is_expired", False),
                    "expiring_soon_7": item.get("expiring_soon_7", False),
                    "expiring_soon_30": item.get("expiring_soon_30", False),
                    "expiry_priority": item.get("expiry_priority", 4),
                }
            else:
                grouped[key]["qty"] += (item.get("qty") or 0)

                # update earliest expiry
                cur_dt = grouped[key].get("expiry_date")
                new_dt = item.get("expiry_date")
                if (new_dt and (not cur_dt or new_dt < cur_dt)):
                    meta = expiry_meta(new_dt)
                    grouped[key]["expiry_date"] = meta["expiry_date"]
                    grouped[key]["expiry_date_str"] = meta["expiry_date_str"]
                    grouped[key]["days_to_expiry"] = meta["days_to_expiry"]
                    grouped[key]["is_expired"] = meta["is_expired"]
                    grouped[key]["expiring_soon_7"] = meta["expiring_soon_7"]
                    grouped[key]["expiring_soon_30"] = meta["expiring_soon_30"]
                    grouped[key]["expiry_priority"] = meta["expiry_priority"]

        inventory = list(grouped.values())

        # ✅ ensure grouped list still sorted by expiry priority
        inventory.sort(key=lambda x: (
            x.get("expiry_priority", 4),
            x.get("expiry_date") or datetime(9999, 12, 31),
            x.get("name") or ""
        ))
    else:
        inventory = raw_inventory

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

import re
from datetime import datetime, timedelta
from flask import Blueprint, render_template, request, jsonify, redirect, url_for, flash, Response, session, send_from_directory
from pymongo import MongoClient
from pymongo.server_api import ServerApi
from bson import ObjectId
from db import db
import uuid
import os
from werkzeug.utils import secure_filename

view_bp = Blueprint('view', __name__)

# ✅ Fixed: Use proper uploads path
UPLOAD_FOLDER = os.path.join(os.getcwd(), 'uploads')
ALLOWED_EXTENSIONS = {'png', 'jpg', 'jpeg'}

# Ensure upload folder exists
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

customers_collection = db["customers"]
payments_collection  = db["payments"]
packages_collection  = db["packages"]   # NEW
users_collection     = db["users"]
products_collection  = db["products"]
inventory_collection = db["inventory"]
inventory_products_outflow_collection = db["inventory_products_outflow"]
inventory_products_outflow_col = db["inventory_products_outflow"]
undelivered_items_col = db["undelivered_items"]

try:
    customers_collection.create_index([("agent_id", 1), ("name", 1)])
    customers_collection.create_index([("agent_id", 1), ("phone_number", 1)])
    payments_collection.create_index([("agent_id", 1), ("date", 1)])
    payments_collection.create_index([("customer_id", 1), ("date", 1)])
except Exception:
    pass

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def _idx_or(idx):
    idx_int = int(idx)
    return [{"product_index": idx_int}, {"product_index": str(idx_int)}]

# ----------------------------
# 📄 View Customers
# ----------------------------
def _build_customer_listing(agent_id, search_query, status_filter, page, per_page):
    try:
        agent_oid = ObjectId(agent_id)
    except Exception:
        agent_oid = None

    agent_doc = None
    favorites_ids = []
    favorites_set = set()
    if agent_oid:
        agent_doc = users_collection.find_one({"_id": agent_oid}, {"favorites_customer_ids": 1})
    for fid in (agent_doc or {}).get("favorites_customer_ids", []) or []:
        try:
            oid = fid if isinstance(fid, ObjectId) else ObjectId(str(fid))
            favorites_ids.append(oid)
            favorites_set.add(str(oid))
        except Exception:
            continue

    base_match = {'agent_id': agent_id}
    all_ids = [c.get('_id') for c in customers_collection.find(base_match, {'_id': 1}) if c.get('_id')]
    total_customers = len(all_ids)

    last_dates = {}
    if all_ids:
        pipeline = [
            {'$match': {'customer_id': {'$in': all_ids}, 'payment_type': 'PRODUCT'}},
            {'$group': {'_id': '$customer_id', 'last_date': {'$max': '$date'}}}
        ]
        for row in payments_collection.aggregate(pipeline):
            last_dates[str(row['_id'])] = row.get('last_date')

    active_count = 0
    not_active_count = 0
    no_payment_count = 0
    status_by_id = {}
    cutoff = (datetime.utcnow() - timedelta(days=14)).date()

    for cid in all_ids:
        cid_str = str(cid)
        date_str = last_dates.get(cid_str)
        if not date_str:
            status = 'No Payment'
            no_payment_count += 1
        else:
            try:
                dt = datetime.strptime(date_str[:10], '%Y-%m-%d').date()
            except Exception:
                dt = None
            if not dt:
                status = 'No Payment'
                no_payment_count += 1
            elif dt < cutoff:
                status = 'Not Active'
                not_active_count += 1
            else:
                status = 'Active'
                active_count += 1
        status_by_id[cid_str] = status

    favorites_count = len(favorites_ids)

    today = datetime.today().strftime('%Y-%m-%d')
    attends_today_customer_ids = payments_collection.distinct(
        "customer_id",
        {
            "agent_id": agent_id,
            "date": today,
            "payment_type": {"$ne": "WITHDRAWAL"}
        }
    )
    attends_today_count = len(attends_today_customer_ids or [])

    total_collected_today = 0
    today_pipeline = [
        {"$match": {"agent_id": agent_id, "date": today, "payment_type": {"$ne": "WITHDRAWAL"}}},
        {"$group": {"_id": None, "sum": {"$sum": {"$toDouble": {"$ifNull": ["$amount", 0]}}}}}
    ]
    today_result = list(payments_collection.aggregate(today_pipeline))
    if today_result:
        try:
            total_collected_today = float(today_result[0].get("sum", 0))
        except Exception:
            total_collected_today = 0

    if status_filter == 'favorites':
        filtered_ids = favorites_ids
    elif status_filter == 'active':
        filtered_ids = [cid for cid in all_ids if status_by_id.get(str(cid)) == 'Active']
    elif status_filter == 'not_active':
        filtered_ids = [cid for cid in all_ids if status_by_id.get(str(cid)) == 'Not Active']
    else:
        filtered_ids = all_ids

    query = {'agent_id': agent_id}
    if status_filter in ('favorites', 'active', 'not_active'):
        query['_id'] = {'$in': filtered_ids} if filtered_ids else {'$in': []}

    if search_query:
        escaped = re.escape(search_query)
        query['$or'] = [
            {'name': {'$regex': escaped, '$options': 'i'}},
            {'phone_number': {'$regex': escaped, '$options': 'i'}}
        ]

    skip = (page - 1) * per_page
    projection = {'name': 1, 'phone_number': 1, 'image_url': 1}
    customers = list(customers_collection.find(query, projection).skip(skip).limit(per_page))

    page_ids = [c.get('_id') for c in customers if c.get('_id')]
    paid_map = {}
    if page_ids:
        pipeline = [
            {'$match': {'customer_id': {'$in': page_ids}, 'payment_type': {'$in': ['PRODUCT', 'WITHDRAWAL']}}},
            {'$group': {
                '_id': '$customer_id',
                'sum_product': {
                    '$sum': {
                        '$cond': [
                            {'$eq': ['$payment_type', 'PRODUCT']},
                            {'$toDouble': {'$ifNull': ['$amount', 0]}},
                            0
                        ]
                    }
                },
                'sum_withdrawal': {
                    '$sum': {
                        '$cond': [
                            {'$and': [
                                {'$eq': ['$payment_type', 'WITHDRAWAL']},
                                {'$ne': ['$product_index', None]}
                            ]},
                            {'$toDouble': {'$ifNull': ['$amount', 0]}},
                            0
                        ]
                    }
                }
            }}
        ]
        for row in payments_collection.aggregate(pipeline):
            paid_map[str(row['_id'])] = round(float(row.get('sum_product', 0)) - float(row.get('sum_withdrawal', 0)), 2)

    customer_data = []
    for customer in customers:
        customer_id = customer.get('_id')
        cid_str = str(customer_id)
        status = status_by_id.get(cid_str, 'No Payment')
        last_payment_date = last_dates.get(cid_str) or 'N/A'
        total_paid = paid_map.get(cid_str, 0)

        customer_data.append({
            '_id': str(customer_id),
            'name': customer.get('name', ''),
            'phone_number': customer.get('phone_number', ''),
            'image_url': customer.get('image_url', ''),
            'status': status,
            'last_payment_date': last_payment_date,
            'total_paid': total_paid,
            'is_favorite': cid_str in favorites_set
        })

    base_context = {
        'customers': customer_data,
        'search_query': search_query,
        'status_filter': status_filter,
        'page': page,
        'total_customers': total_customers,
        'active_count': active_count,
        'not_active_count': not_active_count,
        'favorites_count': favorites_count,
        'attends_today_count': attends_today_count,
        'total_collected_today': total_collected_today,
        'has_prev': page > 1,
        'has_next': len(customer_data) == per_page,
    }
    return base_context


@view_bp.route('/customers', methods=['GET'])
def view_customers():
    search_query = (request.args.get('search') or '').strip()
    status_filter = (request.args.get('status') or 'all').strip().lower()
    page = max(int(request.args.get('page', 1)), 1)
    per_page = 30

    agent_id = session.get('agent_id')
    if not agent_id:
        flash("You must be logged in to view your customers.", "error")
        return redirect(url_for('login.login'))

    context = _build_customer_listing(agent_id, search_query, status_filter, page, per_page)
    return render_template('view_customers.html', **context)

@view_bp.route('/customers/ajax', methods=['GET'])
def view_customers_ajax():
    agent_id = session.get('agent_id')
    if not agent_id:
        return jsonify(ok=False, message="Unauthorized"), 401

    search_query = (request.args.get('search') or '').strip()
    status_filter = (request.args.get('status') or 'all').strip().lower()
    page = max(int(request.args.get('page', 1)), 1)
    per_page = 30

    context = _build_customer_listing(agent_id, search_query, status_filter, page, per_page)
    stats = {
        'total_customers': context['total_customers'],
        'active_count': context['active_count'],
        'not_active_count': context['not_active_count'],
        'favorites_count': context['favorites_count'],
        'attends_today_count': context['attends_today_count'],
        'total_collected_today': context['total_collected_today'],
    }
    return jsonify(
        ok=True,
        customers=context['customers'],
        page=context['page'],
        has_prev=context['has_prev'],
        has_next=context['has_next'],
        status_filter=context['status_filter'],
        search_query=context['search_query'],
        stats=stats
    )


@view_bp.route('/customers/<customer_id>/favorite', methods=['POST'])
def toggle_customer_favorite(customer_id):
    agent_id = session.get('agent_id')
    if not agent_id:
        return jsonify(ok=False, message='Unauthorized'), 401

    try:
        agent_oid = ObjectId(agent_id)
        customer_oid = ObjectId(customer_id)
    except Exception:
        return jsonify(ok=False, message='Invalid id'), 400

    action = (request.form.get('action') or (request.get_json(silent=True) or {}).get('action') or '').lower()
    user = users_collection.find_one({"_id": agent_oid}, {"favorites_customer_ids": 1})
    favorites = [str(x) for x in (user or {}).get('favorites_customer_ids', [])]
    is_fav = str(customer_oid) in favorites

    if action == 'add' or (action == '' and not is_fav):
        users_collection.update_one({"_id": agent_oid}, {"$addToSet": {"favorites_customer_ids": customer_oid}})
        return jsonify(ok=True, is_favorite=True)
    if action == 'remove' or (action == '' and is_fav):
        users_collection.update_one({"_id": agent_oid}, {"$pull": {"favorites_customer_ids": customer_oid}})
        return jsonify(ok=True, is_favorite=False)

    return jsonify(ok=True, is_favorite=is_fav)


@view_bp.route('/customers/<customer_id>/favorite/toggle', methods=['POST'])
def toggle_customer_favorite_toggle(customer_id):
    agent_id = session.get('agent_id')
    if not agent_id:
        return jsonify(ok=False, message='Unauthorized'), 401

    try:
        agent_oid = ObjectId(agent_id)
        customer_oid = ObjectId(customer_id)
    except Exception:
        return jsonify(ok=False, message='Invalid id'), 400

    user = users_collection.find_one({"_id": agent_oid}, {"favorites_customer_ids": 1})
    favorites = [str(x) for x in (user or {}).get('favorites_customer_ids', [])]
    is_fav = str(customer_oid) in favorites

    if is_fav:
        users_collection.update_one({"_id": agent_oid}, {"$pull": {"favorites_customer_ids": customer_oid}})
        return jsonify(ok=True, is_favorite=False, message="Removed from favorites")

    users_collection.update_one({"_id": agent_oid}, {"$addToSet": {"favorites_customer_ids": customer_oid}})
    return jsonify(ok=True, is_favorite=True, message="Added to favorites")

# ----------------------------
# 👤 View Customer Profile
# ----------------------------
@view_bp.route('/customer/<customer_id>', methods=['GET'])
def view_customer_profile(customer_id):
    try:
        customer_obj_id = ObjectId(customer_id)
    except Exception:
        return jsonify({'error': 'Invalid customer ID format'}), 400

    customer = customers_collection.find_one({'_id': customer_obj_id})
    if not customer:
        return jsonify({'error': 'Customer not found'}), 404

    payments = list(payments_collection.find({'customer_id': customer_obj_id}))
    agent_doc = None
    manager_id = None
    agent_id = session.get("agent_id")
    if agent_id:
        try:
            agent_oid = ObjectId(agent_id)
            agent_doc = users_collection.find_one({"_id": agent_oid}, {"manager_id": 1})
            manager_id = agent_doc.get("manager_id") if agent_doc else None
            if manager_id and not isinstance(manager_id, ObjectId):
                manager_id = ObjectId(str(manager_id))
        except Exception:
            agent_doc = None
            manager_id = None

    def _classify_susu_withdraw(p):
        if p.get("payment_type") != "WITHDRAWAL":
            return None
        method_raw = (p.get("method") or "").strip().lower()
        note_raw = (p.get("note") or "").strip().lower()
        is_cash = method_raw in ("susu withdrawal", "manual", "cash", "withdrawal", "susu cash")
        is_profit = method_raw in ("susu profit", "deduction", "susu deduction")
        if "susu" in note_raw:
            if "profit" in note_raw or "deduction" in note_raw:
                is_profit = True
            if "withdraw" in note_raw or "cash" in note_raw or "payout" in note_raw:
                is_cash = True
        if is_cash:
            return "cash"
        if is_profit:
            return "profit"
        return None

    product_deposits = [p for p in payments if p.get("payment_type") not in ("WITHDRAWAL", "SUSU")]
    withdrawals = [p for p in payments if p.get("payment_type") == "WITHDRAWAL"]
    product_withdrawals = [p for p in withdrawals if _classify_susu_withdraw(p) is None]

    susu_payments = [p for p in payments if p.get("payment_type") == "SUSU"]
    susu_withdrawals = [p for p in withdrawals if _classify_susu_withdraw(p) is not None]
    susu_withdraw_cash = sum(p.get("amount", 0) for p in susu_withdrawals if _classify_susu_withdraw(p) == "cash")
    susu_profit = sum(p.get("amount", 0) for p in susu_withdrawals if _classify_susu_withdraw(p) == "profit")

    total_debt = sum(p.get('product', {}).get('total', 0) for p in customer.get('purchases', []))
    deposits_sum = sum(p.get("amount", 0) for p in product_deposits)
    withdrawn_amount = sum(p.get("amount", 0) for p in product_withdrawals)
    total_paid = round(deposits_sum - withdrawn_amount, 2)
    amount_left = round(total_debt - total_paid, 2)

    susu_total = round(sum(p.get("amount", 0) for p in susu_payments), 2)
    susu_withdrawn = round(susu_withdraw_cash + susu_profit, 2)
    susu_left = round(susu_total - susu_withdrawn, 2)

    current_status = customer.get("status", "payment_ongoing")
    if current_status == "payment_ongoing" and amount_left <= 0:
        customers_collection.update_one(
            {'_id': customer_obj_id},
            {'$set': {
                'status': 'completed',
                'status_updated_at': datetime.utcnow()
            }}
        )
        current_status = "completed"

    pending_by_index = {}
    try:
        for r in undelivered_items_col.find(
            {"customer_id": customer_obj_id, "status": "pending"},
            {"product_index": 1}
        ):
            pending_by_index[int(r.get("product_index", -1))] = True
    except Exception:
        pending_by_index = {}

    # Decorate purchases with per-product paid/left & time progress
    for index, purchase in enumerate(customer.get("purchases", [])):
        product = purchase.get("product") or {}
        if "status" not in product or not product.get("status"):
            product["status"] = "active"
        purchase["product"] = product

        purchase_date = purchase.get("purchase_date")
        tracking = calculate_progress(purchase_date)
        purchase["progress"] = tracking["progress"]
        purchase["end_date"] = tracking["end_date"]

        product_total = float(purchase.get("product", {}).get("total", 0))
        product_payments = [
            p for p in product_deposits
            if str(p.get("product_index")) == str(index)
        ]
        product_paid = sum(p.get("amount", 0) for p in product_payments)
        product_left = max(0, round(product_total - product_paid, 2))

        purchase["amount_paid"]  = product_paid
        purchase["amount_left"]  = product_left
        purchase_status = purchase.get("product", {}).get("status")
        purchase["can_submit"] = (product_left == 0 and purchase_status in ("active", "completed"))
        if purchase_status == "closed":
            purchase["can_submit"] = False
        purchase["pending_undelivered"] = bool(pending_by_index.get(index))
        purchase_qty = int((purchase.get("product") or {}).get("quantity") or 1)
        product_def = _resolve_product_def(purchase, manager_id) if manager_id else None
        purchase["components_catalog"] = _build_component_catalog(product_def, purchase_qty, manager_id)

    customer["status"] = current_status
    customer["_id"] = str(customer["_id"])
    penalties = customer.get("penalties", [])

    return render_template(
        'customer_profile.html',
        customer=customer,
        total_debt=total_debt,
        total_paid=total_paid,
        amount_left=amount_left,
        payments=product_deposits,
        withdrawals=withdrawals,
        penalties=penalties,
        withdrawn_amount=withdrawn_amount,
        susu_total=susu_total,
        susu_withdrawn=susu_withdrawn,
        susu_left=susu_left,
        susu_payments=susu_payments,
        susu_withdrawals=susu_withdrawals
    )

# ----------------------------
# 🆕 Submit product for Packaging (non-destructive)
# ----------------------------
@view_bp.route('/customer/<customer_id>/submit_for_packaging/<int:product_index>', methods=['POST'])
def submit_for_packaging(customer_id, product_index):
    """
    Preconditions:
      - agent must be logged in (session['agent_id'])
      - product's amount_left must be 0 (fully paid)
    Actions:
      - mark the product as completed in customer.purchases
      - insert into packages collection (with agent_id, customer snapshot, product)
    """
    now_utc = datetime.utcnow()
    agent_id = session.get('agent_id')
    if not agent_id:
        flash("Please log in as an agent.", "danger")
        return redirect(url_for('login.login'))

    try:
        customer_obj_id = ObjectId(customer_id)
    except Exception:
        flash("Invalid customer ID format.", "danger")
        return redirect(url_for('view.view_customer_profile', customer_id=customer_id))

    customer = customers_collection.find_one({'_id': customer_obj_id})
    if not customer:
        flash("Customer not found.", "danger")
        return redirect(url_for('view.view_customer_profile', customer_id=customer_id))

    purchases = customer.get("purchases", [])
    if product_index < 0 or product_index >= len(purchases):
        flash("Invalid product selection.", "warning")
        return redirect(url_for('view.view_customer_profile', customer_id=customer_id))

    purchase = purchases[product_index]
    product_info = purchase.get("product", {})
    product_status = product_info.get("status") or "active"
    if product_status in ("packaged", "delivered", "completed", "closed") or product_info.get("transfer_status") == "transferred_out":
        flash("This product is already submitted for packaging.", "warning")
        return redirect(url_for('view.view_customer_profile', customer_id=customer_id))

    product_total = float(product_info.get("total", 0))

    # Recompute pay/left for safety
    idx_or = _idx_or(product_index)
    product_deposits = list(payments_collection.find({
        'customer_id': customer_obj_id,
        'payment_type': {'$ne': 'WITHDRAWAL'},
        '$or': idx_or
    }))
    product_withdrawals = list(payments_collection.find({
        'customer_id': customer_obj_id,
        'payment_type': 'WITHDRAWAL',
        '$or': idx_or
    }))

    paid_sum = sum(p.get("amount", 0) for p in product_deposits) - sum(p.get("amount", 0) for p in product_withdrawals)
    amount_left = max(0, round(product_total - paid_sum, 2))

    if amount_left != 0:
        flash("This product is not fully paid yet.", "warning")
        return redirect(url_for('view.view_customer_profile', customer_id=customer_id))

    agent = users_collection.find_one({"_id": ObjectId(agent_id)}, {"branch": 1, "name": 1, "manager_id": 1})
    manager_id = agent.get("manager_id") if agent else None
    if manager_id and not isinstance(manager_id, ObjectId):
        try:
            manager_id = ObjectId(str(manager_id))
        except Exception:
            manager_id = None

    purchase_qty = int(product_info.get("quantity") or 1)
    product_def = _resolve_product_def(purchase, manager_id)
    components_deducted = _deduct_components_silent(product_def, purchase_qty, manager_id, agent_id)

    # Update customer product status
    customers_collection.update_one(
        {'_id': customer_obj_id},
        {'$set': {
            f'purchases.{product_index}.product.status': 'completed',
            f'purchases.{product_index}.status': 'completed',
            f'purchases.{product_index}.completed_at': now_utc,
            f'purchases.{product_index}.submitted_for_packaging_at': now_utc,
            f'purchases.{product_index}.submitted_for_packaging_by': agent_id,
            f'purchases.{product_index}.submitted_for_packaging_by_role': 'agent',
            'updated_at': now_utc
        }}
    )

    # Silent inventory outflow audit (best-effort)
    try:
        product_def_id = product_def.get("_id") if product_def else None
        product_def_name = product_def.get("name") if product_def else None
        product_def_snapshot = None
        if product_def:
            product_def_snapshot = {
                "name": product_def.get("name"),
                "price": product_def.get("price"),
                "cash_price": product_def.get("cash_price"),
                "cost_price": product_def.get("cost_price"),
                "image_url": product_def.get("image_url"),
                "cf_image_id": product_def.get("cf_image_id"),
                "product_type": product_def.get("product_type"),
                "category": product_def.get("category"),
                "package_name": product_def.get("package_name"),
                "components": product_def.get("components") or []
            }

        profit_snapshot = _compute_profit_snapshot(product_def or product_info, purchase_qty, "installment")
        outflow_doc = {
            "created_at": now_utc,
            "source": "Agent_deliveries",
            "customer_id": customer_obj_id,
            "customer_name": customer.get("name"),
            "customer_phone": customer.get("phone_number"),
            "packaged_product_index": product_index,
            "packaged_product": product_info,
            "package_qty": purchase_qty,
            "total_paid_selected_product": paid_sum,
            "product_total": product_total,
            "agent_id": agent_id,
            "agent_name": agent.get("name") if agent else None,
            "agent_branch": agent.get("branch") if agent else None,
            "manager_id": manager_id,
            "package_def_id": str(product_def_id) if product_def_id else None,
            "package_def_name": product_def_name,
            "components_deducted": components_deducted or [],
            "components_status": "no_components" if not components_deducted else "deducted",
            "product_def": product_def_snapshot,
            "profit_amount": profit_snapshot.get("unit_profit", 0.0),
            **profit_snapshot,
            "by_user": agent_id,
            "by_role": "agent"
        }
        inventory_products_outflow_col.insert_one(outflow_doc)
    except Exception:
        pass

    flash("Submitted for packaging successfully.", "success")
    return redirect(url_for('view.view_customer_profile', customer_id=customer_id))

# ----------------------------
# dY"? Update Status (existing)
# ----------------------------
@view_bp.route('/customer/<customer_id>/update_status/<next_status>', methods=['POST'])
def agent_update_status(customer_id, next_status):
    try:
        customer_obj_id = ObjectId(customer_id)
    except:
        flash("Invalid customer ID format.", "danger")
        return redirect(url_for('view.view_customer_profile', customer_id=customer_id))

    allowed_transitions = {
        "approved": "packaging",
        "packaging": "delivering",
        "delivering": "delivered"
    }

    customer = customers_collection.find_one({'_id': customer_obj_id})
    if not customer:
        flash("Customer not found.", "danger")
        return redirect(url_for('view.view_customer_profile', customer_id=customer_id))

    current_status = customer.get("status", "payment_ongoing")
    if allowed_transitions.get(current_status) != next_status:
        flash("Invalid status transition.", "warning")
        return redirect(url_for('view.view_customer_profile', customer_id=customer_id))

    customers_collection.update_one(
        {'_id': customer_obj_id},
        {'$set': {
            'status': next_status,
            'status_updated_at': datetime.utcnow()
        }}
    )
    flash(f"Customer status updated to '{next_status}'.", "success")
    return redirect(url_for('view.view_customer_profile', customer_id=customer_id))

# ----------------------------
# 🖼️ Upload Customer Image
# ----------------------------
@view_bp.route('/customer/<customer_id>/upload_image', methods=['POST'])
def upload_customer_image(customer_id):
    try:
        customer_obj_id = ObjectId(customer_id)

        image = request.files.get('image')
        if not image or not allowed_file(image.filename):
            return jsonify({'error': 'Invalid or missing image'}), 400

        filename = f"{uuid.uuid4().hex}_{secure_filename(image.filename)}"
        image_path = os.path.join(UPLOAD_FOLDER, filename)
        image.save(image_path)

        image_url = f"/uploads/{filename}"

        result = customers_collection.update_one(
            {'_id': customer_obj_id},
            {'$set': {'image_url': image_url}}
        )

        if result.modified_count == 0:
            return jsonify({'error': 'Image not updated'}), 500

        return jsonify({'success': True, 'image_url': image_url})

    except Exception as e:
        print("Upload error:", e)
        return jsonify({'error': str(e)}), 500

# ✅ Route to Serve Uploaded Images
@view_bp.route('/uploads/<filename>')
def serve_uploaded_file(filename):
    return send_from_directory(UPLOAD_FOLDER, filename)

# ----------------------------
# 📍 Update Customer Location
# ----------------------------
@view_bp.route('/customer/<customer_id>/update_location', methods=['POST'])
def update_customer_location(customer_id):
    try:
        customer_obj_id = ObjectId(customer_id)
        data = request.get_json()
        lat = float(data.get("latitude"))
        lon = float(data.get("longitude"))

        customers_collection.update_one(
            {'_id': customer_obj_id},
            {'$set': {
                'coordinates.latitude': lat,
                'coordinates.longitude': lon
            }}
        )
        return jsonify({"success": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 400

# ----------------------------
# 🧮 Helper: Progress Tracker
# ----------------------------
def calculate_progress(purchase_date_str):
    try:
        purchase_date = datetime.strptime(purchase_date_str, "%Y-%m-%d")
    except (ValueError, TypeError):
        return {"progress": 0, "end_date": "N/A"}

    end_date = purchase_date + timedelta(days=180)
    today = datetime.now()

    total_days = (end_date - purchase_date).days
    elapsed_days = (today - purchase_date).days

    progress = max(0, min(100, round((elapsed_days / total_days) * 100))) if total_days > 0 else 0

    return {
        "progress": progress,
        "end_date": end_date.strftime("%Y-%m-%d")
    }


def _resolve_product_def(purchase, manager_id):
    product = purchase.get("product", {}) or {}
    cf_image_id = product.get("cf_image_id")
    name = product.get("name")

    if not manager_id:
        return None

    flt = {"manager_id": manager_id}
    if cf_image_id:
        flt["cf_image_id"] = cf_image_id
        prod = products_collection.find_one(flt, sort=[("created_at", -1)])
        if prod:
            return prod

    if name:
        flt = {"manager_id": manager_id, "name": name}
        return products_collection.find_one(flt, sort=[("created_at", -1)])

    return None


def _to_float(value):
    try:
        return float(value)
    except Exception:
        return 0.0


def _clamp_non_negative(value):
    return max(0.0, _to_float(value))


def _compute_profit_snapshot(product_doc, qty, mode):
    doc = product_doc or {}
    unit_cost = _clamp_non_negative(doc.get("cost_price"))
    unit_selling = _clamp_non_negative(doc.get("price"))
    unit_cash = _clamp_non_negative(doc.get("cash_price"))
    unit_profit_price = _clamp_non_negative(doc.get("profit_price"))
    unit_profit_cash = _clamp_non_negative(doc.get("profit_cash"))

    if unit_profit_price <= 0 and unit_cost and unit_selling:
        unit_profit_price = _clamp_non_negative(unit_selling - unit_cost)
    if unit_profit_cash <= 0 and unit_cost and unit_cash:
        unit_profit_cash = _clamp_non_negative(unit_cash - unit_cost)

    qty_val = int(qty or 0)
    profit_type = "installment" if mode == "installment" else "cash"
    unit_profit = unit_profit_price if profit_type == "installment" else unit_profit_cash
    unit_profit = _clamp_non_negative(unit_profit)
    total_profit = _clamp_non_negative(unit_profit * qty_val)

    # Example: {"profit_type":"installment","unit_profit":50.0,"total_profit":100.0}
    return {
        "unit_cost_price": unit_cost,
        "unit_selling_price": unit_selling,
        "unit_cash_price": unit_cash,
        "unit_profit_price": unit_profit_price,
        "unit_profit_cash": unit_profit_cash,
        "profit_type": profit_type,
        "unit_profit": unit_profit,
        "total_profit": total_profit,
    }


def _build_component_catalog(product_def, purchase_qty, manager_id):
    if not product_def:
        return []
    items = []
    for comp in (product_def.get("components") or []):
        comp_id = comp.get("_id")
        if not comp_id:
            continue
        try:
            required_qty = int(comp.get("quantity", 1)) * int(purchase_qty or 1)
        except Exception:
            required_qty = int(purchase_qty or 1)

        inv_match = {"_id": comp_id}
        if manager_id:
            inv_match["$or"] = [{"manager_id": manager_id}, {"manager_id": str(manager_id)}]
        inv_doc = inventory_collection.find_one(inv_match, {"name": 1, "image_url": 1})
        items.append({
            "inventory_id": str(comp_id),
            "name": (inv_doc or {}).get("name") or "Unknown item",
            "image_url": (inv_doc or {}).get("image_url"),
            "required_qty": required_qty
        })
    return items


def _deduct_components_silent(product_def, purchase_qty, manager_id, agent_id):
    if not product_def:
        return []

    components = product_def.get("components") or []
    deductions = []
    for comp in components:
        try:
            comp_id = comp.get("_id")
            required_qty = int(comp.get("quantity", 1)) * int(purchase_qty or 1)
            inv_match = {"_id": comp_id}
            if manager_id:
                inv_match["$or"] = [{"manager_id": manager_id}, {"manager_id": str(manager_id)}]
            inv_doc = inventory_collection.find_one(inv_match)
            if not inv_doc:
                deductions.append({
                    "inventory_id": comp_id,
                    "inventory_name": None,
                    "required_qty": required_qty,
                    "qty_before": None,
                    "qty_after": None,
                    "status": "missing_inventory"
                })
                continue

            qty_before = inv_doc.get("qty", 0)
            try:
                qty_before = float(qty_before)
            except Exception:
                qty_before = 0
            inventory_collection.update_one(
                inv_match,
                {"$inc": {"qty": -required_qty}}
            )
            deductions.append({
                "inventory_id": comp_id,
                "inventory_name": inv_doc.get("name"),
                "required_qty": required_qty,
                "qty_before": qty_before,
                "qty_after": qty_before - required_qty,
                "inventory_snapshot": inv_doc,
                "status": "deducted"
            })
        except Exception:
            deductions.append({
                "inventory_id": comp.get("_id"),
                "inventory_name": None,
                "required_qty": int(comp.get("quantity", 1)) * int(purchase_qty or 1),
                "qty_before": None,
                "qty_after": None,
                "status": "error"
            })
    return deductions

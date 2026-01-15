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
customer_change_history_collection = db["customer_change_history"]

try:
    customers_collection.create_index([("agent_id", 1), ("name", 1)])
    customers_collection.create_index([("agent_id", 1), ("phone_number", 1)])
    payments_collection.create_index([("agent_id", 1), ("date", 1)])
    payments_collection.create_index([("customer_id", 1), ("date", 1)])
    packages_collection.create_index([("manager_id", 1), ("created_at", -1)])
    inventory_products_outflow_col.create_index([("manager_id", 1), ("created_at", -1)])
    inventory_products_outflow_col.create_index([("customer_id", 1), ("packaged_product_index", 1)])
    customer_change_history_collection.create_index([("customer_id", 1), ("changed_at", -1)])
except Exception:
    pass

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def _idx_or(idx):
    idx_int = int(idx)
    return [{"product_index": idx_int}, {"product_index": str(idx_int)}]


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


def _sum_payments(customer_obj_id, extra_match):
    match = {"customer_id": customer_obj_id}
    match.update(extra_match or {})
    pipeline = [
        {"$match": match},
        {"$group": {"_id": None, "sum": {"$sum": {"$toDouble": {"$ifNull": ["$amount", 0]}}}}}
    ]
    result = list(payments_collection.aggregate(pipeline))
    if result:
        try:
            return float(result[0].get("sum", 0))
        except Exception:
            return 0.0
    return 0.0


def _manager_id_from_session():
    agent_id = session.get("agent_id")
    manager_id = session.get("manager_id")
    if agent_id:
        try:
            agent_oid = ObjectId(agent_id)
            agent_doc = users_collection.find_one({"_id": agent_oid}, {"manager_id": 1})
            manager_id = agent_doc.get("manager_id") if agent_doc else None
            if manager_id and not isinstance(manager_id, ObjectId):
                manager_id = ObjectId(str(manager_id))
        except Exception:
            manager_id = None
    elif manager_id:
        try:
            manager_id = ObjectId(str(manager_id))
        except Exception:
            manager_id = None
    return manager_id

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

    customer = customers_collection.find_one(
        {'_id': customer_obj_id},
        {
            "name": 1,
            "phone_number": 1,
            "location": 1,
            "occupation": 1,
            "comment": 1,
            "agent_name": 1,
            "agent_branch": 1,
            "coordinates": 1,
            "status": 1,
            "image_url": 1,
            "penalties": {"$slice": 1},
            "purchases.product.total": 1
        }
    )
    if not customer:
        return jsonify({'error': 'Customer not found'}), 404

    total_debt = sum(_to_float(p.get('product', {}).get('total', 0)) for p in customer.get('purchases', []))
    has_penalties = bool(customer.get("penalties"))
    customer.pop("penalties", None)
    customer.pop("purchases", None)

    deposits_sum = _sum_payments(customer_obj_id, {"payment_type": {"$nin": ["WITHDRAWAL", "SUSU"]}})
    withdrawn_amount = _sum_payments(customer_obj_id, {"payment_type": "WITHDRAWAL", "product_index": {"$ne": None}})
    total_paid = round(deposits_sum - withdrawn_amount, 2)
    amount_left = round(total_debt - total_paid, 2)

    susu_total = round(_sum_payments(customer_obj_id, {"payment_type": "SUSU"}), 2)
    susu_withdrawn = round(_sum_payments(
        customer_obj_id,
        {
            "payment_type": "WITHDRAWAL",
            "$or": [
                {"method": {"$regex": "susu", "$options": "i"}},
                {"note": {"$regex": "susu", "$options": "i"}}
            ]
        }
    ), 2)
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

    customer["status"] = current_status
    customer["_id"] = str(customer["_id"])

    return render_template(
        'customer_profile.html',
        customer=customer,
        total_debt=total_debt,
        total_paid=total_paid,
        amount_left=amount_left,
        susu_total=susu_total,
        susu_withdrawn=susu_withdrawn,
        susu_left=susu_left,
        has_penalties=has_penalties
    )

# ----------------------------
# Lazy-loaded Customer Tabs
# ----------------------------
@view_bp.route('/customer/<customer_id>/tab/payments', methods=['GET'])
def customer_tab_payments(customer_id):
    if not (session.get("agent_id") or session.get("manager_id")):
        return "Unauthorized", 401

    try:
        customer_obj_id = ObjectId(customer_id)
    except Exception:
        return "Invalid customer ID format", 400

    payments = list(payments_collection.find(
        {
            "customer_id": customer_obj_id,
            "payment_type": {"$nin": ["WITHDRAWAL", "SUSU"]}
        },
        {"amount": 1, "method": 1, "payment_type": 1, "note": 1, "date": 1}
    ).sort("date", -1).limit(300))

    return render_template(
        "partials/customer_tabs/payments.html",
        payments=payments,
        showing_limit=True
    )


@view_bp.route('/customer/<customer_id>/tab/withdrawals', methods=['GET'])
def customer_tab_withdrawals(customer_id):
    if not (session.get("agent_id") or session.get("manager_id")):
        return "Unauthorized", 401

    try:
        customer_obj_id = ObjectId(customer_id)
    except Exception:
        return "Invalid customer ID format", 400

    withdrawals = list(payments_collection.find(
        {
            "customer_id": customer_obj_id,
            "payment_type": "WITHDRAWAL"
        },
        {"amount": 1, "method": 1, "note": 1, "date": 1}
    ).sort("date", -1).limit(300))

    return render_template(
        "partials/customer_tabs/withdrawals.html",
        withdrawals=withdrawals,
        showing_limit=True
    )


@view_bp.route('/customer/<customer_id>/tab/products', methods=['GET'])
def customer_tab_products(customer_id):
    if not (session.get("agent_id") or session.get("manager_id")):
        return "Unauthorized", 401

    try:
        customer_obj_id = ObjectId(customer_id)
    except Exception:
        return "Invalid customer ID format", 400

    customer = customers_collection.find_one(
        {"_id": customer_obj_id},
        {"purchases": 1}
    )
    if not customer:
        return "Customer not found", 404

    product_deposits = list(payments_collection.find(
        {
            "customer_id": customer_obj_id,
            "payment_type": {"$nin": ["WITHDRAWAL", "SUSU"]}
        },
        {"amount": 1, "product_index": 1}
    ))
    product_withdrawals = list(payments_collection.find(
        {
            "customer_id": customer_obj_id,
            "payment_type": "WITHDRAWAL",
            "product_index": {"$ne": None}
        },
        {"amount": 1, "product_index": 1}
    ))

    pending_by_index = {}
    try:
        for r in undelivered_items_col.find(
            {"customer_id": customer_obj_id, "status": "pending"},
            {"product_index": 1}
        ):
            pending_by_index[int(r.get("product_index", -1))] = True
    except Exception:
        pending_by_index = {}

    manager_id = _manager_id_from_session()

    for index, purchase in enumerate(customer.get("purchases", [])):
        product = purchase.get("product") or {}
        if "status" not in product or not product.get("status"):
            product["status"] = "active"
        purchase["product"] = product

        purchase_date = purchase.get("purchase_date")
        tracking = calculate_progress(purchase_date)
        purchase["progress"] = tracking["progress"]
        purchase["end_date"] = tracking["end_date"]

        product_total = _to_float(purchase.get("product", {}).get("total", 0))
        product_payments = [
            p for p in product_deposits
            if str(p.get("product_index")) == str(index)
        ]
        product_withdraw = [
            p for p in product_withdrawals
            if str(p.get("product_index")) == str(index)
        ]
        product_paid = sum(_to_float(p.get("amount", 0)) for p in product_payments) - sum(_to_float(p.get("amount", 0)) for p in product_withdraw)
        product_left = max(0, round(product_total - product_paid, 2))

        purchase["amount_paid"] = product_paid
        purchase["amount_left"] = product_left
        purchase_status = purchase.get("product", {}).get("status")
        purchase["can_submit"] = (product_left == 0 and purchase_status in ("active", "completed"))
        if purchase_status == "closed":
            purchase["can_submit"] = False
        purchase["pending_undelivered"] = bool(pending_by_index.get(index))

        purchase_qty = int((purchase.get("product") or {}).get("quantity") or 1)
        product_def = _resolve_product_def(purchase, manager_id) if manager_id else None
        purchase["components_catalog"] = _build_component_catalog(product_def, purchase_qty, manager_id)

    return render_template(
        "partials/customer_tabs/products.html",
        customer_id=str(customer_obj_id),
        purchases=customer.get("purchases", [])
    )


@view_bp.route('/customer/<customer_id>/tab/susu', methods=['GET'])
def customer_tab_susu(customer_id):
    if not (session.get("agent_id") or session.get("manager_id")):
        return "Unauthorized", 401

    try:
        customer_obj_id = ObjectId(customer_id)
    except Exception:
        return "Invalid customer ID format", 400

    susu_payments = list(payments_collection.find(
        {
            "customer_id": customer_obj_id,
            "payment_type": "SUSU"
        },
        {"amount": 1, "method": 1, "note": 1, "date": 1}
    ).sort("date", -1).limit(300))

    withdrawals = list(payments_collection.find(
        {
            "customer_id": customer_obj_id,
            "payment_type": "WITHDRAWAL"
        },
        {"amount": 1, "method": 1, "note": 1, "date": 1}
    ).sort("date", -1).limit(300))

    susu_withdrawals = [p for p in withdrawals if _classify_susu_withdraw(p) is not None]
    susu_withdraw_cash = sum(_to_float(p.get("amount", 0)) for p in susu_withdrawals if _classify_susu_withdraw(p) == "cash")
    susu_profit = sum(_to_float(p.get("amount", 0)) for p in susu_withdrawals if _classify_susu_withdraw(p) == "profit")

    susu_total = round(sum(_to_float(p.get("amount", 0)) for p in susu_payments), 2)
    susu_withdrawn = round(susu_withdraw_cash + susu_profit, 2)
    susu_left = round(susu_total - susu_withdrawn, 2)

    return render_template(
        "partials/customer_tabs/susu.html",
        susu_payments=susu_payments,
        susu_withdrawals=susu_withdrawals,
        susu_total=susu_total,
        susu_withdrawn=susu_withdrawn,
        susu_left=susu_left,
        showing_limit=True
    )


@view_bp.route('/customer/<customer_id>/tab/penalties', methods=['GET'])
def customer_tab_penalties(customer_id):
    if not (session.get("agent_id") or session.get("manager_id")):
        return "Unauthorized", 401

    try:
        customer_obj_id = ObjectId(customer_id)
    except Exception:
        return "Invalid customer ID format", 400

    customer = customers_collection.find_one(
        {"_id": customer_obj_id},
        {"penalties": 1}
    )
    if not customer:
        return "Customer not found", 404

    penalties = customer.get("penalties", []) or []
    return render_template(
        "partials/customer_tabs/penalties.html",
        penalties=penalties
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
    wants_json = request.headers.get("X-Requested-With") == "XMLHttpRequest" or "application/json" in (request.headers.get("Accept") or "")
    agent_id = session.get('agent_id')
    manager_id = session.get('manager_id')
    if agent_id:
        actor_id = agent_id
        actor_role = "agent"
    elif manager_id:
        actor_id = manager_id
        actor_role = "manager"
    else:
        if wants_json:
            return jsonify(ok=False, message="Unauthorized"), 401
        flash("You are not authorized to submit for packaging.", "danger")
        return redirect(url_for('login.login'))

    def _error(message, status=400, category="danger"):
        if wants_json:
            return jsonify(ok=False, message=message), status
        flash(message, category)
        return redirect(url_for('view.view_customer_profile', customer_id=customer_id))

    try:
        customer_obj_id = ObjectId(customer_id)
    except Exception:
        return _error("Invalid customer ID format.", 400, "danger")

    try:
        customer = customers_collection.find_one({'_id': customer_obj_id})
        if not customer:
            return _error("Customer not found.", 404, "danger")

        purchases = customer.get("purchases", [])
        if product_index < 0 or product_index >= len(purchases):
            return _error("Invalid product selection.", 400, "warning")

        purchase = purchases[product_index]
        product_info = purchase.get("product", {})
        product_status = product_info.get("status") or "active"
        if purchase.get("submitted_for_packaging_at") or product_status in ("packaged", "delivered", "closed") or product_info.get("transfer_status") == "transferred_out":
            return _error("This product is already submitted for packaging.", 409, "warning")

        product_total = _to_float(product_info.get("total", 0))

        # Recompute pay/left for safety
        idx_or = _idx_or(product_index)
        product_deposits = list(payments_collection.find({
            'customer_id': customer_obj_id,
            'payment_type': {'$nin': ['WITHDRAWAL', 'SUSU']},
            '$or': idx_or
        }))
        product_withdrawals = list(payments_collection.find({
            'customer_id': customer_obj_id,
            'payment_type': 'WITHDRAWAL',
            '$or': idx_or
        }))

        paid_sum = sum(_to_float(p.get("amount", 0)) for p in product_deposits) - sum(_to_float(p.get("amount", 0)) for p in product_withdrawals)
        amount_left = max(0.0, round(product_total - paid_sum, 2))

        if amount_left > 0:
            return _error("This product is not fully paid yet.", 400, "warning")

        actor_oid = None
        try:
            actor_oid = ObjectId(str(actor_id))
        except Exception:
            actor_oid = None

        actor_doc = users_collection.find_one({"_id": actor_oid}, {"branch": 1, "name": 1, "manager_id": 1}) if actor_oid else None
        manager_id = None
        if actor_role == "agent":
            manager_id = (actor_doc or {}).get("manager_id")
            if manager_id and not isinstance(manager_id, ObjectId):
                try:
                    manager_id = ObjectId(str(manager_id))
                except Exception:
                    manager_id = None
        else:
            manager_id = actor_oid

        print(
            "submit_for_packaging",
            {
                "customer_id": str(customer_obj_id),
                "product_index": product_index,
                "product_total": product_total,
                "paid_sum": paid_sum,
                "amount_left": amount_left,
                "actor_role": actor_role,
                "actor_id": actor_id
            }
        )

        purchase_qty = int(product_info.get("quantity") or 1)
        product_def = _resolve_product_def(purchase, manager_id)
        components_deducted = _deduct_components_silent(product_def, purchase_qty, manager_id, actor_id)

        existing_package = packages_collection.find_one({
            "customer_id": customer_obj_id,
            "product_index": product_index,
            "status": {"$ne": "cancelled"}
        })
        if existing_package:
            return _error("This product is already in packaging queue.", 409, "warning")

        package_doc = {
            "created_at": now_utc,
            "status": "pending",
            "customer_id": customer_obj_id,
            "customer_name": customer.get("name"),
            "customer_phone": customer.get("phone_number"),
            "product_index": product_index,
            "product": product_info,
            "purchase_type": purchase.get("purchase_type"),
            "qty": purchase_qty,
            "product_total": product_total,
            "total_paid_selected_product": paid_sum,
            "agent_id": actor_id,
            "by_role": actor_role,
            "agent_name": (actor_doc or {}).get("name"),
            "agent_branch": (actor_doc or {}).get("branch"),
            "manager_id": manager_id,
            "source": "customer_profile_submit"
        }
        try:
            package_insert = packages_collection.insert_one(package_doc)
            print("package_inserted_id", str(package_insert.inserted_id))
        except Exception as e:
            print("Package insert error:", e)
            return _error("Failed to submit for packaging. Please try again.", 500, "danger")

        # Update customer product status
        customers_collection.update_one(
            {'_id': customer_obj_id},
            {'$set': {
                f'purchases.{product_index}.product.status': 'submitted_for_packaging',
                f'purchases.{product_index}.product.packaging_status': 'pending',
                f'purchases.{product_index}.status': 'submitted_for_packaging',
                f'purchases.{product_index}.submitted_for_packaging_at': now_utc,
                f'purchases.{product_index}.submitted_for_packaging_by': actor_id,
                f'purchases.{product_index}.submitted_for_packaging_by_role': actor_role,
                'updated_at': now_utc
            }}
        )

        # Inventory outflow audit (best-effort)
        outflow_failed = False
        outflow_insert_id = None
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
                "agent_id": actor_id,
                "agent_name": (actor_doc or {}).get("name"),
                "agent_branch": (actor_doc or {}).get("branch"),
                "manager_id": manager_id,
                "package_def_id": str(product_def_id) if product_def_id else None,
                "package_def_name": product_def_name,
                "components_deducted": components_deducted or [],
                "components_status": "no_components" if not components_deducted else "deducted",
                "product_def": product_def_snapshot,
                "profit_amount": profit_snapshot.get("unit_profit", 0.0),
                **profit_snapshot,
                "by_user": actor_id,
                "by_role": actor_role
            }
            outflow_insert = inventory_products_outflow_col.insert_one(outflow_doc)
            outflow_insert_id = outflow_insert.inserted_id
            print("outflow_inserted_id", str(outflow_insert.inserted_id))
        except Exception as e:
            outflow_failed = True
            print("Inventory outflow insert error:", e)

        if wants_json:
            return jsonify(
                ok=True,
                message="Submitted for packaging.",
                new_status="submitted_for_packaging",
                package_id=str(package_insert.inserted_id),
                outflow_written=not outflow_failed,
                outflow_id=str(outflow_insert_id) if outflow_insert_id else None
            ), 200

        flash("Submitted for packaging successfully.", "success")
        if outflow_failed:
            flash("Submitted, but inventory outflow audit failed - check server logs.", "warning")
        return redirect(url_for('view.view_customer_profile', customer_id=customer_id))
    except Exception as e:
        print("Submit for packaging error:", e)
        if wants_json:
            return jsonify(ok=False, message="Something went wrong while submitting for packaging."), 500
        flash("Something went wrong while submitting for packaging.", "danger")
        return redirect(url_for('view.view_customer_profile', customer_id=customer_id))
# ----------------------------
# Update Customer Details + Change History
# ----------------------------
@view_bp.route('/customer/<customer_id>/update_details', methods=['POST'])
def update_customer_details(customer_id):
    agent_id = session.get("agent_id")
    manager_id = session.get("manager_id")
    if agent_id:
        actor_id = agent_id
        actor_role = "agent"
    elif manager_id:
        actor_id = manager_id
        actor_role = "manager"
    else:
        return jsonify(ok=False, message="Unauthorized"), 401

    try:
        customer_obj_id = ObjectId(customer_id)
    except Exception:
        return jsonify(ok=False, message="Invalid customer ID format"), 400

    customer = customers_collection.find_one({"_id": customer_obj_id})
    if not customer:
        return jsonify(ok=False, message="Customer not found"), 404

    data = request.get_json(silent=True) or {}
    allowed_fields = ["name", "phone_number", "location", "occupation", "comment"]
    if not any(field in data for field in allowed_fields):
        return jsonify(ok=False, message="No fields provided"), 400

    before_snapshot = {field: customer.get(field) for field in allowed_fields}
    updates = {}
    changes = {}
    for field in allowed_fields:
        if field not in data:
            continue
        new_value = data.get(field)
        old_value = customer.get(field)
        if new_value is None or new_value == old_value:
            continue
        updates[field] = new_value
        changes[field] = {"from": old_value, "to": new_value}

    if not updates:
        return jsonify(ok=False, message="No changes detected"), 400

    after_snapshot = dict(before_snapshot)
    after_snapshot.update(updates)

    history_doc = {
        "customer_id": customer_obj_id,
        "changed_at": datetime.utcnow(),
        "changed_by": actor_id,
        "changed_by_role": actor_role,
        "changes": changes,
        "before": before_snapshot,
        "after": after_snapshot
    }
    try:
        customer_change_history_collection.insert_one(history_doc)
    except Exception as e:
        print("Change history insert error:", e)

    customers_collection.update_one(
        {"_id": customer_obj_id},
        {"$set": {**updates, "updated_at": datetime.utcnow()}}
    )

    return jsonify(ok=True, message="Customer updated", changes_count=len(changes))


@view_bp.route('/customer/<customer_id>/change_history', methods=['GET'])
def get_customer_change_history(customer_id):
    agent_id = session.get("agent_id")
    manager_id = session.get("manager_id")
    if not (agent_id or manager_id):
        return jsonify(ok=False, message="Unauthorized"), 401

    try:
        customer_obj_id = ObjectId(customer_id)
    except Exception:
        return jsonify(ok=False, message="Invalid customer ID format"), 400

    history = []
    try:
        cursor = customer_change_history_collection.find(
            {"customer_id": customer_obj_id}
        ).sort("changed_at", -1)
        for row in cursor:
            changed_at = row.get("changed_at")
            history.append({
                "changed_at": changed_at.isoformat() if isinstance(changed_at, datetime) else str(changed_at),
                "changed_by": str(row.get("changed_by") or ""),
                "changed_by_role": row.get("changed_by_role") or "",
                "changes": row.get("changes") or {}
            })
    except Exception as e:
        print("Change history fetch error:", e)
        return jsonify(ok=False, message="Failed to fetch change history"), 500

    return jsonify(ok=True, history=history)

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
 

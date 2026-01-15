from flask import Blueprint, render_template, request, flash, redirect, url_for, session, jsonify
from bson.objectid import ObjectId
from datetime import datetime
import uuid

from db import db

users_col = db.users
customers_col = db.customers
payments_col = db.payments
product_transfers_col = db.product_transfers
undelivered_items_col = db.undelivered_items
transfer_logs_col = db.transfer_logs

transfer_bp = Blueprint('transfer', __name__)

try:
    users_col.create_index([("role", 1), ("name", 1)], background=True)
    users_col.create_index([("role", 1), ("phone", 1)], background=True)
    users_col.create_index([("role", 1), ("manager_id", 1)], background=True)
    users_col.create_index([("role", 1), ("date_registered", -1)], background=True)
except Exception:
    pass


def _manager_id_from_session():
    manager_id = session.get('manager_id')
    if not manager_id:
        return None
    try:
        return ObjectId(str(manager_id))
    except Exception:
        return None


def _require_transfer_user():
    manager_id_raw = session.get('manager_id')
    admin_id_raw = session.get('admin_id')
    executive_id_raw = session.get('executive_id')
    role_raw = (session.get('role') or '').strip().lower()
    user_id_raw = session.get('user_id')
    if not (manager_id_raw or admin_id_raw or executive_id_raw or (role_raw and user_id_raw)):
        return None

    role = "manager"
    actor_id = manager_id_raw or (user_id_raw if role_raw == "manager" else None)
    manager_oid = None
    if admin_id_raw or executive_id_raw:
        role = "admin"
        actor_id = admin_id_raw or executive_id_raw
    elif role_raw in ("admin", "executive"):
        role = "admin"
        actor_id = user_id_raw
    else:
        try:
            manager_oid = ObjectId(str(manager_id_raw or user_id_raw))
        except Exception:
            manager_oid = None

    return {
        "role": role,
        "actor_id": actor_id,
        "manager_oid": manager_oid,
    }


def _manager_agent_ids(manager_id):
    agents = users_col.find({'manager_id': manager_id, 'role': 'agent'}, {'_id': 1})
    return [str(a['_id']) for a in agents]


def _customer_scope_filter(manager_id):
    agent_ids = _manager_agent_ids(manager_id)
    return {
        '$or': [
            {'manager_id': manager_id},
            {'manager_id': str(manager_id)},
            {'agent_id': {'$in': agent_ids}}
        ]
    }


def _get_customer_in_scope(customer_id, manager_id):
    try:
        cust_oid = ObjectId(str(customer_id))
    except Exception:
        return None
    flt = {'_id': cust_oid}
    flt.update(_customer_scope_filter(manager_id))
    return customers_col.find_one(flt)


def _idx_or(idx):
    idx_int = int(idx)
    return [{'product_index': idx_int}, {'product_index': str(idx_int)}]


def _calc_paid(customer_oid, product_index):
    idx_or = _idx_or(product_index)
    deposits = payments_col.find({
        'customer_id': customer_oid,
        'payment_type': 'PRODUCT',
        '$or': idx_or
    })
    withdrawals = payments_col.find({
        'customer_id': customer_oid,
        'payment_type': 'WITHDRAWAL',
        '$or': idx_or
    })
    paid = sum(float(p.get('amount', 0)) for p in deposits) - sum(float(p.get('amount', 0)) for p in withdrawals)
    return round(paid, 2)


def _now_strings():
    now_utc = datetime.utcnow()
    return now_utc, now_utc.strftime('%Y-%m-%d'), now_utc.strftime('%H:%M:%S')


@transfer_bp.route('/transfer', methods=['GET', 'POST'])
def transfer_view():
    auth = _require_transfer_user()
    if not auth:
        return redirect(url_for('login.login'))

    is_admin = auth["role"] == "admin"
    manager_oid = auth["manager_oid"]
    back_url = url_for('login.admin_dashboard') if is_admin else url_for('login.agent_list')

    manager_filter = {'role': 'manager'}
    if manager_oid and not is_admin:
        manager_filter['_id'] = {'$ne': manager_oid}
    managers = list(users_col.find(manager_filter, {"name": 1, "username": 1, "branch": 1}).sort("branch", 1))

    return render_template('transfer.html', managers=managers, back_url=back_url)


@transfer_bp.route('/transfer/search_agents', methods=['GET'])
def transfer_search_agents():
    auth = _require_transfer_user()
    if not auth:
        return jsonify(ok=False, message='Unauthorized'), 401

    name = (request.args.get('name') or '').strip()
    phone = (request.args.get('phone') or '').strip()
    per_page = 10
    try:
        page = max(1, int(request.args.get('page', 1)))
    except Exception:
        page = 1

    flt = {"role": "agent"}
    if auth["manager_oid"] and auth["role"] != "admin":
        flt["manager_id"] = auth["manager_oid"]

    and_filters = []
    if name:
        and_filters.append({"name": {"$regex": name, "$options": "i"}})
    if phone:
        and_filters.append({"phone": {"$regex": phone, "$options": "i"}})
    if and_filters:
        flt["$and"] = and_filters

    total_count = users_col.count_documents(flt)
    total_pages = max(1, (total_count + per_page - 1) // per_page)
    if page > total_pages:
        page = total_pages

    skip = (page - 1) * per_page
    projection = {
        "name": 1,
        "phone": 1,
        "email": 1,
        "image_url": 1,
        "status": 1,
        "branch": 1,
        "manager_id": 1,
    }
    agents = list(
        users_col.find(flt, projection)
        .sort("date_registered", -1)
        .skip(skip)
        .limit(per_page)
    )
    results = [
        {
            "_id": str(a["_id"]),
            "name": a.get("name"),
            "phone": a.get("phone"),
            "email": a.get("email"),
            "image_url": a.get("image_url"),
            "status": a.get("status"),
            "branch": a.get("branch"),
            "manager_id": str(a.get("manager_id")) if a.get("manager_id") else None
        }
        for a in agents
    ]
    return jsonify(
        ok=True,
        results=results,
        page=page,
        per_page=per_page,
        total_count=total_count,
        total_pages=total_pages
    )


@transfer_bp.route('/transfer/submit', methods=['POST'])
def transfer_submit_agent():
    auth = _require_transfer_user()
    if not auth:
        return jsonify(ok=False, message='Unauthorized'), 401

    data = request.get_json(silent=True) or request.form
    agent_id = (data.get('agent_id') or '').strip()
    new_manager_id = (data.get('new_manager_id') or '').strip()
    if not agent_id or not new_manager_id:
        return jsonify(ok=False, message='Agent and new manager are required.'), 400

    try:
        agent_oid = ObjectId(agent_id)
        new_manager_oid = ObjectId(new_manager_id)
    except Exception:
        return jsonify(ok=False, message='Invalid IDs provided.'), 400

    agent_filter = {'_id': agent_oid, 'role': 'agent'}
    if auth["manager_oid"] and auth["role"] != "admin":
        agent_filter['manager_id'] = auth["manager_oid"]
    agent = users_col.find_one(agent_filter)
    if not agent:
        return jsonify(ok=False, message='Agent not found or unauthorized.'), 404

    new_manager = users_col.find_one({'_id': new_manager_oid, 'role': 'manager'})
    if not new_manager:
        return jsonify(ok=False, message='Target manager not found.'), 404

    if str(agent.get('manager_id')) == str(new_manager_oid):
        return jsonify(ok=False, message='Agent already under this manager.'), 409

    users_col.update_one(
        {'_id': agent_oid},
        {'$set': {'manager_id': new_manager_oid, 'branch': new_manager.get('branch'), 'updated_at': datetime.utcnow()}}
    )
    transfer_logs_col.insert_one({
        'agent_id': agent_oid,
        'old_manager_id': agent.get('manager_id'),
        'new_manager_id': new_manager_oid,
        'from_branch': agent.get('branch'),
        'to_branch': new_manager.get('branch'),
        'transferred_by': ObjectId(str(auth["actor_id"])) if auth["actor_id"] else None,
        'transferred_by_role': auth["role"],
        'at': datetime.utcnow()
    })

    return jsonify(ok=True, message="Transferred", agent_id=str(agent_oid), new_manager_id=str(new_manager_oid))


@transfer_bp.route('/manager/transfers/new', methods=['GET'])
def manager_transfer_new():
    manager_id = _manager_id_from_session()
    if not manager_id:
        return redirect(url_for('login.login'))
    return render_template('manager_product_transfer.html')


@transfer_bp.route('/manager/transfers/customers', methods=['GET'])
def manager_transfer_customers():
    manager_id = _manager_id_from_session()
    if not manager_id:
        return jsonify(ok=False, message='Unauthorized'), 401

    q = (request.args.get('q') or '').strip()
    scope = _customer_scope_filter(manager_id)
    if q:
        flt = {
            '$and': [
                scope,
                {'$or': [
                    {'name': {'$regex': q, '$options': 'i'}},
                    {'phone_number': {'$regex': q, '$options': 'i'}},
                ]}
            ]
        }
    else:
        flt = scope

    customers = list(
        customers_col.find(flt, {'name': 1, 'phone_number': 1}).sort('name', 1).limit(5)
    )
    payload = [
        {
            'id': str(c['_id']),
            'name': c.get('name', 'Unknown'),
            'phone': c.get('phone_number', '')
        }
        for c in customers
    ]
    return jsonify(ok=True, customers=payload)


@transfer_bp.route('/manager/transfers/customer_products', methods=['GET'])
def manager_transfer_customer_products():
    manager_id = _manager_id_from_session()
    if not manager_id:
        return jsonify(ok=False, message='Unauthorized'), 401

    customer_id = request.args.get('customer_id')
    customer = _get_customer_in_scope(customer_id, manager_id)
    if not customer:
        return jsonify(ok=False, message='Customer not found'), 404

    purchases = customer.get('purchases', [])
    items = []
    for idx, purchase in enumerate(purchases):
        product = purchase.get('product') or {}
        paid = _calc_paid(customer['_id'], idx)
        total = float(product.get('total', 0) or 0)
        left = max(0, round(total - paid, 2))
        items.append({
            'index': idx,
            'name': product.get('name', 'Unnamed'),
            'total': total,
            'paid': paid,
            'left': left,
            'status': product.get('status') or 'active',
            'transfer_status': product.get('transfer_status')
        })

    return jsonify(ok=True, products=items)


@transfer_bp.route('/manager/transfers/product_balance', methods=['GET'])
def manager_transfer_product_balance():
    manager_id = _manager_id_from_session()
    if not manager_id:
        return jsonify(ok=False, message='Unauthorized'), 401

    customer_id = request.args.get('customer_id')
    product_index = request.args.get('product_index')
    if customer_id is None or product_index is None:
        return jsonify(ok=False, message='Missing customer or product'), 400

    customer = _get_customer_in_scope(customer_id, manager_id)
    if not customer:
        return jsonify(ok=False, message='Customer not found'), 404

    try:
        idx = int(product_index)
    except Exception:
        return jsonify(ok=False, message='Invalid product index'), 400

    purchases = customer.get('purchases', [])
    if idx < 0 or idx >= len(purchases):
        return jsonify(ok=False, message='Product not found'), 404

    product = purchases[idx].get('product') or {}
    total = float(product.get('total', 0) or 0)
    paid = _calc_paid(customer['_id'], idx)
    left = max(0, round(total - paid, 2))

    return jsonify(
        ok=True,
        paid=paid,
        total=total,
        left=left,
        status=product.get('status') or 'active',
        transfer_status=product.get('transfer_status')
    )


@transfer_bp.route('/manager/transfers/execute', methods=['POST'])
def manager_transfer_execute():
    manager_id = _manager_id_from_session()
    if not manager_id:
        return jsonify(ok=False, message='Unauthorized'), 401

    form = request.form
    from_customer_id = form.get('from_customer_id')
    to_customer_id = form.get('to_customer_id')
    from_index = form.get('from_product_index')
    to_index = form.get('to_product_index')
    transfer_amount_raw = form.get('transfer_amount')
    transfer_id = form.get('transfer_id') or uuid.uuid4().hex[:10]

    if not all([from_customer_id, to_customer_id, from_index, to_index, transfer_amount_raw]):
        return jsonify(ok=False, message='Missing required fields'), 400

    if from_customer_id == to_customer_id and str(from_index) == str(to_index):
        return jsonify(ok=False, message='From and To products must be different'), 400

    try:
        from_idx = int(from_index)
        to_idx = int(to_index)
        transfer_amount = float(transfer_amount_raw)
    except Exception:
        return jsonify(ok=False, message='Invalid numeric values'), 400

    if transfer_amount < 20:
        return jsonify(ok=False, message='Transfer amount must be at least GHS 20.'), 400

    if product_transfers_col.find_one({'transfer_id': transfer_id, 'status': 'success'}):
        return jsonify(ok=False, message='Transfer already processed'), 409

    from_customer = _get_customer_in_scope(from_customer_id, manager_id)
    to_customer = _get_customer_in_scope(to_customer_id, manager_id)
    if not from_customer or not to_customer:
        return jsonify(ok=False, message='Customer not found in scope'), 404

    from_purchases = from_customer.get('purchases', [])
    to_purchases = to_customer.get('purchases', [])
    if from_idx < 0 or from_idx >= len(from_purchases) or to_idx < 0 or to_idx >= len(to_purchases):
        return jsonify(ok=False, message='Product index out of range'), 404

    from_product = from_purchases[from_idx].get('product') or {}
    to_product = to_purchases[to_idx].get('product') or {}

    from_status = from_product.get('status') or 'active'
    to_status = to_product.get('status') or 'active'
    from_transfer_status = from_product.get('transfer_status')

    if from_status in ('packaged', 'delivered', 'closed') or from_transfer_status == 'transferred_out':
        return jsonify(ok=False, message='Source product cannot be transferred'), 409

    if to_status in ('packaged', 'delivered', 'closed') or to_product.get('transfer_status') == 'transferred_out':
        return jsonify(ok=False, message='Destination product cannot receive transfer'), 409

    pending_undelivered = undelivered_items_col.find_one({
        'customer_id': from_customer.get('_id'),
        'product_index': from_idx,
        'status': 'pending'
    })
    if pending_undelivered:
        return jsonify(ok=False, message='Source product has pending undelivered items'), 409

    from_paid = _calc_paid(from_customer['_id'], from_idx)
    if from_paid <= 0:
        return jsonify(ok=False, message='Cannot transfer: amount paid not found.'), 409
    if transfer_amount >= round(from_paid, 2):
        return jsonify(ok=False, message=f'Transfer amount must be less than the amount paid (GHS {from_paid:.2f}).'), 409

    now_utc, date_str, time_str = _now_strings()
    from_name = from_product.get('name') or 'product'
    to_name = to_product.get('name') or 'product'
    note = f"Transfer of {from_name} to {to_name}"
    transfer_doc = {
        'transfer_id': transfer_id,
        'manager_id': manager_id,
        'created_at': now_utc,
        'from_customer_id': from_customer.get('_id'),
        'from_customer_name': from_customer.get('name'),
        'from_product_index': from_idx,
        'from_product_name': from_product.get('name'),
        'to_customer_id': to_customer.get('_id'),
        'to_customer_name': to_customer.get('name'),
        'to_product_index': to_idx,
        'to_product_name': to_product.get('name'),
        'transfer_amount_gross': transfer_amount,
        'company_fee_amount': 0,
        'net_amount': transfer_amount,
        'note': note,
        'status': 'initiated'
    }

    transfer_id_db = product_transfers_col.insert_one(transfer_doc).inserted_id

    try:
        payments_col.insert_one({
            'customer_id': from_customer.get('_id'),
            'manager_id': manager_id,
            'agent_id': str(manager_id),
            'payment_type': 'WITHDRAWAL',
            'product_index': from_idx,
            'amount': transfer_amount,
            'method': 'TRANSFER_OUT',
            'note': f"Transfer to {to_customer.get('name')} / {to_product.get('name')} (ID: {transfer_id})",
            'date': date_str,
            'time': time_str,
            'created_at': now_utc
        })

        payments_col.insert_one({
            'customer_id': to_customer.get('_id'),
            'manager_id': manager_id,
            'agent_id': str(manager_id),
            'payment_type': 'PRODUCT',
            'product_index': to_idx,
            'amount': transfer_amount,
            'method': 'TRANSFER_IN',
            'product_name': to_product.get('name'),
            'product_total': float(to_product.get('total', 0) or 0),
            'note': f"Transfer from {from_customer.get('name')} / {from_product.get('name')} (ID: {transfer_id})",
            'date': date_str,
            'time': time_str,
            'created_at': now_utc
        })

        customers_col.update_one(
            {'_id': from_customer.get('_id')},
            {
                '$inc': {f'purchases.{from_idx}.transfer_out_total': transfer_amount},
                '$set': {
                    f'purchases.{from_idx}.product.transfer_status': 'transferred_out',
                    f'purchases.{from_idx}.last_transfer_id': transfer_id,
                    f'purchases.{from_idx}.last_transfer_at': now_utc
                }
            }
        )

        customers_col.update_one(
            {'_id': to_customer.get('_id')},
            {
                '$inc': {f'purchases.{to_idx}.transfer_in_total': transfer_amount},
                '$set': {
                    f'purchases.{to_idx}.product.transfer_status': 'received_transfer',
                    f'purchases.{to_idx}.last_transfer_id': transfer_id,
                    f'purchases.{to_idx}.last_transfer_at': now_utc
                }
            }
        )

        product_transfers_col.update_one({'_id': transfer_id_db}, {'$set': {'status': 'success'}})
        return jsonify(ok=True, transfer_id=transfer_id, redirect_to=f"/view/customer/{to_customer.get('_id')}")
    except Exception as e:
        product_transfers_col.update_one({'_id': transfer_id_db}, {'$set': {'status': 'failed', 'error': str(e)}})
        return jsonify(ok=False, message='Transfer failed. Please retry.'), 500


@transfer_bp.route('/manager/transfers/history', methods=['GET'])
def manager_transfer_history():
    manager_id = _manager_id_from_session()
    if not manager_id:
        return redirect(url_for('login.login'))

    q = (request.args.get('q') or '').strip()
    status = (request.args.get('status') or '').strip()
    start = (request.args.get('start') or '').strip()
    end = (request.args.get('end') or '').strip()

    flt = {'manager_id': manager_id}
    if status:
        flt['status'] = status

    if q:
        flt['$or'] = [
            {'transfer_id': {'$regex': q, '$options': 'i'}},
            {'from_customer_name': {'$regex': q, '$options': 'i'}},
            {'to_customer_name': {'$regex': q, '$options': 'i'}},
            {'from_product_name': {'$regex': q, '$options': 'i'}},
            {'to_product_name': {'$regex': q, '$options': 'i'}},
        ]

    date_range = {}
    if start:
        try:
            date_range['$gte'] = datetime.strptime(start, '%Y-%m-%d')
        except Exception:
            pass
    if end:
        try:
            date_range['$lte'] = datetime.strptime(end, '%Y-%m-%d').replace(hour=23, minute=59, second=59)
        except Exception:
            pass
    if date_range:
        flt['created_at'] = date_range

    transfers = list(product_transfers_col.find(flt).sort('created_at', -1).limit(300))

    return render_template(
        'manager_product_transfer_history.html',
        transfers=transfers,
        q=q,
        status=status,
        start=start,
        end=end
    )

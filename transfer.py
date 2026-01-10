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

transfer_bp = Blueprint('transfer', __name__)


def _manager_id_from_session():
    manager_id = session.get('manager_id')
    if not manager_id:
        return None
    try:
        return ObjectId(str(manager_id))
    except Exception:
        return None


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
    if 'manager_id' not in session:
        return redirect(url_for('login.login'))

    try:
        current_manager_id = ObjectId(session['manager_id'])
    except Exception:
        flash("Invalid session manager ID.", "error")
        return redirect(url_for('login.logout'))

    agents = list(users_col.find({'manager_id': current_manager_id, 'role': 'agent'}))
    managers = list(users_col.find({'role': 'manager', '_id': {'$ne': current_manager_id}}))

    if request.method == 'POST':
        transfer_type = request.form.get('transfer_type')

        if transfer_type == 'agent':
            agent_id = request.form['agent_id']
            new_manager_id = request.form['new_manager_id']

            try:
                agent = users_col.find_one({'_id': ObjectId(agent_id), 'manager_id': current_manager_id})
                new_manager = users_col.find_one({'_id': ObjectId(new_manager_id), 'role': 'manager'})

                if not agent:
                    flash("Agent not found or unauthorized.", "error")
                elif not new_manager:
                    flash("Target manager not found.", "error")
                else:
                    users_col.update_one({'_id': ObjectId(agent_id)}, {'$set': {'manager_id': ObjectId(new_manager_id)}})
                    flash(f"Agent successfully transferred to {new_manager['name']}.", "success")
                    return redirect(url_for('login.agent_list'))
            except Exception as e:
                flash(f"Error transferring agent: {str(e)}", "error")

        elif transfer_type == 'customer':
            source_agent_id = request.form['source_agent_id']
            target_agent_id = request.form['target_agent_id']

            try:
                customers_col.update_many({'agent_id': source_agent_id}, {'$set': {'agent_id': target_agent_id}})

                customers = customers_col.find({'agent_id': target_agent_id})
                for customer in customers:
                    updated = False
                    purchases = customer.get('purchases', [])
                    for purchase in purchases:
                        if purchase.get('agent_id') == source_agent_id:
                            purchase['agent_id'] = target_agent_id
                            updated = True
                    if updated:
                        customers_col.update_one({'_id': customer['_id']}, {'$set': {'purchases': purchases}})

                flash("Customers successfully transferred to the new agent.", "success")
            except Exception as e:
                flash(f"Error transferring customers: {str(e)}", "error")

    return render_template('transfer.html', agents=agents, managers=managers)


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

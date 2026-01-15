from flask import Blueprint, render_template, request, jsonify, url_for
from pymongo import MongoClient
from pymongo.server_api import ServerApi
from bson import ObjectId
from flask_login import login_required, current_user
from datetime import datetime
from dateutil.relativedelta import relativedelta
import uuid
from db import db

sell_bp = Blueprint('sell', __name__)

# Collections
customers_collection = db["customers"]
inventory_collection = db["products"]
users_collection = db["users"]
instant_sales_collection = db["instant_sales"]
commissions_collection = db["commissions"]
inventory_outflow_collection = db["inventory_products_outflow"]


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

    # Example: {"profit_type":"cash","unit_profit":35.0,"total_profit":70.0}
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

# GET: Sell Product Page
@sell_bp.route('/', methods=['GET'])
@login_required
def sell_product_page():
    user = users_collection.find_one({'_id': ObjectId(current_user.id)})
    if not user:
        return "Unauthorized", 403

    role = (user.get("role") or "").lower()
    is_manager = role == "manager"

    if is_manager:
        manager_id = user["_id"]
        customers = []
    else:
        if 'manager_id' not in user:
            return "Unauthorized", 403
        manager_id = user['manager_id']
        customers = list(customers_collection.find(
            {'agent_id': str(current_user.id)},
            {'name': 1, 'phone_number': 1}
        ))

    products = list(inventory_collection.find({
        '$or': [
            {'manager_id': ObjectId(manager_id)},
            {'manager_id': str(manager_id)}
        ]
    }))

    return render_template(
        'sell_product.html',
        customers=customers,
        products=products,
        agent_id=current_user.id,
        is_manager=is_manager,
        preselect_customer_id=request.args.get('customer_id')
    )

# POST: Process Sale
@sell_bp.route('/', methods=['POST'])
@login_required
def sell_product():
    try:
        data = request.get_json()
        product_data = data.get('product') or {}
        purchase_type = data.get('purchase_type')
        purchase_date_str = data.get('purchase_date')
        purchase_date = datetime.strptime(purchase_date_str, "%Y-%m-%d")
        agent_id = str(current_user.id)

        user = users_collection.find_one({'_id': ObjectId(agent_id)})
        if not user:
            return jsonify({'error': 'Unauthorized user'}), 403

        role = (user.get("role") or "").lower()
        is_manager = role == "manager"
        manager_id = user["_id"] if is_manager else user.get("manager_id")
        if not manager_id:
            return jsonify({'error': 'Unauthorized user'}), 403

        # Enforce quantity = 1 on backend
        product_data['quantity'] = 1
        if product_data.get('price') is not None:
            try:
                product_data['total'] = float(product_data.get('price', 0)) * 1
            except Exception:
                pass

        if purchase_type == 'Instant Sale':
            customer_name = data.get('customer_name')
            customer_phone = data.get('customer_phone')
            customer_location = data.get('customer_location')
            payment_method = data.get('payment_method')

            if not all([customer_name, customer_phone, customer_location, payment_method]):
                return jsonify({'error': 'Missing required fields'}), 400

            product_data['_id'] = data.get('product').get('_id')
            product_data['status'] = 'sold'
            product_data['sold_by'] = agent_id
            purchase_id = str(uuid.uuid4()).split('-')[0].upper()
            full_product = inventory_collection.find_one({'_id': ObjectId(product_data['_id'])})
            selected_qty = int(product_data.get("quantity") or 0)
            unit_price = None
            if purchase_type == "Instant Sale":
                unit_price = (full_product or {}).get("cash_price")
                if unit_price is None:
                    unit_price = (full_product or {}).get("price")
            else:
                unit_price = (full_product or {}).get("price")
                if unit_price is None:
                    unit_price = (full_product or {}).get("cash_price")
            unit_price = _clamp_non_negative(unit_price)
            product_data["price"] = unit_price
            product_data["total"] = _clamp_non_negative(unit_price * selected_qty)

            # Save instant sale
            sale = {
                'agent_id': agent_id,
                'manager_id': manager_id,
                'product': product_data,
                'customer_name': customer_name,
                'customer_phone': customer_phone,
                'customer_location': customer_location,
                'payment_method': payment_method,
                'purchase_date': purchase_date_str,
                'purchase_id': purchase_id
            }
            instant_sales_collection.insert_one(sale)

            # Update inventory
            inventory_collection.update_one(
                {'_id': ObjectId(product_data['_id'])},
                {'$inc': {'qty': -int(product_data['quantity'])}}
            )

            # Record inventory outflow
            profit_snapshot = _compute_profit_snapshot(full_product or product_data, selected_qty, "cash")
            inventory_outflow_collection.insert_one({
                "created_at": datetime.utcnow(),
                "source": "instant_sale",
                "customer_id": None,
                "customer_name": customer_name,
                "customer_phone": customer_phone,
                "customer_location": customer_location,
                "payment_method": payment_method,
                "purchase_id": purchase_id,
                "purchase_date": purchase_date_str,
                "selected_product_id": product_data["_id"],
                "selected_product": full_product or product_data,
                "selected_qty": selected_qty,
                "selected_total_price": float(product_data.get("total") or 0),
                **profit_snapshot,
                "agent_id": agent_id,
                "agent_name": user.get("name") or user.get("username"),
                "agent_branch": user.get("branch"),
                "by_user": agent_id,
                "by_role": "manager" if is_manager else "agent",
                "manager_id": str(manager_id)
            })

            # Add commission record (no receipt)
            if not is_manager:
                commissions_collection.insert_one({
                    'agent_id': agent_id,
                    'manager_id': manager_id,
                    'product_id': product_data['_id'],
                    'product_name': product_data['name'],
                    'quantity': product_data['quantity'],
                    'total_price': product_data['total'],
                    'purchase_id': purchase_id,
                    'commission_amount': 0,
                    'status': 'pending',
                    'timestamp': datetime.utcnow()
                })

            msg = 'Instant sale completed, commission recorded!'
            if is_manager:
                msg = 'Instant sale completed!'
            return jsonify({
                'message': msg,
                'sale_id': purchase_id,
                'customer_id': None,
                'customer_name': customer_name,
                'redirect_url': None
            })

        else:
            if is_manager:
                return jsonify({'error': 'Managers can only do Instant Sale.'}), 403
            # Layaway purchase
            customer_id = data.get('customer_id')
            try:
                end_term_months = int(data.get('end_term_months') or 0)
            except Exception:
                end_term_months = 0
            if end_term_months <= 0:
                end_term_months = 6
            if end_term_months > 36:
                return jsonify({'error': 'End term months too large.'}), 400
            end_date = purchase_date + relativedelta(months=end_term_months)
            customer = customers_collection.find_one({'_id': ObjectId(customer_id), 'agent_id': agent_id})

            if not customer:
                return jsonify({'error': 'Customer not found'}), 403

            product_data['status'] = 'payment_ongoing'
            purchase = {
                'agent_id': agent_id,
                'product': product_data,
                'purchase_type': purchase_type,
                'purchase_date': purchase_date_str,
                'end_date': end_date.strftime('%Y-%m-%d'),
                'end_term_months': end_term_months
            }

            customers_collection.update_one(
                {'_id': ObjectId(customer_id)},
                {'$push': {'purchases': purchase}}
            )

            product_index = len(customer.get('purchases', []))
            redirect_url = url_for('payment.add_payment', customer_id=customer_id, product_index=product_index)

            return jsonify({
                'message': 'Product assigned successfully',
                'profile_url': url_for('view.view_customer_profile', customer_id=customer_id),
                'payment_url': redirect_url,
                'redirect_url': redirect_url,
                'customer_id': customer_id,
                'customer_name': customer.get('name'),
                'product_index': product_index,
                'sale_id': None
            })

    except Exception as e:
        print("Error:", str(e))
        return jsonify({'error': 'An error occurred'}), 500

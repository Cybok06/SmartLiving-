from flask import Blueprint, render_template, request, jsonify
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
products_collection = db["products"]
users_collection = db["users"]
instant_sales_collection = db["instant_sales"]
commissions_collection = db["commissions"]

# GET: Sell Product Page
@sell_bp.route('/', methods=['GET'])
@login_required
def sell_product_page():
    agent = users_collection.find_one({'_id': ObjectId(current_user.id)})
    if not agent or 'manager_id' not in agent:
        return "Unauthorized", 403

    manager_id = agent['manager_id']
    customers = list(customers_collection.find(
        {'agent_id': str(current_user.id)},
        {'name': 1, 'phone_number': 1}
    ))

    products = list(products_collection.find({
        '$or': [
            {'manager_id': ObjectId(manager_id)},
            {'manager_id': str(manager_id)}
        ]
    }))

    return render_template('sell_product.html', customers=customers, products=products, agent_id=current_user.id)

# POST: Process Sale
@sell_bp.route('/', methods=['POST'])
@login_required
def sell_product():
    try:
        data = request.get_json()
        product_data = data.get('product')
        purchase_type = data.get('purchase_type')
        purchase_date_str = data.get('purchase_date')
        purchase_date = datetime.strptime(purchase_date_str, "%Y-%m-%d")
        agent_id = str(current_user.id)

        agent = users_collection.find_one({'_id': ObjectId(agent_id)})
        if not agent or 'manager_id' not in agent:
            return jsonify({'error': 'Unauthorized agent'}), 403

        manager_id = agent['manager_id']

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
            products_collection.update_one(
                {'_id': ObjectId(product_data['_id'])},
                {'$inc': {'quantity': -int(product_data['quantity'])}}
            )

            # Add commission record (no receipt)
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

            return jsonify({'message': 'Instant sale completed, commission recorded!'})

        else:
            # Layaway purchase
            customer_id = data.get('customer_id')
            end_date = purchase_date + relativedelta(months=6)
            customer = customers_collection.find_one({'_id': ObjectId(customer_id), 'agent_id': agent_id})

            if not customer:
                return jsonify({'error': 'Customer not found'}), 403

            product_data['status'] = 'payment_ongoing'
            purchase = {
                'agent_id': agent_id,
                'product': product_data,
                'purchase_type': purchase_type,
                'purchase_date': purchase_date_str,
                'end_date': end_date.strftime('%Y-%m-%d')
            }

            customers_collection.update_one(
                {'_id': ObjectId(customer_id)},
                {'$push': {'purchases': purchase}}
            )

            return jsonify({'message': 'Product assigned successfully'})

    except Exception as e:
        print("Error:", str(e))
        return jsonify({'error': 'An error occurred'}), 500

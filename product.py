from flask import Blueprint, render_template, request, jsonify, session, redirect, url_for
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from bson.objectid import ObjectId
from db import db

product_bp = Blueprint('product', __name__)

# MongoDB connection

products_collection = db["products"]
users_collection = db["users"]

# Route to show the products page (main view)
@product_bp.route('/', methods=['GET'])
def index():
    if 'agent_id' not in session:
        return redirect(url_for('login.login'))

    agent = users_collection.find_one({'_id': ObjectId(session['agent_id'])})
    if not agent:
        return redirect(url_for('login.login'))

    manager_id = agent.get('manager_id')
    products = list(products_collection.find({'manager_id': manager_id}))

    categorized_products = {}
    for product in products:
        product_type = product.get('product_type', 'Others')
        if product_type not in categorized_products:
            categorized_products[product_type] = []
        categorized_products[product_type].append(product)

    return render_template('products.html', categorized_products=categorized_products)

# Separate route for /products (optional duplicate)
@product_bp.route('/products', methods=['GET'])
def products():
    if 'agent_id' not in session:
        return redirect(url_for('login.login'))

    agent = users_collection.find_one({'_id': ObjectId(session['agent_id'])})
    if not agent:
        return redirect(url_for('login.login'))

    manager_id = agent.get('manager_id')
    products = list(products_collection.find({'manager_id': manager_id}))

    categorized_products = {}
    for product in products:
        product_type = product.get('product_type', 'Others')
        if product_type not in categorized_products:
            categorized_products[product_type] = []
        categorized_products[product_type].append(product)

    return render_template('products.html', categorized_products=categorized_products)

from flask import Blueprint, render_template, session, redirect, url_for
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from bson.objectid import ObjectId
from db import db
manager_product_bp = Blueprint('manager_product', __name__)

# MongoDB connection

products_collection = db["products"]

# Route to show the manager's products page
@manager_product_bp.route('/manager/products', methods=['GET'])
def view_products():
    if 'manager_id' not in session:
        return redirect(url_for('login.login'))

    manager_id = ObjectId(session['manager_id'])

    # Fetch products belonging to this manager
    products = list(products_collection.find({'manager_id': manager_id}))

    # Group by product type
    categorized_products = {}
    for product in products:
        product_type = product.get('product_type', 'Others')
        if product_type not in categorized_products:
            categorized_products[product_type] = []
        categorized_products[product_type].append(product)

    return render_template('manager_products.html', categorized_products=categorized_products)

# Optional duplicate route
@manager_product_bp.route('/manager/products/list', methods=['GET'])
def product_list():
    if 'manager_id' not in session:
        return redirect(url_for('login.login'))

    manager_id = ObjectId(session['manager_id'])
    products = list(products_collection.find({'manager_id': manager_id}))

    categorized_products = {}
    for product in products:
        product_type = product.get('product_type', 'Others')
        if product_type not in categorized_products:
            categorized_products[product_type] = []
        categorized_products[product_type].append(product)

    return render_template('manager_products.html', categorized_products=categorized_products)

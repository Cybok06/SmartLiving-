from flask import Blueprint, render_template, request
from bson.objectid import ObjectId
from db import db
import math

added_products_bp = Blueprint('added_products', __name__)
products_col = db.products
users_col = db.users
deleted_col = db.deleted

@added_products_bp.route('/')
def view_added_products():
    selected_branch = request.args.get('branch')
    selected_manager = request.args.get('manager')
    selected_type = request.args.get('product_type')
    selected_category = request.args.get('category')
    search = request.args.get('search', '').strip()
    page = int(request.args.get('page', 1))
    per_page = 12

    # Build query
    query = {}
    if selected_branch:
        managers = list(users_col.find({'branch': selected_branch, 'role': 'manager'}))
        manager_ids = [m['_id'] for m in managers]
        query['manager_id'] = {'$in': manager_ids}

    if selected_manager:
        manager = users_col.find_one({'name': selected_manager, 'role': 'manager'})
        if manager:
            query['manager_id'] = manager['_id']

    if selected_type:
        query['product_type'] = selected_type

    if selected_category:
        query['category'] = selected_category

    if search:
        # Match only if name field exists and matches search term (case-insensitive)
        query['name'] = {"$exists": True, "$regex": search, "$options": "i"}

    # Count and paginate
    total_products = products_col.count_documents(query)
    total_pages = math.ceil(total_products / per_page)
    skip = (page - 1) * per_page

    # Fetch products
    products = list(products_col.find(query).skip(skip).limit(per_page))

    for product in products:
        manager = users_col.find_one({'_id': product.get('manager_id')})
        product['manager_name'] = manager['name'] if manager else 'Unknown'
        product['manager_branch'] = manager['branch'] if manager else '—'

    # Dropdown options
    branches = users_col.distinct('branch', {'role': 'manager'})
    managers = list(users_col.find({'role': 'manager'}))
    product_types = products_col.distinct('product_type')
    categories = products_col.distinct('category')

    return render_template('added_products.html',
                           products=products,
                           branches=branches,
                           managers=managers,
                           product_types=product_types,
                           categories=categories,
                           selected_branch=selected_branch,
                           selected_manager=selected_manager,
                           selected_type=selected_type,
                           selected_category=selected_category,
                           search=search,
                           current_page=page,
                           total_pages=total_pages)

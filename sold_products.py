from flask import Blueprint, render_template, session, redirect, url_for, flash
from bson.objectid import ObjectId
from pymongo.server_api import ServerApi
from pymongo.mongo_client import MongoClient
from db import db

sold_products_bp = Blueprint('sold_products', __name__)


users_col = db.users
customers_col = db.customers

@sold_products_bp.route('/sold_products')
def sold_products():
    if 'manager_id' not in session:
        return redirect(url_for('login.login'))

    try:
        manager_oid = ObjectId(session['manager_id'])
    except Exception:
        flash("Invalid manager session.", "error")
        return redirect(url_for('login.logout'))

    # Get agents under this manager
    agents = list(users_col.find({"manager_id": manager_oid, "role": "agent"}, {"_id": 1}))
    agent_ids = [str(agent['_id']) for agent in agents]

    # Aggregate all purchases for customers whose agent_id is in agent_ids
    pipeline = [
        {"$match": {"agent_id": {"$in": agent_ids}}},
        {"$unwind": "$purchases"},
        {"$project": {
            "customer_name": "$name",
            "name": "$purchases.product.name",
            "image_url": "$purchases.product.image_url",
            "price": "$purchases.product.price",
            "quantity": "$purchases.product.quantity",
            "total": "$purchases.product.total"
        }}
    ]

    products = list(customers_col.aggregate(pipeline))

    # Convert any BSON decimals or ints to Python types if needed, e.g.:
    for p in products:
        # If price/quantity/total are dicts (like {"$numberInt": "852"}), unwrap:
        for key in ['price', 'quantity', 'total']:
            val = p.get(key)
            if isinstance(val, dict):
                # try to extract numberInt or numberDouble
                if "$numberInt" in val:
                    p[key] = int(val["$numberInt"])
                elif "$numberDouble" in val:
                    p[key] = float(val["$numberDouble"])
            # else keep as is

    return render_template('sold_products.html', products=products)

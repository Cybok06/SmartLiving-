from flask import Blueprint, render_template, session, redirect, url_for, request, flash
from pymongo import MongoClient
from pymongo.server_api import ServerApi
from bson.objectid import ObjectId
from datetime import datetime
from db import db
manager_inventory_bp = Blueprint('manager_inventory', __name__)

# MongoDB connection

# Collections
inventory_col = db.inventory
topup_requests_col = db.topup_requests  # ✅ For manager top-up requests

@manager_inventory_bp.route('/manager/inventory')
def view_manager_inventory():
    if 'manager_id' not in session:
        return redirect(url_for('login.login'))

    manager_id = ObjectId(session['manager_id'])

    # ✅ Fetch manager's inventory
    inventory_items = list(inventory_col.find({"manager_id": manager_id}))

    # ✅ Fetch their top-up requests
    raw_requests = list(topup_requests_col.find({"manager_id": manager_id}).sort("requested_at", -1))

    # Attach product names to requests
    topup_requests = []
    for req in raw_requests:
        product = inventory_col.find_one({"_id": req["product_id"]})
        topup_requests.append({
            "product_name": product["name"] if product else "Unknown",
            "requested_qty": req["requested_qty"],
            "status": req["status"]
        })

    return render_template('manager_inventory.html',
                           inventory_items=inventory_items,
                           topup_requests=topup_requests)


@manager_inventory_bp.route('/manager/request_topup/<product_id>', methods=['POST'])
def request_topup(product_id):
    if 'manager_id' not in session:
        return redirect(url_for('login.login'))

    try:
        requested_qty = int(request.form.get('requested_qty', 0))
        if requested_qty < 1:
            raise ValueError("Quantity must be at least 1")

        topup_requests_col.insert_one({
            "product_id": ObjectId(product_id),
            "manager_id": ObjectId(session['manager_id']),
            "requested_qty": requested_qty,
            "status": "Pending",
            "requested_at": datetime.utcnow()
        })

        flash("Top-up request submitted successfully!", "success")
    except Exception as e:
        flash(f"Error: {str(e)}", "error")

    return redirect(url_for('manager_inventory.view_manager_inventory'))

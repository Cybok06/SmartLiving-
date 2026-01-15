from flask import Blueprint, render_template, redirect, url_for, request, flash
from pymongo import MongoClient
from pymongo.server_api import ServerApi
from bson.objectid import ObjectId
from datetime import datetime
from db import db

admin_topups_bp = Blueprint('admin_topups', __name__)



inventory_col = db.inventory
topup_requests_col = db.topup_requests
users_col = db.users

@admin_topups_bp.route('/admin/topups')
def admin_topup_list():
    requests = list(topup_requests_col.find().sort("requested_at", -1))

    topup_data = []
    for req in requests:
        product = inventory_col.find_one({"_id": req["product_id"]})
        manager = users_col.find_one({"_id": req["manager_id"]})
        topup_data.append({
            "_id": str(req["_id"]),
            "product_name": product["name"] if product else "Unknown",
            "manager_name": manager["name"] if manager else "Unknown",
            "requested_qty": req["requested_qty"],
            "status": req["status"],
            "date": req["requested_at"].strftime("%Y-%m-%d %H:%M"),
        })

    return render_template('admin_topups.html', topups=topup_data)

@admin_topups_bp.route('/admin/topups/approve/<request_id>')
def approve_topup(request_id):
    request_doc = topup_requests_col.find_one({"_id": ObjectId(request_id)})
    if not request_doc:
        flash("Top-up request not found.", "error")
        return redirect(url_for('admin_topups.admin_topup_list'))

    # Update inventory quantity
    inventory_col.update_one(
        {"_id": request_doc["product_id"]},
        {"$inc": {"qty": request_doc["requested_qty"]}}
    )

    # Mark request as approved
    topup_requests_col.update_one(
        {"_id": ObjectId(request_id)},
        {"$set": {"status": "Approved", "approved_at": datetime.utcnow()}}
    )

    flash("Top-up approved and inventory updated.", "success")
    return redirect(url_for('admin_topups.admin_topup_list'))

@admin_topups_bp.route('/admin/topups/decline/<request_id>')
def decline_topup(request_id):
    topup_requests_col.update_one(
        {"_id": ObjectId(request_id)},
        {"$set": {"status": "Declined"}}
    )
    flash("Top-up request declined.", "info")
    return redirect(url_for('admin_topups.admin_topup_list'))

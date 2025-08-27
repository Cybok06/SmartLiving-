from flask import Blueprint, render_template, session, redirect, url_for
from pymongo import MongoClient
from pymongo.server_api import ServerApi
from bson.objectid import ObjectId
from db import db
manager_inventory_analysis_bp = Blueprint('manager_inventory_analysis', __name__)

# MongoDB connection

inventory_col = db.inventory

@manager_inventory_analysis_bp.route('/manager/inventory/analysis')
def manager_inventory_analysis():
    if 'manager_id' not in session:
        return redirect(url_for('login.login'))

    manager_id = ObjectId(session['manager_id'])

    inventory_items = list(inventory_col.find({'manager_id': manager_id}))

    labels = [item.get('name', 'Unnamed') for item in inventory_items]
    quantities = [item.get('qty', 0) for item in inventory_items]

    return render_template(
        'manager_inventory_analysis.html',
        labels=labels,
        quantities=quantities
    )

from flask import Blueprint, render_template, session, redirect, url_for, flash
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from bson.objectid import ObjectId
from db import db

manager_profile_bp = Blueprint('manager_profile', __name__)

# MongoDB connection

users_col = db.users

@manager_profile_bp.route('/manager_profile')
def manager_profile():
    if 'manager_id' not in session:
        return redirect(url_for('login.login'))

    try:
        manager_oid = ObjectId(session['manager_id'])
    except Exception:
        flash("Invalid manager session.", "error")
        return redirect(url_for('login.logout'))

    manager = users_col.find_one({"_id": manager_oid, "role": "manager"}, {"password": 0})  # exclude password

    if not manager:
        flash("Manager profile not found.", "error")
        return redirect(url_for('login.logout'))

    return render_template('manager_profile.html', manager=manager)

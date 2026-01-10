from flask import Blueprint, render_template, session, redirect, url_for, flash
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from bson.objectid import ObjectId
from db import db

admin_profile_bp = Blueprint('admin_profile', __name__)


users_col = db.users

@admin_profile_bp.route('/admin_profile')
def admin_profile():
    if 'admin_id' not in session:
        return redirect(url_for('login.login'))

    try:
        admin_oid = ObjectId(session['admin_id'])
    except Exception:
        flash("Invalid admin session.", "error")
        return redirect(url_for('login.logout'))

    admin = users_col.find_one({"_id": admin_oid, "role": "admin"}, {"password": 0})  # exclude password

    if not admin:
        flash("Admin profile not found.", "error")
        return redirect(url_for('login.logout'))

    return render_template('admin_profile.html', admin=admin)

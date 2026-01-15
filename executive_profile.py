# executive_profile.py
from flask import Blueprint, render_template, request, redirect, url_for, flash, session
from flask_bcrypt import Bcrypt
from bson.objectid import ObjectId
from datetime import datetime

from db import users_collection as users_col
from services.login_audit import get_login_logs_for_user, get_login_stats_for_user, annotate_login_logs

executive_profile_bp = Blueprint("executive_profile", __name__, url_prefix="/executive")
bcrypt = Bcrypt()

@executive_profile_bp.record_once
def on_load(state):
    bcrypt.init_app(state.app)

# ---- Guard ----
def executive_required(fn):
    def wrapper(*args, **kwargs):
        if "executive_id" not in session:
            flash("Please sign in as an executive.", "error")
            return redirect(url_for("login.login"))
        return fn(*args, **kwargs)
    wrapper.__name__ = fn.__name__
    return wrapper

# ---- Profile (view) ----
@executive_profile_bp.route("/profile", methods=["GET"])
@executive_required
def profile():
    try:
        exec_oid = ObjectId(session["executive_id"])
    except Exception:
        flash("Invalid session. Please log in again.", "error")
        return redirect(url_for("login.logout"))

    user = users_col.find_one({"_id": exec_oid, "role": "executive"})
    if not user:
        flash("Executive profile not found.", "error")
        return redirect(url_for("login.logout"))

    logs = annotate_login_logs(get_login_logs_for_user(str(user["_id"]), limit=20))
    stats = get_login_stats_for_user(str(user["_id"]), days=30)

    return render_template(
        "executive_profile.html",
        user=user,
        login_logs=logs,
        login_stats=stats,
    )

# ---- Change Password ----
@executive_profile_bp.route("/profile/password", methods=["POST"])
@executive_required
def change_password():
    current_password = (request.form.get("current_password") or "").strip()
    new_password     = (request.form.get("new_password") or "").strip()
    confirm_password = (request.form.get("confirm_password") or "").strip()

    if len(new_password) < 6:
        flash("New password must be at least 6 characters.", "error")
        return redirect(url_for("executive_profile.profile"))

    if new_password != confirm_password:
        flash("New password and confirmation do not match.", "error")
        return redirect(url_for("executive_profile.profile"))

    try:
        exec_oid = ObjectId(session["executive_id"])
    except Exception:
        flash("Invalid session. Please log in again.", "error")
        return redirect(url_for("login.logout"))

    user = users_col.find_one({"_id": exec_oid, "role": "executive"})
    if not user:
        flash("Executive profile not found.", "error")
        return redirect(url_for("login.logout"))

    stored_hash = user.get("password")
    if not (stored_hash and bcrypt.check_password_hash(stored_hash, current_password)):
        flash("Current password is incorrect.", "error")
        return redirect(url_for("executive_profile.profile"))

    new_hash = bcrypt.generate_password_hash(new_password).decode("utf-8")
    users_col.update_one({"_id": exec_oid}, {"$set": {"password": new_hash, "updated_at": datetime.utcnow()}})

    flash("Password updated successfully.", "success")
    return redirect(url_for("executive_profile.profile"))

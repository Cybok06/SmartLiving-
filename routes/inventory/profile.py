# routes/inventory/profile.py
from __future__ import annotations
from flask import Blueprint, render_template, request, redirect, url_for, session, flash
from flask_bcrypt import Bcrypt
from bson import ObjectId
from datetime import datetime
import db as dbmod  # import the module, then resolve collections safely
from services.activity_audit import audit_action

# --- Resolve collections with graceful fallbacks ---
def _resolve_col(name_primary: str, name_alt: str, default_name: str):
    """
    Try db.<name_primary> -> db.<name_alt> -> db.db[default_name]
    This lets the route work with either `users_col` or `users_collection`,
    and falls back to raw db handle if only `db` is exported.
    """
    if hasattr(dbmod, name_primary) and getattr(dbmod, name_primary) is not None:
        return getattr(dbmod, name_primary)
    if hasattr(dbmod, name_alt) and getattr(dbmod, name_alt) is not None:
        return getattr(dbmod, name_alt)
    # last resort: db.db['collection_name']
    if hasattr(dbmod, "db"):
        return dbmod.db[default_name]
    raise ImportError(f"Could not resolve collection for {default_name}. Ensure it's exposed in db.py")

users_col       = _resolve_col("users_col", "users_collection", "users")
login_logs_col  = _resolve_col("login_logs_col", "login_logs_collection", "login_logs")

inventory_profile_bp = Blueprint(
    "inventory_profile",
    __name__,
    url_prefix="/inventory/profile"
)

bcrypt = Bcrypt()

@inventory_profile_bp.record_once
def on_load(state):
    bcrypt.init_app(state.app)

# --- Guard ---
def require_inventory(f):
    from functools import wraps
    @wraps(f)
    def wrapper(*args, **kwargs):
        if "inventory_id" not in session:
            return redirect(url_for("login.login"))
        return f(*args, **kwargs)
    return wrapper

# --- Helpers ---
def _to_object_id(val: str | ObjectId | None) -> ObjectId | None:
    if isinstance(val, ObjectId):
        return val
    if not val:
        return None
    try:
        return ObjectId(str(val))
    except Exception:
        return None

def current_user():
    """Return the logged-in inventory user document or None."""
    oid = _to_object_id(session.get("inventory_id"))
    if not oid:
        return None
    return users_col.find_one({"_id": oid})

def email_or_username_taken(email: str, username: str, exclude_id: ObjectId) -> tuple[bool, str]:
    """Check unique constraints for email/username excluding current user."""
    if email:
        exists = users_col.find_one({"email": email, "_id": {"$ne": exclude_id}})
        if exists:
            return True, "Email is already in use."
    if username:
        exists = users_col.find_one({"username": username, "_id": {"$ne": exclude_id}})
        if exists:
            return True, "Username is already in use."
    return False, ""

# --- Views ---
@inventory_profile_bp.route("/me", methods=["GET"])
@require_inventory
def view_profile():
    user = current_user()
    if not user:
        return redirect(url_for("login.logout"))

    # Recent logins (safe if collection absent)
    recent_logins = []
    try:
        recent_logins = list(
            login_logs_col.find(
                {"inventory_id": str(user["_id"])}
            ).sort("timestamp", -1).limit(5)
        )
    except Exception:
        recent_logins = []

    return render_template("inventory/profile.html", user=user, recent_logins=recent_logins)

@inventory_profile_bp.route("/update", methods=["POST"])
@require_inventory
@audit_action("inventory.profile_updated", "Updated Inventory Profile", entity_type="user")
def update_profile():
    user = current_user()
    if not user:
        return redirect(url_for("login.logout"))

    # Read form
    username   = (request.form.get("username") or "").strip()
    name       = (request.form.get("name") or "").strip()
    phone      = (request.form.get("phone") or "").strip()
    email      = (request.form.get("email") or "").strip()
    gender     = (request.form.get("gender") or "").strip()
    branch     = (request.form.get("branch") or "").strip()
    position   = (request.form.get("position") or "").strip()
    location   = (request.form.get("location") or "").strip()
    start_date = (request.form.get("start_date") or "").strip()
    status     = (request.form.get("status") or "").strip()
    image_url  = (request.form.get("image_url") or "").strip()
    assets_raw = (request.form.get("assets") or "").strip()

    # Validate unique fields
    taken, msg = email_or_username_taken(email, username, user["_id"])
    if taken:
        flash(msg, "error")
        return redirect(url_for("inventory_profile.view_profile"))

    # Parse assets
    assets = [x.strip() for x in assets_raw.split(",") if x.strip()]

    update_doc = {
        "username": username,
        "name": name,
        "phone": phone,
        "email": email,
        "gender": gender,
        "branch": branch,
        "position": position,
        "location": location,
        "start_date": start_date or None,
        "status": status or user.get("status", "Active"),
        "image_url": image_url or None,
        "assets": assets,
        "updated_at": datetime.utcnow()
    }

    users_col.update_one({"_id": user["_id"]}, {"$set": update_doc})

    # Keep session display name fresh
    session["inventory_name"] = name or username or session.get("inventory_name", "")

    flash("Profile updated successfully.", "success")
    return redirect(url_for("inventory_profile.view_profile"))

@inventory_profile_bp.route("/password", methods=["POST"])
@require_inventory
@audit_action("inventory.password_updated", "Updated Inventory Password", entity_type="user")
def change_password():
    user = current_user()
    if not user:
        return redirect(url_for("login.logout"))

    current_pwd = request.form.get("current_password") or ""
    new_pwd     = request.form.get("new_password") or ""
    confirm_pwd = request.form.get("confirm_password") or ""

    # user.get("password") may be None on legacy records
    stored_hash = user.get("password") or ""
    try:
        ok = bcrypt.check_password_hash(stored_hash, current_pwd)
    except Exception:
        ok = False

    if not ok:
        flash("Current password is incorrect.", "error")
        return redirect(url_for("inventory_profile.view_profile"))

    if len(new_pwd) < 6:
        flash("New password must be at least 6 characters.", "error")
        return redirect(url_for("inventory_profile.view_profile"))

    if new_pwd != confirm_pwd:
        flash("New password and confirmation do not match.", "error")
        return redirect(url_for("inventory_profile.view_profile"))

    new_hash = bcrypt.generate_password_hash(new_pwd).decode("utf-8")
    users_col.update_one(
        {"_id": user["_id"]},
        {"$set": {"password": new_hash, "password_changed_at": datetime.utcnow()}}
    )

    flash("Password changed successfully.", "success")
    return redirect(url_for("inventory_profile.view_profile"))

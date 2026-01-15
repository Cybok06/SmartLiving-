# routes/admin_profile.py
from __future__ import annotations

from flask import Blueprint, render_template, request, jsonify, session, redirect, url_for, flash
from bson import ObjectId
from datetime import datetime
import re, bcrypt

from db import db
from services.activity_audit import audit_action

admin_profile_bp = Blueprint("admin_profile", __name__, url_prefix="/admin/profile")

users_col = db["users"]

ALLOWED_GENDERS = {"Male", "Female", "Other"}

def _admin_required_json():
    """Return (ok, admin_id or None). For AJAX: don’t redirect; for view: redirect in route."""
    aid = session.get("admin_id")
    if not aid:
        return False, None
    return True, aid

def _normalize_phone(p: str | None) -> str | None:
    if not p: return None
    s = p.strip().replace(" ", "").replace("-", "").replace("+","")
    if s.startswith("0") and len(s) == 10:  # e.g. 057xxxxxxx
        return "233" + s[1:]
    if s.startswith("233") and len(s) == 12:
        return s
    # fall back to original if it’s at least digits
    return s if s.isdigit() else None

def _valid_email(e: str | None) -> bool:
    if not e: return False
    return re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", e) is not None

# ---------- VIEW ----------
@admin_profile_bp.get("/")
def profile_page():
    if "admin_id" not in session:
        return redirect(url_for("login.login"))

    try:
        me = users_col.find_one({"_id": ObjectId(session["admin_id"])})
    except Exception:
        me = None

    if not me:
        flash("Admin not found.", "danger")
        return redirect(url_for("login.login"))

    # stringify id for template safety
    me["_id"] = str(me["_id"])
    return render_template("admin_profile.html", me=me)

# ---------- UPDATE PROFILE (AJAX) ----------
@admin_profile_bp.post("/update")
@audit_action("admin.profile_updated", "Updated Admin Profile", entity_type="user")
def update_profile():
    ok, aid = _admin_required_json()
    if not ok: 
        return jsonify(ok=False, message="Unauthorized"), 401

    # pull current
    try:
        me = users_col.find_one({"_id": ObjectId(aid)})
        if not me:
            return jsonify(ok=False, message="Admin not found"), 404
    except Exception:
        return jsonify(ok=False, message="Invalid admin id"), 400

    name     = (request.form.get("name") or "").strip()
    email    = (request.form.get("email") or "").strip()
    phone    = (request.form.get("phone") or "").strip()
    gender   = (request.form.get("gender") or "").strip()
    branch   = (request.form.get("branch") or "").strip()
    position = (request.form.get("position") or "").strip()
    location = (request.form.get("location") or "").strip()
    image_url= (request.form.get("image_url") or "").strip()

    # minimal validation
    if name == "":
        return jsonify(ok=False, message="Name is required"), 400
    if email and not _valid_email(email):
        return jsonify(ok=False, message="Invalid email"), 400
    phone_norm = _normalize_phone(phone) if phone else phone
    if phone and not phone_norm:
        return jsonify(ok=False, message="Invalid phone number"), 400
    if gender and gender not in ALLOWED_GENDERS:
        return jsonify(ok=False, message="Invalid gender"), 400

    update = {
        "name": name,
        "email": email,
        "phone": phone_norm or phone,
        "gender": gender,
        "branch": branch,
        "position": position,
        "location": location,
        "image_url": image_url,
        "updated_at": datetime.utcnow(),
    }
    # Don’t overwrite with empty strings if fields were blank in form by mistake
    update = {k:v for k,v in update.items() if v is not None}

    users_col.update_one({"_id": ObjectId(aid)}, {"$set": update})
    return jsonify(ok=True, message="Profile updated")

# ---------- CHANGE PASSWORD (AJAX) ----------
@admin_profile_bp.post("/password")
@audit_action("admin.password_updated", "Updated Admin Password", entity_type="user")
def change_password():
    ok, aid = _admin_required_json()
    if not ok:
        return jsonify(ok=False, message="Unauthorized"), 401

    try:
        me = users_col.find_one({"_id": ObjectId(aid)})
        if not me:
            return jsonify(ok=False, message="Admin not found"), 404
    except Exception:
        return jsonify(ok=False, message="Invalid admin id"), 400

    current_pw = request.form.get("current_password") or ""
    new_pw     = request.form.get("new_password") or ""
    confirm_pw = request.form.get("confirm_password") or ""

    if len(new_pw) < 8:
        return jsonify(ok=False, message="New password must be at least 8 characters"), 400
    if new_pw != confirm_pw:
        return jsonify(ok=False, message="New passwords do not match"), 400

    # verify bcrypt
    stored_hash = (me.get("password") or "").encode("utf-8")
    try:
        if not bcrypt.checkpw(current_pw.encode("utf-8"), stored_hash):
            return jsonify(ok=False, message="Current password is incorrect"), 400
    except Exception:
        return jsonify(ok=False, message="Password verification failed"), 400

    # re-hash with 12 rounds (matches your sample)
    new_hash = bcrypt.hashpw(new_pw.encode("utf-8"), bcrypt.gensalt(rounds=12)).decode("utf-8")
    users_col.update_one({"_id": ObjectId(aid)}, {"$set": {"password": new_hash, "updated_at": datetime.utcnow()}})
    return jsonify(ok=True, message="Password updated successfully")

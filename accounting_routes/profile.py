# accounting_routes/profile.py
from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Tuple

import bcrypt as bcrypt_lib
from bson import ObjectId
from flask import Blueprint, render_template, request, redirect, url_for, session, flash, jsonify

from config_constants import DEFAULT_PROFILE_IMAGE_URL

from db import db

# If you already have users_collection imported elsewhere, you can reuse that.
users_collection = db["users"]

acc_profile = Blueprint(
    "acc_profile",
    __name__,
    template_folder="../templates",
)


def _safe_status(user: Dict[str, Any] | None) -> str:
    if not user:
        return "unknown"
    return (user.get("status") or "active").strip().lower()


def _status_blocked(user: Dict[str, Any]) -> bool:
    status_val = str(user.get("status") or "").strip().lower()
    if status_val in ("not active", "disabled", "inactive"):
        return True
    if user.get("account_locked") is True:
        return True
    if user.get("is_active") is False:
        return True
    return False


def get_current_user_doc() -> Tuple[Dict[str, Any] | None, str]:
    role = (session.get("role") or "").lower().strip()
    user_id = str(session.get("user_id") or "")
    if not user_id:
        user_id = str(session.get("accounting_id") or session.get("executive_id") or session.get("admin_id") or "")

    if not user_id:
        return None, role

    if ObjectId.is_valid(user_id):
        user = users_collection.find_one({"_id": ObjectId(user_id)})
    else:
        user = users_collection.find_one({"_id": user_id})
    return user, role


@acc_profile.route("/accounting/profile", methods=["GET"])
def profile():
    user, role = get_current_user_doc()

    if not user or role not in {"accounting", "executive", "admin"}:
        flash("You must be logged in as an accounting user to access the profile page.", "danger")
        return redirect(url_for("login.login"))

    if _status_blocked(user):
        flash("Your profile is not active. Contact an administrator.", "warning")
        session.clear()
        return redirect(url_for("login.login"))

    # --- Build profile data for template ---
    created_at = user.get("created_at")
    updated_at = user.get("updated_at")
    date_registered = user.get("date_registered")

    def _fmt(dt: Any) -> str | None:
        if isinstance(dt, datetime):
            # You can adjust format to your taste
            return dt.strftime("%Y-%m-%d %H:%M")
        return None

    profile = {
        "username": user.get("username") or "",
        "name": user.get("name") or user.get("username") or "",
        "role": (user.get("role") or "").capitalize() or "Accounting",
        "position": user.get("position") or "Accounting User",
        "status": _safe_status(user).capitalize(),
        "phone": user.get("phone") or "",
        "email": user.get("email") or "",
        "branch": user.get("branch") or "",
        "location": user.get("location") or "",
        "image_url": user.get("image_url") or "",
        "date_registered": _fmt(date_registered or created_at),
        "created_at": _fmt(created_at),
        "updated_at": _fmt(updated_at),
    }

    # Simple initial for avatar
    profile["initial"] = (profile["name"] or profile["username"] or "A")[0].upper()

    return render_template(
        "accounting/profile.html",
        profile=profile,
        default_profile_image=DEFAULT_PROFILE_IMAGE_URL,
    )


@acc_profile.post("/accounting/profile/update")
def profile_update():
    user, role = get_current_user_doc()
    if not user or role not in {"accounting", "executive", "admin"}:
        return jsonify({"ok": False, "message": "Not authorized."}), 401
    if _status_blocked(user):
        return jsonify({"ok": False, "message": "Your profile is not active."}), 403

    payload = request.get_json(silent=True) or {}
    form = request.form

    def _val(key: str) -> str:
        return (payload.get(key) or form.get(key) or "").strip()

    update = {
        "name": _val("name"),
        "phone": _val("phone"),
        "email": _val("email"),
        "branch": _val("branch"),
        "position": _val("position"),
        "location": _val("location"),
        "image_url": _val("image_url") or None,
        "updated_at": datetime.utcnow(),
    }

    users_collection.update_one({"_id": user["_id"]}, {"$set": update})
    return jsonify({"ok": True, "message": "Profile updated successfully."})


@acc_profile.post("/accounting/profile/change_password")
def profile_change_password():
    user, role = get_current_user_doc()
    if not user or role not in {"accounting", "executive", "admin"}:
        return jsonify({"ok": False, "message": "Not authorized."}), 401
    if _status_blocked(user):
        return jsonify({"ok": False, "message": "Your profile is not active."}), 403

    payload = request.get_json(silent=True) or {}
    form = request.form
    current_pw = (payload.get("current_password") or form.get("current_password") or "").strip()
    new_pw = (payload.get("new_password") or form.get("new_password") or "").strip()
    confirm_pw = (payload.get("confirm_password") or form.get("confirm_password") or "").strip()

    if not current_pw or not new_pw or not confirm_pw:
        return jsonify({"ok": False, "message": "All password fields are required."}), 400
    if len(new_pw) < 8:
        return jsonify({"ok": False, "message": "New password must be at least 8 characters."}), 400
    if new_pw != confirm_pw:
        return jsonify({"ok": False, "message": "Password confirmation does not match."}), 400

    stored_hash = user.get("password") or ""
    ok = False
    if str(stored_hash).startswith("$2"):
        try:
            ok = bcrypt_lib.checkpw(current_pw.encode("utf-8"), str(stored_hash).encode("utf-8"))
        except Exception:
            ok = False
    else:
        ok = current_pw == str(stored_hash)
        if ok:
            try:
                new_hash = bcrypt_lib.hashpw(current_pw.encode("utf-8"), bcrypt_lib.gensalt(rounds=12)).decode("utf-8")
                users_collection.update_one({"_id": user["_id"]}, {"$set": {"password": new_hash}})
                stored_hash = new_hash
            except Exception:
                pass

    if not ok:
        return jsonify({"ok": False, "message": "Current password is incorrect."}), 400

    new_hash = bcrypt_lib.hashpw(new_pw.encode("utf-8"), bcrypt_lib.gensalt(rounds=12)).decode("utf-8")
    users_collection.update_one(
        {"_id": user["_id"]},
        {"$set": {"password": new_hash, "updated_at": datetime.utcnow()}},
    )
    return jsonify({"ok": True, "message": "Password updated successfully."})

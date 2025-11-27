# accounting_routes/profile.py
from __future__ import annotations

from flask import Blueprint, render_template, request, redirect, url_for, session, flash
from werkzeug.security import check_password_hash, generate_password_hash
from datetime import datetime
from typing import Any, Dict

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


@acc_profile.route("/accounting/profile", methods=["GET", "POST"])
def profile():
    # --- Guard: must be logged in and accounting role ---
    username = session.get("username")
    role = (session.get("role") or "").lower()

    if not username or role != "accounting":
        flash("You must be logged in as an accounting user to access the profile page.", "danger")
        return redirect(url_for("login.login"))

    # Load user from DB
    user = users_collection.find_one({"username": username})
    if not user:
        flash("User not found in the system. Contact an administrator.", "danger")
        session.clear()
        return redirect(url_for("login.login"))

    if _safe_status(user) != "active":
        flash("Your profile is not active. Contact an administrator.", "warning")
        session.clear()
        return redirect(url_for("login.login"))

    # --- Handle password change POST ---
    if request.method == "POST":
        action = (request.form.get("action") or "").strip().lower()
        if action == "change_password":
            current_pw = request.form.get("current_password", "") or ""
            new_pw = request.form.get("new_password", "") or ""
            confirm_pw = request.form.get("confirm_password", "") or ""

            # Basic validations
            if not current_pw or not new_pw or not confirm_pw:
                flash("All password fields are required.", "warning")
                return redirect(url_for("acc_profile.profile"))

            if not check_password_hash(user.get("password", "") or "", current_pw):
                flash("Current password is incorrect.", "danger")
                return redirect(url_for("acc_profile.profile"))

            if new_pw != confirm_pw:
                flash("New password and confirmation do not match.", "warning")
                return redirect(url_for("acc_profile.profile"))

            if len(new_pw) < 8:
                flash("New password must be at least 8 characters long.", "warning")
                return redirect(url_for("acc_profile.profile"))

            # Update password (keep same scrypt format as other users)
            new_hash = generate_password_hash(new_pw, method="scrypt")

            users_collection.update_one(
                {"_id": user["_id"]},
                {
                    "$set": {
                        "password": new_hash,
                        "updated_at": datetime.utcnow(),
                    }
                },
            )

            flash("Password updated successfully.", "success")
            return redirect(url_for("acc_profile.profile"))

    # --- Build profile data for template ---
    created_at = user.get("created_at")
    updated_at = user.get("updated_at")

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
        "created_at": _fmt(created_at),
        "updated_at": _fmt(updated_at),
    }

    # Simple initial for avatar
    profile["initial"] = (profile["name"] or profile["username"] or "A")[0].upper()

    return render_template(
        "accounting/profile.html",
        profile=profile,
    )

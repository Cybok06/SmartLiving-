# hr_backend/hr_employee_add.py

from datetime import datetime
from typing import Dict, Any, List
import traceback
import re

import requests
from werkzeug.utils import secure_filename
from bson import ObjectId
from flask import (
    request,
    redirect,
    url_for,
    jsonify,
)

from flask_bcrypt import Bcrypt

from db import db
from services.activity_audit import audit_action
from hr_backend.hr_dashboard import hr_bp, _hr_access_guard
from hr_backend.hr_employees_directory import (
    RECRUITMENT_DEFAULT_STEPS,
    users_col,
)
from hr_backend.hr_roles import roles_col, DEFAULT_ROLE_KEYS, normalize_role_key

bcrypt = Bcrypt()

# -------------------------------
# Cloudflare Images
# -------------------------------
CF_ACCOUNT_ID   = "63e6f91eec9591f77699c4b434ab44c6"
CF_IMAGES_TOKEN = "Brz0BEfl_GqEUjEghS2UEmLZhK39EUmMbZgu_hIo"
CF_HASH         = "h9fmMoa1o2c2P55TcWJGOg"
DEFAULT_VARIANT = "public"

ALLOWED_EXTENSIONS = {"png", "jpg", "jpeg", "gif"}

images_col = db.images


def _allowed_file(filename: str) -> bool:
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS


def _clean_phone(s: str) -> str:
    s = (s or "").strip()
    # keep + and digits only
    s = re.sub(r"[^\d+]", "", s)
    return s


def _clean_username(s: str) -> str:
    s = (s or "").strip()
    s = re.sub(r"\s+", "", s)
    return s.lower()


def _upload_employee_image_to_cloudflare(file_obj) -> str | None:
    if not file_obj or not file_obj.filename:
        return None

    if not _allowed_file(file_obj.filename):
        return None

    try:
        direct_url = f"https://api.cloudflare.com/client/v4/accounts/{CF_ACCOUNT_ID}/images/v2/direct_upload"
        headers = {"Authorization": f"Bearer {CF_IMAGES_TOKEN}"}

        res = requests.post(direct_url, headers=headers, data={}, timeout=20)
        try:
            j = res.json()
        except Exception:
            traceback.print_exc()
            return None

        if not j.get("success"):
            traceback.print_exc()
            return None

        upload_url = j["result"]["uploadURL"]
        image_id   = j["result"]["id"]

        up = requests.post(
            upload_url,
            files={
                "file": (
                    secure_filename(file_obj.filename),
                    file_obj.stream,
                    file_obj.mimetype or "application/octet-stream",
                )
            },
            timeout=60,
        )

        try:
            uj = up.json()
        except Exception:
            traceback.print_exc()
            return None

        if not uj.get("success"):
            traceback.print_exc()
            return None

        variant   = DEFAULT_VARIANT
        image_url = f"https://imagedelivery.net/{CF_HASH}/{image_id}/{variant}"

        images_col.insert_one(
            {
                "provider": "cloudflare_images",
                "context": "hr_employee",
                "image_id": image_id,
                "variant": variant,
                "url": image_url,
                "original_filename": secure_filename(file_obj.filename),
                "mimetype": file_obj.mimetype,
                "created_at": datetime.utcnow(),
            }
        )

        return image_url

    except Exception:
        traceback.print_exc()
        return None


@hr_bp.route("/employees/add", methods=["POST"])
@audit_action("employee.created", "Created Employee", entity_type="employee")
def add_employee():
    if not _hr_access_guard():
        if request.headers.get("X-Requested-With") == "XMLHttpRequest":
            return jsonify({"success": False, "message": "Not authorized."}), 401
        return redirect(url_for("login.login"))

    form = request.form

    name = (form.get("name") or "").strip()
    phone = _clean_phone(form.get("phone"))
    email = (form.get("email") or "").strip()

    username = _clean_username(form.get("username"))
    role = (form.get("role") or "").strip().lower()
    branch = (form.get("branch") or "").strip()

    gender = (form.get("gender") or "").strip()
    position = (form.get("position") or "").strip()
    location = (form.get("location") or "").strip()
    gps_address = (form.get("gps_address") or "").strip()

    employment_status = (form.get("status") or "").strip() or "Probation"
    ghana_card = (form.get("ghana_card") or "").strip()

    manager_id_str = (form.get("manager_id") or "").strip()

    # DOB / Birthday
    dob_raw = (form.get("dob") or "").strip()
    dob_dt = None
    dob_month = None
    dob_day = None
    dob_md = None  # "MM-DD" for easy upcoming birthdays query
    if dob_raw:
        try:
            dob_dt = datetime.strptime(dob_raw, "%Y-%m-%d")
            dob_month = int(dob_dt.strftime("%m"))
            dob_day = int(dob_dt.strftime("%d"))
            dob_md = dob_dt.strftime("%m-%d")
        except Exception:
            dob_dt = None

    start_date_raw = (form.get("start_date") or "").strip()
    start_date = start_date_raw or datetime.utcnow().strftime("%Y-%m-%d")

    source_type = (form.get("source_type") or "").strip() or "Other"
    referrer_name = (form.get("referrer_name") or "").strip()
    referrer_phone = _clean_phone(form.get("referrer_phone"))

    guarantor_name = (form.get("guarantor_name") or "").strip()
    guarantor_phone = _clean_phone(form.get("guarantor_phone"))
    guarantor_address = (form.get("guarantor_address") or "").strip()
    guarantor_relationship = (form.get("guarantor_relationship") or "").strip()

    image_url = (form.get("image_url") or "").strip()
    image_file = request.files.get("image_file")

    other_income = (form.get("other_income") or "").strip()

    # Custom fields
    labels = form.getlist("custom_field_label[]")
    values = form.getlist("custom_field_value[]")
    custom_fields: List[Dict[str, Any]] = []
    for label, value in zip(labels, values):
        l = (label or "").strip()
        v = (value or "").strip()
        if l or v:
            custom_fields.append({"label": l, "value": v})

    # ---------------- Validation ----------------
    if not name or not username or not phone or not role:
        msg = "Name, username, phone and role are required."
        if request.headers.get("X-Requested-With") == "XMLHttpRequest":
            return jsonify({"success": False, "message": msg}), 400
        return redirect(url_for("hr.employees"))

    # Enforce username uniqueness
    if users_col.find_one({"username": username}):
        msg = "Username already exists. Choose a different username."
        if request.headers.get("X-Requested-With") == "XMLHttpRequest":
            return jsonify({"success": False, "message": msg}), 409
        return redirect(url_for("hr.employees"))

    # Optional: enforce phone uniqueness (recommended)
    if users_col.find_one({"phone": phone}):
        msg = "Phone number already exists for another employee."
        if request.headers.get("X-Requested-With") == "XMLHttpRequest":
            return jsonify({"success": False, "message": msg}), 409
        return redirect(url_for("hr.employees"))

    # Role validation (allow defaults + hr_roles)
    if role not in DEFAULT_ROLE_KEYS:
        existing_role = roles_col.find_one({"$or": [{"key": role}, {"name": role}]})
        if not existing_role:
            role_name = role.replace("_", " ").title()
            role_key = normalize_role_key(role_name)
            if role_key:
                try:
                    roles_col.insert_one({"name": role_name, "key": role_key, "created_at": datetime.utcnow()})
                    role = role_key
                except Exception:
                    msg = "Invalid role selected."
                    if request.headers.get("X-Requested-With") == "XMLHttpRequest":
                        return jsonify({"success": False, "message": msg}), 400
                    return redirect(url_for("hr.employees"))
            else:
                msg = "Invalid role selected."
                if request.headers.get("X-Requested-With") == "XMLHttpRequest":
                    return jsonify({"success": False, "message": msg}), 400
                return redirect(url_for("hr.employees"))

    # Manager / branch enforcement
    manager_oid = None
    if role == "manager":
        if manager_id_str:
            msg = "Managers should not be assigned to another manager."
            if request.headers.get("X-Requested-With") == "XMLHttpRequest":
                return jsonify({"success": False, "message": msg}), 400
            return redirect(url_for("hr.employees"))
        if not branch:
            msg = "Branch is required for managers."
            if request.headers.get("X-Requested-With") == "XMLHttpRequest":
                return jsonify({"success": False, "message": msg}), 400
            return redirect(url_for("hr.employees"))
    else:
        if not manager_id_str:
            msg = "Please select a manager for this employee."
            if request.headers.get("X-Requested-With") == "XMLHttpRequest":
                return jsonify({"success": False, "message": msg}), 400
            return redirect(url_for("hr.employees"))

        try:
            manager_oid = ObjectId(manager_id_str)
        except Exception:
            msg = "Invalid manager selected."
            if request.headers.get("X-Requested-With") == "XMLHttpRequest":
                return jsonify({"success": False, "message": msg}), 400
            return redirect(url_for("hr.employees"))

        manager_doc = users_col.find_one({"_id": manager_oid, "role": "manager"})
        if not manager_doc:
            msg = "Selected manager does not exist."
            if request.headers.get("X-Requested-With") == "XMLHttpRequest":
                return jsonify({"success": False, "message": msg}), 400
            return redirect(url_for("hr.employees"))
        manager_branch = (manager_doc.get("branch") or "").strip()
        if not manager_branch:
            msg = "Selected manager does not have a branch."
            if request.headers.get("X-Requested-With") == "XMLHttpRequest":
                return jsonify({"success": False, "message": msg}), 400
            return redirect(url_for("hr.employees"))
        branch = manager_branch

    # Upload to Cloudflare if file provided
    if image_file and image_file.filename:
        cf_url = _upload_employee_image_to_cloudflare(image_file)
        if cf_url:
            image_url = cf_url

    default_password = "1234"
    pw_hash = bcrypt.generate_password_hash(default_password).decode("utf-8")

    now = datetime.utcnow()

    recruitment_checklist = [
        {"key": s["key"], "status": "Pending", "completed_on": None, "completed_by": None}
        for s in RECRUITMENT_DEFAULT_STEPS
    ]

    probation = {
        "start_date": start_date,
        "end_date": None,
        "status": "Ongoing" if employment_status in ("Probation", "On Probation") else employment_status,
        "manager_comments": "",
        "executive_comments": "",
    }

    user_doc: Dict[str, Any] = {
        "username": username,
        "password": pw_hash,
        "role": role,
        "name": name,
        "phone": phone,
        "email": email,
        "gender": gender,
        "branch": branch,
        "position": position,
        "location": location,
        "start_date": start_date,
        "image_url": image_url,
        "status": "Active",
        "assets": [],
        "date_registered": start_date,
        "employment_status": employment_status,
        "gps_address": gps_address,
        "ghana_card": ghana_card,

        # Recruitment meta
        "recruitment_source": {
            "type": source_type,
            "referrer_name": referrer_name,
            "referrer_phone": referrer_phone,
        },
        "guarantor": {
            "name": guarantor_name,
            "phone": guarantor_phone,
            "address": guarantor_address,
            "relationship": guarantor_relationship,
        },
        "recruitment_stage": "Documentation Approved",
        "recruitment_checklist": recruitment_checklist,
        "probation": probation,

        "other_income": other_income,
        "created_at": now,
        "updated_at": now,
    }

    # ✅ DOB fields (supports top 10 upcoming birthdays on dashboard)
    if dob_dt:
        user_doc["dob"] = dob_dt               # datetime
        user_doc["dob_raw"] = dob_raw          # "YYYY-MM-DD" for display consistency
        user_doc["dob_month"] = dob_month      # int
        user_doc["dob_day"] = dob_day          # int
        user_doc["dob_md"] = dob_md            # "MM-DD"

    if role != "manager" and manager_oid:
        user_doc["manager_id"] = manager_oid

    if custom_fields:
        user_doc["custom_fields"] = custom_fields

    users_col.insert_one(user_doc)

    redirect_url = url_for("hr.employees")
    success_message = f"Employee '{name}' added successfully."

    if request.headers.get("X-Requested-With") == "XMLHttpRequest":
        return jsonify({"success": True, "message": success_message, "redirect_url": redirect_url})

    return redirect(redirect_url)

# hr_backend/hr_employee_profile.py

from datetime import datetime, date, timedelta
from typing import Dict, Any, List

from flask import (
    render_template,
    request,
    redirect,
    url_for,
    jsonify,
)
from bson import ObjectId
import traceback
import requests

from db import db
from hr_backend.hr_dashboard import hr_bp, _hr_access_guard, _is_ajax

users_col = db["users"]

# -------------------------------
# Cloudflare (same module style as add_product)
# -------------------------------
CF_ACCOUNT_ID   = "63e6f91eec9591f77699c4b434ab44c6"
CF_IMAGES_TOKEN = "Brz0BEfl_GqEUjEghS2UEmLZhK39EUmMbZgu_hIo"
CF_HASH         = "h9fmMoa1o2c2P55TcWJGOg"
DEFAULT_VARIANT = "public"
HR_ALLOWED_EXT = {"png", "jpg", "jpeg", "gif"}  # HR files: pictures only (as requested)

# -------------------------------
# Recruitment constants
# (we'll use these later for the Recruitment tab)
# -------------------------------
RECRUITMENT_STAGES: List[str] = [
    "Documentation Approved",
    "Investigation",
    "Residence Check",
    "Training 1 – Mission & Vision",
    "Training 2 – Field Work",
    "Certification",
    "Contract",
    "Probation",
    "Full Staff (After 3 Months)",
]

RECRUITMENT_DEFAULT_STEPS = [
    {"key": "documentation", "label": "Documentation Approval (CV & Letter)"},
    {"key": "investigation", "label": "Investigation Completed"},
    {"key": "residence", "label": "Residence Check Completed"},
    {"key": "training1", "label": "Training 1 — Mission & Vision"},
    {"key": "training2", "label": "Training 2 — Field Work"},
    {"key": "certification", "label": "Certification Done"},
    {"key": "contract", "label": "Contract Prepared & Signed"},
    {"key": "probation", "label": "Probation Started"},
    {"key": "full_staff", "label": "Full Staff Confirmed (after 3 months)"},
]


# -------------------------------
# Helpers
# -------------------------------
def _parse_date_generic(value) -> date | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        try:
            return datetime.strptime(value[:10], "%Y-%m-%d").date()
        except Exception:
            return None
    return None


def _build_recruitment_steps(emp: dict) -> List[dict]:
    """
    Merge default steps + stored checklist from DB.
    stored shape: recruitment_checklist: [{key, status, completed_on, completed_by}, ...]
    """
    stored = {s.get("key"): s for s in (emp.get("recruitment_checklist") or [])}
    steps: List[dict] = []
    for base in RECRUITMENT_DEFAULT_STEPS:
        existing = stored.get(base["key"]) or {}
        steps.append(
            {
                "key": base["key"],
                "label": base["label"],
                "status": existing.get("status", "Pending"),
                "completed_on": existing.get("completed_on"),
                "completed_by": existing.get("completed_by"),
            }
        )
    return steps


def _build_probation_info(emp: dict) -> dict:
    probation = emp.get("probation") or {}
    start_date = _parse_date_generic(probation.get("start_date"))
    end_date = _parse_date_generic(probation.get("end_date"))

    # Default end_date = 90 days after start
    if start_date and not end_date:
        end_date = start_date + timedelta(days=90)

    status = probation.get("status") or emp.get("employment_status") or "Active"

    # Auto-complete probation if date has passed but status still "Probation"
    today = datetime.utcnow().date()
    if (
        start_date
        and end_date
        and today >= end_date
        and emp.get("employment_status") in ("Probation", "On Probation")
    ):
        users_col.update_one(
            {"_id": emp["_id"]},
            {
                "$set": {
                    "employment_status": "Active",
                    "probation.status": "Completed",
                    "probation.end_date": end_date.strftime("%Y-%m-%d"),
                    "updated_at": datetime.utcnow(),
                }
            },
        )
        status = "Completed"

    return {
        "stage": emp.get("recruitment_stage") or "Documentation Approved",
        "start_date": start_date.strftime("%Y-%m-%d") if start_date else "",
        "end_date": end_date.strftime("%Y-%m-%d") if end_date else "",
        "status": status,
        "manager_comments": probation.get("manager_comments", ""),
        "executive_comments": probation.get("executive_comments", ""),
    }


# -------------------------------
# EMPLOYEE PROFILE – PAGE VIEW
# -------------------------------
@hr_bp.route(
    "/employee/<employee_id>/profile",
    methods=["GET"],
    endpoint="employee_profile",
)
def employee_profile(employee_id):
    """
    HR employee profile PAGE (opens in a new tab).

    For now we focus on:
    - Overview details (registration info, contact, branch, role, etc.)
    - Rating & Employee Level (Beginner / Intermediate / Pro)
    - Custom fields
    - Profile image (Cloudflare image_url)

    Other sections (Recruitment, Wages, Attendance, Assets, Files, Cases)
    will be built in separate partials/tabs later.
    """
    if not _hr_access_guard():
        return redirect(url_for("login.login"))

    # Validate ID
    try:
        oid = ObjectId(employee_id)
    except Exception:
        return render_template(
            "hr_pages/employee_profile_page.html",
            employee_error="Invalid employee ID.",
        ), 400

    emp = users_col.find_one({"_id": oid})
    if not emp:
        return render_template(
            "hr_pages/employee_profile_page.html",
            employee_error="Employee not found.",
        ), 404

    # Normalize / enrich employee for template
    employee = {**emp, "_id": str(emp["_id"])}

    employee["name"] = employee.get("name") or employee.get("username") or "Unnamed"
    employee["username"] = employee.get("username") or ""
    employee["employee_id"] = (
        employee.get("employee_id")
        or employee.get("username")
        or str(employee["_id"])[:6].upper()
    )
    employee["phone"] = employee.get("phone") or ""
    employee["email"] = employee.get("email") or ""
    employee["gender"] = employee.get("gender") or ""
    employee["location"] = employee.get("location") or ""
    employee["branch"] = employee.get("branch") or ""
    employee["department"] = employee.get("department") or ""
    employee["role"] = employee.get("role") or ""
    employee["employment_status"] = employee.get("employment_status") or "Active"
    employee["gps_address"] = employee.get("gps_address") or ""
    employee["ghana_card"] = employee.get("ghana_card") or ""
    employee["other_income"] = employee.get("other_income") or ""
    employee["image_url"] = employee.get("image_url") or ""

    dob = employee.get("dob")
    age = None
    if isinstance(dob, datetime):
        today = datetime.utcnow().date()
        years = today.year - dob.year - (
            (today.month, today.day) < (dob.month, dob.day)
        )
        age = years

    # Date strings
    start_date = employee.get("start_date") or ""
    if isinstance(employee.get("date_employed"), datetime):
        start_date = employee["date_employed"].strftime("%Y-%m-%d")
    exit_date = (employee.get("exit_info") or {}).get("exit_date", "")

    # Recruitment source
    recruitment_source = employee.get("recruitment_source") or {}
    # Guarantor
    guarantor = employee.get("guarantor") or {}

    # Rating + level
    star_rating = int(employee.get("star_rating") or 0) or 3
    sales_level = employee.get("sales_level") or "Beginner"

    # Custom fields
    custom_fields = employee.get("custom_fields") or []

    # We still compute these for later tabs, but UI will use them when ready
    recruitment_steps = _build_recruitment_steps(employee)
    recruitment_stage = employee.get("recruitment_stage") or "Documentation Approved"
    probation_info = _build_probation_info(employee)

    completed_steps = sum(1 for s in recruitment_steps if s["status"] == "Completed")
    total_steps = len(recruitment_steps) or 1
    recruitment_progress_pct = int((completed_steps / total_steps) * 100)

    context = {
        "employee": employee,
        "age": age,
        "dob": dob,
        "start_date": start_date,
        "exit_date": exit_date,
        "recruitment_source": recruitment_source,
        "guarantor": guarantor,
        "custom_fields": custom_fields,
        "star_rating": star_rating,
        "sales_level": sales_level,
        # For future sections:
        "recruitment_steps": recruitment_steps,
        "recruitment_stage": recruitment_stage,
        "recruitment_stages": RECRUITMENT_STAGES,
        "recruitment_progress_pct": recruitment_progress_pct,
        "completed_steps": completed_steps,
        "total_steps": total_steps,
        "probation": probation_info,
    }

    # We no longer use modal partial here – always render full page
    return render_template(
        "hr_pages/employee_profile_page.html",
        employee_error=None,
        **context,
    )


# -------------------------------
# UPDATE EMPLOYEE OVERVIEW (basic details + custom fields)
# -------------------------------
@hr_bp.route("/employee/<employee_id>/overview", methods=["POST"])
def update_employee_overview(employee_id):
    if not _hr_access_guard():
        if _is_ajax(request):
            return jsonify(ok=False, message="Unauthorized"), 401
        return redirect(url_for("login.login"))

    try:
        oid = ObjectId(employee_id)
    except Exception:
        if _is_ajax(request):
            return jsonify(ok=False, message="Invalid employee ID"), 400
        return redirect(url_for("hr.employees"))

    form = request.form
    now = datetime.utcnow()
    update: Dict[str, Any] = {}

    # Core identity
    name = (form.get("name") or "").strip()
    username = (form.get("username") or "").strip()
    employee_id_val = (form.get("employee_id") or "").strip()

    if name:
        update["name"] = name
    if username:
        update["username"] = username
    if employee_id_val:
        update["employee_id"] = employee_id_val

    # Contact & personal
    phone = (form.get("phone") or "").strip()
    email = (form.get("email") or "").strip()
    gender = (form.get("gender") or "").strip()
    location = (form.get("location") or "").strip()
    gps_address = (form.get("gps_address") or "").strip()
    ghana_card = (form.get("ghana_card") or "").strip()
    dob_raw = (form.get("dob") or "").strip()

    if phone:
        update["phone"] = phone
    if email:
        update["email"] = email
    if gender:
        update["gender"] = gender
    if location:
        update["location"] = location
    if gps_address:
        update["gps_address"] = gps_address
    if ghana_card:
        update["ghana_card"] = ghana_card
    if dob_raw:
        try:
            update["dob"] = datetime.strptime(dob_raw, "%Y-%m-%d")
        except Exception:
            pass

    # Org info
    branch = (form.get("branch") or "").strip()
    department = (form.get("department") or "").strip()
    role = (form.get("role") or "").strip()
    employment_status = (form.get("employment_status") or "").strip()
    start_date = (form.get("start_date") or "").strip()
    exit_date = (form.get("exit_date") or "").strip()

    if branch:
        update["branch"] = branch
    if department:
        update["department"] = department
    if role:
        update["role"] = role
    if employment_status:
        update["employment_status"] = employment_status

    if start_date:
        update["date_employed"] = start_date
        update["start_date"] = start_date

    if exit_date:
        update["exit_info.exit_date"] = exit_date
        if not employment_status:
            update["employment_status"] = "Exited"

    # Recruitment source
    source_type = (form.get("source_type") or "").strip()
    referrer_name = (form.get("referrer_name") or "").strip()
    referrer_phone = (form.get("referrer_phone") or "").strip()

    if source_type:
        update["recruitment_source.type"] = source_type
    if referrer_name:
        update["recruitment_source.referrer_name"] = referrer_name
    if referrer_phone:
        update["recruitment_source.referrer_phone"] = referrer_phone

    # Guarantor
    guarantor_name = (form.get("guarantor_name") or "").strip()
    guarantor_phone = (form.get("guarantor_phone") or "").strip()
    guarantor_address = (form.get("guarantor_address") or "").strip()
    guarantor_relationship = (form.get("guarantor_relationship") or "").strip()

    if guarantor_name:
        update["guarantor.name"] = guarantor_name
    if guarantor_phone:
        update["guarantor.phone"] = guarantor_phone
    if guarantor_address:
        update["guarantor.address"] = guarantor_address
    if guarantor_relationship:
        update["guarantor.relationship"] = guarantor_relationship

    # Other income
    other_income = (form.get("other_income") or "").strip()
    update["other_income"] = other_income

    # Optional: star_rating + sales_level in same form (if present)
    star_rating_raw = form.get("star_rating")
    if star_rating_raw:
        try:
            rating_int = int(star_rating_raw)
            if 1 <= rating_int <= 5:
                update["star_rating"] = rating_int
        except Exception:
            pass

    sales_level = (form.get("sales_level") or "").strip()
    if sales_level in ["Beginner", "Intermediate", "Pro"]:
        update["sales_level"] = sales_level

    # Custom fields
    labels = form.getlist("custom_field_label[]")
    values = form.getlist("custom_field_value[]")
    custom_fields: List[Dict[str, Any]] = []
    for label, value in zip(labels, values):
        l = (label or "").strip()
        v = (value or "").strip()
        if l or v:
            custom_fields.append({"label": l, "value": v})
    update["custom_fields"] = custom_fields

    update["updated_at"] = now

    if update:
        users_col.update_one({"_id": oid}, {"$set": update})

    if _is_ajax(request):
        return jsonify(ok=True, message="Employee overview updated.")

    return redirect(url_for("hr.employee_profile", employee_id=employee_id))


# -------------------------------
# AJAX: STAR RATING
# (used by the stars in the overview header)
# -------------------------------
@hr_bp.route("/employee/<employee_id>/update_rating", methods=["POST"])
def update_employee_rating(employee_id):
    if not _hr_access_guard():
        return jsonify(ok=False, message="Unauthorized"), 401

    payload = request.get_json(silent=True) or {}
    rating = int(payload.get("rating", 0) or 0)
    if rating < 1 or rating > 5:
        return jsonify(ok=False, message="Invalid rating"), 400

    try:
        oid = ObjectId(employee_id)
    except Exception:
        return jsonify(ok=False, message="Invalid employee ID"), 400

    users_col.update_one(
        {"_id": oid},
        {"$set": {"star_rating": rating, "updated_at": datetime.utcnow()}},
    )
    return jsonify(ok=True, rating=rating)


# -------------------------------
# AJAX: SALES LEVEL
# (Beginner / Intermediate / Pro)
# -------------------------------
@hr_bp.route("/employee/<employee_id>/update_sales_level", methods=["POST"])
def update_employee_sales_level(employee_id):
    if not _hr_access_guard():
        return jsonify(ok=False, message="Unauthorized"), 401

    payload = request.get_json(silent=True) or {}
    level = (payload.get("sales_level") or "").strip()
    if level not in ["Beginner", "Intermediate", "Pro"]:
        return jsonify(ok=False, message="Invalid sales level"), 400

    try:
        oid = ObjectId(employee_id)
    except Exception:
        return jsonify(ok=False, message="Invalid employee ID"), 400

    users_col.update_one(
        {"_id": oid},
        {"$set": {"sales_level": level, "updated_at": datetime.utcnow()}},
    )
    return jsonify(ok=True, sales_level=level)


# -------------------------------
# PROFILE IMAGE UPLOAD (Cloudflare) – Overview
# -------------------------------
@hr_bp.route("/employee/<employee_id>/upload_profile_image", methods=["POST"])
def upload_profile_image(employee_id):
    """
    Upload main profile picture for employee.
    Saves image_url directly on employee document.
    """
    if not _hr_access_guard():
        return jsonify(ok=False, message="Unauthorized"), 401

    try:
        oid = ObjectId(employee_id)
    except Exception:
        return jsonify(ok=False, message="Invalid employee ID"), 400

    if "file" not in request.files:
        return jsonify(ok=False, message="No file part"), 400

    f = request.files["file"]
    if f.filename == "":
        return jsonify(ok=False, message="No file selected"), 400

    ext = f.filename.rsplit(".", 1)[-1].lower() if "." in f.filename else ""
    if ext not in HR_ALLOWED_EXT:
        return jsonify(
            ok=False,
            message="Only image files are allowed (png, jpg, jpeg, gif).",
        ), 400

    try:
        # Step 1: direct upload URL
        direct_url = (
            f"https://api.cloudflare.com/client/v4/accounts/"
            f"{CF_ACCOUNT_ID}/images/v2/direct_upload"
        )
        headers = {"Authorization": f"Bearer {CF_IMAGES_TOKEN}"}
        res = requests.post(direct_url, headers=headers, data={}, timeout=20)
        j = res.json()
        if not j.get("success"):
            return (
                jsonify(
                    ok=False,
                    message="Cloudflare direct_upload failed",
                    details=j,
                ),
                502,
            )

        upload_url = j["result"]["uploadURL"]
        image_id = j["result"]["id"]

        # Step 2: upload actual file
        up = requests.post(
            upload_url,
            files={
                "file": (
                    f.filename,
                    f.stream,
                    f.mimetype or "application/octet-stream",
                )
            },
            timeout=60,
        )
        uj = up.json()
        if not uj.get("success"):
            return (
                jsonify(
                    ok=False,
                    message="Cloudflare upload failed",
                    details=uj,
                ),
                502,
            )

        variant = request.args.get("variant", DEFAULT_VARIANT)
        image_url = f"https://imagedelivery.net/{CF_HASH}/{image_id}/{variant}"

        users_col.update_one(
            {"_id": oid},
            {
                "$set": {
                    "image_url": image_url,
                    "updated_at": datetime.utcnow(),
                }
            },
        )

        return jsonify(
            ok=True,
            message="Profile image updated.",
            image_url=image_url,
        )

    except Exception as e:
        tracback.print_exc()
        return jsonify(ok=False, message=str(e)), 500

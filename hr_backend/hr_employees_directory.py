# hr_backend/hr_employees_directory.py

from datetime import datetime
from typing import Dict, Any, List
import io
import csv
import math

from flask import (
    render_template,
    request,
    redirect,
    url_for,
    Response,
)
from bson import ObjectId

from db import db
from hr_backend.hr_dashboard import hr_bp, _hr_access_guard
from hr_backend.hr_roles import get_role_options, DEFAULT_ROLE_KEYS

# -------------------------------
# Collections
# -------------------------------
users_col = db["users"]

# -------------------------------
# Recruitment constants
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
def _mask_ghana_card(card: str | None) -> str:
    if not card:
        return ""
    card = card.strip()
    if len(card) <= 6:
        return card
    return card[:4] + "****" + card[-3:]


def _build_employee_query(args) -> Dict[str, Any]:
    q: Dict[str, Any] = {}

    search = (args.get("search") or "").strip()
    branch = (args.get("branch") or "").strip()
    department = (args.get("department") or "").strip()
    status = (args.get("status") or "").strip()
    role = (args.get("role") or "").strip()

    if search:
        q["$or"] = [
            {"name": {"$regex": search, "$options": "i"}},
            {"phone": {"$regex": search, "$options": "i"}},
            {"username": {"$regex": search, "$options": "i"}},
            {"employee_id": {"$regex": search, "$options": "i"}},
            {"ghana_card": {"$regex": search, "$options": "i"}},
        ]

    if branch:
        q["branch"] = branch

    if department:
        q["department"] = department

    if status:
        q["employment_status"] = status

    if role:
        q["role"] = role

    return q


def _serialize_employee(doc: dict) -> dict:
    if not doc:
        return {}

    e = {**doc}
    e["_id"] = str(doc["_id"])

    e["employee_id"] = e.get("employee_id") or e.get("username") or e["_id"][:6].upper()
    e["name"] = e.get("name") or e.get("username") or "Unnamed"
    e["phone"] = e.get("phone") or ""
    e["role"] = (e.get("role") or "").title()
    e["branch"] = e.get("branch") or "—"
    e["department"] = e.get("department") or "—"

    e["employment_status"] = e.get("employment_status") or "Active"

    src = e.get("recruitment_source") or {}
    e["source_type"] = src.get("type") or e.get("source_type") or "Unknown"
    e["referrer_name"] = src.get("referrer_name") or e.get("referrer_name")
    e["referrer_phone"] = src.get("referrer_phone") or e.get("referrer_phone")

    e["gps_address"] = e.get("gps_address") or ""
    guar = e.get("guarantor") or {}
    e["guarantor_name"] = guar.get("name")
    e["guarantor_phone"] = guar.get("phone")
    e["guarantor_address"] = guar.get("address")
    e["guarantor_relationship"] = guar.get("relationship")

    # Profile image
    e["image_url"] = e.get("image_url") or ""

    if isinstance(e.get("date_employed"), datetime):
        e["date_employed_str"] = e["date_employed"].strftime("%Y-%m-%d")
    else:
        e["date_employed_str"] = e.get("date_employed") or e.get("start_date", "")

    e["ghana_card_masked"] = _mask_ghana_card(e.get("ghana_card"))
    e["recruitment_stage"] = e.get("recruitment_stage") or "Documentation Approved"

    return e


def _compute_employee_stats() -> Dict[str, Any]:
    total = users_col.count_documents({})
    by_role = {r: users_col.count_documents({"role": r}) for r in DEFAULT_ROLE_KEYS}
    return {"total": total, "by_role": by_role}


# -------------------------------
# EMPLOYEE DIRECTORY (with pagination + AJAX)
# -------------------------------
@hr_bp.route("/employees")
def employees():
    if not _hr_access_guard():
        return redirect(url_for("login.login"))

    query = _build_employee_query(request.args)

    # Pagination params
    try:
        page = int(request.args.get("page", 1) or 1)
    except ValueError:
        page = 1
    page = max(page, 1)

    try:
        per_page = int(request.args.get("per_page", 18) or 18)
    except ValueError:
        per_page = 18
    per_page = min(max(per_page, 6), 60)

    total = users_col.count_documents(query)
    total_pages = max(1, math.ceil(total / per_page))
    if page > total_pages:
        page = total_pages

    skip = (page - 1) * per_page

    cursor = (
        users_col.find(query)
        .sort("name", 1)
        .skip(skip)
        .limit(per_page)
    )
    employees_list = [_serialize_employee(doc) for doc in cursor]

    branches = ["All Branches", "Kasoa", "Ofankor", "Lapaz"]
    departments = ["All Departments", "Sales", "HR", "Inventory", "Accounts"]
    roles = get_role_options()

    stats = _compute_employee_stats()

    # Manager list (for Add Employee modal)
    hr_managers = list(users_col.find({"role": "manager"}).sort("name", 1))

    start_index = (skip + 1) if total > 0 else 0
    end_index = min(skip + per_page, total)

    pagination = {
        "page": page,
        "per_page": per_page,
        "total": total,
        "total_pages": total_pages,
        "has_prev": page > 1,
        "has_next": page < total_pages,
        "start_index": start_index,
        "end_index": end_index,
    }

    context = {
        "employees": employees_list,
        "branches": branches,
        "departments": departments,
        "roles": roles,
        "hr_roles": roles,
        "stats": stats,
        "filters": {
            "search": request.args.get("search", ""),
            "branch": request.args.get("branch", ""),
            "department": request.args.get("department", ""),
            "status": request.args.get("status", ""),
            "role": request.args.get("role", ""),
        },
        "pagination": pagination,
        "hr_managers": hr_managers,
        "active_page": "employees",
        "hr_branches": None,
        "current_branch": None,
        "hr_content_template": "hr_pages/partials/hr_employees_inner.html",
    }

    # AJAX request → only inner partial (no shell)
    if request.headers.get("X-Requested-With") == "XMLHttpRequest":
        return render_template("hr_pages/partials/hr_employees_inner.html", **context)

    # Normal request / refresh → full HR shell (includes inner partial)
    return render_template("hr_pages/hr_shell.html", **context)


# -------------------------------
# EXPORT EMPLOYEES TO CSV
# -------------------------------
@hr_bp.route("/employees/export")
def employees_export():
    if not _hr_access_guard():
        return redirect(url_for("login.login"))

    query = _build_employee_query(request.args)
    cursor = users_col.find(query).sort("name", 1)

    output = io.StringIO()
    writer = csv.writer(output)

    writer.writerow(
        [
            "Employee ID",
            "Name",
            "Role",
            "Branch",
            "Department",
            "Status",
            "Phone",
            "Email",
            "GPS Address",
            "Guarantor Name",
            "Guarantor Phone",
            "Guarantor Address",
            "Recruitment Stage",
            "Start Date",
        ]
    )

    for doc in cursor:
        e = _serialize_employee(doc)
        writer.writerow(
            [
                e.get("employee_id", ""),
                e.get("name", ""),
                e.get("role", ""),
                e.get("branch", ""),
                e.get("department", ""),
                e.get("employment_status", ""),
                e.get("phone", ""),
                doc.get("email", ""),
                e.get("gps_address", ""),
                e.get("guarantor_name", ""),
                e.get("guarantor_phone", ""),
                e.get("guarantor_address", ""),
                e.get("recruitment_stage", ""),
                e.get("date_employed_str", ""),
            ]
        )

    csv_data = output.getvalue()
    output.close()

    filename = f"employees_export_{datetime.utcnow().strftime('%Y%m%d')}.csv"
    resp = Response(csv_data, mimetype="text/csv")
    resp.headers["Content-Disposition"] = f"attachment; filename={filename}"
    return resp

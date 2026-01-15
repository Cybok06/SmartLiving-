# hr_backend/hr_exits.py
from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from bson import ObjectId
from flask import jsonify, request, render_template

from db import db
from services.activity_audit import audit_action
from hr_backend.hr_dashboard import hr_bp, _hr_access_guard

# ---------------------------
# Collections
# ---------------------------
users_col      = db["users"]
customers_col  = db["customers"]
cases_col      = db.get_collection("hr_cases") if "hr_cases" in db.list_collection_names() else db.get_collection("cases")
assets_col     = db.get_collection("assets") if "assets" in db.list_collection_names() else None
debts_col      = db.get_collection("hr_debts") if "hr_debts" in db.list_collection_names() else None
exits_col      = db["hr_exits"]  # main exits store


# ---------------------------
# Helpers
# ---------------------------
def _now() -> datetime:
    return datetime.utcnow()


def _safe_oid(x: Any) -> Optional[ObjectId]:
    try:
        return ObjectId(str(x))
    except Exception:
        return None


def _dt_to_str(dt: Any) -> str:
    if isinstance(dt, datetime):
        return dt.isoformat()
    return str(dt) if dt else ""


def _audit(action: str, by: str = "HR", note: str = "") -> Dict[str, Any]:
    return {"action": action, "by": by, "at": _now(), "note": note}


EXIT_STATUSES = [
    "Initiated",
    "Handover In Progress",
    "Clearance Check",
    "Final Settlement",
    "Closed",
]

EXIT_TYPES = [
    "Resignation",
    "Termination",
    "Absconded",
    "Contract Ended",
    "Other",
]

CLEARANCE_KEYS = [
    "cases_checked",
    "debts_checked",
    "assets_returned",
    "access_disabled",
    "documents_archived",
    "customers_transferred",  # ✅ must be system-verified
]


def _employee_snapshot(user_doc: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "user_id": str(user_doc.get("_id")),
        "name": user_doc.get("name") or "",
        "employee_code": user_doc.get("employee_code") or user_doc.get("staff_id") or "",
        "role": user_doc.get("role") or user_doc.get("position") or "",
        "branch": user_doc.get("branch") or user_doc.get("store_name") or user_doc.get("store") or "",
        "phone": user_doc.get("phone") or user_doc.get("mobile") or "",
        "email": user_doc.get("email") or "",
        "image_url": user_doc.get("image_url") or "",
        "created_at": _dt_to_str(user_doc.get("created_at")),
    }


def _compute_customer_query(employee_oid: ObjectId) -> Dict[str, Any]:
    """
    Ownership fields differ across deployments; we try common patterns safely.
    """
    sid = str(employee_oid)
    return {
        "$or": [
            {"agent_id": sid},
            {"agent_id": employee_oid},
            {"assigned_agent_id": sid},
            {"assigned_agent_id": employee_oid},
            {"assigned_to": sid},
            {"assigned_to": employee_oid},
            {"assigned_to_id": sid},
            {"assigned_to_id": employee_oid},
            {"user_id": sid},
            {"user_id": employee_oid},
        ]
    }


def _customer_count_for_user(employee_oid: ObjectId) -> int:
    try:
        cq = _compute_customer_query(employee_oid)
        return int(customers_col.count_documents(cq))
    except Exception:
        return 0


def _sample_customer_ids_for_user(employee_oid: ObjectId, limit: int = 5) -> List[str]:
    """
    Helpful debugging for HR when verification fails.
    """
    try:
        cq = _compute_customer_query(employee_oid)
        cur = customers_col.find(cq, {"_id": 1}).sort("_id", -1).limit(max(1, min(10, limit)))
        return [str(x.get("_id")) for x in cur]
    except Exception:
        return []


def _set_user_exited(user_oid: ObjectId, exit_id: ObjectId) -> None:
    users_col.update_one(
        {"_id": user_oid},
        {
            "$set": {
                "is_active": False,
                "employment_status": "Exited",
                "account_locked": True,
                "exit_ref_id": str(exit_id),
                "updated_at": _now(),
            }
        },
    )


def _refresh_clearance_counts(employee_oid: ObjectId) -> Dict[str, int]:
    # cases open
    cases_open = 0
    try:
        if cases_col:
            cases_open = cases_col.count_documents({
                "is_deleted": {"$ne": True},
                "status": {"$nin": ["Closed", "Resolved", "Done"]},
                "$or": [
                    {"employee_id": str(employee_oid)},
                    {"employee_id": employee_oid},
                    {"user_id": str(employee_oid)},
                    {"user_id": employee_oid},
                ],
            })
    except Exception:
        cases_open = 0

    # debts open
    debts_open = 0
    try:
        if debts_col:
            debts_open = debts_col.count_documents({
                "is_deleted": {"$ne": True},
                "status": {"$nin": ["Closed", "Paid", "Resolved", "Done"]},
                "$or": [
                    {"employee_id": str(employee_oid)},
                    {"employee_id": employee_oid},
                    {"user_id": str(employee_oid)},
                    {"user_id": employee_oid},
                ],
            })
    except Exception:
        debts_open = 0

    # assets open (not returned)
    assets_open = 0
    try:
        if assets_col:
            assets_open = assets_col.count_documents({
                "is_deleted": {"$ne": True},
                "$or": [
                    {"assigned_to": str(employee_oid)},
                    {"assigned_to": employee_oid},
                    {"user_id": str(employee_oid)},
                    {"user_id": employee_oid},
                ],
                "status": {"$nin": ["Returned", "Closed", "Resolved"]},
            })
    except Exception:
        assets_open = 0

    return {
        "cases_open_count": int(cases_open),
        "debts_open_count": int(debts_open),
        "assets_open_count": int(assets_open),
    }


def _transfers_block(doc: Dict[str, Any], remaining_customers_count: Optional[int] = None) -> Dict[str, Any]:
    transfers = doc.get("transfers") or {}
    records = transfers.get("records") or transfers.get("items") or []  # backward compat
    # NOTE: old "items" represented per-customer transfers; we now use "records" for transfer notes only
    if transfers.get("items") and not transfers.get("records"):
        # keep old items visible but do not treat as records
        records = transfers.get("records") or []

    return {
        "total_customers": int(transfers.get("total_customers") or 0),  # snapshot at exit creation
        "transferred_count": int(transfers.get("transferred_count") or 0),
        "pending_count": int(transfers.get("pending_count") or 0),
        "remaining_customers_count": int(remaining_customers_count if remaining_customers_count is not None else (transfers.get("pending_count") or 0)),
        "last_transfer_to_user_id": transfers.get("last_transfer_to_user_id") or "",
        "last_transfer_to_name": transfers.get("last_transfer_to_name") or "",
        "last_verified_at": _dt_to_str(transfers.get("last_verified_at")),
        "records": records,  # [{to_user_id,to_name,transferred_on,by,at,note}]
    }


def _serialize_exit(doc: Dict[str, Any], remaining_customers_count: Optional[int] = None) -> Dict[str, Any]:
    clearance = doc.get("clearance") or {}
    transfers_obj = _transfers_block(doc, remaining_customers_count=remaining_customers_count)

    return {
        "id": str(doc.get("_id")),
        "status": doc.get("status") or "Initiated",
        "exit_type": doc.get("exit_type") or "Other",
        "reason": doc.get("reason") or "",
        "notice_date": _dt_to_str(doc.get("notice_date")),
        "last_working_day": _dt_to_str(doc.get("last_working_day")),
        "initiated_at": _dt_to_str(doc.get("initiated_at")),
        "closed_at": _dt_to_str(doc.get("closed_at")),
        "created_at": _dt_to_str(doc.get("created_at")),
        "updated_at": _dt_to_str(doc.get("updated_at")),

        "employee": doc.get("employee") or {},

        "clearance": {
            "checks": clearance.get("checks") or {k: False for k in CLEARANCE_KEYS},
            "notes": clearance.get("notes") or "",
            "overrides": clearance.get("overrides") or [],  # [{key, by, at, reason}]
            "cases_open_count": int(clearance.get("cases_open_count") or 0),
            "debts_open_count": int(clearance.get("debts_open_count") or 0),
            "assets_open_count": int(clearance.get("assets_open_count") or 0),
        },

        "transfers": transfers_obj,

        "settlement": doc.get("settlement") or {
            "salary_due": 0,
            "commission_due": 0,
            "deductions": 0,
            "net_pay": 0,
            "paid": False,
            "paid_at": "",
            "method": "",
            "reference": "",
            "note": "",
        },

        "audit": doc.get("audit") or [],
    }


# ---------------------------
# Page
# ---------------------------
@hr_bp.route("/exits", methods=["GET"], endpoint="exits_page")
def exits_page():
    if not _hr_access_guard():
        return render_template("unauthorized.html"), 401
    return render_template("hr_pages/hr_exits_page.html")


# ---------------------------
# Employees list (picker)
# ---------------------------
@hr_bp.route("/exits/employees_list", methods=["GET"], endpoint="exits_employees_list")
def exits_employees_list():
    if not _hr_access_guard():
        return jsonify(ok=False, message="Unauthorized"), 401

    cursor = users_col.find(
        {"is_deleted": {"$ne": True}},
        {"name": 1, "role": 1, "position": 1, "branch": 1, "store_name": 1, "store": 1,
         "employee_code": 1, "staff_id": 1, "employment_status": 1, "is_active": 1},
    ).sort("name", 1)

    employees: List[Dict[str, Any]] = []
    branches = set()
    roles = set()

    for u in cursor:
        br = u.get("branch") or u.get("store_name") or u.get("store") or ""
        rl = u.get("role") or u.get("position") or ""
        if br:
            branches.add(br)
        if rl:
            roles.add(rl)

        employees.append({
            "id": str(u["_id"]),
            "name": u.get("name") or "",
            "branch": br,
            "role": rl,
            "employee_code": u.get("employee_code") or u.get("staff_id") or "",
            "is_active": bool(u.get("is_active", True)),
            "employment_status": u.get("employment_status") or "",
        })

    return jsonify(ok=True, employees=employees, branches=sorted(list(branches)), roles=sorted(list(roles)))


# ---------------------------
# Create exit
# ---------------------------
@hr_bp.route("/exits/create", methods=["POST"], endpoint="exits_create")
@audit_action("hr_exit.created", "Created HR Exit", entity_type="hr_exit")
def exits_create():
    if not _hr_access_guard():
        return jsonify(ok=False, message="Unauthorized"), 401

    payload = request.get_json(silent=True) or {}
    employee_id = (payload.get("employee_id") or "").strip()
    exit_type = (payload.get("exit_type") or "Other").strip()
    reason = (payload.get("reason") or "").strip()
    notice_date = (payload.get("notice_date") or "").strip()
    last_working_day = (payload.get("last_working_day") or "").strip()

    emp_oid = _safe_oid(employee_id)
    if not emp_oid:
        return jsonify(ok=False, message="Invalid employee ID."), 400

    emp = users_col.find_one({"_id": emp_oid, "is_deleted": {"$ne": True}})
    if not emp:
        return jsonify(ok=False, message="Employee not found."), 404

    if exit_type not in EXIT_TYPES:
        exit_type = "Other"

    def parse_date(s: str) -> Optional[datetime]:
        if not s:
            return None
        s = str(s).strip()
        try:
            if "T" in s:
                return datetime.strptime(s[:16], "%Y-%m-%dT%H:%M")
            return datetime.strptime(s[:10], "%Y-%m-%d")
        except Exception:
            return None

    nd = parse_date(notice_date)
    lwd = parse_date(last_working_day)

    # prevent duplicate open exit for same employee
    open_exists = exits_col.find_one({
        "is_deleted": {"$ne": True},
        "employee.user_id": str(emp_oid),
        "status": {"$ne": "Closed"},
    })
    if open_exists:
        return jsonify(ok=False, message="This employee already has an open exit record."), 400

    now = _now()
    snapshot = _employee_snapshot(emp)

    clearance_counts = _refresh_clearance_counts(emp_oid)
    initial_customers = _customer_count_for_user(emp_oid)

    doc = {
        "employee": snapshot,
        "status": "Initiated",
        "exit_type": exit_type,
        "reason": reason,
        "notice_date": nd,
        "last_working_day": lwd,

        "initiated_at": now,
        "closed_at": None,

        "clearance": {
            "checks": {k: False for k in CLEARANCE_KEYS},
            "notes": "",
            "overrides": [],
            **clearance_counts,
        },

        # ✅ Managers do transfers; HR only records notes + verifies remaining customers
        "transfers": {
            "records": [],  # [{to_user_id,to_name,transferred_on,by,at,note}]
            "total_customers": int(initial_customers),  # snapshot at exit initiation
            "transferred_count": 0,
            "pending_count": int(initial_customers),  # updated on get/verify
            "last_transfer_to_user_id": "",
            "last_transfer_to_name": "",
            "last_verified_at": None,
        },

        "settlement": {
            "salary_due": 0,
            "commission_due": 0,
            "deductions": 0,
            "net_pay": 0,
            "paid": False,
            "paid_at": None,
            "method": "",
            "reference": "",
            "note": "",
        },

        "audit": [
            _audit("Exit initiated", "HR", f"{exit_type}" + (f" • {reason}" if reason else "")),
        ],

        "is_deleted": False,
        "created_at": now,
        "updated_at": now,
    }

    ins = exits_col.insert_one(doc)

    # lock & mark user exited immediately
    _set_user_exited(emp_oid, ins.inserted_id)

    return jsonify(ok=True, message="Exit created and employee locked.", exit_id=str(ins.inserted_id))


# ---------------------------
# List exits (filters + KPIs)
# ---------------------------
@hr_bp.route("/exits/list", methods=["GET"], endpoint="exits_list")
def exits_list():
    if not _hr_access_guard():
        return jsonify(ok=False, message="Unauthorized"), 401

    q = (request.args.get("q") or "").strip().lower()
    status = (request.args.get("status") or "").strip()
    exit_type = (request.args.get("exit_type") or "").strip()
    branch = (request.args.get("branch") or "").strip()
    limit = request.args.get("limit") or "200"

    try:
        limit_i = max(20, min(1000, int(limit)))
    except Exception:
        limit_i = 200

    base = {"is_deleted": {"$ne": True}}
    cur = exits_col.find(base).sort("created_at", -1).limit(limit_i)

    rows: List[Dict[str, Any]] = []
    kpis = {"total": 0, "open": 0, "closed": 0, "pending_transfer": 0, "pending_clearance": 0}

    for doc in cur:
        row = _serialize_exit(doc, remaining_customers_count=None)

        if branch and (row.get("employee") or {}).get("branch") != branch:
            continue
        if status and row.get("status") != status:
            continue
        if exit_type and row.get("exit_type") != exit_type:
            continue
        if q:
            hay = " ".join([
                (row.get("employee") or {}).get("name", ""),
                (row.get("employee") or {}).get("employee_code", ""),
                row.get("exit_type", ""),
                row.get("status", ""),
                row.get("reason", ""),
            ]).lower()
            if q not in hay:
                continue

        # KPIs
        kpis["total"] += 1
        if row.get("status") == "Closed":
            kpis["closed"] += 1
        else:
            kpis["open"] += 1

        # use stored pending_count for list view (fast)
        if (row.get("transfers") or {}).get("pending_count", 0) > 0:
            kpis["pending_transfer"] += 1

        checks = ((row.get("clearance") or {}).get("checks") or {})
        if not all(bool(checks.get(k)) for k in CLEARANCE_KEYS):
            kpis["pending_clearance"] += 1

        rows.append(row)

    meta = {"statuses": EXIT_STATUSES, "types": EXIT_TYPES}
    return jsonify(ok=True, exits=rows, kpis=kpis, meta=meta)


# ---------------------------
# Get exit detail (live transfer remaining count)
# ---------------------------
@hr_bp.route("/exits/<exit_id>/get", methods=["GET"], endpoint="exits_get")
def exits_get(exit_id):
    if not _hr_access_guard():
        return jsonify(ok=False, message="Unauthorized"), 401

    oid = _safe_oid(exit_id)
    if not oid:
        return jsonify(ok=False, message="Invalid exit ID."), 400

    doc = exits_col.find_one({"_id": oid, "is_deleted": {"$ne": True}})
    if not doc:
        return jsonify(ok=False, message="Exit record not found."), 404

    emp_oid = _safe_oid((doc.get("employee") or {}).get("user_id"))

    remaining = None
    if emp_oid:
        # refresh dynamic counts
        counts = _refresh_clearance_counts(emp_oid)
        remaining = _customer_count_for_user(emp_oid)

        exits_col.update_one(
            {"_id": oid},
            {"$set": {
                "clearance.cases_open_count": counts["cases_open_count"],
                "clearance.debts_open_count": counts["debts_open_count"],
                "clearance.assets_open_count": counts["assets_open_count"],
                "transfers.pending_count": int(remaining),
                "updated_at": _now(),
            }},
        )

        # update doc in memory too
        doc["clearance"] = doc.get("clearance") or {}
        doc["clearance"]["cases_open_count"] = counts["cases_open_count"]
        doc["clearance"]["debts_open_count"] = counts["debts_open_count"]
        doc["clearance"]["assets_open_count"] = counts["assets_open_count"]
        doc["transfers"] = doc.get("transfers") or {}
        doc["transfers"]["pending_count"] = int(remaining)

    return jsonify(ok=True, exit=_serialize_exit(doc, remaining_customers_count=remaining))


# ---------------------------
# Update exit status / stage
# ---------------------------
@hr_bp.route("/exits/<exit_id>/status", methods=["POST"], endpoint="exits_update_status")
def exits_update_status(exit_id):
    if not _hr_access_guard():
        return jsonify(ok=False, message="Unauthorized"), 401

    oid = _safe_oid(exit_id)
    if not oid:
        return jsonify(ok=False, message="Invalid exit ID."), 400

    payload = request.get_json(silent=True) or {}
    status = (payload.get("status") or "").strip()
    note = (payload.get("note") or "").strip()

    if status not in EXIT_STATUSES:
        return jsonify(ok=False, message="Invalid status."), 400

    now = _now()
    upd = {
        "$set": {"status": status, "updated_at": now},
        "$push": {"audit": {"$each": [_audit(f"Exit status → {status}", "HR", note)], "$position": 0}},
    }

    res = exits_col.update_one({"_id": oid, "is_deleted": {"$ne": True}}, upd)
    if not res.matched_count:
        return jsonify(ok=False, message="Exit record not found."), 404

    return jsonify(ok=True, message="Exit stage updated.")


# ---------------------------
# Clearance: update checks + notes + override
# ✅ customers_transferred is protected by DB verification
# ---------------------------
@hr_bp.route("/exits/<exit_id>/clearance", methods=["POST"], endpoint="exits_update_clearance")
def exits_update_clearance(exit_id):
    if not _hr_access_guard():
        return jsonify(ok=False, message="Unauthorized"), 401

    oid = _safe_oid(exit_id)
    if not oid:
        return jsonify(ok=False, message="Invalid exit ID."), 400

    payload = request.get_json(silent=True) or {}
    checks_in = payload.get("checks") or {}
    notes = (payload.get("notes") or "").strip()
    override_key = (payload.get("override_key") or "").strip()
    override_reason = (payload.get("override_reason") or "").strip()

    doc = exits_col.find_one({"_id": oid, "is_deleted": {"$ne": True}})
    if not doc:
        return jsonify(ok=False, message="Exit record not found."), 404

    current = (doc.get("clearance") or {}).get("checks") or {k: False for k in CLEARANCE_KEYS}

    # enforce protected marking for customers_transferred
    wants_mark_customers_transferred = ("customers_transferred" in checks_in and bool(checks_in.get("customers_transferred")))

    if wants_mark_customers_transferred:
        emp_oid = _safe_oid((doc.get("employee") or {}).get("user_id"))
        if emp_oid:
            remaining = _customer_count_for_user(emp_oid)
            if remaining > 0:
                sample = _sample_customer_ids_for_user(emp_oid, limit=5)
                return jsonify(
                    ok=False,
                    message=f"Cannot mark customers_transferred. {remaining} customer(s) still assigned to this staff. Transfer must be completed by Manager first.",
                    remaining=remaining,
                    sample_customer_ids=sample,
                ), 400
        else:
            return jsonify(ok=False, message="Cannot verify customer ownership for this exit file."), 400

    for k in CLEARANCE_KEYS:
        if k in checks_in:
            current[k] = bool(checks_in.get(k))

    upd: Dict[str, Any] = {
        "$set": {"clearance.checks": current, "clearance.notes": notes, "updated_at": _now()}
    }

    audit_note = "Clearance updated"
    if override_key and override_key in CLEARANCE_KEYS and override_reason:
        ov = {"key": override_key, "by": "HR", "at": _now(), "reason": override_reason}
        upd.setdefault("$push", {})
        upd["$push"]["clearance.overrides"] = {"$each": [ov], "$position": 0}
        audit_note = f"Override: {override_key} • {override_reason}"

    upd.setdefault("$push", {})
    upd["$push"]["audit"] = {"$each": [_audit("Clearance updated", "HR", audit_note)], "$position": 0}

    exits_col.update_one({"_id": oid}, upd)
    return jsonify(ok=True, message="Clearance saved.")


# ---------------------------
# Transfers (NEW): add transfer record (notes only)
# ---------------------------
@hr_bp.route("/exits/<exit_id>/transfer/record", methods=["POST"], endpoint="exits_transfer_add_record")
def exits_transfer_add_record(exit_id):
    if not _hr_access_guard():
        return jsonify(ok=False, message="Unauthorized"), 401

    oid = _safe_oid(exit_id)
    if not oid:
        return jsonify(ok=False, message="Invalid exit ID."), 400

    payload = request.get_json(silent=True) or {}
    to_user_id = (payload.get("to_user_id") or "").strip()
    transferred_on = (payload.get("transferred_on") or "").strip()  # YYYY-MM-DD
    note = (payload.get("note") or "").strip()

    to_oid = _safe_oid(to_user_id)
    if not to_oid:
        return jsonify(ok=False, message="Select a valid target staff."), 400
    if not transferred_on or len(transferred_on) < 10:
        return jsonify(ok=False, message="Transfer date is required."), 400

    to_user = users_col.find_one({"_id": to_oid, "is_deleted": {"$ne": True}}, {"name": 1})
    if not to_user:
        return jsonify(ok=False, message="Target staff not found."), 404

    doc = exits_col.find_one({"_id": oid, "is_deleted": {"$ne": True}})
    if not doc:
        return jsonify(ok=False, message="Exit record not found."), 404

    rec = {
        "to_user_id": str(to_oid),
        "to_name": to_user.get("name") or "",
        "transferred_on": transferred_on[:10],
        "by": "HR",
        "at": _now(),
        "note": note,
    }

    exits_col.update_one(
        {"_id": oid},
        {
            "$push": {"transfers.records": {"$each": [rec], "$position": 0},
                      "audit": {"$each": [_audit("Transfer record added", "HR", f"To: {to_user.get('name') or ''}")], "$position": 0}},
            "$set": {
                "transfers.last_transfer_to_user_id": str(to_oid),
                "transfers.last_transfer_to_name": to_user.get("name") or "",
                "updated_at": _now(),
            },
        },
    )

    return jsonify(ok=True, message="Transfer record added.")


# ---------------------------
# Transfers (NEW): verify remaining customers and mark customers_transferred if zero
# ---------------------------
@hr_bp.route("/exits/<exit_id>/transfer/verify", methods=["POST"], endpoint="exits_transfer_verify")
def exits_transfer_verify(exit_id):
    if not _hr_access_guard():
        return jsonify(ok=False, message="Unauthorized"), 401

    oid = _safe_oid(exit_id)
    if not oid:
        return jsonify(ok=False, message="Invalid exit ID."), 400

    doc = exits_col.find_one({"_id": oid, "is_deleted": {"$ne": True}})
    if not doc:
        return jsonify(ok=False, message="Exit record not found."), 404

    emp_oid = _safe_oid((doc.get("employee") or {}).get("user_id"))
    if not emp_oid:
        return jsonify(ok=False, message="Employee reference is invalid."), 400

    remaining = _customer_count_for_user(emp_oid)
    sample = _sample_customer_ids_for_user(emp_oid, limit=5)

    now = _now()
    if remaining > 0:
        # update pending_count snapshot for dashboards
        exits_col.update_one(
            {"_id": oid},
            {"$set": {"transfers.pending_count": int(remaining), "transfers.last_verified_at": now, "updated_at": now},
             "$push": {"audit": {"$each": [_audit("Transfer verification failed", "HR", f"Remaining: {remaining}")], "$position": 0}}},
        )
        return jsonify(
            ok=False,
            message=f"Verification failed: {remaining} customer(s) still assigned to exiting staff.",
            remaining=remaining,
            sample_customer_ids=sample,
        ), 400

    # ✅ success: mark customers_transferred
    exits_col.update_one(
        {"_id": oid},
        {"$set": {
            "clearance.checks.customers_transferred": True,
            "transfers.pending_count": 0,
            "transfers.last_verified_at": now,
            "updated_at": now,
        },
         "$push": {"audit": {"$each": [_audit("Customers transferred verified", "HR", "0 customers remaining")], "$position": 0}}},
    )
    return jsonify(ok=True, message="Verified: 0 customers remaining. customers_transferred marked.")


# ---------------------------
# Legacy transfer endpoints: keep safe (do not break old calls)
# ---------------------------
@hr_bp.route("/exits/<exit_id>/transfer/target", methods=["POST"], endpoint="exits_transfer_set_target")
def exits_transfer_set_target(exit_id):
    # kept for compatibility; HR no longer sets a single target used for DB transfer
    if not _hr_access_guard():
        return jsonify(ok=False, message="Unauthorized"), 401
    return jsonify(ok=False, message="Transfer is handled by Managers. Use 'Transfer Notes' tab to record a transfer and verify."), 400


@hr_bp.route("/exits/<exit_id>/transfer/bulk", methods=["POST"], endpoint="exits_transfer_bulk")
def exits_transfer_bulk(exit_id):
    if not _hr_access_guard():
        return jsonify(ok=False, message="Unauthorized"), 401
    return jsonify(ok=False, message="Bulk transfer is handled by Managers. HR can only record transfer notes and verify."), 400


@hr_bp.route("/exits/<exit_id>/transfer/one", methods=["POST"], endpoint="exits_transfer_one")
def exits_transfer_one(exit_id):
    if not _hr_access_guard():
        return jsonify(ok=False, message="Unauthorized"), 401
    return jsonify(ok=False, message="Customer transfer is handled by Managers. HR can only record transfer notes and verify."), 400


# ---------------------------
# Settlement update
# ---------------------------
@hr_bp.route("/exits/<exit_id>/settlement", methods=["POST"], endpoint="exits_update_settlement")
def exits_update_settlement(exit_id):
    if not _hr_access_guard():
        return jsonify(ok=False, message="Unauthorized"), 401

    oid = _safe_oid(exit_id)
    if not oid:
        return jsonify(ok=False, message="Invalid exit ID."), 400

    payload = request.get_json(silent=True) or {}

    def num(x) -> float:
        try:
            return float(x)
        except Exception:
            return 0.0

    salary_due = num(payload.get("salary_due"))
    commission_due = num(payload.get("commission_due"))
    deductions = num(payload.get("deductions"))
    net_pay = salary_due + commission_due - deductions

    paid = bool(payload.get("paid", False))
    method = (payload.get("method") or "").strip()
    reference = (payload.get("reference") or "").strip()
    note = (payload.get("note") or "").strip()

    now = _now()
    upd = {
        "$set": {
            "settlement.salary_due": salary_due,
            "settlement.commission_due": commission_due,
            "settlement.deductions": deductions,
            "settlement.net_pay": net_pay,
            "settlement.paid": paid,
            "settlement.method": method,
            "settlement.reference": reference,
            "settlement.note": note,
            "settlement.paid_at": now if paid else None,
            "updated_at": now,
        },
        "$push": {"audit": {"$each": [_audit("Settlement updated", "HR", ("PAID" if paid else "Not paid"))], "$position": 0}},
    }

    exits_col.update_one({"_id": oid, "is_deleted": {"$ne": True}}, upd)
    return jsonify(ok=True, message="Settlement saved.", net_pay=net_pay)


# ---------------------------
# Close exit (with guards)
# ---------------------------
@hr_bp.route("/exits/<exit_id>/close", methods=["POST"], endpoint="exits_close")
@audit_action("hr_exit.closed", "Closed HR Exit", entity_type="hr_exit", entity_id_from="exit_id")
def exits_close(exit_id):
    if not _hr_access_guard():
        return jsonify(ok=False, message="Unauthorized"), 401

    oid = _safe_oid(exit_id)
    if not oid:
        return jsonify(ok=False, message="Invalid exit ID."), 400

    doc = exits_col.find_one({"_id": oid, "is_deleted": {"$ne": True}})
    if not doc:
        return jsonify(ok=False, message="Exit record not found."), 404

    checks = ((doc.get("clearance") or {}).get("checks") or {k: False for k in CLEARANCE_KEYS})
    overrides = (doc.get("clearance") or {}).get("overrides") or []
    overridden_keys = set(x.get("key") for x in overrides if x.get("key"))

    # hard-guard customers_transferred: must be verified OR explicitly overridden
    if not bool(checks.get("customers_transferred")) and ("customers_transferred" not in overridden_keys):
        emp_oid = _safe_oid((doc.get("employee") or {}).get("user_id"))
        if emp_oid:
            remaining = _customer_count_for_user(emp_oid)
            if remaining > 0:
                return jsonify(ok=False, message=f"Cannot close exit. {remaining} customer(s) still assigned to exiting staff."), 400

    missing = [k for k in CLEARANCE_KEYS if not bool(checks.get(k)) and k not in overridden_keys]
    if missing:
        return jsonify(ok=False, message="Cannot close exit. Pending clearance: " + ", ".join(missing)), 400

    now = _now()
    exits_col.update_one(
        {"_id": oid},
        {
            "$set": {"status": "Closed", "closed_at": now, "updated_at": now},
            "$push": {"audit": {"$each": [_audit("Exit closed", "HR")], "$position": 0}},
        },
    )
    return jsonify(ok=True, message="Exit closed successfully.")


# ---------------------------
# Soft delete exit record (rare)
# ---------------------------
@hr_bp.route("/exits/<exit_id>/delete", methods=["POST"], endpoint="exits_delete")
def exits_delete(exit_id):
    if not _hr_access_guard():
        return jsonify(ok=False, message="Unauthorized"), 401

    oid = _safe_oid(exit_id)
    if not oid:
        return jsonify(ok=False, message="Invalid exit ID."), 400

    now = _now()
    res = exits_col.update_one(
        {"_id": oid},
        {"$set": {"is_deleted": True, "updated_at": now},
         "$push": {"audit": {"$each": [_audit("Exit deleted", "HR")], "$position": 0}}},
    )
    if not res.matched_count:
        return jsonify(ok=False, message="Exit record not found."), 404

    return jsonify(ok=True, message="Exit deleted.")

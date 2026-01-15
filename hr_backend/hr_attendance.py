# hr_backend/hr_attendance.py
from datetime import datetime, date, timedelta
from typing import Any, Dict, List, Optional, Tuple

from flask import jsonify, request, render_template
from bson import ObjectId

from db import db
from services.activity_audit import audit_action
from hr_backend.hr_dashboard import hr_bp, _hr_access_guard

users_col       = db["users"]
payments_col    = db["payments"]
meetings_col    = db["hr_meeting_attendance"]      # meeting attendance (global page)
manual_att_col  = db["hr_manual_attendance"]       # manual attendance overrides
audit_col       = db["hr_audit_logs"]              # ✅ NEW: audit trail logs
critical_col    = db["hr_critical_days"]           # ✅ NEW: critical days config
leave_settings  = db["hr_leave_settings"]          # ✅ NEW: org leave policy defaults (types, carry-over)


# -------------------------------
# Constants / Defaults
# -------------------------------
LEAVE_TYPES = [
    "Annual",
    "Sick",
    "Maternity",
    "Paternity",
    "Emergency",
    "Unpaid",
    "Study",
    "Compassionate",
]

LEAVE_STATUSES = ["Draft", "Pending", "Approved", "Rejected", "Cancelled"]


# -------------------------------
# Helpers
# -------------------------------
def _date_from_str(s: str) -> Optional[date]:
    if not s:
        return None
    try:
        return datetime.strptime(str(s)[:10], "%Y-%m-%d").date()
    except Exception:
        return None


def _safe_oid(s: str) -> Optional[ObjectId]:
    try:
        return ObjectId(s)
    except Exception:
        return None


def _now() -> datetime:
    return datetime.utcnow()


def _year_bounds(y: int) -> Tuple[str, str]:
    return (f"{y}-01-01", f"{y}-12-31")


def _actor_name() -> str:
    """
    Best effort. If you later store session user, replace with session["user"]["name"] etc.
    """
    return (request.headers.get("X-Actor-Name") or "HR")


def _audit(action: str, entity: str, entity_id: str = "", meta: Optional[Dict[str, Any]] = None):
    try:
        audit_col.insert_one({
            "action": action,
            "entity": entity,
            "entity_id": entity_id,
            "meta": meta or {},
            "actor": _actor_name(),
            "created_at": _now(),
        })
    except Exception:
        # never block primary flow due to audit failure
        pass


def _leave_end_date(start_d: date, days_int: int) -> date:
    return start_d + timedelta(days=max(0, days_int - 1))


def _overlaps(a_start: date, a_end: date, b_start: date, b_end: date) -> bool:
    # overlap if ranges intersect
    return not (a_end < b_start or b_end < a_start)


def _get_org_leave_policy() -> Dict[str, Any]:
    """
    Org-wide defaults:
      {
        "types": {
          "Annual": {"max_days": 18},
          "Sick": {"max_days": 10},
          ...
        },
        "carry_over_max": 5
      }
    """
    doc = leave_settings.find_one({"key": "org_leave_policy"}) or {}
    policy = doc.get("policy") or {}

    # defaults if nothing saved
    if not policy.get("types"):
        policy["types"] = {t: {"max_days": None} for t in LEAVE_TYPES}
    if policy.get("carry_over_max") is None:
        policy["carry_over_max"] = 5

    return policy


def _get_employee_leave_policy(emp: Dict[str, Any]) -> Dict[str, Any]:
    """
    Employee policy overrides:
      emp.leave_policy = {
        "types": {
          "Annual": {"max_days": 24},
          "Sick": {"max_days": 12},
        },
        "carry_over_max": 5
      }
    Falls back to org policy if not set.
    """
    org = _get_org_leave_policy()
    ep = (emp.get("leave_policy") or {})

    types = org.get("types") or {}
    # merge overrides
    for t, cfg in (ep.get("types") or {}).items():
        types.setdefault(t, {})
        if cfg is not None:
            types[t].update(cfg)

    carry = ep.get("carry_over_max", org.get("carry_over_max", 5))

    return {"types": types, "carry_over_max": carry}


def _calc_leave_balances(emp: Dict[str, Any], year: int) -> Dict[str, Any]:
    """
    Returns balances per type (Approved only for used), and pending counts.
    Carry-over rule applies only to Annual by default (simple and common).
    """
    year_start, year_end = _year_bounds(year)
    prev_year_start, prev_year_end = _year_bounds(year - 1)

    policy = _get_employee_leave_policy(emp)
    types_cfg = policy.get("types") or {}
    carry_max = int(policy.get("carry_over_max") or 0)

    leaves = emp.get("leaves") or []

    # used + pending by type in current year
    used: Dict[str, int] = {t: 0 for t in LEAVE_TYPES}
    pending: Dict[str, int] = {t: 0 for t in LEAVE_TYPES}

    for lv in leaves:
        sd_raw = str(lv.get("start_date") or "")[:10]
        if not sd_raw:
            continue

        lt = (lv.get("leave_type") or "Annual").strip()
        if lt not in used:
            used[lt] = 0
            pending[lt] = 0

        status = (lv.get("status") or "Approved").strip()
        try:
            days_val = int(lv.get("days") or 0)
        except Exception:
            days_val = 0
        if days_val <= 0:
            continue

        if year_start <= sd_raw <= year_end:
            if status == "Approved":
                used[lt] = used.get(lt, 0) + days_val
            elif status in ("Pending", "Draft"):
                pending[lt] = pending.get(lt, 0) + days_val

    # carry-over: compute unused annual from previous year and carry to current year (cap)
    annual_entitlement = types_cfg.get("Annual", {}).get("max_days")
    try:
        annual_entitlement_int = int(annual_entitlement) if annual_entitlement is not None else None
    except Exception:
        annual_entitlement_int = None

    prev_used_annual = 0
    for lv in leaves:
        sd_raw = str(lv.get("start_date") or "")[:10]
        if not (prev_year_start <= sd_raw <= prev_year_end):
            continue
        if (lv.get("status") or "Approved") != "Approved":
            continue
        lt = (lv.get("leave_type") or "Annual").strip()
        if lt != "Annual":
            continue
        try:
            prev_used_annual += int(lv.get("days") or 0)
        except Exception:
            pass

    carry_over = 0
    if annual_entitlement_int is not None and annual_entitlement_int > 0:
        unused_prev = max(0, annual_entitlement_int - prev_used_annual)
        carry_over = min(carry_max, unused_prev)

    # compute remaining per type
    balances: Dict[str, Dict[str, Any]] = {}
    for t, cfg in types_cfg.items():
        ent = cfg.get("max_days")
        try:
            ent_int = int(ent) if ent is not None else None
        except Exception:
            ent_int = None

        # add carry-over only to Annual entitlement
        effective_ent = ent_int
        if t == "Annual" and effective_ent is not None:
            effective_ent = effective_ent + carry_over

        u = used.get(t, 0)
        p = pending.get(t, 0)
        rem = (effective_ent - u) if (effective_ent is not None) else None

        balances[t] = {
            "entitlement": effective_ent,
            "used": u,
            "pending": p,
            "remaining": rem,
            "carry_over": carry_over if t == "Annual" else 0,
            "flag_low": (rem is not None and rem <= 2),
        }

    return {
        "year": year,
        "carry_over_max": carry_max,
        "balances": balances,
    }


def _get_critical_days(start: date, end: date) -> List[Dict[str, Any]]:
    """
    Critical days stored like:
      { "date": "2025-12-24", "label": "Stock Count", "kind": "audit|market|campaign|other", "is_deleted": False }
    """
    start_str = start.strftime("%Y-%m-%d")
    end_str = end.strftime("%Y-%m-%d")
    cur = critical_col.find(
        {"is_deleted": {"$ne": True}, "date": {"$gte": start_str, "$lte": end_str}},
        {"date": 1, "label": 1, "kind": 1},
    ).sort("date", 1)
    return [{"date": str(x.get("date"))[:10], "label": x.get("label") or "Critical Day", "kind": x.get("kind") or "other"} for x in cur]


# -------------------------------
# HR Attendance Page
# -------------------------------
@hr_bp.route("/attendance", methods=["GET"], endpoint="attendance_page")
def attendance_page():
    if not _hr_access_guard():
        return render_template("unauthorized.html"), 401
    return render_template("hr_pages/hr_attendance_page.html")


# -------------------------------
# Meeting attendance (list + delete)
# -------------------------------
@hr_bp.route("/attendance/meetings/list", methods=["GET"], endpoint="list_meeting_attendance")
def list_meeting_attendance():
    if not _hr_access_guard():
        return jsonify(ok=False, message="Unauthorized"), 401

    cursor = meetings_col.find(
        {"is_deleted": {"$ne": True}},
        {"title": 1, "meeting_date": 1, "attendees": 1, "notes": 1},
    ).sort("meeting_date", -1)

    meetings = []
    for m in cursor:
        meetings.append(
            {
                "id": str(m.get("_id")),
                "title": m.get("title") or "",
                "meeting_date": str(m.get("meeting_date") or "")[:10],
                "attendees": m.get("attendees") or [],
                "notes": m.get("notes") or "",
            }
        )

    return jsonify(ok=True, meetings=meetings)


@hr_bp.route("/attendance/meetings/create", methods=["POST"], endpoint="create_meeting_attendance")
def create_meeting_attendance():
    if not _hr_access_guard():
        return jsonify(ok=False, message="Unauthorized"), 401

    payload = request.get_json(silent=True) or {}
    title = (payload.get("title") or "").strip()
    meeting_date = (payload.get("meeting_date") or "").strip()
    notes = (payload.get("notes") or "").strip()
    attendee_ids = payload.get("attendee_ids") or []

    if not title or not meeting_date:
        return jsonify(ok=False, message="Meeting title and date are required."), 400

    d = _date_from_str(meeting_date)
    if not d:
        return jsonify(ok=False, message="Invalid meeting date."), 400

    if not isinstance(attendee_ids, list) or not attendee_ids:
        return jsonify(ok=False, message="Select at least one attendee."), 400

    oids = []
    for s in attendee_ids:
        oid = _safe_oid(str(s))
        if oid:
            oids.append(oid)

    if not oids:
        return jsonify(ok=False, message="No valid attendees selected."), 400

    cursor = users_col.find(
        {"_id": {"$in": oids}},
        {"name": 1, "role": 1, "position": 1, "branch": 1, "store_name": 1, "store": 1, "employee_code": 1, "staff_id": 1},
    )

    attendees = []
    for u in cursor:
        attendees.append(
            {
                "employee_id": str(u.get("_id")),
                "name": u.get("name") or "",
                "role": u.get("role") or u.get("position") or "",
                "branch": u.get("branch") or u.get("store_name") or u.get("store") or "",
                "employee_code": u.get("employee_code") or u.get("staff_id") or "",
            }
        )

    if not attendees:
        return jsonify(ok=False, message="No valid attendees found."), 400

    doc = {
        "title": title,
        "meeting_date": meeting_date,
        "notes": notes,
        "attendees": attendees,
        "is_deleted": False,
        "created_at": _now(),
        "updated_at": _now(),
    }

    res = meetings_col.insert_one(doc)
    _audit(
        "meeting_created",
        "meeting_attendance",
        entity_id=str(res.inserted_id),
        meta={"meeting_date": meeting_date, "attendees": len(attendees)},
    )

    return jsonify(ok=True, message="Meeting attendance saved.", meeting_id=str(res.inserted_id))


@hr_bp.route("/attendance/meetings/<meeting_id>/delete", methods=["POST"], endpoint="delete_meeting_attendance")
def delete_meeting_attendance(meeting_id):
    if not _hr_access_guard():
        return jsonify(ok=False, message="Unauthorized"), 401

    oid = _safe_oid(meeting_id)
    if not oid:
        return jsonify(ok=False, message="Invalid meeting ID."), 400

    res = meetings_col.update_one(
        {"_id": oid},
        {"$set": {"is_deleted": True, "updated_at": _now()}},
    )
    if not res.matched_count:
        return jsonify(ok=False, message="Meeting not found."), 404

    _audit("meeting_deleted", "meeting_attendance", entity_id=str(oid))
    return jsonify(ok=True, message="Meeting deleted.")


@hr_bp.route("/attendance/meetings/stats", methods=["GET"], endpoint="meeting_attendance_stats")
def meeting_attendance_stats():
    if not _hr_access_guard():
        return jsonify(ok=False, message="Unauthorized"), 401

    meetings_cursor = meetings_col.find(
        {"is_deleted": {"$ne": True}},
        {"attendees": 1},
    )

    meetings_count = 0
    attendance_count = 0
    attendee_counts: Dict[str, Dict[str, Any]] = {}

    for m in meetings_cursor:
        meetings_count += 1
        attendees = m.get("attendees") or []
        attendance_count += len(attendees)
        for a in attendees:
            name = (a.get("name") or "").strip()
            if not name:
                continue
            if name not in attendee_counts:
                attendee_counts[name] = {"name": name, "count": 0}
            attendee_counts[name]["count"] += 1

    top_attendees = sorted(attendee_counts.values(), key=lambda x: x["count"], reverse=True)[:10]

    return jsonify(
        ok=True,
        stats={"meetings": meetings_count, "attendances": attendance_count},
        top_attendees=top_attendees,
    )

# -------------------------------
# HR Leave Page
# -------------------------------
@hr_bp.route("/leave", methods=["GET"], endpoint="leave_page")
def leave_page():
    if not _hr_access_guard():
        return render_template("unauthorized.html"), 401
    return render_template("hr_pages/hr_leave_page.html")


# -------------------------------
# Employees list (for meeting picker / leave picker / manual attendance picker)
# -------------------------------
@hr_bp.route("/attendance/employees_list", methods=["GET"], endpoint="attendance_employees_list")
def attendance_employees_list():
    if not _hr_access_guard():
        return jsonify(ok=False, message="Unauthorized"), 401

    cursor = users_col.find(
        {"is_deleted": {"$ne": True}},
        {"name": 1, "role": 1, "position": 1, "branch": 1, "store_name": 1, "store": 1, "employee_code": 1, "staff_id": 1},
    ).sort("name", 1)

    employees = []
    for u in cursor:
        employees.append({
            "id": str(u["_id"]),
            "name": u.get("name") or "",
            "role": u.get("role") or u.get("position") or "",
            "branch": u.get("branch") or u.get("store_name") or u.get("store") or "",
            "employee_code": u.get("employee_code") or "",
            "staff_id": u.get("staff_id") or "",
        })

    return jsonify(ok=True, employees=employees)


@hr_bp.route("/leave/employees_list", methods=["GET"], endpoint="leave_employees_list")
def leave_employees_list():
    """
    Used by Leave page:
    - returns employee list + per-type balances (Approved used only) + policy entitlements
    """
    if not _hr_access_guard():
        return jsonify(ok=False, message="Unauthorized"), 401

    year = int(request.args.get("year") or _now().date().year)

    cursor = users_col.find(
        {"is_deleted": {"$ne": True}},
        {"name": 1, "employee_code": 1, "staff_id": 1, "role": 1, "position": 1, "branch": 1, "store_name": 1, "store": 1, "leave_policy": 1, "leaves": 1},
    ).sort("name", 1)

    employees = []
    for u in cursor:
        balances = _calc_leave_balances(u, year)
        employees.append({
            "id": str(u["_id"]),
            "name": u.get("name") or "",
            "role": u.get("role") or u.get("position") or "",
            "branch": u.get("branch") or u.get("store_name") or u.get("store") or "",
            "employee_code": u.get("employee_code") or u.get("staff_id") or "",
            "balances": balances,
        })

    return jsonify(ok=True, employees=employees, leave_types=LEAVE_TYPES, statuses=LEAVE_STATUSES)


# -------------------------------
# Leave policy setup (org + per-employee)
# -------------------------------
@hr_bp.route("/leave/policy/org/get", methods=["GET"], endpoint="get_org_leave_policy")
def get_org_leave_policy():
    if not _hr_access_guard():
        return jsonify(ok=False, message="Unauthorized"), 401
    return jsonify(ok=True, policy=_get_org_leave_policy(), leave_types=LEAVE_TYPES)


@hr_bp.route("/leave/policy/org/set", methods=["POST"], endpoint="set_org_leave_policy")
def set_org_leave_policy():
    """
    Payload:
      {
        "types": { "Annual": {"max_days": 18}, ... },
        "carry_over_max": 5
      }
    """
    if not _hr_access_guard():
        return jsonify(ok=False, message="Unauthorized"), 401

    payload = request.get_json(silent=True) or {}
    types = payload.get("types") or {}
    carry = payload.get("carry_over_max")

    # validate
    clean_types: Dict[str, Dict[str, Any]] = {}
    for t in LEAVE_TYPES:
        cfg = types.get(t) or {}
        md = cfg.get("max_days")
        if md is None or md == "":
            clean_types[t] = {"max_days": None}
        else:
            try:
                v = int(md)
                if v <= 0:
                    raise ValueError()
                clean_types[t] = {"max_days": v}
            except Exception:
                return jsonify(ok=False, message=f"Invalid max_days for {t}"), 400

    try:
        carry_int = int(carry) if carry is not None and carry != "" else 5
        if carry_int < 0:
            raise ValueError()
    except Exception:
        return jsonify(ok=False, message="carry_over_max must be a non-negative integer."), 400

    policy = {"types": clean_types, "carry_over_max": carry_int}

    leave_settings.update_one(
        {"key": "org_leave_policy"},
        {"$set": {"key": "org_leave_policy", "policy": policy, "updated_at": _now()}},
        upsert=True,
    )

    _audit("policy_updated", "leave_policy_org", meta={"policy": policy})
    return jsonify(ok=True, message="Org leave policy updated.", policy=policy)


@hr_bp.route("/leave/policy/employee/<employee_id>/set", methods=["POST"], endpoint="set_employee_leave_policy")
def set_employee_leave_policy(employee_id):
    """
    Set per-employee overrides:
      { "types": {"Annual": {"max_days": 24}}, "carry_over_max": 5 }
    """
    if not _hr_access_guard():
        return jsonify(ok=False, message="Unauthorized"), 401

    oid = _safe_oid(employee_id)
    if not oid:
        return jsonify(ok=False, message="Invalid employee ID."), 400

    payload = request.get_json(silent=True) or {}
    types = payload.get("types") or {}
    carry = payload.get("carry_over_max")

    # validate partial overrides only (you can send subset)
    clean_types: Dict[str, Dict[str, Any]] = {}
    for t, cfg in types.items():
        if t not in LEAVE_TYPES:
            continue
        md = (cfg or {}).get("max_days")
        if md is None or md == "":
            clean_types[t] = {"max_days": None}
        else:
            try:
                v = int(md)
                if v <= 0:
                    raise ValueError()
                clean_types[t] = {"max_days": v}
            except Exception:
                return jsonify(ok=False, message=f"Invalid max_days for {t}"), 400

    update_doc: Dict[str, Any] = {"updated_at": _now()}
    if clean_types:
        update_doc["leave_policy.types"] = clean_types
    if carry is not None:
        try:
            carry_int = int(carry)
            if carry_int < 0:
                raise ValueError()
            update_doc["leave_policy.carry_over_max"] = carry_int
        except Exception:
            return jsonify(ok=False, message="carry_over_max must be a non-negative integer."), 400

    res = users_col.update_one({"_id": oid}, {"$set": update_doc})
    if not res.matched_count:
        return jsonify(ok=False, message="Employee not found."), 404

    _audit("policy_updated", "leave_policy_employee", entity_id=str(oid), meta={"update": update_doc})
    return jsonify(ok=True, message="Employee leave policy updated.")


# -------------------------------
# Leave page APIs (list + stats + balances)
# -------------------------------
@hr_bp.route("/leave/types", methods=["GET"], endpoint="leave_types")
def leave_types():
    if not _hr_access_guard():
        return jsonify(ok=False, message="Unauthorized"), 401
    return jsonify(ok=True, leave_types=LEAVE_TYPES, statuses=LEAVE_STATUSES, org_policy=_get_org_leave_policy())


@hr_bp.route("/leave/balances/<employee_id>", methods=["GET"], endpoint="leave_balances")
def leave_balances(employee_id):
    """
    Returns entitlement/used/pending/remaining per type for an employee.
    """
    if not _hr_access_guard():
        return jsonify(ok=False, message="Unauthorized"), 401

    oid = _safe_oid(employee_id)
    if not oid:
        return jsonify(ok=False, message="Invalid employee ID."), 400

    year = int(request.args.get("year") or _now().date().year)
    emp = users_col.find_one({"_id": oid}, {"leave_policy": 1, "leaves": 1, "name": 1})
    if not emp:
        return jsonify(ok=False, message="Employee not found."), 404

    balances = _calc_leave_balances(emp, year)
    return jsonify(ok=True, employee_id=str(oid), name=emp.get("name") or "", balances=balances)


@hr_bp.route("/leave/list", methods=["GET"], endpoint="leave_list")
def leave_list():
    """
    Flatten users.leaves[] into a list, with filters:
      ?status=Approved&leave_type=Annual&branch=xxx&from=YYYY-MM-DD&to=YYYY-MM-DD&q=search
    """
    if not _hr_access_guard():
        return jsonify(ok=False, message="Unauthorized"), 401

    f_status = (request.args.get("status") or "").strip()
    f_type   = (request.args.get("leave_type") or "").strip()
    f_branch = (request.args.get("branch") or "").strip()
    f_role   = (request.args.get("role") or "").strip()
    q        = (request.args.get("q") or "").strip().lower()

    from_str = (request.args.get("from") or "").strip()
    to_str   = (request.args.get("to") or "").strip()
    from_d = _date_from_str(from_str) if from_str else None
    to_d   = _date_from_str(to_str) if to_str else None

    cursor = users_col.find(
        {"is_deleted": {"$ne": True}, "leaves": {"$exists": True}},
        {"name": 1, "employee_code": 1, "staff_id": 1, "role": 1, "position": 1, "branch": 1, "store_name": 1, "store": 1, "leaves": 1, "leave_policy": 1},
    ).sort("name", 1)

    rows: List[Dict[str, Any]] = []
    for u in cursor:
        branch = u.get("branch") or u.get("store_name") or u.get("store") or ""
        role = u.get("role") or u.get("position") or ""

        if f_branch and f_branch.lower() not in str(branch).lower():
            continue
        if f_role and f_role.lower() not in str(role).lower():
            continue

        for lv in (u.get("leaves") or []):
            sd_raw = str(lv.get("start_date") or "")[:10]
            if not sd_raw:
                continue

            lt = (lv.get("leave_type") or "Annual").strip()
            st = (lv.get("status") or "Approved").strip()

            # filter
            if f_status and st != f_status:
                continue
            if f_type and lt != f_type:
                continue

            sd = _date_from_str(sd_raw)
            try:
                days_val = int(lv.get("days") or 0)
            except Exception:
                days_val = 0
            ed = _leave_end_date(sd, days_val).strftime("%Y-%m-%d") if (sd and days_val > 0) else ""

            # date range filter by start_date
            if from_d and sd and sd < from_d:
                continue
            if to_d and sd and sd > to_d:
                continue

            row = {
                "leave_id": str(lv.get("_id")) if lv.get("_id") else "",
                "employee_id": str(u["_id"]),
                "employee_name": u.get("name") or "",
                "employee_code": u.get("employee_code") or u.get("staff_id") or "",
                "role": role,
                "branch": branch,
                "leave_type": lt,
                "status": st,
                "start_date": sd_raw,
                "end_date": ed,
                "days": days_val,
                "reason": lv.get("reason") or "",
                "requested_by": lv.get("requested_by") or "",
                "approved_by": lv.get("approved_by") or "",
                "approved_at": lv.get("approved_at").isoformat() if lv.get("approved_at") else None,
                "rejected_reason": lv.get("rejected_reason") or "",
                "created_at": lv.get("created_at").isoformat() if lv.get("created_at") else None,
                "updated_at": lv.get("updated_at").isoformat() if lv.get("updated_at") else None,
            }

            if q:
                hay = f"{row['employee_name']} {row['employee_code']} {row['reason']} {row['requested_by']} {row['approved_by']} {row['branch']} {row['role']} {row['leave_type']} {row['status']}".lower()
                if q not in hay:
                    continue

            rows.append(row)

    rows.sort(key=lambda x: (x.get("start_date") or ""), reverse=True)
    return jsonify(ok=True, leaves=rows)


@hr_bp.route("/leave/stats", methods=["GET"], endpoint="leave_stats")
def leave_stats():
    """
    Year-to-date stats:
      - records
      - total approved days
      - top employees by approved days
    Only Approved counts as used.
    """
    if not _hr_access_guard():
        return jsonify(ok=False, message="Unauthorized"), 401

    year = int(request.args.get("year") or _now().date().year)
    year_start, year_end = _year_bounds(year)

    cursor = users_col.find(
        {"is_deleted": {"$ne": True}},
        {"name": 1, "leaves": 1, "branch": 1, "store_name": 1, "store": 1},
    )

    total_days = 0
    total_records = 0
    per_emp: Dict[str, Dict[str, Any]] = {}

    for u in cursor:
        emp_id = str(u["_id"])
        name = u.get("name") or ""

        for lv in (u.get("leaves") or []):
            sd_raw = str(lv.get("start_date") or "")[:10]
            if not (year_start <= sd_raw <= year_end):
                continue
            if (lv.get("status") or "Approved") != "Approved":
                continue

            try:
                d = int(lv.get("days") or 0)
            except Exception:
                d = 0
            if d <= 0:
                continue

            total_days += d
            total_records += 1
            per_emp.setdefault(emp_id, {"employee_id": emp_id, "name": name, "days": 0})
            per_emp[emp_id]["days"] += d

    top = sorted(per_emp.values(), key=lambda x: x["days"], reverse=True)[:10]
    top_name = top[0]["name"] if top else "—"

    return jsonify(ok=True, stats={"records": total_records, "total_days": total_days, "top_employee": top_name}, top=top)


# -------------------------------
# Leave request / workflow (Create + Approve + Reject + Cancel)
# -------------------------------
@hr_bp.route("/leave/request/create", methods=["POST"], endpoint="create_leave_request")
@audit_action("leave.requested", "Created Leave Request", entity_type="leave")
def create_leave_request():
    """
    Creates a leave request (status defaults to Pending).
    Payload:
      {
        "employee_id": "...",
        "start_date": "YYYY-MM-DD",
        "days": 3,
        "leave_type": "Annual",
        "reason": "...",
        "requested_by": "..."
      }
    Validates:
      - overlap
      - critical days conflicts
      - limits per type (Approved only counts, but we still block if approving would exceed)
    """
    if not _hr_access_guard():
        return jsonify(ok=False, message="Unauthorized"), 401

    payload = request.get_json(silent=True) or {}
    employee_id = (payload.get("employee_id") or "").strip()
    start_raw   = (payload.get("start_date") or "").strip()
    days_raw    = str(payload.get("days") or "").strip()
    leave_type  = (payload.get("leave_type") or "Annual").strip()
    reason      = (payload.get("reason") or "").strip()
    requested_by= (payload.get("requested_by") or _actor_name()).strip()

    oid = _safe_oid(employee_id)
    if not oid:
        return jsonify(ok=False, message="Invalid employee ID."), 400

    start_d = _date_from_str(start_raw)
    if not start_d:
        return jsonify(ok=False, message="Invalid start date."), 400

    try:
        days_int = int(days_raw)
        if days_int <= 0:
            raise ValueError()
    except Exception:
        return jsonify(ok=False, message="Days must be a positive integer."), 400

    if leave_type not in LEAVE_TYPES:
        return jsonify(ok=False, message="Invalid leave type."), 400

    end_d = _leave_end_date(start_d, days_int)

    emp = users_col.find_one({"_id": oid}, {"name": 1, "leaves": 1, "leave_policy": 1, "branch": 1, "store_name": 1, "store": 1})
    if not emp:
        return jsonify(ok=False, message="Employee not found."), 404

    # overlap validation (check existing Approved/Pending/Draft not Cancelled/Rejected)
    for lv in (emp.get("leaves") or []):
        st = (lv.get("status") or "Approved").strip()
        if st in ("Cancelled", "Rejected"):
            continue

        sd = _date_from_str(str(lv.get("start_date") or "")[:10])
        try:
            dd = int(lv.get("days") or 0)
        except Exception:
            dd = 0
        if not sd or dd <= 0:
            continue

        ed = _leave_end_date(sd, dd)
        if _overlaps(start_d, end_d, sd, ed):
            return jsonify(ok=False, message="Leave overlaps with an existing leave entry."), 400

    # critical days conflict
    criticals = _get_critical_days(start_d, end_d)
    if criticals:
        # block hard for now; later you can allow override with a flag
        return jsonify(ok=False, message="Leave conflicts with critical days.", conflicts=criticals), 400

    # Create request (Pending by default)
    leave_doc: Dict[str, Any] = {
        "_id": ObjectId(),
        "leave_type": leave_type,
        "status": "Pending",
        "start_date": start_raw[:10],
        "days": days_int,
        "reason": reason,
        "requested_by": requested_by,
        "approved_by": "",
        "approved_at": None,
        "rejected_reason": "",
        "created_at": _now(),
        "updated_at": _now(),
    }

    res = users_col.update_one({"_id": oid}, {"$push": {"leaves": leave_doc}, "$set": {"updated_at": _now()}})
    if not res.matched_count:
        return jsonify(ok=False, message="Employee not found."), 404

    _audit("leave_created", "leave", entity_id=str(leave_doc["_id"]), meta={"employee_id": str(oid), "leave_type": leave_type, "days": days_int})
    return jsonify(ok=True, message="Leave request submitted.", leave_id=str(leave_doc["_id"]), status="Pending")


@hr_bp.route("/leave/request/<employee_id>/<leave_id>/approve", methods=["POST"], endpoint="approve_leave_request")
@audit_action("leave.approved", "Approved Leave Request", entity_type="leave", entity_id_from="leave_id")
def approve_leave_request(employee_id, leave_id):
    """
    Approve leave request:
      - validates limits per type (Approved only counts)
      - sets status Approved
    """
    if not _hr_access_guard():
        return jsonify(ok=False, message="Unauthorized"), 401

    emp_oid = _safe_oid(employee_id)
    lv_oid = _safe_oid(leave_id)
    if not emp_oid or not lv_oid:
        return jsonify(ok=False, message="Invalid IDs."), 400

    emp = users_col.find_one({"_id": emp_oid}, {"leaves": 1, "leave_policy": 1})
    if not emp:
        return jsonify(ok=False, message="Employee not found."), 404

    # locate leave
    target = None
    for lv in (emp.get("leaves") or []):
        if lv.get("_id") == lv_oid:
            target = lv
            break
    if not target:
        return jsonify(ok=False, message="Leave record not found."), 404

    if (target.get("status") or "") == "Approved":
        return jsonify(ok=True, message="Already approved.")

    start_d = _date_from_str(str(target.get("start_date") or "")[:10])
    try:
        days_int = int(target.get("days") or 0)
    except Exception:
        days_int = 0
    leave_type = (target.get("leave_type") or "Annual").strip()

    if not start_d or days_int <= 0:
        return jsonify(ok=False, message="Invalid leave record."), 400

    # enforce type entitlement (Approved only)
    policy = _get_employee_leave_policy(emp)
    ent = (policy.get("types") or {}).get(leave_type, {}).get("max_days")

    try:
        ent_int = int(ent) if ent is not None else None
    except Exception:
        ent_int = None

    year = start_d.year
    balances = _calc_leave_balances(emp, year)
    used_now = balances["balances"].get(leave_type, {}).get("used", 0)
    carry = balances["balances"].get("Annual", {}).get("carry_over", 0) if leave_type == "Annual" else 0

    effective_ent = ent_int
    if leave_type == "Annual" and effective_ent is not None:
        effective_ent = effective_ent + int(carry or 0)

    if effective_ent is not None and (used_now + days_int) > effective_ent:
        remaining = max(0, effective_ent - used_now)
        return jsonify(ok=False, message=f"Leave limit exceeded for {leave_type}. Remaining: {remaining} day(s)."), 400

    res = users_col.update_one(
        {"_id": emp_oid, "leaves._id": lv_oid},
        {"$set": {
            "leaves.$.status": "Approved",
            "leaves.$.approved_by": _actor_name(),
            "leaves.$.approved_at": _now(),
            "leaves.$.rejected_reason": "",
            "leaves.$.updated_at": _now(),
            "updated_at": _now(),
        }},
    )
    if not res.matched_count:
        return jsonify(ok=False, message="Unable to approve."), 400

    _audit("leave_approved", "leave", entity_id=str(lv_oid), meta={"employee_id": str(emp_oid)})
    return jsonify(ok=True, message="Leave approved.")


@hr_bp.route("/leave/request/<employee_id>/<leave_id>/reject", methods=["POST"], endpoint="reject_leave_request")
@audit_action("leave.rejected", "Rejected Leave Request", entity_type="leave", entity_id_from="leave_id")
def reject_leave_request(employee_id, leave_id):
    if not _hr_access_guard():
        return jsonify(ok=False, message="Unauthorized"), 401

    emp_oid = _safe_oid(employee_id)
    lv_oid = _safe_oid(leave_id)
    if not emp_oid or not lv_oid:
        return jsonify(ok=False, message="Invalid IDs."), 400

    payload = request.get_json(silent=True) or {}
    rejected_reason = (payload.get("rejected_reason") or "").strip()

    res = users_col.update_one(
        {"_id": emp_oid, "leaves._id": lv_oid},
        {"$set": {
            "leaves.$.status": "Rejected",
            "leaves.$.approved_by": "",
            "leaves.$.approved_at": None,
            "leaves.$.rejected_reason": rejected_reason,
            "leaves.$.updated_at": _now(),
            "updated_at": _now(),
        }},
    )
    if not res.matched_count:
        return jsonify(ok=False, message="Leave record not found."), 404

    _audit("leave_rejected", "leave", entity_id=str(lv_oid), meta={"employee_id": str(emp_oid), "reason": rejected_reason})
    return jsonify(ok=True, message="Leave rejected.")


@hr_bp.route("/leave/request/<employee_id>/<leave_id>/cancel", methods=["POST"], endpoint="cancel_leave_request")
@audit_action("leave.cancelled", "Cancelled Leave Request", entity_type="leave", entity_id_from="leave_id")
def cancel_leave_request(employee_id, leave_id):
    if not _hr_access_guard():
        return jsonify(ok=False, message="Unauthorized"), 401

    emp_oid = _safe_oid(employee_id)
    lv_oid = _safe_oid(leave_id)
    if not emp_oid or not lv_oid:
        return jsonify(ok=False, message="Invalid IDs."), 400

    res = users_col.update_one(
        {"_id": emp_oid, "leaves._id": lv_oid},
        {"$set": {
            "leaves.$.status": "Cancelled",
            "leaves.$.updated_at": _now(),
            "updated_at": _now(),
        }},
    )
    if not res.matched_count:
        return jsonify(ok=False, message="Leave record not found."), 404

    _audit("leave_cancelled", "leave", entity_id=str(lv_oid), meta={"employee_id": str(emp_oid)})
    return jsonify(ok=True, message="Leave cancelled.")


# -------------------------------
# Calendar view API (month)
# -------------------------------
@hr_bp.route("/leave/calendar", methods=["GET"], endpoint="leave_calendar")
def leave_calendar():
    """
    Returns approved leave blocks within a month.
    Query:
      ?month=YYYY-MM (required)
      &branch=...
    """
    if not _hr_access_guard():
        return jsonify(ok=False, message="Unauthorized"), 401

    month = (request.args.get("month") or "").strip()
    if not month or len(month) < 7:
        return jsonify(ok=False, message="month is required (YYYY-MM)."), 400

    branch_filter = (request.args.get("branch") or "").strip().lower()

    try:
        y = int(month[:4])
        m = int(month[5:7])
        first = date(y, m, 1)
        # next month
        if m == 12:
            nxt = date(y + 1, 1, 1)
        else:
            nxt = date(y, m + 1, 1)
        last = nxt - timedelta(days=1)
    except Exception:
        return jsonify(ok=False, message="Invalid month format."), 400

    cursor = users_col.find(
        {"is_deleted": {"$ne": True}, "leaves": {"$exists": True}},
        {"name": 1, "branch": 1, "store_name": 1, "store": 1, "employee_code": 1, "staff_id": 1, "leaves": 1},
    )

    blocks: List[Dict[str, Any]] = []
    for u in cursor:
        branch = (u.get("branch") or u.get("store_name") or u.get("store") or "")
        if branch_filter and branch_filter not in str(branch).lower():
            continue

        for lv in (u.get("leaves") or []):
            if (lv.get("status") or "Approved") != "Approved":
                continue
            sd = _date_from_str(str(lv.get("start_date") or "")[:10])
            try:
                days_int = int(lv.get("days") or 0)
            except Exception:
                days_int = 0
            if not sd or days_int <= 0:
                continue

            ed = _leave_end_date(sd, days_int)
            if not _overlaps(sd, ed, first, last):
                continue

            blocks.append({
                "employee_id": str(u["_id"]),
                "name": u.get("name") or "",
                "branch": branch,
                "employee_code": u.get("employee_code") or u.get("staff_id") or "",
                "leave_type": (lv.get("leave_type") or "Annual"),
                "start_date": sd.strftime("%Y-%m-%d"),
                "end_date": ed.strftime("%Y-%m-%d"),
                "days": days_int,
            })

    return jsonify(ok=True, month=month, blocks=blocks)


# -------------------------------
# Export API (CSV data)
# -------------------------------
@hr_bp.route("/leave/export/csv", methods=["GET"], endpoint="leave_export_csv")
def leave_export_csv():
    """
    Returns CSV-ready data (frontend can download).
    Same filters as /leave/list.
    """
    if not _hr_access_guard():
        return jsonify(ok=False, message="Unauthorized"), 401

    # reuse list endpoint logic by calling it internally
    # simplest: just call leave_list() and reformat (no response object reuse)
    # We'll reproduce minimal filter logic by calling /leave/list in frontend and building CSV there.
    return jsonify(ok=False, message="Use /leave/list to fetch data then build CSV on frontend."), 400


# -------------------------------
# Manual attendance (override payment-based attendance)
# -------------------------------
@hr_bp.route("/attendance/manual/mark", methods=["POST"], endpoint="manual_mark_attendance")
def manual_mark_attendance():
    if not _hr_access_guard():
        return jsonify(ok=False, message="Unauthorized"), 401

    payload = request.get_json(silent=True) or {}
    employee_id = (payload.get("employee_id") or "").strip()
    date_raw = (payload.get("date") or "").strip()
    status = (payload.get("status") or "").strip()   # Present / Absent
    reason = (payload.get("reason") or "").strip()

    oid = _safe_oid(employee_id)
    if not oid:
        return jsonify(ok=False, message="Invalid employee ID."), 400

    d = _date_from_str(date_raw)
    if not d:
        return jsonify(ok=False, message="Invalid date."), 400

    if status not in ("Present", "Absent"):
        return jsonify(ok=False, message="Invalid status."), 400

    doc = {
        "employee_id": str(oid),
        "date": d.strftime("%Y-%m-%d"),
        "status": status,
        "reason": reason,
        "updated_at": _now(),
        "created_at": _now(),
        "is_deleted": False,
    }

    manual_att_col.update_one(
        {"employee_id": doc["employee_id"], "date": doc["date"], "is_deleted": {"$ne": True}},
        {"$set": doc},
        upsert=True,
    )

    _audit("attendance_manual_mark", "attendance", entity_id=f"{doc['employee_id']}:{doc['date']}", meta={"status": status, "reason": reason})
    return jsonify(ok=True, message="Manual attendance saved.")


# -------------------------------
# Employee attendance overview (profile tab)
# -------------------------------
@hr_bp.route("/employee/<employee_id>/attendance_overview", methods=["GET"], endpoint="get_attendance_overview")
def get_attendance_overview(employee_id):
    """
    Attendance = days (Mon–Sat) where payments > 10, unless manually overridden.
    Sundays excluded.
    Last 12 months.
    """
    if not _hr_access_guard():
        return jsonify(ok=False, message="Unauthorized"), 401

    oid = _safe_oid(employee_id)
    if not oid:
        return jsonify(ok=False, message="Invalid employee ID"), 400

    emp = users_col.find_one({"_id": oid}, {"leaves": 1, "name": 1})
    if not emp:
        return jsonify(ok=False, message="Employee not found"), 404

    today = _now().date()
    start_date = today - timedelta(days=365)
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = today.strftime("%Y-%m-%d")

    payments_cursor = payments_col.find(
        {
            "agent_id": str(oid),
            "date": {"$gte": start_str, "$lte": end_str},
            "payment_type": {"$ne": "WITHDRAWAL"},
        },
        {"date": 1},
    )

    date_counts: Dict[str, int] = {}
    for p in payments_cursor:
        ds = p.get("date")
        if not ds:
            continue
        ds = str(ds)[:10]
        date_counts[ds] = date_counts.get(ds, 0) + 1

    overrides_cursor = manual_att_col.find(
        {"employee_id": str(oid), "date": {"$gte": start_str, "$lte": end_str}, "is_deleted": {"$ne": True}},
        {"date": 1, "status": 1},
    )
    manual_map = {str(x.get("date") or "")[:10]: (x.get("status") or "") for x in overrides_cursor}

    total_working_days = 0
    days_present = 0
    months_map: Dict[str, Dict[str, int]] = {}
    recent_days: List[Dict[str, Any]] = []

    delta_days = (today - start_date).days
    for i in range(delta_days + 1):
        d = start_date + timedelta(days=i)
        if d.weekday() == 6:
            continue

        d_str = d.strftime("%Y-%m-%d")
        payments_count = date_counts.get(d_str, 0)

        total_working_days += 1
        if d_str in manual_map:
            worked = (manual_map[d_str] == "Present")
        else:
            worked = payments_count > 10

        if worked:
            days_present += 1

        month_key = d.strftime("%Y-%m")
        months_map.setdefault(month_key, {"present_days": 0, "working_days": 0})
        months_map[month_key]["working_days"] += 1
        if worked:
            months_map[month_key]["present_days"] += 1

        if (today - d).days <= 30:
            recent_days.append({
                "date": d_str,
                "status": "Present" if worked else "Absent",
                "payments_count": payments_count,
            })

    attendance_rate = (days_present / total_working_days) * 100 if total_working_days else 0.0

    months_list = [{
        "month": mk,
        "present_days": months_map[mk]["present_days"],
        "working_days": months_map[mk]["working_days"],
    } for mk in sorted(months_map.keys())]

    leaves_raw = emp.get("leaves") or []
    leaves: List[Dict[str, Any]] = []
    for lv in leaves_raw:
        start_raw = lv.get("start_date", "")
        try:
            days_val = int(lv.get("days", 0) or 0)
        except Exception:
            days_val = 0
        start_d = _date_from_str(start_raw)

        end_str2 = ""
        if start_d and days_val > 0:
            end_str2 = _leave_end_date(start_d, days_val).strftime("%Y-%m-%d")

        leaves.append({
            "id": str(lv.get("_id")) if lv.get("_id") else None,
            "start_date": start_d.strftime("%Y-%m-%d") if start_d else start_raw,
            "end_date": end_str2,
            "days": days_val,
            "leave_type": (lv.get("leave_type") or "Annual"),
            "status": (lv.get("status") or "Approved"),
            "requested_by": (lv.get("requested_by") or ""),
            "approved_by": (lv.get("approved_by") or ""),
            "approved_at": lv.get("approved_at").isoformat() if lv.get("approved_at") else None,
            "rejected_reason": (lv.get("rejected_reason") or ""),
            "granted_by": lv.get("granted_by", ""),  # backwards compatibility
            "reason": lv.get("reason", ""),
        })

    return jsonify(
        ok=True,
        summary={
            "total_working_days": total_working_days,
            "days_present": days_present,
            "attendance_rate": attendance_rate,
            "from": start_str,
            "to": end_str,
        },
        months=months_list,
        recent_days=sorted(recent_days, key=lambda x: x["date"], reverse=True),
        leaves=leaves,
    )


# -------------------------------
# Backwards-compatible: employee tab "Record Leave"
# Now creates Pending request (instead of instantly Approved)
# -------------------------------
@hr_bp.route("/employee/<employee_id>/leaves/add", methods=["POST"], endpoint="add_leave")
def add_leave(employee_id):
    """
    Legacy endpoint used by employee attendance tab.
    We now create a Pending request with leave_type = Annual by default.
    Approval will enforce per-type limits.
    """
    if not _hr_access_guard():
        return jsonify(ok=False, message="Unauthorized"), 401

    oid = _safe_oid(employee_id)
    if not oid:
        return jsonify(ok=False, message="Invalid employee ID"), 400

    payload = request.get_json(silent=True) or {}
    start_date_raw = (payload.get("start_date") or "").strip()
    days_raw = (str(payload.get("days") or "")).strip()
    granted_by = (payload.get("granted_by") or "").strip()  # legacy; maps to requested_by if provided
    reason = (payload.get("reason") or "").strip()

    if not start_date_raw or not days_raw:
        return jsonify(ok=False, message="Start date and number of days are required."), 400

    try:
        days_int = int(days_raw)
        if days_int <= 0:
            raise ValueError()
    except Exception:
        return jsonify(ok=False, message="Days must be a positive integer."), 400

    start_d = _date_from_str(start_date_raw)
    if not start_d:
        return jsonify(ok=False, message="Invalid start date format."), 400

    end_d = _leave_end_date(start_d, days_int)

    emp = users_col.find_one({"_id": oid}, {"leaves": 1, "leave_policy": 1})
    if not emp:
        return jsonify(ok=False, message="Employee not found."), 404

    # overlap validation
    for lv in (emp.get("leaves") or []):
        st = (lv.get("status") or "Approved").strip()
        if st in ("Cancelled", "Rejected"):
            continue
        sd = _date_from_str(str(lv.get("start_date") or "")[:10])
        try:
            dd = int(lv.get("days") or 0)
        except Exception:
            dd = 0
        if not sd or dd <= 0:
            continue
        ed = _leave_end_date(sd, dd)
        if _overlaps(start_d, end_d, sd, ed):
            return jsonify(ok=False, message="Leave overlaps with an existing leave entry."), 400

    # critical days conflict
    criticals = _get_critical_days(start_d, end_d)
    if criticals:
        return jsonify(ok=False, message="Leave conflicts with critical days.", conflicts=criticals), 400

    leave_doc: Dict[str, Any] = {
        "_id": ObjectId(),
        "leave_type": "Annual",
        "status": "Pending",
        "start_date": start_date_raw[:10],
        "days": days_int,
        "reason": reason,
        "requested_by": granted_by or _actor_name(),
        "approved_by": "",
        "approved_at": None,
        "rejected_reason": "",
        "created_at": _now(),
        "updated_at": _now(),
    }

    result = users_col.update_one(
        {"_id": oid},
        {"$push": {"leaves": leave_doc}, "$set": {"updated_at": _now()}},
    )
    if not result.matched_count:
        return jsonify(ok=False, message="Employee not found."), 404

    _audit("leave_created", "leave", entity_id=str(leave_doc["_id"]), meta={"employee_id": str(oid), "leave_type": "Annual", "days": days_int, "via": "employee_tab"})
    return jsonify(ok=True, message="Leave request submitted (Pending).", leave_id=str(leave_doc["_id"]), status="Pending")

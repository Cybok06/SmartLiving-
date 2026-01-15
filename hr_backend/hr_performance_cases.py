# hr_backend/hr_performance_cases.py
from datetime import datetime, timedelta, date
from typing import Any, Dict, List, Optional, Tuple

from flask import jsonify, request, render_template
from bson import ObjectId

from db import db
from hr_backend.hr_dashboard import hr_bp, _hr_access_guard

users_col = db["users"]


# -------------------------------
# Constants (simple + extensible)
# -------------------------------
CASE_SEVERITIES = ["Low", "Normal", "High", "Critical"]
CASE_STATUSES   = ["Open", "Under Review", "Escalated", "Resolved", "Closed"]


# -------------------------------
# Helpers
# -------------------------------
def _now() -> datetime:
    return datetime.utcnow()


def _safe_oid(s: str) -> Optional[ObjectId]:
    try:
        return ObjectId(str(s))
    except Exception:
        return None


def _date_from_str(s: str) -> Optional[date]:
    try:
        return datetime.strptime(str(s)[:10], "%Y-%m-%d").date()
    except Exception:
        return None


def _dt_to_str(dt: Any) -> str:
    if isinstance(dt, datetime):
        return dt.strftime("%Y-%m-%d %H:%M")
    if isinstance(dt, date):
        return dt.strftime("%Y-%m-%d")
    return str(dt) if dt else ""


def _clamp_int(x: Any, lo: int, hi: int, default: int) -> int:
    try:
        v = int(x)
    except Exception:
        v = default
    if v < lo:
        v = lo
    if v > hi:
        v = hi
    return v


def _serialize_performance(emp: Dict[str, Any], raw: Any) -> Dict[str, Any]:
    doc = raw if isinstance(raw, dict) else {"title": str(raw) if raw is not None else ""}

    created = doc.get("created_at")
    rating = _clamp_int(doc.get("rating"), 0, 5, 0)

    return {
        "id": str(doc.get("_id")) if doc.get("_id") else None,

        "employee_id": str(emp.get("_id")),
        "employee_name": emp.get("name") or "",
        "employee_code": emp.get("employee_code") or emp.get("staff_id") or "",
        "branch": emp.get("branch") or emp.get("store_name") or emp.get("store") or "",
        "role": emp.get("role") or emp.get("position") or "",

        "title": doc.get("title") or "",
        "details": doc.get("details") or "",
        "rating": rating,
        "created_at": _dt_to_str(created),
        "recorded_by": doc.get("recorded_by") or "HR",
    }


def _serialize_case(emp: Dict[str, Any], raw: Any) -> Dict[str, Any]:
    doc = raw if isinstance(raw, dict) else {"title": str(raw) if raw is not None else ""}

    severity = (doc.get("severity") or "Normal").strip()
    if severity not in CASE_SEVERITIES:
        severity = "Normal"

    status = (doc.get("status") or "Open").strip()
    if status not in CASE_STATUSES:
        status = "Open"

    created = doc.get("created_at")
    updated = doc.get("updated_at")
    due     = doc.get("due_date")

    fups_raw = doc.get("followups") or []
    if not isinstance(fups_raw, list):
        fups_raw = []

    followups = []
    for f in fups_raw:
        if not isinstance(f, dict):
            continue
        followups.append({
            "id": str(f.get("_id")) if f.get("_id") else None,
            "note": f.get("note") or "",
            "created_at": _dt_to_str(f.get("created_at")),
            "recorded_by": f.get("recorded_by") or "",
        })

    case_type = (doc.get("case_type") or "Misconduct").strip()
    loss_amt  = doc.get("loss_amount")
    try:
        loss_amt = float(loss_amt) if loss_amt not in (None, "", "None") else None
    except Exception:
        loss_amt = None

    return {
        "id": str(doc.get("_id")) if doc.get("_id") else None,
        "employee_id": str(emp.get("_id")),
        "employee_name": emp.get("name") or "",
        "employee_code": emp.get("employee_code") or emp.get("staff_id") or "",
        "branch": emp.get("branch") or emp.get("store_name") or emp.get("store") or "",
        "role": emp.get("role") or emp.get("position") or "",

        "title": doc.get("title") or "",
        "details": doc.get("details") or "",
        "case_type": case_type,
        "severity": severity,
        "status": status,

        "loss_amount": loss_amt,
        "incident_date": str(doc.get("incident_date") or "")[:10],
        "due_date": str(due)[:10] if due else "",
        "created_at": _dt_to_str(created),
        "updated_at": _dt_to_str(updated),
        "recorded_by": doc.get("recorded_by") or "HR",

        "followups": followups,
        "followups_count": len(followups),
        "last_followup_at": followups[0]["created_at"] if followups else "",
    }


# -------------------------------
# Performance flatten + KPIs
# -------------------------------
def _flatten_performance(filters: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], Dict[str, Any], Dict[str, Any]]:
    """
    Global performance list from users.performance_records[].
    Returns: rows, kpis, charts
    """
    q = (filters.get("q") or "").strip().lower()
    branch = (filters.get("branch") or "").strip()
    role = (filters.get("role") or "").strip()
    rating_min = _clamp_int(filters.get("rating_min"), 0, 5, 0)
    rating_max = _clamp_int(filters.get("rating_max"), 0, 5, 5)
    date_from = _date_from_str(filters.get("from") or "")
    date_to   = _date_from_str(filters.get("to") or "")
    limit = _clamp_int(filters.get("limit"), 10, 5000, 1000)

    cursor = users_col.find(
        {"is_deleted": {"$ne": True}, "performance_records": {"$exists": True}},
        {
            "name": 1, "employee_code": 1, "staff_id": 1, "branch": 1, "store_name": 1, "store": 1,
            "role": 1, "position": 1, "performance_records": 1,
        },
    )

    rows: List[Dict[str, Any]] = []

    # charts
    rating_dist = {str(i): 0 for i in range(0, 6)}     # 0..5
    branch_avg: Dict[str, Dict[str, Any]] = {}         # {branch:{sum,count}}
    monthly_avg: Dict[str, Dict[str, Any]] = {}        # YYYY-MM

    # KPIs
    kpis = {
        "records": 0,
        "avg_rating": 0.0,
        "top_employee": "—",
        "top_employee_avg": 0.0,
    }

    per_emp: Dict[str, Dict[str, Any]] = {}  # avg per employee (for leaderboard)

    for emp in cursor:
        emp_branch = emp.get("branch") or emp.get("store_name") or emp.get("store") or ""
        emp_role = emp.get("role") or emp.get("position") or ""

        if branch and emp_branch != branch:
            continue
        if role and emp_role != role:
            continue

        perf_raw = emp.get("performance_records") or []
        if not isinstance(perf_raw, list):
            perf_raw = [perf_raw]

        for p in perf_raw:
            if not isinstance(p, dict):
                continue

            row = _serialize_performance(emp, p)

            # date filter
            created_d = _date_from_str(row.get("created_at") or "")
            if date_from and created_d and created_d < date_from:
                continue
            if date_to and created_d and created_d > date_to:
                continue

            # rating filter
            r = int(row.get("rating") or 0)
            if r < rating_min or r > rating_max:
                continue

            # search filter
            if q:
                hay = " ".join([
                    row.get("employee_name",""), row.get("employee_code",""),
                    row.get("branch",""), row.get("role",""),
                    row.get("title",""), row.get("details",""),
                    row.get("recorded_by",""),
                ]).lower()
                if q not in hay:
                    continue

            # collect
            rows.append(row)

            # stats aggregation
            rating_dist[str(r)] = rating_dist.get(str(r), 0) + 1

            if emp_branch:
                branch_avg.setdefault(emp_branch, {"sum": 0, "count": 0})
                branch_avg[emp_branch]["sum"] += r
                branch_avg[emp_branch]["count"] += 1

            if created_d:
                mk = created_d.strftime("%Y-%m")
                monthly_avg.setdefault(mk, {"sum": 0, "count": 0})
                monthly_avg[mk]["sum"] += r
                monthly_avg[mk]["count"] += 1

            eid = row["employee_id"]
            per_emp.setdefault(eid, {"employee_id": eid, "name": row["employee_name"], "sum": 0, "count": 0})
            per_emp[eid]["sum"] += r
            per_emp[eid]["count"] += 1

    # sort newest first
    rows.sort(key=lambda x: x.get("created_at") or "", reverse=True)
    rows = rows[:limit]

    # KPIs
    total = sum(int(v) for v in rating_dist.values())
    kpis["records"] = total

    if total:
        sum_rating = 0
        for k, v in rating_dist.items():
            sum_rating += int(k) * int(v)
        kpis["avg_rating"] = round(sum_rating / total, 2)

    # top employee (by avg rating + min entries)
    top = []
    for e in per_emp.values():
        if e["count"] <= 0:
            continue
        avg = e["sum"] / e["count"]
        top.append({"employee_id": e["employee_id"], "name": e["name"], "avg": avg, "count": e["count"]})
    top.sort(key=lambda x: (x["avg"], x["count"]), reverse=True)
    if top:
        kpis["top_employee"] = top[0]["name"]
        kpis["top_employee_avg"] = round(top[0]["avg"], 2)

    # charts payload
    branches_chart = []
    for br, agg in branch_avg.items():
        if agg["count"] <= 0:
            continue
        branches_chart.append({"label": br, "value": round(agg["sum"] / agg["count"], 2)})
    branches_chart.sort(key=lambda x: x["value"], reverse=True)

    months_chart = []
    for mk, agg in monthly_avg.items():
        if agg["count"] <= 0:
            continue
        months_chart.append({"label": mk, "value": round(agg["sum"] / agg["count"], 2)})
    months_chart.sort(key=lambda x: x["label"])

    charts = {
        "rating_dist": [{"label": str(i), "value": rating_dist[str(i)]} for i in range(0, 6)],
        "branch_avg": branches_chart[:12],
        "monthly_avg": months_chart[-12:],
        "leaderboard": [{"employee_id": x["employee_id"], "name": x["name"], "avg": round(x["avg"],2), "count": x["count"]} for x in top[:10]],
    }

    meta = {
        "branches": sorted(list(branch_avg.keys())),
        "roles": sorted(list({(r.get("role") or "").strip() for r in rows if (r.get("role") or "").strip()})),
    }

    return rows, kpis, {**charts, **meta}


# -------------------------------
# Pages
# -------------------------------
@hr_bp.route("/cases", methods=["GET"], endpoint="cases_page")
def cases_page():
    if not _hr_access_guard():
        return render_template("unauthorized.html"), 401
    return render_template("hr_pages/hr_cases_page.html")


@hr_bp.route("/performance", methods=["GET"], endpoint="performance_page")
def performance_page():
    if not _hr_access_guard():
        return render_template("unauthorized.html"), 401
    return render_template("hr_pages/hr_performance_page.html")


# -------------------------------
# Shared employee list (for picker)
# -------------------------------
@hr_bp.route("/cases/employees_list", methods=["GET"], endpoint="cases_employees_list")
def cases_employees_list():
    if not _hr_access_guard():
        return jsonify(ok=False, message="Unauthorized"), 401

    cursor = users_col.find(
        {"is_deleted": {"$ne": True}},
        {"name": 1, "role": 1, "position": 1, "branch": 1, "store_name": 1, "store": 1, "employee_code": 1, "staff_id": 1},
    ).sort("name", 1)

    employees = []
    branches = set()

    for u in cursor:
        br = u.get("branch") or u.get("store_name") or u.get("store") or ""
        if br:
            branches.add(br)

        employees.append({
            "id": str(u["_id"]),
            "name": u.get("name") or "",
            "role": u.get("role") or u.get("position") or "",
            "branch": br,
            "employee_code": u.get("employee_code") or u.get("staff_id") or "",
        })

    return jsonify(ok=True, employees=employees, branches=sorted(list(branches)))


# -------------------------------
# Performance APIs (global)
# -------------------------------
@hr_bp.route("/performance/list", methods=["GET"], endpoint="performance_list")
def performance_list():
    if not _hr_access_guard():
        return jsonify(ok=False, message="Unauthorized"), 401

    filters = {
        "q": request.args.get("q"),
        "branch": request.args.get("branch"),
        "role": request.args.get("role"),
        "rating_min": request.args.get("rating_min"),
        "rating_max": request.args.get("rating_max"),
        "from": request.args.get("from"),
        "to": request.args.get("to"),
        "limit": request.args.get("limit"),
    }

    rows, kpis, charts = _flatten_performance(filters)
    return jsonify(ok=True, records=rows, kpis=kpis, charts=charts)


# -------------------------------
# Global list + KPIs (cases)
# -------------------------------
def _flatten_cases(filters: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    q = (filters.get("q") or "").strip().lower()
    status = (filters.get("status") or "").strip()
    severity = (filters.get("severity") or "").strip()
    branch = (filters.get("branch") or "").strip()
    date_from = _date_from_str(filters.get("from") or "")
    date_to   = _date_from_str(filters.get("to") or "")
    limit = _clamp_int(filters.get("limit"), 10, 1000, 200)

    cursor = users_col.find(
        {"is_deleted": {"$ne": True}, "case_records": {"$exists": True}},
        {
            "name": 1, "employee_code": 1, "staff_id": 1, "branch": 1, "store_name": 1, "store": 1,
            "role": 1, "position": 1,
            "case_records": 1,
        },
    )

    rows: List[Dict[str, Any]] = []
    kpis = {"open": 0, "overdue": 0, "high_critical": 0, "total": 0}
    today = _now().date()

    for emp in cursor:
        cases_raw = emp.get("case_records") or []
        if not isinstance(cases_raw, list):
            cases_raw = [cases_raw]

        for c in cases_raw:
            if not isinstance(c, dict):
                continue

            emp_branch = (emp.get("branch") or emp.get("store_name") or emp.get("store") or "")
            if branch and emp_branch != branch:
                continue

            row = _serialize_case(emp, c)

            created_date = _date_from_str(row.get("created_at") or "")
            if date_from and created_date and created_date < date_from:
                continue
            if date_to and created_date and created_date > date_to:
                continue

            if status and row.get("status") != status:
                continue
            if severity and row.get("severity") != severity:
                continue

            if q:
                hay = " ".join([
                    row.get("employee_name",""), row.get("employee_code",""),
                    row.get("branch",""), row.get("role",""),
                    row.get("title",""), row.get("details",""),
                    row.get("case_type",""), row.get("recorded_by",""),
                    row.get("status",""), row.get("severity",""),
                ]).lower()
                if q not in hay:
                    continue

            kpis["total"] += 1
            if row.get("status") in ("Open", "Under Review", "Escalated"):
                kpis["open"] += 1
            if row.get("severity") in ("High", "Critical"):
                kpis["high_critical"] += 1

            due_s = row.get("due_date") or ""
            due_d = _date_from_str(due_s)
            if due_d and due_d < today and row.get("status") not in ("Resolved", "Closed"):
                kpis["overdue"] += 1

            rows.append(row)

    sev_rank = {"Critical": 0, "High": 1, "Normal": 2, "Low": 3}
    rows.sort(key=lambda r: (sev_rank.get(r.get("severity","Normal"), 9), r.get("created_at","")), reverse=False)
    rows = rows[:limit]

    return rows, kpis


@hr_bp.route("/cases/list", methods=["GET"], endpoint="cases_list")
def cases_list():
    if not _hr_access_guard():
        return jsonify(ok=False, message="Unauthorized"), 401

    filters = {
        "q": request.args.get("q"),
        "status": request.args.get("status"),
        "severity": request.args.get("severity"),
        "branch": request.args.get("branch"),
        "from": request.args.get("from"),
        "to": request.args.get("to"),
        "limit": request.args.get("limit"),
    }

    rows, kpis = _flatten_cases(filters)
    return jsonify(ok=True, cases=rows, kpis=kpis, severities=CASE_SEVERITIES, statuses=CASE_STATUSES)


@hr_bp.route("/cases/stats", methods=["GET"], endpoint="cases_stats")
def cases_stats():
    if not _hr_access_guard():
        return jsonify(ok=False, message="Unauthorized"), 401

    rows, kpis = _flatten_cases({"limit": 1000})
    per_emp: Dict[str, Dict[str, Any]] = {}
    for r in rows:
        if r.get("status") in ("Resolved", "Closed"):
            continue
        eid = r.get("employee_id")
        if not eid:
            continue
        per_emp.setdefault(eid, {"employee_id": eid, "name": r.get("employee_name"), "count": 0})
        per_emp[eid]["count"] += 1

    top = sorted(per_emp.values(), key=lambda x: x["count"], reverse=True)[:10]
    return jsonify(ok=True, kpis=kpis, top=top)


# -------------------------------
# Employee profile tab API
# -------------------------------
@hr_bp.route("/employee/<employee_id>/performance_cases", methods=["GET"], endpoint="get_performance_cases")
def get_performance_cases(employee_id):
    if not _hr_access_guard():
        return jsonify(ok=False, message="Unauthorized"), 401

    oid = _safe_oid(employee_id)
    if not oid:
        return jsonify(ok=False, message="Invalid employee ID"), 400

    emp = users_col.find_one({"_id": oid}, {"performance_records": 1, "case_records": 1, "name": 1, "employee_code": 1, "staff_id": 1, "branch": 1, "store_name": 1, "store": 1, "role": 1, "position": 1})
    if not emp:
        return jsonify(ok=False, message="Employee not found"), 404

    perf_raw = emp.get("performance_records") or []
    if not isinstance(perf_raw, list):
        perf_raw = [perf_raw]

    cases_raw = emp.get("case_records") or []
    if not isinstance(cases_raw, list):
        cases_raw = [cases_raw]

    performances = [_serialize_performance(emp, p) for p in perf_raw]
    cases = [_serialize_case(emp, c) for c in cases_raw]

    ratings = [p["rating"] for p in performances if p["rating"] > 0]
    avg_rating = round(sum(ratings) / len(ratings), 2) if ratings else 0.0

    open_cases = sum(1 for c in cases if c["status"] in ("Open","Under Review","Escalated"))
    closed_cases = sum(1 for c in cases if c["status"] in ("Resolved","Closed"))

    summary = {
        "avg_rating": avg_rating,
        "performance_count": len(performances),
        "case_count": len(cases),
        "open_cases": open_cases,
        "closed_cases": closed_cases,
    }

    return jsonify(ok=True, performances=performances, cases=cases, summary=summary)


# -------------------------------
# Add Performance (kept)
# -------------------------------
@hr_bp.route("/employee/<employee_id>/performance/add", methods=["POST"], endpoint="add_performance")
def add_performance(employee_id):
    if not _hr_access_guard():
        return jsonify(ok=False, message="Unauthorized"), 401

    oid = _safe_oid(employee_id)
    if not oid:
        return jsonify(ok=False, message="Invalid employee ID"), 400

    payload = request.get_json(silent=True) or {}
    title = (payload.get("title") or "").strip()
    details = (payload.get("details") or "").strip()
    rating = _clamp_int(payload.get("rating"), 0, 5, 0)

    if not title:
        return jsonify(ok=False, message="Performance title is required."), 400

    now = _now()
    perf_doc: Dict[str, Any] = {
        "_id": ObjectId(),
        "title": title,
        "details": details,
        "rating": rating,
        "created_at": now,
        "recorded_by": "HR",
    }

    res = users_col.update_one({"_id": oid}, {"$push": {"performance_records": perf_doc}, "$set": {"updated_at": now}})
    if not res.matched_count:
        return jsonify(ok=False, message="Employee not found"), 404

    return jsonify(ok=True, message="Performance recorded.")


# -------------------------------
# Create / Update Cases (kept)
# -------------------------------
@hr_bp.route("/employee/<employee_id>/cases/add", methods=["POST"], endpoint="add_case")
def add_case(employee_id):
    if not _hr_access_guard():
        return jsonify(ok=False, message="Unauthorized"), 401

    oid = _safe_oid(employee_id)
    if not oid:
        return jsonify(ok=False, message="Invalid employee ID"), 400

    payload = request.get_json(silent=True) or {}
    title = (payload.get("title") or "").strip()
    details = (payload.get("details") or "").strip()
    severity = (payload.get("severity") or "Normal").strip()
    status = (payload.get("status") or "Open").strip()
    case_type = (payload.get("case_type") or "Misconduct").strip()

    incident_date = (payload.get("incident_date") or "").strip()
    due_date = (payload.get("due_date") or "").strip()

    loss_amount = payload.get("loss_amount")
    try:
        loss_amount = float(loss_amount) if loss_amount not in (None, "", "None") else None
    except Exception:
        loss_amount = None

    if not title:
        return jsonify(ok=False, message="Case title is required."), 400

    if severity not in CASE_SEVERITIES:
        severity = "Normal"
    if status not in CASE_STATUSES:
        status = "Open"

    if incident_date and not _date_from_str(incident_date):
        return jsonify(ok=False, message="Invalid incident_date format."), 400
    if due_date and not _date_from_str(due_date):
        return jsonify(ok=False, message="Invalid due_date format."), 400

    now = _now()
    case_doc: Dict[str, Any] = {
        "_id": ObjectId(),
        "title": title,
        "details": details,
        "case_type": case_type,
        "severity": severity,
        "status": status,
        "incident_date": incident_date[:10] if incident_date else "",
        "due_date": due_date[:10] if due_date else "",
        "loss_amount": loss_amount,
        "followups": [],
        "created_at": now,
        "updated_at": now,
        "recorded_by": "HR",
    }

    res = users_col.update_one({"_id": oid}, {"$push": {"case_records": case_doc}, "$set": {"updated_at": now}})
    if not res.matched_count:
        return jsonify(ok=False, message="Employee not found."), 404

    return jsonify(ok=True, message="Case recorded.", case_id=str(case_doc["_id"]))


@hr_bp.route("/employee/<employee_id>/cases/<case_id>/status", methods=["POST"], endpoint="update_case_status")
def update_case_status(employee_id, case_id):
    if not _hr_access_guard():
        return jsonify(ok=False, message="Unauthorized"), 401

    eoid = _safe_oid(employee_id)
    coid = _safe_oid(case_id)
    if not eoid or not coid:
        return jsonify(ok=False, message="Invalid IDs."), 400

    payload = request.get_json(silent=True) or {}
    status = (payload.get("status") or "").strip()
    if status not in CASE_STATUSES:
        return jsonify(ok=False, message="Invalid status."), 400

    note = (payload.get("note") or "").strip()

    now = _now()
    followup_doc = None
    if note:
        followup_doc = {
            "_id": ObjectId(),
            "note": f"[STATUS → {status}] {note}",
            "created_at": now,
            "recorded_by": "HR",
        }

    update = {"$set": {"updated_at": now, "case_records.$[c].status": status, "case_records.$[c].updated_at": now}}
    if followup_doc:
        update["$push"] = {"case_records.$[c].followups": {"$each": [followup_doc], "$position": 0}}

    res = users_col.update_one({"_id": eoid}, update, array_filters=[{"c._id": coid}])

    if not res.matched_count:
        return jsonify(ok=False, message="Employee not found."), 404
    if not res.modified_count:
        return jsonify(ok=False, message="Case not found or no changes."), 404

    return jsonify(ok=True, message="Case status updated.")


@hr_bp.route("/employee/<employee_id>/cases/<case_id>/followup", methods=["POST"], endpoint="add_case_followup")
def add_case_followup(employee_id, case_id):
    if not _hr_access_guard():
        return jsonify(ok=False, message="Unauthorized"), 401

    eoid = _safe_oid(employee_id)
    coid = _safe_oid(case_id)
    if not eoid or not coid:
        return jsonify(ok=False, message="Invalid IDs."), 400

    payload = request.get_json(silent=True) or {}
    note = (payload.get("note") or "").strip()
    if not note:
        return jsonify(ok=False, message="Follow-up note is required."), 400

    now = _now()
    followup_doc = {"_id": ObjectId(), "note": note, "created_at": now, "recorded_by": "HR"}

    res = users_col.update_one(
        {"_id": eoid},
        {
            "$push": {"case_records.$[c].followups": {"$each": [followup_doc], "$position": 0}},
            "$set": {"case_records.$[c].updated_at": now, "updated_at": now},
        },
        array_filters=[{"c._id": coid}],
    )

    if not res.matched_count:
        return jsonify(ok=False, message="Employee not found."), 404
    if not res.modified_count:
        return jsonify(ok=False, message="Case not found."), 404

    return jsonify(ok=True, message="Follow-up added.")

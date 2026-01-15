from datetime import datetime, timedelta
from bson import ObjectId
from flask import Blueprint, render_template, jsonify
from db import db

employee_rating_bp = Blueprint("employee_rating", __name__, url_prefix="/hr")

users_col     = db["users"]
customers_col = db["customers"]
payments_col  = db["payments"]


# ---------------- Helpers ----------------
def _oid(s: str):
    try:
        return ObjectId(s)
    except Exception:
        return None

def _date_to_str(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d")

def _this_month_range():
    today = datetime.utcnow()
    start = today.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

    if today.month == 12:
        next_month = start.replace(year=today.year + 1, month=1, day=1)
    else:
        next_month = start.replace(month=today.month + 1, day=1)

    end = next_month - timedelta(microseconds=1)
    return start, end

def _clamp(n, mn, mx):
    return max(mn, min(mx, n))

def _stars_from_score(score100: float) -> int:
    s = _clamp(score100, 0, 100)
    if s >= 85: return 5
    if s >= 70: return 4
    if s >= 55: return 3
    if s >= 40: return 2
    return 1

def _safe_float(x):
    try:
        return float(x or 0)
    except Exception:
        return 0.0


# ---------------- Core metrics (PAYMENTS + CUSTOMERS only) ----------------
def _agent_metrics(agent_id_str: str, start_dt: datetime, end_dt: datetime):
    start_str = _date_to_str(start_dt)
    end_str   = _date_to_str(end_dt)

    pay_q = {
        "agent_id": agent_id_str,
        "payment_type": {"$ne": "WITHDRAWAL"},
        "date": {"$gte": start_str, "$lte": end_str},
    }

    # total sales
    pipe = [
        {"$match": pay_q},
        {"$group": {"_id": None, "total": {"$sum": {"$ifNull": ["$amount", 0]}}}}
    ]
    agg = list(payments_col.aggregate(pipe))
    total_sales = _safe_float(agg[0]["total"]) if agg else 0.0

    # total customers (all-time)
    total_customers = customers_col.count_documents({"agent_id": agent_id_str})

    # active customers (range)
    active_customer_ids = payments_col.distinct("customer_id", pay_q)
    active_customers = len(active_customer_ids) if active_customer_ids else 0

    # working days (attendance proxy) = distinct payment days in range
    distinct_days = payments_col.distinct("date", pay_q)
    present_days = len(distinct_days) if distinct_days else 0

    # calendar working days for the month (Mon-Sat or Mon-Fri?) -> choose Mon-Sat for agents usually.
    # Here I’ll use Mon-Sat (exclude Sundays).
    cur = start_dt.date()
    endd = end_dt.date()
    working_days = 0
    while cur <= endd:
        if cur.weekday() != 6:  # 6 = Sunday
            working_days += 1
        cur += timedelta(days=1)

    return {
        "total_sales": round(total_sales, 2),
        "total_customers": int(total_customers),
        "active_customers": int(active_customers),
        "present_days": int(present_days),
        "working_days": int(working_days),
        "leads_total": 0,  # reserved for later
    }


def _calc_rating(summary: dict):
    # weights you want
    W_ACTIVE = 0.35
    W_SALES  = 0.35
    W_LEADS  = 0.20
    W_WORK   = 0.10

    # targets (tune later per branch/role)
    TARGET_SALES = 15000
    TARGET_LEADS = 30

    total_sales      = _safe_float(summary.get("total_sales"))
    active_customers = int(summary.get("active_customers") or 0)
    total_customers  = int(summary.get("total_customers") or 0)
    leads_total      = int(summary.get("leads_total") or 0)
    present_days     = int(summary.get("present_days") or 0)
    working_days     = int(summary.get("working_days") or 0)

    score_active = _clamp((active_customers / total_customers) * 100, 0, 100) if total_customers > 0 else 0
    score_sales  = _clamp((total_sales / TARGET_SALES) * 100, 0, 100) if TARGET_SALES > 0 else 0
    score_leads  = _clamp((leads_total / TARGET_LEADS) * 100, 0, 100) if TARGET_LEADS > 0 else 0
    score_work   = _clamp((present_days / working_days) * 100, 0, 100) if working_days > 0 else 0

    total_score = (
        W_ACTIVE * score_active +
        W_SALES  * score_sales +
        W_LEADS  * score_leads +
        W_WORK   * score_work
    )

    return {
        "score": float(total_score),
        "stars": int(_stars_from_score(total_score)),
        "weights": {
            "active_customers": W_ACTIVE,
            "sales": W_SALES,
            "leads": W_LEADS,
            "working_days": W_WORK,
        },
        "breakdown": {
            "score_active_customers": float(score_active),
            "score_sales": float(score_sales),
            "score_leads": float(score_leads),
            "score_working_days": float(score_work),
        }
    }


# ---------------- Routes ----------------
@employee_rating_bp.route("/employee/<employee_id>/ratings-tab", methods=["GET"])
def ratings_tab(employee_id):
    emp_oid = _oid(employee_id)
    if not emp_oid:
        return "Invalid employee id", 400

    employee = users_col.find_one(
        {"_id": emp_oid},
        {"name": 1, "role": 1, "branch": 1, "phone": 1, "image_url": 1}
    )
    if not employee:
        return "Employee not found", 404

    return render_template(
        "hr_pages/partials/employee_rating.html",
        employee=employee,
        employee_id=str(emp_oid),
    )


@employee_rating_bp.route("/employee/<employee_id>/ratings/summary", methods=["GET"])
def rating_summary(employee_id):
    emp_oid = _oid(employee_id)
    if not emp_oid:
        return jsonify(ok=False, message="Invalid employee id"), 400

    employee = users_col.find_one({"_id": emp_oid}, {"role": 1, "name": 1})
    if not employee:
        return jsonify(ok=False, message="Employee not found"), 404

    role = (employee.get("role") or "").lower()
    start_dt, end_dt = _this_month_range()

    # AGENT-like (anything not manager counts as individual performer)
    if role != "manager":
        summary = _agent_metrics(str(emp_oid), start_dt, end_dt)
        result = _calc_rating(summary)
        return jsonify(
            ok=True,
            source="payments+customers",
            employee={"id": employee_id, "role": role, "name": employee.get("name", "")},
            range={"start": _date_to_str(start_dt), "end": _date_to_str(end_dt)},
            summary=summary,
            result=result
        )

    # MANAGER aggregate: agents under manager_id (ObjectId or string)
    team_agents = list(users_col.find(
        {"role": "agent", "$or": [{"manager_id": emp_oid}, {"manager_id": str(emp_oid)}]},
        {"_id": 1}
    ))

    if not team_agents:
        summary = {
            "total_sales": 0.0,
            "total_customers": 0,
            "active_customers": 0,
            "present_days": 0,
            "working_days": 0,
            "leads_total": 0,
            "team_size": 0
        }
        result = _calc_rating(summary)
        return jsonify(
            ok=True,
            source="payments+customers",
            employee={"id": employee_id, "role": role, "name": employee.get("name", "")},
            range={"start": _date_to_str(start_dt), "end": _date_to_str(end_dt)},
            summary=summary,
            result=result,
            note="No agents found under this manager."
        )

    total_sales = 0.0
    total_customers = 0
    active_customers = 0
    present_days = 0
    working_days = 0

    for a in team_agents:
        s = _agent_metrics(str(a["_id"]), start_dt, end_dt)
        total_sales      += _safe_float(s.get("total_sales"))
        total_customers  += int(s.get("total_customers") or 0)
        active_customers += int(s.get("active_customers") or 0)
        present_days     += int(s.get("present_days") or 0)
        working_days      = int(s.get("working_days") or working_days)  # same month, so same

    summary = {
        "total_sales": round(total_sales, 2),
        "total_customers": int(total_customers),
        "active_customers": int(active_customers),
        "present_days": int(present_days),
        "working_days": int(working_days),
        "leads_total": 0,
        "team_size": len(team_agents)
    }
    result = _calc_rating(summary)

    return jsonify(
        ok=True,
        source="payments+customers",
        employee={"id": employee_id, "role": role, "name": employee.get("name", "")},
        range={"start": _date_to_str(start_dt), "end": _date_to_str(end_dt)},
        summary=summary,
        result=result
    )

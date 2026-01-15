from flask import Blueprint, render_template, request, abort
from flask_login import login_required, current_user
from bson.objectid import ObjectId
from datetime import datetime
from db import db

sales_close_agent_bp = Blueprint("sales_close_agent", __name__, url_prefix="/sales-close")

users_col        = db["users"]
sales_close_col  = db["sales_close"]
payments_col     = db["payments"]   # ← we read "today" totals from here


# ---------- helpers ----------
def _parse_date(s: str | None):
    if not s:
        return None
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except Exception:
        return None

def _role_guard():
    """
    Returns (role, user_id, user_doc)
    role ∈ {"agent","manager"} (admins/executives are treated like managers)
    """
    try:
        user_doc = users_col.find_one({"_id": ObjectId(current_user.id)})
    except Exception:
        user_doc = users_col.find_one({"_id": current_user.id})

    if not user_doc:
        abort(403)

    role = (user_doc.get("role") or "").lower()
    if role == "agent":
        return "agent", str(user_doc["_id"]), user_doc
    if role in ("manager", "admin", "executive"):
        return "manager", str(user_doc["_id"]), user_doc

    abort(403)

def _scope_agent_ids(role: str, user_id: str):
    """
    Scope = which agents to include:
    - agent  -> [current agent]
    - manager/admin/executive -> all agents with manager_id == user_id
    """
    if role == "agent":
        return [user_id]
    agents = list(users_col.find({"role": "agent", "manager_id": str(user_id)}, {"_id": 1}))
    return [str(a["_id"]) for a in agents]

def _range_totals_from_sales_close(agent_ids, start_date=None, end_date=None):
    """
    Sum range totals from sales_close (authoritative roll-up).
    Returns (range_total_ghs: float, range_count: int)
    """
    if not agent_ids:
        return 0.0, 0

    query = {"agent_id": {"$in": agent_ids}}
    if start_date and end_date:
        query["date"] = {"$gte": start_date.strftime("%Y-%m-%d"), "$lte": end_date.strftime("%Y-%m-%d")}
    elif start_date:
        query["date"] = {"$gte": start_date.strftime("%Y-%m-%d")}
    elif end_date:
        query["date"] = {"$lte": end_date.strftime("%Y-%m-%d")}

    pipeline = [
        {"$match": query},
        {"$group": {
            "_id": None,
            "sum_amount": {"$sum": {"$toDouble": "$total_amount"}},
            "sum_count":  {"$sum": {"$toInt": "$count"}}
        }}
    ]
    agg = list(sales_close_col.aggregate(pipeline))
    if not agg:
        return 0.0, 0
    return float(agg[0].get("sum_amount", 0.0)), int(agg[0].get("sum_count", 0))

def _today_total_from_payments(agent_ids):
    """
    Sum of today's payments (from payments collection). Excludes withdrawals.
    Uses YYYY-MM-DD in Ghana/UTC (Accra is UTC+0).
    Returns (today_total_ghs: float, today_count: int)
    """
    if not agent_ids:
        return 0.0, 0

    today_str = datetime.utcnow().strftime("%Y-%m-%d")
    query = {
        "agent_id": {"$in": agent_ids},
        "date": today_str,
        "payment_type": {"$ne": "WITHDRAWAL"}
    }
    pipeline = [
        {"$match": query},
        {"$group": {
            "_id": None,
            "sum_amount": {"$sum": {"$toDouble": "$amount"}},
            "sum_count":  {"$sum": 1}
        }}
    ]
    agg = list(payments_col.aggregate(pipeline))
    if not agg:
        return 0.0, 0
    return float(agg[0].get("sum_amount", 0.0)), int(agg[0].get("sum_count", 0))


# ---------- route ----------
@sales_close_agent_bp.route("/", methods=["GET"])
@login_required
def view_sales_close():
    """
    Dashboard showing only summary cards:
      - Range Total (GHS): from sales_close within optional start/end
      - Payments (count):  from sales_close within optional start/end
      - Today Total (GHS): from payments (today only)
      - Agents Covered:    count of agents in scope (manager sees all their agents)
    No history table. No CSV export.
    """
    role, user_id, _ = _role_guard()
    agent_ids = _scope_agent_ids(role, user_id)

    start = _parse_date(request.args.get("start"))
    end   = _parse_date(request.args.get("end"))

    range_total, range_count = _range_totals_from_sales_close(agent_ids, start, end)
    today_total, today_count = _today_total_from_payments(agent_ids)

    summaries = {
        "range_total": f"{range_total:,.2f}",
        "range_count": range_count,
        "today_total": f"{today_total:,.2f}",
        "today_count": today_count,
        "agents_count": (1 if role == "agent" else len(agent_ids))
    }

    return render_template(
        "sales_close_agent.html",
        role=role,
        summaries=summaries,
        # keep filter inputs working
        start_val=(start.strftime("%Y-%m-%d") if start else ""),
        end_val=(end.strftime("%Y-%m-%d") if end else "")
    )

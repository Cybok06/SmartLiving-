from flask import Blueprint, render_template, request, jsonify, session, redirect, url_for
from bson.objectid import ObjectId
from datetime import datetime, timedelta
from db import db

manager_expense_bp = Blueprint("manager_expense", __name__, url_prefix="/manager-expense")

users_col    = db["users"]
expenses_col = db["manager_expenses"]

# Allowed categories (data quality guard)
ALLOWED_CATEGORIES = [
    "Vehicle servicing",
    "Transportation",
    "Carriage Inwards",
    "Creditors",
    "Eggs",
    "Delievery",
    "Fuel",
    "SUSU Withdrawal",
    "Marketing (Activations)",
    "Stock (mini)",
    "Airtime",
    "Pre Paid light",
    "Serving",
    "Utilities",
    "Miscellaneous",
    "Salary (Monthly)",
    "Salaries",
    "Police Arrest",
    "AMA",
    "Rewards (commisions)"
]

# ------------------------------ Indexes ------------------------------
def _ensure_indexes():
    try:
        expenses_col.create_index([("manager_id", 1), ("created_at", -1)])
        expenses_col.create_index([("manager_id", 1), ("date", -1)])
        expenses_col.create_index([("manager_id", 1), ("category", 1)])
        expenses_col.create_index([("manager_id", 1), ("status", 1), ("created_at", -1)])  # for status filters + recency
    except Exception:
        pass
_ensure_indexes()

# ------------------------------ Helpers ------------------------------
def _is_ajax(req) -> bool:
    return (req.headers.get("X-Requested-With") or "").lower() == "xmlhttprequest"

def _today_str() -> str:
    return datetime.utcnow().strftime("%Y-%m-%d")

def _current_manager_session():
    """
    Accepts manager/admin/executive roles like the rest of the app.
    Returns (session_key, uid) or (None, None).
    """
    if session.get("manager_id"):
        return "manager_id", session["manager_id"]
    if session.get("admin_id"):
        return "admin_id", session["admin_id"]
    if session.get("executive_id"):
        return "executive_id", session["executive_id"]
    return None, None

def _ensure_manager_scope_or_redirect():
    """
    Ensure requester is manager/admin/executive via session (NOT flask_login).
    Returns (manager_id_str, manager_doc) or a redirect response to login.
    """
    _, uid = _current_manager_session()
    if not uid:
        return redirect(url_for("login.login"))
    try:
        manager_doc = users_col.find_one({"_id": ObjectId(uid)})
    except Exception:
        manager_doc = users_col.find_one({"_id": uid})
    if not manager_doc:
        return redirect(url_for("login.login"))
    role = (manager_doc.get("role") or "").lower()
    if role not in ("manager", "admin", "executive"):
        return redirect(url_for("login.login"))
    return str(manager_doc["_id"]), manager_doc

def _parse_amount(raw) -> float:
    try:
        return float(str(raw).replace(",", "").strip())
    except Exception:
        return 0.0

def _range_dates(range_key: str):
    """
    Return (start_utc, end_utc) for range:
      - today:      00:00 today → +1 day
      - week:       ISO week (Mon 00:00) → now
      - last7:      now - 7d → now
      - last30:     now - 30d → now
      - month:      first of month 00:00 → first of next month 00:00
    """
    now = datetime.utcnow()
    if range_key == "today":
        start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        return start, start + timedelta(days=1)
    if range_key == "week":
        start = (now - timedelta(days=now.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
        return start, now
    if range_key == "last7":
        return now - timedelta(days=7), now
    if range_key == "last30":
        return now - timedelta(days=30), now
    if range_key == "month":
        start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        end = start.replace(year=start.year + 1, month=1) if start.month == 12 else start.replace(month=start.month + 1)
        return start, end
    # default
    return now - timedelta(days=30), now

# ------------------------------ Views ------------------------------
@manager_expense_bp.route("/", methods=["GET"])
def expense_page():
    """
    Renders the manager expense page:
      - Table of last 30 days (Unapproved first, then newest)
      - Template also loads charts + KPI cards via /stats on the client
    """
    scope = _ensure_manager_scope_or_redirect()
    if not isinstance(scope, tuple):
        return scope
    manager_id, manager_doc = scope

    start, end = _range_dates("last30")
    cursor = expenses_col.find(
        {"manager_id": manager_id, "created_at": {"$gte": start, "$lte": end}},
        {"_id": 1, "date": 1, "time": 1, "category": 1, "description": 1, "amount": 1, "status": 1}
    ).sort([("status", 1), ("created_at", -1)])  # Unapproved (alphabetically earlier) first, then newest

    rows, total_last30 = [], 0.0
    for d in cursor:
        amt = float(d.get("amount", 0) or 0)
        total_last30 += amt
        rows.append({
            "_id": str(d["_id"]),
            "date": d.get("date", ""),
            "time": d.get("time", ""),
            "category": d.get("category", ""),
            "description": d.get("description", ""),
            "status": d.get("status", "Unapproved"),
            "amount": f"{amt:,.2f}"
        })

    return render_template(
        "manager_expense.html",
        manager_name=manager_doc.get("name", "Manager"),
        categories=ALLOWED_CATEGORIES,
        today=_today_str(),
        rows=rows,
        total_last30=f"{total_last30:,.2f}"
    )

@manager_expense_bp.route("/create", methods=["POST"])
def create_expense():
    """
    Creates a new expense as Unapproved by default.
    Body: date, category, amount, description
    """
    scope = _ensure_manager_scope_or_redirect()
    if not isinstance(scope, tuple):
        if _is_ajax(request):
            return jsonify(ok=False, message="Please log in."), 401
        return scope
    manager_id, _ = scope

    date_str = request.form.get("date") or _today_str()
    category = (request.form.get("category") or "").strip()
    amount   = _parse_amount(request.form.get("amount"))
    desc     = (request.form.get("description") or "").strip()

    if category not in ALLOWED_CATEGORIES:
        msg = "Invalid category."
        return (jsonify(ok=False, message=msg), 400) if _is_ajax(request) else (msg, 400)
    if amount <= 0:
        msg = "Amount must be greater than zero."
        return (jsonify(ok=False, message=msg), 400) if _is_ajax(request) else (msg, 400)

    now = datetime.utcnow()
    time_str = now.strftime("%H:%M:%S")

    doc = {
        "manager_id": manager_id,
        "category": category,
        "amount": float(round(amount, 2)),
        "description": desc,
        "date": date_str,     # YYYY-MM-DD
        "time": time_str,     # HH:MM:SS
        "status": "Unapproved",
        "created_at": now,
        "updated_at": now,
        # audit placeholders for future approval flow
        "approved_at": None,
        "approved_by": None
    }
    res = expenses_col.insert_one(doc)

    payload = {
        "ok": True,
        "message": "Expense recorded and pending approval.",
        "item": {
            "_id": str(res.inserted_id),
            "date": date_str,
            "time": time_str,
            "category": category,
            "description": desc,
            "status": "Unapproved",
            "amount": f"{amount:,.2f}"
        }
    }
    return jsonify(payload) if _is_ajax(request) else "Expense recorded and pending approval."

@manager_expense_bp.route("/stats", methods=["GET"])
def expense_stats():
    """
    Grouped totals for a time range.

    Query params:
      range  = today | week | last7 | last30 | month   (default: last30)
      status = Approved | Unapproved | All            (default: Approved)

    Response:
      {
        ok: true,
        total: float,
        by_category: [{category, total}],
        top5: [...]
      }
    """
    scope = _ensure_manager_scope_or_redirect()
    if not isinstance(scope, tuple):
        return jsonify(ok=False, message="Please log in."), 401
    manager_id, _ = scope

    rng = request.args.get("range", "last30")
    status = (request.args.get("status") or "Approved").title()
    if status not in ("Approved", "Unapproved", "All"):
        status = "Approved"

    start, end = _range_dates(rng)

    match = {
        "manager_id": manager_id,
        "created_at": {"$gte": start, "$lte": end}
    }
    if status != "All":
        match["status"] = status

    pipeline = [
        {"$match": match},
        {"$group": {
            "_id": "$category",
            "sum_amount": {"$sum": {"$toDouble": {"$ifNull": ["$amount", 0]}}}
        }},
        {"$sort": {"sum_amount": -1}}
    ]
    agg = list(expenses_col.aggregate(pipeline))

    by_cat = [{"category": a["_id"], "total": float(a["sum_amount"])} for a in agg]
    total = sum(x["total"] for x in by_cat)
    return jsonify(ok=True, total=round(total, 2), by_category=by_cat, top5=by_cat[:5])



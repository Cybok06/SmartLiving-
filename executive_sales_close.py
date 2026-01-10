# executive_sales_close.py
from flask import Blueprint, render_template, request, jsonify, session, redirect, url_for
from bson.objectid import ObjectId
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple
from db import db

executive_sales_close_bp = Blueprint(
    "executive_sales_close",
    __name__,
    url_prefix="/executive-close"
)

users_col             = db["users"]
sales_close_col       = db["sales_close"]
manager_expenses_col  = db["manager_expenses"]

# ---------- optional: indexes (safe to run repeatedly) ----------
def _ensure_indexes():
    try:
        sales_close_col.create_index([("agent_id", 1), ("date", -1)])
        sales_close_col.create_index([("agent_id", 1), ("updated_at", -1)])
        manager_expenses_col.create_index([("manager_id", 1), ("date", -1), ("status", 1)])
    except Exception:
        pass

_ensure_indexes()

# -------------------------------------------------------------------
# Example helpful indexes (for reference, not executed here):
# db.sales_close.createIndex({ agent_id: 1, date: -1 })
# db.sales_close.createIndex({ agent_id: 1, total_amount: 1 })
# db.sales_close.createIndex({ date: -1, updated_at: -1 })
# db.users.createIndex({ role: 1 })
# db.manager_expenses.createIndex({ manager_id: 1, date: -1, status: 1 })
# -------------------------------------------------------------------

# ---------- basic helpers ----------

def _is_ajax(req) -> bool:
    return req.headers.get("X-Requested-With", "").lower() == "xmlhttprequest"

def _today_str() -> str:
    # Use UTC "today" for daily docs; adjust later if you localize.
    return datetime.utcnow().strftime("%Y-%m-%d")

def _ensure_executive_or_redirect():
    """
    Require an Executive session (not flask_login).
    Returns (exec_id_str, exec_doc) or a redirect to /login.
    """
    exec_id = session.get("executive_id")
    if not exec_id:
        return redirect(url_for("login.login"))

    try:
        exec_doc = users_col.find_one({"_id": ObjectId(exec_id)})
    except Exception:
        exec_doc = users_col.find_one({"_id": exec_id})

    if not exec_doc:
        return redirect(url_for("login.login"))

    role = (exec_doc.get("role") or "").lower()
    if role != "executive":
        return redirect(url_for("login.login"))

    return str(exec_doc["_id"]), exec_doc

def _sum_ledger_all_dates(owner_id_str: str) -> float:
    """
    Sum total_amount across ALL sales_close docs for the given owner_id (agent_id in ledger).
    (Handles string/number types safely.)
    """
    pipeline = [
        {"$match": {"agent_id": owner_id_str}},
        {"$group": {"_id": None, "sum_amount": {"$sum": {
            "$toDouble": {"$ifNull": ["$total_amount", 0]}
        }}}}
    ]
    agg = list(sales_close_col.aggregate(pipeline))
    if not agg:
        return 0.0
    try:
        return float(agg[0].get("sum_amount", 0.0))
    except Exception:
        return 0.0

# ---------- date & expense / gross helpers (mirrors manager logic) ----------

def _date_range_strings(key: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Returns (start_str, end_str) as 'YYYY-MM-DD' for:
      - 'today' : today only
      - 'week'  : Monday -> today
      - 'month' : 1st of month -> today
    If key is unrecognised, returns (None, None) meaning "all time".
    """
    today = datetime.utcnow().date()

    if key == "today":
        s = today.strftime("%Y-%m-%d")
        return s, s

    if key == "week":
        start = today - timedelta(days=today.weekday())
        return start.strftime("%Y-%m-%d"), today.strftime("%Y-%m-%d")

    if key == "month":
        start = today.replace(day=1)
        return start.strftime("%Y-%m-%d"), today.strftime("%Y-%m-%d")

    return None, None  # all time

def _manager_approved_expenses_total(manager_id_str: Optional[str], range_key: Optional[str]) -> float:
    """
    Sum of APPROVED expenses:
      - If manager_id_str is given: only that manager's expenses.
      - If manager_id_str is None: all managers' expenses.
    range_key: None/'total' -> all time, otherwise 'today'|'week'|'month'.
    """
    match: Dict[str, Any] = {
        "status": "Approved",
    }
    if manager_id_str is not None:
        match["manager_id"] = manager_id_str

    if range_key and range_key != "total":
        start_str, end_str = _date_range_strings(range_key)
        if start_str:
            match["date"] = {"$gte": start_str, "$lte": end_str}

    pipeline = [
        {"$match": match},
        {"$group": {
            "_id": None,
            "sum_amount": {"$sum": {"$toDouble": {"$ifNull": ["$amount", 0]}}}
        }}
    ]
    agg = list(manager_expenses_col.aggregate(pipeline))
    if not agg:
        return 0.0
    try:
        return float(agg[0].get("sum_amount", 0.0))
    except Exception:
        return 0.0

def _owner_ledger_flow_for_range(owner_id_str: Optional[str], range_key: Optional[str]) -> Dict[str, float]:
    """
    For sales_close docs:
      - If owner_id_str is given: only that owner's ledger (agent_id == owner_id_str).
      - If owner_id_str is None: ALL docs (all owners).

    For a given period:
      available = sum of current total_amount (remaining)   in docs in that date range
      withdrawn = sum of withdrawals[].amount              in docs in that date range
      gross_before_expense = available + withdrawn
    """
    match: Dict[str, Any] = {}
    if owner_id_str is not None:
        match["agent_id"] = owner_id_str

    if range_key and range_key != "total":
        start_str, end_str = _date_range_strings(range_key)
        if start_str:
            match["date"] = {"$gte": start_str, "$lte": end_str}

    pipeline = [
        {"$match": match},
        {
            "$project": {
                "bal_num": {
                    "$toDouble": {"$ifNull": ["$total_amount", 0]}
                },
                "withdrawals_amounts": {
                    "$map": {
                        "input": {"$ifNull": ["$withdrawals", []]},
                        "as": "w",
                        "in": {
                            "$toDouble": {"$ifNull": ["$$w.amount", 0]}
                        },
                    }
                },
            }
        },
        {
            "$project": {
                "bal_num": 1,
                "withdrawals_sum": {"$sum": "$withdrawals_amounts"},
            }
        },
        {
            "$group": {
                "_id": None,
                "sum_bal": {"$sum": "$bal_num"},
                "sum_withdrawn": {"$sum": "$withdrawals_sum"},
            }
        },
    ]

    agg = list(sales_close_col.aggregate(pipeline))
    if not agg:
        return {"available": 0.0, "withdrawn": 0.0, "gross": 0.0}

    doc = agg[0]
    available = float(doc.get("sum_bal", 0.0))
    withdrawn = float(doc.get("sum_withdrawn", 0.0))
    gross = available + withdrawn

    return {"available": available, "withdrawn": withdrawn, "gross": gross}

def _manager_balance_breakdown(manager_id_str: str) -> Dict[str, Dict[str, float]]:
    """
    Per-manager / per-branch breakdown for:
      - today, week, month, total

    For each period:
      col   = Σ(total_amount + withdrawals.amount) in that manager's ledger
      exp   = approved expenses for that manager in that period
      gross = col − exp
    """
    periods = ["total", "month", "week", "today"]
    out: Dict[str, Dict[str, float]] = {}

    for p in periods:
        rng = None if p == "total" else p
        flow = _owner_ledger_flow_for_range(manager_id_str, rng)
        col = flow["gross"]
        exp = _manager_approved_expenses_total(manager_id_str, rng)
        gross_after = col - exp
        out[p] = {"col": col, "exp": exp, "gross": gross_after}

    return out

def _all_branches_balance_breakdown(manager_ids: List[str]) -> Dict[str, Dict[str, float]]:
    """
    Sum of all manager branches:
      For each period, we sum:
        - col (collections)
        - exp (expenses)
        - gross (col - exp)
    """
    periods = ["total", "month", "week", "today"]
    agg_out: Dict[str, Dict[str, float]] = {
        p: {"col": 0.0, "exp": 0.0, "gross": 0.0} for p in periods
    }

    for mid in manager_ids:
        mb = _manager_balance_breakdown(mid)
        for p in periods:
            agg_out[p]["col"]   += mb[p]["col"]
            agg_out[p]["exp"]   += mb[p]["exp"]
            agg_out[p]["gross"] += mb[p]["gross"]

    return agg_out

# ---------- role grouping helper (existing) ----------

def _group_totals_for_roles(roles: List[str]) -> List[Dict[str, Any]]:
    """
    FAST path: one aggregation to get TOTAL (all dates) per user for the given roles.
      - Group sales_close by agent_id
      - $lookup users by stringified _id, filter by roles (lowercased)
      - Return: { user_id(str), total(float), name, phone, role(lower) }, sorted DESC.
    """
    roles = [r.lower() for r in roles]
    pipeline = [
        {"$group": {"_id": "$agent_id", "total": {"$sum": {
            "$toDouble": {"$ifNull": ["$total_amount", 0]}
        }}}},
        {"$lookup": {
            "from": "users",
            "let": {"aid": "$_id"},
            "pipeline": [
                {"$addFields": {"_id_str": {"$toString": "$_id"}}},
                {"$match": {"$expr": {"$and": [
                    {"$eq": ["$_id_str", "$$aid"]},
                    {"$in": [{"$toLower": "$role"}, roles]}
                ]}}},
                {"$project": {"name": 1, "username": 1, "phone": 1, "role": 1}}
            ],
            "as": "user"
        }},
        {"$unwind": "$user"},
        {"$project": {
            "_id": 0,
            "user_id": "$_id",
            "total": 1,
            "name": {"$ifNull": ["$user.name", "$user.username"]},
            "phone": "$user.phone",
            "role": {"$toLower": "$user.role"},
        }},
        {"$sort": {"total": -1}}
    ]
    return list(sales_close_col.aggregate(pipeline))

def _format_user_total_row(row: Dict[str, Any]) -> Dict[str, Any]:
    total = float(row.get("total", 0.0))
    return {
        "_id": row["user_id"],
        "name": row.get("name") or "User",
        "phone": row.get("phone", ""),
        "role": row["role"],
        "available": f"{total:,.2f}",
        "available_num": total,
    }

def _sum_totals_for_roles(roles: List[str]) -> float:
    roles = [r.lower() for r in roles]
    pipeline = [
        {"$group": {"_id": "$agent_id", "total": {"$sum": {
            "$toDouble": {"$ifNull": ["$total_amount", 0]}
        }}}},
        {"$lookup": {
            "from": "users",
            "let": {"aid": "$_id"},
            "pipeline": [
                {"$addFields": {"_id_str": {"$toString": "$_id"}}},
                {"$match": {"$expr": {"$and": [
                    {"$eq": ["$_id_str", "$$aid"]},
                    {"$in": [{"$toLower": "$role"}, roles]}
                ]}}}
            ],
            "as": "user"
        }},
        {"$match": {"user.0": {"$exists": True}}},
        {"$group": {"_id": None, "sum_total": {"$sum": "$total"}}},
    ]
    agg = list(sales_close_col.aggregate(pipeline))
    if not agg:
        return 0.0
    try:
        return float(agg[0].get("sum_total", 0.0))
    except Exception:
        return 0.0

# ---------- views ----------

@executive_sales_close_bp.route("/", methods=["GET"])
def executive_close_page():
    """
    Executive dashboard:
      - All Branches Gross Today / Week / Month / Total:
           Gross = (Σ total_amount + Σ withdrawals.amount into manager ledgers) − Σ Approved manager expenses
      - Branch overview per manager (Gross per period + available)
      - Close Total (Executive ledger, all dates)
      - Unclose Total (sum of Admin+Manager+Agent balances, all dates)
      - Grids for Admins, Managers, Agents (same as before for withdrawals).
    """
    scope = _ensure_executive_or_redirect()
    if not isinstance(scope, tuple):
        return scope
    exec_id, exec_doc = scope

    today = _today_str()
    return render_template(
        "executive_sales_close.html",
        executive_name=exec_doc.get("name", "Executive"),
        today=today,
        branches=[],
        admins=[],
        managers=[],
        agents=[]
    )

def _build_executive_close_payload(exec_id: str, exec_doc: dict, include_agents: bool = True) -> Dict[str, Any]:
    today = _today_str()

    if include_agents:
        grouped = _group_totals_for_roles(["admin", "manager", "agent"])
    else:
        grouped = _group_totals_for_roles(["admin", "manager"])

    admins   = [_format_user_total_row(r) for r in grouped if r["role"] == "admin"   and r["user_id"] != exec_id]
    managers = [_format_user_total_row(r) for r in grouped if r["role"] == "manager"]
    agents   = [_format_user_total_row(r) for r in grouped if r["role"] == "agent"]

    manager_ids = [m["_id"] for m in managers]
    all_branches = _all_branches_balance_breakdown(manager_ids)

    def _fmt_val(v: float) -> Dict[str, Any]:
        return {"value": float(v), "formatted": f"{float(v):,.2f}"}

    all_gross_total = _fmt_val(all_branches["total"]["gross"])
    all_gross_month = _fmt_val(all_branches["month"]["gross"])
    all_gross_week  = _fmt_val(all_branches["week"]["gross"])
    all_gross_today = _fmt_val(all_branches["today"]["gross"])

    all_col_total = _fmt_val(all_branches["total"]["col"])
    all_col_month = _fmt_val(all_branches["month"]["col"])
    all_col_week  = _fmt_val(all_branches["week"]["col"])
    all_col_today = _fmt_val(all_branches["today"]["col"])

    all_exp_total = _fmt_val(all_branches["total"]["exp"])
    all_exp_month = _fmt_val(all_branches["month"]["exp"])
    all_exp_week  = _fmt_val(all_branches["week"]["exp"])
    all_exp_today = _fmt_val(all_branches["today"]["exp"])

    branches: List[Dict[str, Any]] = []
    for m in managers:
        mid = m["_id"]
        mb  = _manager_balance_breakdown(mid)

        try:
            m_doc = users_col.find_one(
                {"_id": ObjectId(mid)},
                {"branch": 1, "branch_name": 1, "name": 1, "username": 1}
            )
        except Exception:
            m_doc = users_col.find_one(
                {"_id": mid},
                {"branch": 1, "branch_name": 1, "name": 1, "username": 1}
            )

        branch_label = (
            (m_doc or {}).get("branch_name")
            or (m_doc or {}).get("branch")
            or m["name"]
        )

        branches.append({
            "manager_id": mid,
            "branch": branch_label,
            "name": m["name"],
            "phone": m["phone"],
            "available": m["available"],
            "available_num": m["available_num"],

            "gross_today": f"{mb['today']['gross']:,.2f}",
            "gross_week":  f"{mb['week']['gross']:,.2f}",
            "gross_month": f"{mb['month']['gross']:,.2f}",
            "gross_total": f"{mb['total']['gross']:,.2f}",

            "col_today": f"{mb['today']['col']:,.2f}",
            "col_week":  f"{mb['week']['col']:,.2f}",
            "col_month": f"{mb['month']['col']:,.2f}",
            "col_total": f"{mb['total']['col']:,.2f}",

            "exp_today": f"{mb['today']['exp']:,.2f}",
            "exp_week":  f"{mb['week']['exp']:,.2f}",
            "exp_month": f"{mb['month']['exp']:,.2f}",
            "exp_total": f"{mb['total']['exp']:,.2f}",
        })

    close_total_val   = _sum_ledger_all_dates(exec_id)
    if include_agents:
        unclose_total_val = float(sum(r["available_num"] for r in admins + managers + agents))
    else:
        unclose_total_val = _sum_totals_for_roles(["admin", "manager", "agent"])

    payload = {
        "ok": True,
        "executive_name": exec_doc.get("name", "Executive"),
        "today": today,
        "all_gross_total": all_gross_total,
        "all_gross_month": all_gross_month,
        "all_gross_week": all_gross_week,
        "all_gross_today": all_gross_today,
        "all_col_total": all_col_total,
        "all_col_month": all_col_month,
        "all_col_week": all_col_week,
        "all_col_today": all_col_today,
        "all_exp_total": all_exp_total,
        "all_exp_month": all_exp_month,
        "all_exp_week": all_exp_week,
        "all_exp_today": all_exp_today,
        "close_total": _fmt_val(close_total_val),
        "unclose_total": _fmt_val(unclose_total_val),
        "branches": branches,
        "admins": admins,
        "managers": managers,
    }
    if include_agents:
        payload["agents"] = agents
    return payload

def _summary_payload(exec_doc: dict, manager_ids: List[str]) -> Dict[str, Any]:
    today = _today_str()
    all_branches = _all_branches_balance_breakdown(manager_ids)

    def _fmt_val(v: float) -> Dict[str, Any]:
        return {"value": float(v), "formatted": f"{float(v):,.2f}"}

    close_total_val = _sum_ledger_all_dates(str(exec_doc.get("_id")))
    unclose_total_val = _sum_totals_for_roles(["admin", "manager", "agent"])

    return {
        "ok": True,
        "executive_name": exec_doc.get("name", "Executive"),
        "today": today,
        "all_gross_total": _fmt_val(all_branches["total"]["gross"]),
        "all_gross_month": _fmt_val(all_branches["month"]["gross"]),
        "all_gross_week": _fmt_val(all_branches["week"]["gross"]),
        "all_gross_today": _fmt_val(all_branches["today"]["gross"]),
        "all_col_total": _fmt_val(all_branches["total"]["col"]),
        "all_col_month": _fmt_val(all_branches["month"]["col"]),
        "all_col_week": _fmt_val(all_branches["week"]["col"]),
        "all_col_today": _fmt_val(all_branches["today"]["col"]),
        "all_exp_total": _fmt_val(all_branches["total"]["exp"]),
        "all_exp_month": _fmt_val(all_branches["month"]["exp"]),
        "all_exp_week": _fmt_val(all_branches["week"]["exp"]),
        "all_exp_today": _fmt_val(all_branches["today"]["exp"]),
        "close_total": _fmt_val(close_total_val),
        "unclose_total": _fmt_val(unclose_total_val),
    }

@executive_sales_close_bp.route("/api/summary", methods=["GET"])
def executive_close_summary():
    scope = _ensure_executive_or_redirect()
    if not isinstance(scope, tuple):
        return jsonify(ok=False, message="Unauthorized"), 401
    _, exec_doc = scope
    grouped_managers = _group_totals_for_roles(["manager"])
    manager_ids = [r["user_id"] for r in grouped_managers if r.get("user_id")]
    payload = _summary_payload(exec_doc, manager_ids)
    return jsonify(payload)

@executive_sales_close_bp.route("/api/admins", methods=["GET"])
def executive_close_admins():
    scope = _ensure_executive_or_redirect()
    if not isinstance(scope, tuple):
        return jsonify(ok=False, message="Unauthorized"), 401
    exec_id, _ = scope
    grouped_admins = _group_totals_for_roles(["admin"])
    admins = [
        _format_user_total_row(r)
        for r in grouped_admins
        if r["role"] == "admin" and r["user_id"] != exec_id
    ]
    return jsonify(ok=True, admins=admins)

@executive_sales_close_bp.route("/api/managers", methods=["GET"])
def executive_close_managers():
    scope = _ensure_executive_or_redirect()
    if not isinstance(scope, tuple):
        return jsonify(ok=False, message="Unauthorized"), 401
    grouped_managers = _group_totals_for_roles(["manager"])
    managers = [_format_user_total_row(r) for r in grouped_managers if r["role"] == "manager"]
    return jsonify(ok=True, managers=managers)

@executive_sales_close_bp.route("/api/branches", methods=["GET"])
def executive_close_branches():
    scope = _ensure_executive_or_redirect()
    if not isinstance(scope, tuple):
        return jsonify(ok=False, message="Unauthorized"), 401
    grouped_managers = _group_totals_for_roles(["manager"])
    managers = [_format_user_total_row(r) for r in grouped_managers if r["role"] == "manager"]
    branches: List[Dict[str, Any]] = []
    for m in managers:
        mid = m["_id"]
        mb = _manager_balance_breakdown(mid)

        try:
            m_doc = users_col.find_one(
                {"_id": ObjectId(mid)},
                {"branch": 1, "branch_name": 1, "name": 1, "username": 1}
            )
        except Exception:
            m_doc = users_col.find_one(
                {"_id": mid},
                {"branch": 1, "branch_name": 1, "name": 1, "username": 1}
            )

        branch_label = (
            (m_doc or {}).get("branch_name")
            or (m_doc or {}).get("branch")
            or m["name"]
        )

        branches.append({
            "manager_id": mid,
            "branch": branch_label,
            "name": m["name"],
            "phone": m["phone"],
            "available": m["available"],
            "available_num": m["available_num"],
            "gross_today": f"{mb['today']['gross']:,.2f}",
            "gross_week":  f"{mb['week']['gross']:,.2f}",
            "gross_month": f"{mb['month']['gross']:,.2f}",
            "gross_total": f"{mb['total']['gross']:,.2f}",
            "col_today": f"{mb['today']['col']:,.2f}",
            "col_week":  f"{mb['week']['col']:,.2f}",
            "col_month": f"{mb['month']['col']:,.2f}",
            "col_total": f"{mb['total']['col']:,.2f}",
            "exp_today": f"{mb['today']['exp']:,.2f}",
            "exp_week":  f"{mb['week']['exp']:,.2f}",
            "exp_month": f"{mb['month']['exp']:,.2f}",
            "exp_total": f"{mb['total']['exp']:,.2f}",
        })
    return jsonify(ok=True, branches=branches)

@executive_sales_close_bp.route("/api/agents", methods=["GET"])
def executive_close_agents():
    scope = _ensure_executive_or_redirect()
    if not isinstance(scope, tuple):
        return jsonify(ok=False, message="Unauthorized"), 401
    grouped = _group_totals_for_roles(["agent"])
    agents = [_format_user_total_row(r) for r in grouped if r["role"] == "agent"]
    return jsonify(ok=True, agents=agents)

@executive_sales_close_bp.route("/withdraw", methods=["POST"])
def executive_withdraw():
    """
    POST: target_id, amount, note (optional)

    Behaviour:
      - Debits across multiple sales_close docs of the TARGET (today first, then most recent -> older),
        using $expr/$toDouble so both numeric and string balances are handled.
      - Credits the EXECUTIVE'S TODAY doc with the total actually withdrawn.
      - Returns refreshed totals (all dates) + per-date debit breakdown.
    """
    scope = _ensure_executive_or_redirect()
    if not isinstance(scope, tuple):
        if _is_ajax(request):
            return jsonify(ok=False, message="Please log in."), 401
        return scope
    exec_id, exec_doc = scope

    target_id = (request.form.get("target_id") or (request.json.get("target_id") if request.is_json else "")) or ""
    amount_in = request.form.get("amount") or (request.json.get("amount") if request.is_json else None)
    note      = (request.form.get("note") or (request.json.get("note") if request.is_json else "")) or ""

    try:
        amount = float(amount_in)
    except Exception:
        amount = 0.0

    if not target_id or amount <= 0:
        msg = "Target and a positive amount are required."
        return (jsonify(ok=False, message=msg), 400) if _is_ajax(request) else (msg, 400)

    # Load target user (any role: agent/manager/admin)
    try:
        tgt_doc = users_col.find_one({"_id": ObjectId(target_id)})
    except Exception:
        tgt_doc = users_col.find_one({"_id": target_id})
    if not tgt_doc:
        msg = "Target user not found."
        return (jsonify(ok=False, message=msg), 404) if _is_ajax(request) else (msg, 404)

    tgt_role = (tgt_doc.get("role") or "").lower()
    if tgt_role not in ("agent", "manager", "admin"):
        msg = "You can only withdraw from agents, managers, or admins."
        return (jsonify(ok=False, message=msg), 403) if _is_ajax(request) else (msg, 403)

    # --- Build candidate docs to debit: today first, then recent->older; only with positive balance ---
    today   = _today_str()
    now_utc = datetime.utcnow()
    time_str = now_utc.strftime("%H:%M:%S")

    pipeline = [
        {"$match": {"agent_id": str(target_id)}},
        {"$addFields": {
            "bal_num": {"$toDouble": {"$ifNull": ["$total_amount", 0]}},
            "is_today": {"$cond": [{"$eq": ["$date", today]}, 1, 0]}
        }},
        {"$match": {"bal_num": {"$gt": 0}}},
        {"$sort": {"is_today": -1, "date": -1, "updated_at": -1}}
    ]
    docs = list(sales_close_col.aggregate(pipeline))

    # Quick total across all dates
    total_all = float(sum(float(d.get("bal_num", 0.0)) for d in docs))
    if total_all + 1e-9 < amount:
        msg = f"Insufficient balance. Target total across all days: GHS {total_all:,.2f}"
        return (jsonify(ok=False, message=msg, available=f"{total_all:,.2f}"), 409) if _is_ajax(request) else (msg, 409)

    remaining = amount
    debits: List[Dict[str, Any]] = []  # breakdown: [{date, debited}]

    # --- Debit across multiple docs until covered ---
    for d in docs:
        if remaining <= 1e-9:
            break
        doc_id   = d["_id"]
        date_str = d.get("date", "")
        available = float(d.get("bal_num", 0.0))
        if available <= 0:
            continue

        take = min(available, remaining)

        # Safe compare even if total_amount is string
        filter_q = {
            "_id": doc_id,
            "$expr": {"$gte": [
                {"$toDouble": {"$ifNull": ["$total_amount", 0]}},
                take
            ]}
        }
        update_q = {
            "$inc": {"total_amount": -take},
            "$set": {"updated_at": now_utc, "last_withdrawal_at": now_utc},
            "$push": {"withdrawals": {
                "amount": float(round(take, 2)),
                "by_executive_id": exec_id,
                "by_executive_name": exec_doc.get("name", ""),
                "by_role": "executive",
                "date": date_str,
                "time": time_str,
                "at": now_utc,
                "note": note
            }}
        }
        res = sales_close_col.update_one(filter_q, update_q)
        if res.modified_count == 1:
            debits.append({"date": date_str, "debited": take})
            remaining -= take
        # Else: concurrent change; try next doc.

    actually_debited = amount - remaining
    if actually_debited <= 0:
        # Concurrency edge-case: recompute and respond
        current_total = _sum_ledger_all_dates(str(target_id))
        msg = f"Insufficient balance due to concurrent changes. Current total: GHS {current_total:,.2f}"
        return (jsonify(ok=False, message=msg, available=f"{current_total:,.2f}"), 409) if _is_ajax(request) else (msg, 409)

    # --- Credit EXECUTIVE TODAY doc by the amount actually debited ---
    exec_filter = {"agent_id": exec_id, "date": today}
    exec_update = {
        "$setOnInsert": {"agent_id": exec_id, "manager_id": exec_id, "date": today, "created_at": now_utc},
        "$inc": {"total_amount": actually_debited, "count": 1},
        "$set": {"updated_at": now_utc, "last_payment_at": now_utc}
    }
    sales_close_col.update_one(exec_filter, exec_update, upsert=True)

    # --- Recompute refreshed totals (ALL dates) — minimal roundtrips ---
    target_total  = _sum_ledger_all_dates(str(target_id))                # one agg
    grouped       = _group_totals_for_roles(["admin", "manager", "agent"])  # one agg for unclose
    unclose_total = float(sum(float(r.get("total", 0.0)) for r in grouped))
    close_total   = _sum_ledger_all_dates(exec_id)                       # one agg

    payload = {
        "ok": True,
        "message": (
            f"Withdrew GHS {actually_debited:,.2f} across {len(debits)} day(s) "
            f"from {tgt_role} and credited executive account."
        ),
        "requested": f"{amount:,.2f}",
        "debited_breakdown": [{"date": x["date"], "amount": f"{x['debited']:,.2f}"} for x in debits],
        "target_id": str(target_id),
        "target_role": tgt_role,
        "available": f"{target_total:,.2f}",     # target TOTAL (all dates)
        "unclose_total": f"{unclose_total:,.2f}",
        "close_total": f"{close_total:,.2f}"
    }
    return jsonify(payload) if _is_ajax(request) else (
        f"OK. Debited: {payload['requested']} | "
        f"Target total: {payload['available']} | "
        f"Unclose Total: {payload['unclose_total']} | "
        f"Close Total: {payload['close_total']}"
    )

@executive_sales_close_bp.route("/user/<user_id>/withdrawals", methods=["GET"])
def user_withdrawals(user_id):
    """
    Returns JSON history of withdrawals for the given user (any allowed role) across ALL dates.
    Each item: { amount, date, time, note, by_name, by_role, at_iso }
    """
    scope = _ensure_executive_or_redirect()
    if not isinstance(scope, tuple):
        return jsonify(ok=False, message="Please log in."), 401

    # Ensure target user exists & is allowed
    try:
        tgt_doc = users_col.find_one({"_id": ObjectId(user_id)})
    except Exception:
        tgt_doc = users_col.find_one({"_id": user_id})
    if not tgt_doc:
        return jsonify(ok=False, message="User not found."), 404

    tgt_role = (tgt_doc.get("role") or "").lower()
    if tgt_role not in ("agent", "manager", "admin"):
        return jsonify(ok=False, message="History available only for agent/manager/admin."), 403

    # Project withdrawals only (lightweight)
    cursor = sales_close_col.find({"agent_id": str(user_id)}, {"withdrawals": 1})
    items: List[Dict[str, Any]] = []
    for d in cursor:
        for w in (d.get("withdrawals") or []):
            at = w.get("at")
            if isinstance(at, datetime):
                at_iso = at.isoformat()
            else:
                at_iso = f"{w.get('date','')}T{w.get('time','00:00:00')}"

            by_name = (
                w.get("by_name")
                or w.get("by_executive_name")
                or w.get("by_admin_name")
                or w.get("by_manager_name")
                or ""
            )
            by_role = (
                w.get("by_role")
                or ("executive" if w.get("by_executive_id") else
                    ("admin" if w.get("by_admin_id") else
                     ("manager" if w.get("by_manager_id") else "")))
            )

            items.append({
                "amount": float(w.get("amount", 0.0)),
                "date": w.get("date", ""),
                "time": w.get("time", ""),
                "note": w.get("note", ""),
                "by_name": by_name,
                "by_role": by_role,
                "at_iso": at_iso
            })

    items.sort(key=lambda x: x.get("at_iso", ""), reverse=True)

    return jsonify(
        ok=True,
        user={
            "_id": str(tgt_doc["_id"]),
            "name": tgt_doc.get("name") or tgt_doc.get("username") or "User",
            "phone": tgt_doc.get("phone", ""),
            "role": tgt_role,
        },
        withdrawals=items
    )

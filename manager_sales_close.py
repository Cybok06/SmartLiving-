from flask import Blueprint, render_template, request, jsonify, session, redirect, url_for
from bson.objectid import ObjectId
from datetime import datetime, timedelta
from typing import Dict, Any, List, Tuple, Optional

from db import db

manager_sales_close_bp = Blueprint("manager_sales_close", __name__, url_prefix="/manager-close")

users_col        = db["users"]
sales_close_col  = db["sales_close"]
expenses_col     = db["manager_expenses"]   # for manager expenses (Approved / Pending)

# ---------- optional: indexes (safe to run repeatedly) ----------
def _ensure_indexes():
    try:
        sales_close_col.create_index([("agent_id", 1), ("date", -1), ("updated_at", -1)])
        sales_close_col.create_index([("agent_id", 1), ("date", -1)])
        sales_close_col.create_index([("agent_id", 1), ("updated_at", -1)])
        expenses_col.create_index([("manager_id", 1), ("date", -1), ("status", 1)])
        users_col.create_index([("role", 1), ("manager_id", 1)])
    except Exception:
        pass

_ensure_indexes()

# ---------- helpers ----------

def _is_ajax(req) -> bool:
    return req.headers.get("X-Requested-With", "").lower() == "xmlhttprequest"

def _today_str() -> str:
    # Use UTC 'today' for the ledger; adjust later if you want local time.
    return datetime.utcnow().strftime("%Y-%m-%d")

def _current_manager_session() -> Tuple[Optional[str], Optional[str]]:
    """
    Returns (session_key, user_id_str) for manager-like roles, or (None, None) if not logged in.
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
    Returns (manager_id_str, manager_doc) or redirects to login.
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

def _agents_under_manager(manager_id_str: str) -> List[Dict[str, Any]]:
    """ Agents stored with manager_id as ObjectId in your DB. """
    try:
        m_oid = ObjectId(manager_id_str)
    except Exception:
        m_oid = manager_id_str

    agents = list(
        users_col.find(
            {"role": "agent", "manager_id": m_oid},
            {"_id": 1, "name": 1, "username": 1, "phone": 1},
        )
    )
    out: List[Dict[str, Any]] = []
    for a in agents:
        out.append(
            {
                "_id": str(a["_id"]),
                "name": a.get("name") or a.get("username") or "Agent",
                "phone": a.get("phone", ""),
            }
        )
    return out

def _agent_total_unclosed_all_dates(agent_id_str: str) -> float:
    """Sum total_amount across ALL dates for one agent (handles strings/numbers)."""
    pipeline = [
        {"$match": {"agent_id": agent_id_str}},
        {
            "$group": {
                "_id": None,
                "sum_amount": {
                    "$sum": {"$toDouble": {"$ifNull": ["$total_amount", 0]}}
                },
            }
        },
    ]
    agg = list(sales_close_col.aggregate(pipeline))
    if not agg:
        return 0.0
    try:
        return float(agg[0].get("sum_amount", 0.0))
    except Exception:
        return 0.0

def _unclose_total_all_agents(manager_id_str: str) -> float:
    """Sum of ALL agents' balances across ALL dates (unclosed), aggregated."""
    try:
        m_oid = ObjectId(manager_id_str)
    except Exception:
        m_oid = manager_id_str

    pipeline = [
        {"$match": {"role": "agent", "manager_id": m_oid}},
        {"$addFields": {"agent_id_str": {"$toString": "$_id"}}},
        {
            "$lookup": {
                "from": "sales_close",
                "let": {"aid": "$agent_id_str"},
                "pipeline": [
                    {"$match": {"$expr": {"$eq": ["$agent_id", "$$aid"]}}},
                    {
                        "$group": {
                            "_id": None,
                            "sum_amount": {
                                "$sum": {"$toDouble": {"$ifNull": ["$total_amount", 0]}}
                            },
                        }
                    },
                ],
                "as": "close_sum",
            }
        },
        {"$addFields": {"available_num": {"$ifNull": [{"$first": "$close_sum.sum_amount"}, 0]}}},
        {"$group": {"_id": None, "sum_total": {"$sum": "$available_num"}}},
    ]

    agg = list(users_col.aggregate(pipeline))
    if not agg:
        return 0.0
    try:
        return float(agg[0].get("sum_total", 0.0))
    except Exception:
        return 0.0

def _manager_close_available_alltime(manager_id_str: str) -> float:
    """
    Total AVAILABLE amount currently in the manager's own sales_close documents
    (sum of total_amount across all dates where agent_id == manager_id).
    This is the "money not yet withdrawn" from the manager upwards.
    """
    pipeline = [
        {"$match": {"agent_id": manager_id_str}},
        {
            "$group": {
                "_id": None,
                "sum_amount": {
                    "$sum": {"$toDouble": {"$ifNull": ["$total_amount", 0]}}
                },
            }
        },
    ]
    agg = list(sales_close_col.aggregate(pipeline))
    if not agg:
        return 0.0
    try:
        return float(agg[0].get("sum_amount", 0.0))
    except Exception:
        return 0.0

# ---------- date range + expenses + gross helpers ----------

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

def _manager_approved_expenses_total(manager_id_str: str, range_key: Optional[str]) -> float:
    """
    Sum of APPROVED expenses for this manager for a given period.
    range_key: None/'total' -> all time, otherwise 'today'|'week'|'month'.
    Uses the 'date' field (YYYY-MM-DD) on the expenses docs.
    """
    match: Dict[str, Any] = {
        "manager_id": manager_id_str,
        "status": "Approved",
    }

    if range_key and range_key != "total":
        start_str, end_str = _date_range_strings(range_key)
        if start_str:
            match["date"] = {"$gte": start_str, "$lte": end_str}

    pipeline = [
        {"$match": match},
        {
            "$group": {
                "_id": None,
                "sum_amount": {
                    "$sum": {"$toDouble": {"$ifNull": ["$amount", 0]}}
                },
            }
        },
    ]
    agg = list(expenses_col.aggregate(pipeline))
    if not agg:
        return 0.0
    try:
        return float(agg[0].get("sum_amount", 0.0))
    except Exception:
        return 0.0

def _manager_ledger_flow_for_range(manager_id_str: str, range_key: Optional[str]) -> Dict[str, float]:
    """
    For the manager's OWN ledger (where sales_close.agent_id == manager_id):

    For a given period:
      available = sum of current total_amount (remaining)   in docs in that date range
      withdrawn = sum of withdrawals[].amount              in docs in that date range
      gross_before_expense = available + withdrawn

    This matches your rule:
      Gross (before expenses) = Σ(total_amount in each doc + Σ(withdrawals.amount in that doc))

    We filter by 'date' on the sales_close docs if range_key is not None.
    """
    match: Dict[str, Any] = {"agent_id": manager_id_str}

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
    available = float(doc.get("sum_bal", 0.0))         # what is still left
    withdrawn = float(doc.get("sum_withdrawn", 0.0))   # what has been withdrawn upwards
    gross = available + withdrawn                      # total collections into manager for that period

    return {"available": available, "withdrawn": withdrawn, "gross": gross}

def _manager_balance_breakdown(manager_id_str: str) -> Dict[str, Dict[str, float]]:
    """
    Compose the per-period metrics for the manager dashboard.

    For each period:
      col   = GROSS collections before expenses:
              Σ(total_amount + withdrawals.amount) in that period
      exp   = APPROVED expenses in that period
      gross = col − exp    (this is the "Gross" you described: amount + withdrawn − expenses)

    Returns:
      {
        "total": {"col": X, "exp": Y, "gross": Z},
        "month": {...},
        "week":  {...},
        "today": {...},
      }
    """
    periods = ["total", "month", "week", "today"]
    out: Dict[str, Dict[str, float]] = {}

    for p in periods:
        rng = None if p == "total" else p
        flow = _manager_ledger_flow_for_range(manager_id_str, rng)

        col = flow["gross"]                     # collections before expense
        exp = _manager_approved_expenses_total(manager_id_str, rng)
        gross_after = col - exp                 # your "Gross" (amount + withdrawn − expense)

        out[p] = {
            "col": col,
            "exp": exp,
            "gross": gross_after,
        }
    return out

def _agents_with_balances(manager_id_str: str) -> List[Dict[str, Any]]:
    """Return agents under manager with total available balance across all dates."""
    try:
        m_oid = ObjectId(manager_id_str)
    except Exception:
        m_oid = manager_id_str

    pipeline = [
        {"$match": {"role": "agent", "manager_id": m_oid}},
        {"$addFields": {"agent_id_str": {"$toString": "$_id"}}},
        {
            "$lookup": {
                "from": "sales_close",
                "let": {"aid": "$agent_id_str"},
                "pipeline": [
                    {"$match": {"$expr": {"$eq": ["$agent_id", "$$aid"]}}},
                    {
                        "$group": {
                            "_id": None,
                            "sum_amount": {
                                "$sum": {"$toDouble": {"$ifNull": ["$total_amount", 0]}}
                            },
                        }
                    },
                ],
                "as": "close_sum",
            }
        },
        {"$addFields": {"available_num": {"$ifNull": [{"$first": "$close_sum.sum_amount"}, 0]}}},
        {
            "$project": {
                "_id": 1,
                "name": 1,
                "username": 1,
                "phone": 1,
                "available_num": 1,
            }
        },
        {"$sort": {"available_num": -1}},
    ]

    out: List[Dict[str, Any]] = []
    for a in users_col.aggregate(pipeline):
        name = a.get("name") or a.get("username") or "Agent"
        phone = a.get("phone", "")
        bal = float(a.get("available_num", 0.0) or 0.0)
        out.append(
            {
                "_id": str(a["_id"]),
                "name": name,
                "phone": phone,
                "available_num": bal,
                "available": f"{bal:,.2f}",
            }
        )
    return out

# ---------- views ----------

@manager_sales_close_bp.route("/", methods=["GET"])
def manager_close_page():
    """
    Manager Sales Close dashboard.

    Shows:
      - Summary cards for gross/collections/expenses and close totals (loaded via API)
      - Agent cards loaded on demand
    """
    scope = _ensure_manager_scope_or_redirect()
    if not isinstance(scope, tuple):
        return scope  # redirect
    manager_id, manager_doc = scope

    today = _today_str()

    return render_template(
        "manager_sales_close.html",
        manager_name=manager_doc.get("name", "Manager"),
        today=today,
    )

@manager_sales_close_bp.route("/api/summary", methods=["GET"])
def manager_close_summary():
    scope = _ensure_manager_scope_or_redirect()
    if not isinstance(scope, tuple):
        return jsonify(ok=False, message="Please log in."), 401
    manager_id, manager_doc = scope

    today = _today_str()
    unclose_total        = _unclose_total_all_agents(manager_id)
    close_available_all  = _manager_close_available_alltime(manager_id)
    breakdown            = _manager_balance_breakdown(manager_id)

    return jsonify(
        ok=True,
        manager_name=manager_doc.get("name", "Manager"),
        today=today,
        gross_total=f"{breakdown['total']['gross']:,.2f}",
        gross_month=f"{breakdown['month']['gross']:,.2f}",
        gross_week=f"{breakdown['week']['gross']:,.2f}",
        gross_today=f"{breakdown['today']['gross']:,.2f}",
        col_total=f"{breakdown['total']['col']:,.2f}",
        col_month=f"{breakdown['month']['col']:,.2f}",
        col_week=f"{breakdown['week']['col']:,.2f}",
        col_today=f"{breakdown['today']['col']:,.2f}",
        exp_total=f"{breakdown['total']['exp']:,.2f}",
        exp_month=f"{breakdown['month']['exp']:,.2f}",
        exp_week=f"{breakdown['week']['exp']:,.2f}",
        exp_today=f"{breakdown['today']['exp']:,.2f}",
        unclose_total=f"{unclose_total:,.2f}",
        close_available_total=f"{close_available_all:,.2f}",
    )

@manager_sales_close_bp.route("/api/agents", methods=["GET"])
def manager_close_agents_api():
    scope = _ensure_manager_scope_or_redirect()
    if not isinstance(scope, tuple):
        return jsonify(ok=False, message="Please log in."), 401
    manager_id, _ = scope

    agents = _agents_with_balances(manager_id)
    return jsonify(ok=True, agents=agents)

@manager_sales_close_bp.route("/withdraw", methods=["POST"])
def manager_withdraw():
    """
    POST: agent_id, amount, note (optional)

    Behaviour:
      - Debits across multiple agent sales_close docs if needed:
          * today's docs first,
          * then most recent -> older,
          * only where total_amount > 0.
      - Credits the MANAGER'S TODAY doc by the amount actually debited.
      - Returns refreshed aggregates for the UI:
          * agent available balance
          * unclose total (all agents)
          * manager available close total (all dates)
          * Gross cards (Total/Month/Week/Today)
          * Collections & Expenses per period.
    """
    scope = _ensure_manager_scope_or_redirect()
    if not isinstance(scope, tuple):
        if _is_ajax(request):
            return jsonify(ok=False, message="Please log in."), 401
        return scope
    manager_id, manager_doc = scope

    # Inputs from form or JSON
    agent_id = (
        request.form.get("agent_id")
        or (request.json.get("agent_id") if request.is_json else "")
        or ""
    )
    amount_in = (
        request.form.get("amount")
        or (request.json.get("amount") if request.is_json else None)
    )
    note = (
        request.form.get("note")
        or (request.json.get("note") if request.is_json else "")
        or ""
    )

    try:
        amount = float(amount_in)
    except Exception:
        amount = 0.0

    if not agent_id or amount <= 0:
        msg = "Agent and a positive amount are required."
        return (
            jsonify(ok=False, message=msg),
            400,
        ) if _is_ajax(request) else (msg, 400)

    # Ensure agent belongs to this manager
    try:
        m_oid = ObjectId(manager_id)
    except Exception:
        m_oid = manager_id

    try:
        agent_doc = users_col.find_one(
            {"_id": ObjectId(agent_id), "role": "agent", "manager_id": m_oid}
        )
    except Exception:
        agent_doc = users_col.find_one(
            {"_id": agent_id, "role": "agent", "manager_id": m_oid}
        )

    if not agent_doc:
        msg = "Agent not found or not in your team."
        return (
            jsonify(ok=False, message=msg),
            404,
        ) if _is_ajax(request) else (msg, 404)

    today = _today_str()
    now_utc = datetime.utcnow()
    time_str = now_utc.strftime("%H:%M:%S")

    # --- Select candidate docs to debit: today first, then recent->older ---
    pipeline = [
        {"$match": {"agent_id": str(agent_id)}},
        {
            "$addFields": {
                "bal_num": {
                    "$toDouble": {"$ifNull": ["$total_amount", 0]}
                },
                "is_today": {
                    "$cond": [{"$eq": ["$date", today]}, 1, 0]
                },
            }
        },
        {"$match": {"bal_num": {"$gt": 0}}},  # only docs with positive balance
        {"$sort": {"is_today": -1, "date": -1, "updated_at": -1}},
    ]
    docs = list(sales_close_col.aggregate(pipeline))

    # Quick aggregate of all balances for guard check
    total_all = float(sum(float(d.get("bal_num", 0.0)) for d in docs))
    if total_all + 1e-9 < amount:
        msg = (
            f"Insufficient balance. Agent total across all days: "
            f"GHS {total_all:,.2f}"
        )
        return (
            jsonify(
                ok=False,
                message=msg,
                available=f"{total_all:,.2f}",
            ),
            409,
        ) if _is_ajax(request) else (msg, 409)

    remaining = amount
    debits: List[Dict[str, Any]] = []  # [{date, debited}, ...]

    # --- Debit across multiple docs until requested amount is covered ---
    for d in docs:
        if remaining <= 1e-9:
            break

        doc_id = d["_id"]
        date_str = d.get("date", "")
        available = float(d.get("bal_num", 0.0))
        if available <= 0:
            continue

        take = min(available, remaining)

        # Safe update using $expr to ensure current balance >= take
        filter_q = {
            "_id": doc_id,
            "$expr": {
                "$gte": [
                    {"$toDouble": {"$ifNull": ["$total_amount", 0]}},
                    take,
                ]
            },
        }
        update_q = {
            "$inc": {"total_amount": -take},
            "$set": {
                "updated_at": now_utc,
                "last_withdrawal_at": now_utc,
            },
            "$push": {
                "withdrawals": {
                    "amount": float(round(take, 2)),
                    "by_manager_id": manager_id,
                    "by_manager_name": manager_doc.get("name", ""),
                    "date": date_str,
                    "time": time_str,
                    "at": now_utc,
                    "note": note,
                }
            },
        }
        res = sales_close_col.update_one(filter_q, update_q)
        if res.modified_count == 1:
            debits.append({"date": date_str, "debited": take})
            remaining -= take
        # If not modified, doc changed concurrently; skip to next candidate.

    actually_debited = amount - remaining
    if actually_debited <= 0:
        # Extreme concurrency case: recompute and report
        current_total = _agent_total_unclosed_all_dates(str(agent_id))
        msg = (
            "Insufficient balance due to concurrent changes. "
            f"Current total: GHS {current_total:,.2f}"
        )
        return (
            jsonify(
                ok=False,
                message=msg,
                available=f"{current_total:,.2f}",
            ),
            409,
        ) if _is_ajax(request) else (msg, 409)

    # --- Credit MANAGER'S TODAY doc by the amount actually debited ---
    mgr_filter = {"agent_id": manager_id, "date": today}
    mgr_update = {
        "$setOnInsert": {
            "agent_id": manager_id,
            "manager_id": manager_id,
            "date": today,
            "created_at": now_utc,
        },
        "$inc": {
            "total_amount": actually_debited,
            "count": 1,
        },
        "$set": {
            "updated_at": now_utc,
            "last_payment_at": now_utc,
        },
    }
    sales_close_col.update_one(mgr_filter, mgr_update, upsert=True)

    # --- Recompute aggregates for the response (ALL DATES, per your design) ---
    new_agent_total      = _agent_total_unclosed_all_dates(str(agent_id))
    unclose_total        = _unclose_total_all_agents(manager_id)
    close_available_all  = _manager_close_available_alltime(manager_id)
    breakdown            = _manager_balance_breakdown(manager_id)

    payload = {
        "ok": True,
        "message": (
            f"Withdrew GHS {actually_debited:,.2f} "
            f"across {len(debits)} day(s) and credited manager account."
        ),
        "requested": f"{amount:,.2f}",
        "debited_breakdown": [
            {"date": x["date"], "amount": f"{x['debited']:,.2f}"}
            for x in debits
        ],
        "agent_id": str(agent_id),

        # Agent-level
        "available": f"{new_agent_total:,.2f}",          # agent total across all dates

        # Totals for second row (all dates)
        "unclose_total": f"{unclose_total:,.2f}",        # all agents, all dates
        "close_available_total": f"{close_available_all:,.2f}",  # manager, all dates

        # Gross per period (amount + withdrawn − expenses)
        "gross_total": f"{breakdown['total']['gross']:,.2f}",
        "gross_month": f"{breakdown['month']['gross']:,.2f}",
        "gross_week":  f"{breakdown['week']['gross']:,.2f}",
        "gross_today": f"{breakdown['today']['gross']:,.2f}",

        # Collections per period (before expense)
        "collections_total": f"{breakdown['total']['col']:,.2f}",
        "collections_month": f"{breakdown['month']['col']:,.2f}",
        "collections_week":  f"{breakdown['week']['col']:,.2f}",
        "collections_today": f"{breakdown['today']['col']:,.2f}",

        # Approved expenses per period
        "expenses_total": f"{breakdown['total']['exp']:,.2f}",
        "expenses_month": f"{breakdown['month']['exp']:,.2f}",
        "expenses_week":  f"{breakdown['week']['exp']:,.2f}",
        "expenses_today": f"{breakdown['today']['exp']:,.2f}",
    }

    if _is_ajax(request):
        return jsonify(payload)

    # Non-AJAX fallback (rare)
    return (
        "OK. Debited: {req} | Agent total: {avail} | "
        "Unclose Total: {unc} | Available Close: {close} | "
        "Gross Total: {gt}".format(
            req=payload["requested"],
            avail=payload["available"],
            unc=payload["unclose_total"],
            close=payload["close_available_total"],
            gt=payload["gross_total"],
        )
    )

@manager_sales_close_bp.route("/agent/<agent_id>/withdrawals", methods=["GET"])
def agent_withdrawals(agent_id: str):
    """
    Returns JSON history of withdrawals for the given agent across ALL dates.

    Each item:
      {
        amount, date, time, note,
        by_manager_id, by_manager_name,
        at_iso
      }
    Only withdrawals done by MANAGERS are surfaced here (for now).
    """
    scope = _ensure_manager_scope_or_redirect()
    if not isinstance(scope, tuple):
        return jsonify(ok=False, message="Please log in."), 401
    manager_id, _ = scope

    # Ensure this agent belongs to this manager
    try:
        m_oid = ObjectId(manager_id)
    except Exception:
        m_oid = manager_id

    try:
        a_doc = users_col.find_one(
            {"_id": ObjectId(agent_id), "role": "agent", "manager_id": m_oid}
        )
    except Exception:
        a_doc = users_col.find_one(
            {"_id": agent_id, "role": "agent", "manager_id": m_oid}
        )

    if not a_doc:
        return jsonify(ok=False, message="Agent not found or not in your team."), 404

    # Collect withdrawals from all sales_close docs for this agent
    cursor = sales_close_col.find(
        {"agent_id": str(agent_id)},
        {"withdrawals": 1},
    )

    items: List[Dict[str, Any]] = []
    for d in cursor:
        for w in d.get("withdrawals", []) or []:
            at = w.get("at")
            if isinstance(at, datetime):
                at_iso = at.isoformat()
            else:
                at_iso = f"{w.get('date','')}T{w.get('time','00:00:00')}"

            items.append(
                {
                    "amount": float(w.get("amount", 0.0)),
                    "date": w.get("date", ""),
                    "time": w.get("time", ""),
                    "note": w.get("note", ""),
                    "by_manager_id": w.get("by_manager_id", ""),
                    "by_manager_name": w.get("by_manager_name", ""),
                    "at_iso": at_iso,
                }
            )

    items.sort(key=lambda x: x.get("at_iso", ""), reverse=True)

    return jsonify(
        ok=True,
        agent={
            "_id": str(a_doc["_id"]),
            "name": a_doc.get("name") or a_doc.get("username") or "Agent",
            "phone": a_doc.get("phone", ""),
        },
        withdrawals=items,
    )

# ---------- NEW: manager withdrawals by admin/executive ----------

@manager_sales_close_bp.route("/manager-withdrawals", methods=["GET"])
def manager_withdrawals_history():
    """
    Returns JSON history of withdrawals done FROM the manager's own ledger
    by ADMIN or EXECUTIVE (money taken from manager upwards).

    Each item:
      {
        amount, date, time, note,
        by_name, by_role, at_iso
      }

    Optional query param:
      ?limit=50 (default 50)
    """
    scope = _ensure_manager_scope_or_redirect()
    if not isinstance(scope, tuple):
        return jsonify(ok=False, message="Please log in."), 401
    manager_id, manager_doc = scope

    # Only makes sense for actual manager; but admins/executives can also call
    # to inspect a specific manager if we extend later. For now, scope is
    # always "current user's ledger".
    limit_param = request.args.get("limit", "").strip()
    try:
        limit = int(limit_param) if limit_param else 50
    except Exception:
        limit = 50
    if limit <= 0:
        limit = 50
    if limit > 500:
        limit = 500

    # Get all withdrawals from manager's own docs
    cursor = sales_close_col.find(
        {"agent_id": str(manager_id)},
        {"withdrawals": 1},
    )

    items: List[Dict[str, Any]] = []
    for d in cursor:
        for w in (d.get("withdrawals") or []):
            # Only keep ones done by admin or executive (i.e. upwards)
            by_role = ""
            by_name = ""
            if w.get("by_executive_id"):
                by_role = "executive"
                by_name = w.get("by_executive_name", "")
            elif w.get("by_admin_id"):
                by_role = "admin"
                by_name = w.get("by_admin_name", "")
            else:
                # This is probably a manager's own withdrawal from agents; skip
                continue

            at = w.get("at")
            if isinstance(at, datetime):
                at_iso = at.isoformat()
            else:
                at_iso = f"{w.get('date','')}T{w.get('time','00:00:00')}"

            items.append({
                "amount": float(w.get("amount", 0.0)),
                "date": w.get("date", ""),
                "time": w.get("time", ""),
                "note": w.get("note", ""),
                "by_name": by_name,
                "by_role": by_role,
                "at_iso": at_iso,
            })

    # Sort newest first
    items.sort(key=lambda x: x.get("at_iso", ""), reverse=True)
    if len(items) > limit:
        items = items[:limit]

    return jsonify(
        ok=True,
        manager={
            "_id": manager_id,
            "name": manager_doc.get("name") or manager_doc.get("username") or "Manager",
            "phone": manager_doc.get("phone", ""),
        },
        withdrawals=items,
        count=len(items),
    )

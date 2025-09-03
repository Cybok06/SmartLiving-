from flask import Blueprint, render_template, request, jsonify, session, redirect, url_for
from bson.objectid import ObjectId
from datetime import datetime
from db import db

manager_sales_close_bp = Blueprint("manager_sales_close", __name__, url_prefix="/manager-close")

users_col       = db["users"]
sales_close_col = db["sales_close"]

# ---------- optional: indexes (safe to run repeatedly) ----------
def _ensure_indexes():
    try:
        sales_close_col.create_index([("agent_id", 1), ("date", -1)])
        sales_close_col.create_index([("agent_id", 1), ("updated_at", -1)])
    except Exception:
        pass

_ensure_indexes()

# ---------- helpers ----------

def _is_ajax(req) -> bool:
    return req.headers.get("X-Requested-With", "").lower() == "xmlhttprequest"

def _today_str() -> str:
    # Use UTC 'today' for the ledger; adjust later if you want local time.
    return datetime.utcnow().strftime("%Y-%m-%d")

def _current_manager_session():
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

def _agents_under_manager(manager_id_str: str):
    """ Agents stored with manager_id as ObjectId in your DB. """
    try:
        m_oid = ObjectId(manager_id_str)
    except Exception:
        m_oid = manager_id_str

    agents = list(users_col.find(
        {"role": "agent", "manager_id": m_oid},
        {"_id": 1, "name": 1, "username": 1, "phone": 1}
    ))
    out = []
    for a in agents:
        out.append({
            "_id": str(a["_id"]),
            "name": a.get("name") or a.get("username") or "Agent",
            "phone": a.get("phone", "")
        })
    return out

def _agent_total_unclosed_all_dates(agent_id_str: str) -> float:
    """Sum total_amount across ALL dates for one agent (handles strings/numbers)."""
    pipeline = [
        {"$match": {"agent_id": agent_id_str}},
        {"$group": {"_id": None, "sum_amount": {"$sum": {"$toDouble": {"$ifNull": ["$total_amount", 0]}}}}}
    ]
    agg = list(sales_close_col.aggregate(pipeline))
    if not agg:
        return 0.0
    try:
        return float(agg[0].get("sum_amount", 0.0))
    except Exception:
        return 0.0

def _unclose_total_all_agents(manager_id_str: str) -> float:
    """Sum of ALL agents’ balances across ALL dates (unclosed)."""
    agents = _agents_under_manager(manager_id_str)
    total = 0.0
    for a in agents:
        total += _agent_total_unclosed_all_dates(a["_id"])
    return total

def _manager_close_total_alltime(manager_id_str: str) -> float:
    """
    Total amount in the manager's own sales_close documents (sum across all dates).
    We use agent_id == manager_id for the manager's ledger.
    """
    pipeline = [
        {"$match": {"agent_id": manager_id_str}},
        {"$group": {"_id": None, "sum_amount": {"$sum": {"$toDouble": {"$ifNull": ["$total_amount", 0]}}}}}
    ]
    agg = list(sales_close_col.aggregate(pipeline))
    if not agg:
        return 0.0
    try:
        return float(agg[0].get("sum_amount", 0.0))
    except Exception:
        return 0.0

# ---------- views ----------

@manager_sales_close_bp.route("/", methods=["GET"])
def manager_close_page():
    """
    Front page:
      - Unclose Total (all agents, all dates)
      - Close Total (manager ledger, all dates)
      - Agent cards show each agent's TOTAL (sum across all their sales_close docs)
      - Sorted DESC by that total
    """
    scope = _ensure_manager_scope_or_redirect()
    if not isinstance(scope, tuple):
        return scope  # redirect
    manager_id, manager_doc = scope

    today = _today_str()

    # Totals for cards
    unclose_total = _unclose_total_all_agents(manager_id)        # all agents, all dates
    close_total   = _manager_close_total_alltime(manager_id)     # manager ledger, all dates

    # Build agent list with TOTAL balances (all dates) and sort DESC
    agents = _agents_under_manager(manager_id)
    for a in agents:
        bal = _agent_total_unclosed_all_dates(a["_id"])
        a["available_num"] = bal
        a["available"] = f"{bal:,.2f}"
    agents.sort(key=lambda x: x.get("available_num", 0.0), reverse=True)

    return render_template(
        "manager_sales_close.html",
        manager_name=manager_doc.get("name", "Manager"),
        today=today,
        unclose_total=f"{unclose_total:,.2f}",
        close_total=f"{close_total:,.2f}",
        agents=agents
    )

@manager_sales_close_bp.route("/withdraw", methods=["POST"])
def manager_withdraw():
    """
    POST: agent_id, amount, note (optional)

    Behavior (improved):
      - Debits across multiple sales_close docs if needed (today first, then most recent -> older),
        using $expr/$toDouble so both numeric and string balances are handled.
      - Credits the MANAGER'S TODAY doc with the total withdrawn.
      - Returns refreshed totals (all dates) + per-date debit breakdown.
    """
    scope = _ensure_manager_scope_or_redirect()
    if not isinstance(scope, tuple):
        if _is_ajax(request):
            return jsonify(ok=False, message="Please log in."), 401
        return scope
    manager_id, manager_doc = scope

    agent_id  = (request.form.get("agent_id") or (request.json.get("agent_id") if request.is_json else "")) or ""
    amount_in = request.form.get("amount") or (request.json.get("amount") if request.is_json else None)
    note      = (request.form.get("note") or (request.json.get("note") if request.is_json else "")) or ""

    try:
        amount = float(amount_in)
    except Exception:
        amount = 0.0

    if not agent_id or amount <= 0:
        msg = "Agent and a positive amount are required."
        return (jsonify(ok=False, message=msg), 400) if _is_ajax(request) else (msg, 400)

    # Ensure agent belongs to this manager
    try:
        m_oid = ObjectId(manager_id)
    except Exception:
        m_oid = manager_id
    try:
        agent_doc = users_col.find_one({"_id": ObjectId(agent_id), "role": "agent", "manager_id": m_oid})
    except Exception:
        agent_doc = users_col.find_one({"_id": agent_id, "role": "agent", "manager_id": m_oid})
    if not agent_doc:
        msg = "Agent not found or not in your team."
        return (jsonify(ok=False, message=msg), 404) if _is_ajax(request) else (msg, 404)

    today = _today_str()
    now_utc = datetime.utcnow()
    time_str = now_utc.strftime("%H:%M:%S")

    # --- Gather candidate docs to debit: today first, then recent->older; only with positive balance ---
    pipeline = [
        {"$match": {"agent_id": str(agent_id)}},
        {"$addFields": {
            "bal_num": {"$toDouble": {"$ifNull": ["$total_amount", 0]}},
            "is_today": {"$cond": [{"$eq": ["$date", today]}, 1, 0]}
        }},
        {"$match": {"bal_num": {"$gt": 0}}},
        {"$sort": {"is_today": -1, "date": -1, "updated_at": -1}}
    ]
    docs = list(sales_close_col.aggregate(pipeline))

    # Quick total across all dates
    total_all = sum(float(d.get("bal_num", 0.0)) for d in docs)
    if total_all + 1e-9 < amount:
        msg = f"Insufficient balance. Agent total across all days: GHS {total_all:,.2f}"
        return (jsonify(ok=False, message=msg, available=f"{total_all:,.2f}"), 409) if _is_ajax(request) else (msg, 409)

    remaining = amount
    debits = []  # breakdown: [{date, debited}]

    # --- Debit across multiple docs until covered ---
    for d in docs:
        if remaining <= 1e-9:
            break
        doc_id = d["_id"]
        date_str = d.get("date", "")
        available = float(d.get("bal_num", 0.0))
        if available <= 0:
            continue

        take = min(available, remaining)

        # Use $expr + $toDouble to compare safely even if total_amount is a string
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
                "by_manager_id": manager_id,
                "by_manager_name": manager_doc.get("name", ""),
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
        # If not modified, the doc changed concurrently; skip to next candidate.

    actually_debited = amount - remaining
    if actually_debited <= 0:
        # Concurrency edge-case: recompute and respond
        current_total = _agent_total_unclosed_all_dates(str(agent_id))
        msg = f"Insufficient balance due to concurrent changes. Current total: GHS {current_total:,.2f}"
        return (jsonify(ok=False, message=msg, available=f"{current_total:,.2f}"), 409) if _is_ajax(request) else (msg, 409)

    # --- Credit MANAGER'S TODAY doc by the amount actually debited ---
    mgr_filter = {"agent_id": manager_id, "date": today}
    mgr_update = {
        "$setOnInsert": {"agent_id": manager_id, "manager_id": manager_id, "date": today, "created_at": now_utc},
        "$inc": {"total_amount": actually_debited, "count": 1},
        "$set": {"updated_at": now_utc, "last_payment_at": now_utc}
    }
    sales_close_col.update_one(mgr_filter, mgr_update, upsert=True)

    # --- Recompute totals (ALL DATES) ---
    new_agent_total = _agent_total_unclosed_all_dates(str(agent_id))
    unclose_total   = _unclose_total_all_agents(manager_id)
    close_total     = _manager_close_total_alltime(manager_id)

    payload = {
        "ok": True,
        "message": (
            f"Withdrew GHS {actually_debited:,.2f} across {len(debits)} day(s) "
            f"and credited manager account."
        ),
        "requested": f"{amount:,.2f}",
        "debited_breakdown": [{"date": x["date"], "amount": f"{x['debited']:,.2f}"} for x in debits],
        "agent_id": str(agent_id),
        "available": f"{new_agent_total:,.2f}",    # agent total across all dates
        "unclose_total": f"{unclose_total:,.2f}",  # all agents, all dates
        "close_total": f"{close_total:,.2f}"       # manager, all dates
    }
    return jsonify(payload) if _is_ajax(request) else (
        f"OK. Debited: {payload['requested']} | "
        f"Agent total: {payload['available']} | "
        f"Unclose Total: {payload['unclose_total']} | "
        f"Close Total: {payload['close_total']}"
    )

@manager_sales_close_bp.route("/agent/<agent_id>/withdrawals", methods=["GET"])
def agent_withdrawals(agent_id):
    """
    Returns JSON history of withdrawals for the given agent across ALL dates.
    Each item: { amount, date, time, note, by_manager_id, by_manager_name, at_iso }
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
        a_doc = users_col.find_one({"_id": ObjectId(agent_id), "role": "agent", "manager_id": m_oid})
    except Exception:
        a_doc = users_col.find_one({"_id": agent_id, "role": "agent", "manager_id": m_oid})
    if not a_doc:
        return jsonify(ok=False, message="Agent not found or not in your team."), 404

    # Collect withdrawals from all sales_close docs for this agent
    cursor = sales_close_col.find({"agent_id": str(agent_id)}, {"withdrawals": 1})
    items = []
    for d in cursor:
        for w in d.get("withdrawals", []) or []:
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
                "by_manager_id": w.get("by_manager_id", ""),
                "by_manager_name": w.get("by_manager_name", ""),
                "at_iso": at_iso
            })

    items.sort(key=lambda x: x.get("at_iso", ""), reverse=True)

    return jsonify(ok=True, agent={
        "_id": str(a_doc["_id"]),
        "name": a_doc.get("name") or a_doc.get("username") or "Agent",
        "phone": a_doc.get("phone", "")
    }, withdrawals=items)

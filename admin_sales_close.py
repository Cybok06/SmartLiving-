# admin_sales_close.py
from flask import Blueprint, render_template, request, jsonify, session, redirect, url_for
from bson.objectid import ObjectId
from datetime import datetime
from db import db

admin_sales_close_bp = Blueprint("admin_sales_close", __name__, url_prefix="/admin-close")

users_col       = db["users"]
sales_close_col = db["sales_close"]

# ---------- helpers ----------

def _is_ajax(req) -> bool:
    return req.headers.get("X-Requested-With", "").lower() == "xmlhttprequest"

def _today_str() -> str:
    # Use UTC 'today' for the ledger; adjust later if you want local time.
    return datetime.utcnow().strftime("%Y-%m-%d")

def _current_admin_session():
    """
    Returns (session_key, user_id_str) for admin-like roles, or (None, None) if not logged in.
    """
    if session.get("admin_id"):
        return "admin_id", session["admin_id"]
    if session.get("executive_id"):
        return "executive_id", session["executive_id"]
    return None, None

def _ensure_admin_scope_or_redirect():
    """
    Ensure requester is admin/executive via session (NOT flask_login).
    Returns (admin_id_str, admin_doc) or redirects to login.
    """
    _, uid = _current_admin_session()
    if not uid:
        return redirect(url_for("login.login"))

    try:
        admin_doc = users_col.find_one({"_id": ObjectId(uid)})
    except Exception:
        admin_doc = users_col.find_one({"_id": uid})

    if not admin_doc:
        return redirect(url_for("login.login"))

    role = (admin_doc.get("role") or "").lower()
    if role not in ("admin", "executive"):
        return redirect(url_for("login.login"))

    return str(admin_doc["_id"]), admin_doc

def _list_managers():
    """Return basic list of all managers."""
    managers = list(users_col.find(
        {"role": "manager"},
        {"_id": 1, "name": 1, "username": 1, "phone": 1}
    ))
    out = []
    for m in managers:
        out.append({
            "_id": str(m["_id"]),
            "name": m.get("name") or m.get("username") or "Manager",
            "phone": m.get("phone", "")
        })
    return out

def _sum_ledger_total_for_entity(entity_id_str: str) -> float:
    """
    Sum total_amount across ALL dates for an entity's own ledger:
    i.e., documents where sales_close.agent_id == entity_id_str
    """
    pipeline = [
        {"$match": {"agent_id": entity_id_str}},
        {"$group": {"_id": None, "sum_amount": {"$sum": {"$toDouble": "$total_amount"}}}}
    ]
    agg = list(sales_close_col.aggregate(pipeline))
    if not agg:
        return 0.0
    try:
        return float(agg[0].get("sum_amount", 0.0))
    except Exception:
        return 0.0

def _unclose_total_all_managers() -> float:
    """Sum of ALL managers’ balances across ALL dates (unclosed)."""
    total = 0.0
    for m in _list_managers():
        total += _sum_ledger_total_for_entity(m["_id"])
    return total

# ---------- views ----------

@admin_sales_close_bp.route("/", methods=["GET"])
def admin_close_page():
    """
    Front page (Admin):
      - Unclose Total (all managers, all dates)
      - Close Total (admin ledger, all dates)
      - Manager cards show each manager's TOTAL (sum across all their sales_close docs)
      - Sorted DESC by that total
    """
    scope = _ensure_admin_scope_or_redirect()
    if not isinstance(scope, tuple):
        return scope  # redirect
    admin_id, admin_doc = scope

    today = _today_str()

    # Totals for cards
    unclose_total = _unclose_total_all_managers()          # all managers, all dates
    close_total   = _sum_ledger_total_for_entity(admin_id) # admin ledger, all dates

    # Build manager list with TOTAL balances (all dates) and sort DESC
    managers = _list_managers()
    for m in managers:
        bal = _sum_ledger_total_for_entity(m["_id"])
        m["available_num"] = bal
        m["available"] = f"{bal:,.2f}"
    managers.sort(key=lambda x: x.get("available_num", 0.0), reverse=True)

    return render_template(
        "admin_sales_close.html",   # create this template (duplicate of manager version but wording for Admin/Managers)
        admin_name=admin_doc.get("name", "Admin"),
        today=today,
        unclose_total=f"{unclose_total:,.2f}",
        close_total=f"{close_total:,.2f}",
        managers=managers
    )

@admin_sales_close_bp.route("/withdraw", methods=["POST"])
def admin_withdraw():
    """
    POST: manager_id, amount, note (optional)
    Behavior:
      - Withdraws without a date picker:
          1) Try to debit the MANAGER's TODAY sales_close doc (their ledger).
          2) If not enough, try the MOST RECENT doc with sufficient balance.
      - Credits the ADMIN'S TODAY doc (admin’s own ledger).
      - Returns refreshed:
          - manager’s TOTAL unclosed (all dates),
          - Unclose Total (all managers),
          - Close Total (admin).
    """
    scope = _ensure_admin_scope_or_redirect()
    if not isinstance(scope, tuple):
        if _is_ajax(request):
            return jsonify(ok=False, message="Please log in."), 401
        return scope
    admin_id, admin_doc = scope

    manager_id  = (request.form.get("manager_id") or (request.json.get("manager_id") if request.is_json else "")) or ""
    amount_in   = request.form.get("amount") or (request.json.get("amount") if request.is_json else None)
    note        = (request.form.get("note") or (request.json.get("note") if request.is_json else "")) or ""

    try:
        amount = float(amount_in)
    except Exception:
        amount = 0.0

    if not manager_id or amount <= 0:
        msg = "Manager and a positive amount are required."
        return (jsonify(ok=False, message=msg), 400) if _is_ajax(request) else (msg, 400)

    # Ensure target is a manager
    try:
        mgr_doc = users_col.find_one({"_id": ObjectId(manager_id), "role": "manager"})
    except Exception:
        mgr_doc = users_col.find_one({"_id": manager_id, "role": "manager"})
    if not mgr_doc:
        msg = "Manager not found."
        return (jsonify(ok=False, message=msg), 404) if _is_ajax(request) else (msg, 404)

    today = _today_str()
    now_utc = datetime.utcnow()
    time_str = now_utc.strftime("%H:%M:%S")

    # --- 1) Try to debit MANAGER's TODAY doc (their ledger uses agent_id == manager_id) ---
    chosen_date = today
    mgr_filter_debit = {"agent_id": str(manager_id), "date": chosen_date, "total_amount": {"$gte": amount}}
    mgr_update_debit = {
        "$inc": {"total_amount": -amount},
        "$set": {"updated_at": now_utc, "last_withdrawal_at": now_utc},
        "$push": {"withdrawals": {
            "amount": amount,
            "by_admin_id": admin_id,
            "by_admin_name": admin_doc.get("name", ""),
            "date": chosen_date,
            "time": time_str,
            "at": now_utc,
            "note": note
        }}
    }
    res = sales_close_col.update_one(mgr_filter_debit, mgr_update_debit)

    # --- 2) If today failed, try most recent doc with enough balance ---
    if res.modified_count == 0:
        cursor = sales_close_col.find(
            {"agent_id": str(manager_id), "total_amount": {"$gte": amount}},
            {"date": 1}
        ).sort([("date", -1), ("updated_at", -1)]).limit(1)
        doc = next(cursor, None)
        if not doc:
            total_all = _sum_ledger_total_for_entity(str(manager_id))
            msg = f"Insufficient balance. Manager total across all days: GHS {total_all:,.2f}"
            return (jsonify(ok=False, message=msg, available=f"{total_all:,.2f}"), 409) if _is_ajax(request) else (msg, 409)

        chosen_date = doc.get("date")
        mgr_filter_debit = {"agent_id": str(manager_id), "date": chosen_date, "total_amount": {"$gte": amount}}
        mgr_update_debit = {
            "$inc": {"total_amount": -amount},
            "$set": {"updated_at": now_utc, "last_withdrawal_at": now_utc},
            "$push": {"withdrawals": {
                "amount": amount,
                "by_admin_id": admin_id,
                "by_admin_name": admin_doc.get("name", ""),
                "date": chosen_date,
                "time": time_str,
                "at": now_utc,
                "note": note
            }}
        }
        res2 = sales_close_col.update_one(mgr_filter_debit, mgr_update_debit)
        if res2.modified_count == 0:
            total_all = _sum_ledger_total_for_entity(str(manager_id))
            msg = f"Insufficient balance. Manager total across all days: GHS {total_all:,.2f}"
            return (jsonify(ok=False, message=msg, available=f"{total_all:,.2f}"), 409) if _is_ajax(request) else (msg, 409)

    # --- 3) Credit ADMIN'S TODAY doc (admin uses their own id as agent_id) ---
    admin_filter_credit = {"agent_id": admin_id, "date": today}
    admin_update_credit = {
        "$setOnInsert": {"agent_id": admin_id, "manager_id": admin_id, "date": today, "created_at": now_utc},
        "$inc": {"total_amount": amount, "count": 1},
        "$set": {"updated_at": now_utc, "last_payment_at": now_utc}
    }
    sales_close_col.update_one(admin_filter_credit, admin_update_credit, upsert=True)

    # --- 4) Compute updated numbers (ALL DATES) ---
    new_mgr_total = _sum_ledger_total_for_entity(str(manager_id))
    unclose_total = _unclose_total_all_managers()
    close_total   = _sum_ledger_total_for_entity(admin_id)

    payload = {
        "ok": True,
        "message": f"Withdrew GHS {amount:,.2f} (debited {chosen_date}) and credited admin account.",
        "manager_id": str(manager_id),
        "available": f"{new_mgr_total:,.2f}",   # manager total across all dates
        "unclose_total": f"{unclose_total:,.2f}",
        "close_total": f"{close_total:,.2f}"
    }
    return jsonify(payload) if _is_ajax(request) else (
        f"OK. Manager total: {payload['available']}, "
        f"Unclose Total: {payload['unclose_total']}, "
        f"Close Total: {payload['close_total']}"
    )

@admin_sales_close_bp.route("/manager/<manager_id>/withdrawals", methods=["GET"])
def manager_withdrawals(manager_id):
    """
    Returns JSON history of withdrawals performed on the given MANAGER across ALL dates.
    Each item: { amount, date, time, note, by_admin_id, by_admin_name, at_iso }
    """
    scope = _ensure_admin_scope_or_redirect()
    if not isinstance(scope, tuple):
        return jsonify(ok=False, message="Please log in."), 401

    # Ensure this target is a manager
    try:
        m_doc = users_col.find_one({"_id": ObjectId(manager_id), "role": "manager"})
    except Exception:
        m_doc = users_col.find_one({"_id": manager_id, "role": "manager"})
    if not m_doc:
        return jsonify(ok=False, message="Manager not found."), 404

    # Collect withdrawals from all sales_close docs for this manager (agent_id == manager_id)
    cursor = sales_close_col.find({"agent_id": str(manager_id)}, {"withdrawals": 1})
    items = []
    for d in cursor:
        for w in (d.get("withdrawals") or []):
            # We include all, but primarily interested in entries with by_admin_id/by_admin_name
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
                "by_admin_id": w.get("by_admin_id", ""),
                "by_admin_name": w.get("by_admin_name", ""),
                "at_iso": at_iso
            })

    items.sort(key=lambda x: x.get("at_iso", ""), reverse=True)

    return jsonify(ok=True, manager={
        "_id": str(m_doc["_id"]),
        "name": m_doc.get("name") or m_doc.get("username") or "Manager",
        "phone": m_doc.get("phone", "")
    }, withdrawals=items)

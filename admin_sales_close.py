# admin_sales_close.py
from flask import Blueprint, render_template, request, jsonify, session, redirect, url_for
from bson.objectid import ObjectId
from datetime import datetime
from db import db

admin_sales_close_bp = Blueprint("admin_sales_close", __name__, url_prefix="/admin-close")

users_col             = db["users"]
sales_close_col       = db["sales_close"]
admin_withdrawals_col = db["admin_withdrawals"]  # NEW: consolidated log

# ---------- optional: indexes (safe to run repeatedly) ----------
def _ensure_indexes():
    try:
        sales_close_col.create_index([("agent_id", 1), ("date", -1)])
        sales_close_col.create_index([("agent_id", 1), ("updated_at", -1)])
        # consolidated log indexes
        admin_withdrawals_col.create_index([("manager_id", 1), ("created_at", -1)])
        admin_withdrawals_col.create_index([("by_admin_id", 1), ("created_at", -1)])
    except Exception:
        pass

_ensure_indexes()

# ---------- helpers ----------
def _is_ajax(req) -> bool:
    return req.headers.get("X-Requested-With", "").lower() == "xmlhttprequest"

def _today_str() -> str:
    return datetime.utcnow().strftime("%Y-%m-%d")

def _current_admin_session():
    if session.get("admin_id"):
        return "admin_id", session["admin_id"]
    if session.get("executive_id"):
        return "executive_id", session["executive_id"]
    return None, None

def _ensure_admin_scope_or_redirect():
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
    pipeline = [
        {"$match": {"agent_id": entity_id_str}},
        {"$group": {"_id": None, "sum_amount": {"$sum": {"$toDouble": {"$ifNull": ["$total_amount", 0]}}}}}
    ]
    agg = list(sales_close_col.aggregate(pipeline))
    if not agg:
        return 0.0
    try:
        return float(agg[0].get("sum_amount", 0.0))
    except Exception:
        return 0.0

def _unclose_total_all_managers() -> float:
    total = 0.0
    for m in _list_managers():
        total += _sum_ledger_total_for_entity(m["_id"])
    return total

# ---------- views ----------
@admin_sales_close_bp.route("/", methods=["GET"])
def admin_close_page():
    scope = _ensure_admin_scope_or_redirect()
    if not isinstance(scope, tuple):
        return scope
    admin_id, admin_doc = scope

    today = _today_str()
    unclose_total = _unclose_total_all_managers()
    close_total   = _sum_ledger_total_for_entity(admin_id)

    managers = _list_managers()
    for m in managers:
        bal = _sum_ledger_total_for_entity(m["_id"])
        m["available_num"] = bal
        m["available"] = f"{bal:,.2f}"
    managers.sort(key=lambda x: x.get("available_num", 0.0), reverse=True)

    return render_template(
        "admin_sales_close.html",
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
      - Debits manager ledger across days (today first→older) internally.
      - Credits ADMIN today's ledger by the full requested amount.
      - Logs ONE consolidated record in admin_withdrawals (for clean UI).
      - Returns refreshed totals.
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

    # Verify manager
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

    # --- Gather positive-balance docs for manager
    pipeline = [
        {"$match": {"agent_id": str(manager_id)}},
        {"$addFields": {
            "bal_num": {"$toDouble": {"$ifNull": ["$total_amount", 0]}}},
        },
        {"$match": {"bal_num": {"$gt": 0}}},
        {"$sort": {"date": -1, "updated_at": -1}}
    ]
    docs = list(sales_close_col.aggregate(pipeline))

    total_all = sum(float(d.get("bal_num", 0.0)) for d in docs)
    if total_all + 1e-9 < amount:
        msg = f"Insufficient balance. Manager total across all days: GHS {total_all:,.2f}"
        return (jsonify(ok=False, message=msg, available=f"{total_all:,.2f}"), 409) if _is_ajax(request) else (msg, 409)

    # --- Debit internally (may span days), but build a breakdown for audit
    remaining = amount
    breakdown = []  # [{date, amount}]
    for d in docs:
        if remaining <= 1e-9:
            break
        doc_id = d["_id"]
        date_str = d.get("date", "")
        available = float(d.get("bal_num", 0.0))
        if available <= 0:
            continue

        take = min(available, remaining)
        res = sales_close_col.update_one(
            {
                "_id": doc_id,
                "$expr": {"$gte": [{"$toDouble": {"$ifNull": ["$total_amount", 0]}}, take]}
            },
            {
                "$inc": {"total_amount": -take},
                "$set": {"updated_at": now_utc, "last_withdrawal_at": now_utc},
                "$push": {"withdrawals": {
                    "amount": float(round(take, 2)),
                    "by_admin_id": admin_id,
                    "by_admin_name": admin_doc.get("name", ""),
                    "date": date_str,
                    "time": time_str,
                    "at": now_utc,
                    "note": note
                }}
            }
        )
        if res.modified_count == 1:
            breakdown.append({"date": date_str, "amount": float(round(take, 2))})
            remaining -= take

    actually_debited = amount - remaining
    if actually_debited <= 0:
        current_total = _sum_ledger_total_for_entity(str(manager_id))
        msg = f"Insufficient balance due to concurrent changes. Current total: GHS {current_total:,.2f}"
        return (jsonify(ok=False, message=msg, available=f"{current_total:,.2f}"), 409) if _is_ajax(request) else (msg, 409)

    # --- Credit ADMIN today's ledger by full amount
    sales_close_col.update_one(
        {"agent_id": admin_id, "date": today},
        {
            "$setOnInsert": {"agent_id": admin_id, "manager_id": admin_id, "date": today, "created_at": now_utc},
            "$inc": {"total_amount": actually_debited, "count": 1},
            "$set": {"updated_at": now_utc, "last_payment_at": now_utc}
        },
        upsert=True
    )

    # --- Log ONE consolidated action (NEW)
    admin_withdrawals_col.insert_one({
        "manager_id": str(manager_id),
        "manager_name": mgr_doc.get("name") or mgr_doc.get("username") or "Manager",
        "by_admin_id": admin_id,
        "by_admin_name": admin_doc.get("name", ""),
        "amount": float(round(actually_debited, 2)),
        "note": note,
        "created_at": now_utc,
        "date": today,
        "time": time_str,
        "breakdown": breakdown  # preserved for audit, but not shown in UI
    })

    # --- Recompute updated numbers
    new_mgr_total = _sum_ledger_total_for_entity(str(manager_id))
    unclose_total = _unclose_total_all_managers()
    close_total   = _sum_ledger_total_for_entity(admin_id)

    payload = {
        "ok": True,
        "message": f"Withdrew GHS {actually_debited:,.2f} and credited admin account.",
        "requested": f"{amount:,.2f}",
        "manager_id": str(manager_id),
        "available": f"{new_mgr_total:,.2f}",
        "unclose_total": f"{unclose_total:,.2f}",
        "close_total": f"{close_total:,.2f}"
        # (breakdown kept server-side; not needed for UI anymore)
    }
    return jsonify(payload) if _is_ajax(request) else (
        f"OK. Debited: {payload['requested']} | "
        f"Manager total: {payload['available']} | "
        f"Unclose Total: {payload['unclose_total']} | "
        f"Close Total: {payload['close_total']}"
    )

@admin_sales_close_bp.route("/manager/<manager_id>/withdrawals", methods=["GET"])
def manager_withdrawals(manager_id):
    """
    Returns JSON history of withdrawals (ONE row per admin action), newest first.
    Each item: { amount, date, time, note, by_admin_id, by_admin_name, at_iso }
    """
    scope = _ensure_admin_scope_or_redirect()
    if not isinstance(scope, tuple):
        return jsonify(ok=False, message="Please log in."), 401

    # Check target manager exists
    try:
        m_doc = users_col.find_one({"_id": ObjectId(manager_id), "role": "manager"})
    except Exception:
        m_doc = users_col.find_one({"_id": manager_id, "role": "manager"})
    if not m_doc:
        return jsonify(ok=False, message="Manager not found."), 404

    # Fetch consolidated logs (not per-day splits)
    cur = admin_withdrawals_col.find(
        {"manager_id": str(manager_id)},
        {"_id": 0, "amount": 1, "note": 1, "by_admin_id":1, "by_admin_name":1, "created_at":1, "date":1, "time":1}
    ).sort("created_at", -1)

    items = []
    for w in cur:
        at = w.get("created_at")
        at_iso = at.isoformat() if isinstance(at, datetime) else f"{w.get('date','')}T{w.get('time','00:00:00')}"
        items.append({
            "amount": float(w.get("amount", 0.0)),
            "date": w.get("date", ""),
            "time": w.get("time", ""),
            "note": w.get("note", ""),
            "by_admin_id": w.get("by_admin_id", ""),
            "by_admin_name": w.get("by_admin_name", ""),
            "at_iso": at_iso
        })

    return jsonify(ok=True, manager={
        "_id": str(m_doc["_id"]),
        "name": m_doc.get("name") or m_doc.get("username") or "Manager",
        "phone": m_doc.get("phone", "")
    }, withdrawals=items)

# executive_sales_close.py (FAST, single-aggregation dashboard)
from flask import Blueprint, render_template, request, jsonify, session, redirect, url_for
from bson.objectid import ObjectId
from datetime import datetime
from typing import List, Dict, Any, Optional
from db import db

executive_sales_close_bp = Blueprint(
    "executive_sales_close",
    __name__,
    url_prefix="/executive-close"
)

users_col       = db["users"]
sales_close_col = db["sales_close"]

# -------------------------------------------------------------------
# (You said indexes are in place; keeping here for reference)
# db.sales_close.createIndex({ agent_id: 1, date: -1 })
# db.sales_close.createIndex({ agent_id: 1, total_amount: 1 })
# db.sales_close.createIndex({ date: -1, updated_at: -1 })
# db.users.createIndex({ role: 1 })
# -------------------------------------------------------------------

# ---------- helpers ----------

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
    """
    pipeline = [
        {"$match": {"agent_id": owner_id_str}},
        {"$group": {"_id": None, "sum_amount": {"$sum": {"$toDouble": "$total_amount"}}}},
    ]
    agg = list(sales_close_col.aggregate(pipeline))
    if not agg:
        return 0.0
    try:
        return float(agg[0].get("sum_amount", 0.0))
    except Exception:
        return 0.0

def _group_totals_for_roles(roles: List[str]) -> List[Dict[str, Any]]:
    """
    FAST path: one aggregation to get TOTAL (all dates) per user for the given roles.
      - Group sales_close by agent_id
      - $lookup users by stringified _id, filter by roles (lowercased)
      - Return: { user_id(str), total(float), name, phone, role(lower) }, sorted DESC.
    """
    roles = [r.lower() for r in roles]
    pipeline = [
        {"$group": {"_id": "$agent_id", "total": {"$sum": {"$toDouble": "$total_amount"}}}},
        {
            "$lookup": {
                "from": "users",
                "let": {"aid": "$_id"},
                "pipeline": [
                    {"$addFields": {"_id_str": {"$toString": "$_id"}}},
                    {
                        "$match": {
                            "$expr": {
                                "$and": [
                                    {"$eq": ["$_id_str", "$$aid"]},
                                    {"$in": [{"$toLower": "$role"}, roles]}
                                ]
                            }
                        }
                    },
                    {"$project": {"name": 1, "username": 1, "phone": 1, "role": 1}}
                ],
                "as": "user"
            }
        },
        {"$unwind": "$user"},
        {
            "$project": {
                "_id": 0,
                "user_id": "$_id",
                "total": 1,
                "name": {"$ifNull": ["$user.name", "$user.username"]},
                "phone": "$user.phone",
                "role": {"$toLower": "$user.role"},
            }
        },
        {"$sort": {"total": -1}}
    ]
    return list(sales_close_col.aggregate(pipeline))

def _attempt_debit_single_doc(owner_id_str: str, ymd_str: str, amount: float, who: Dict[str, str], note: str) -> bool:
    """
    Try to debit exactly the (owner_id_str, ymd_str) doc if it has enough balance.
    Appends a withdrawal record with executive info.
    """
    now_utc = datetime.utcnow()
    time_str = now_utc.strftime("%H:%M:%S")

    filt = {"agent_id": owner_id_str, "date": ymd_str, "total_amount": {"$gte": amount}}
    upd = {
        "$inc": {"total_amount": -amount},
        "$set": {"updated_at": now_utc, "last_withdrawal_at": now_utc},
        "$push": {"withdrawals": {
            "amount": amount,
            "by_executive_id": who["id"],
            "by_executive_name": who["name"],
            "by_role": "executive",
            "date": ymd_str,
            "time": time_str,
            "at": now_utc,
            "note": note
        }}
    }
    res = sales_close_col.update_one(filt, upd)
    return res.modified_count > 0

def _attempt_debit_latest_doc_with_funds(owner_id_str: str, amount: float, who: Dict[str, str], note: str) -> Optional[str]:
    """
    Find the most recent doc (by date, updated_at) that has enough balance and debit it.
    Returns the debited date string or None if none found.
    """
    now_utc = datetime.utcnow()
    time_str = now_utc.strftime("%H:%M:%S")

    doc_cur = sales_close_col.find(
        {"agent_id": owner_id_str, "total_amount": {"$gte": amount}},
        {"date": 1}
    ).sort([("date", -1), ("updated_at", -1)]).limit(1)
    doc = next(doc_cur, None)
    if not doc:
        return None

    ymd = doc.get("date")
    filt = {"agent_id": owner_id_str, "date": ymd, "total_amount": {"$gte": amount}}
    upd = {
        "$inc": {"total_amount": -amount},
        "$set": {"updated_at": now_utc, "last_withdrawal_at": now_utc},
        "$push": {"withdrawals": {
            "amount": amount,
            "by_executive_id": who["id"],
            "by_executive_name": who["name"],
            "by_role": "executive",
            "date": ymd,
            "time": time_str,
            "at": now_utc,
            "note": note
        }}
    }
    res = sales_close_col.update_one(filt, upd)
    return ymd if res.modified_count > 0 else None

# ---------- views ----------

@executive_sales_close_bp.route("/", methods=["GET"])
def executive_close_page():
    """
    Executive dashboard:
      - Close Total (Executive ledger, all dates)
      - Unclose Total (sum of Agent+Manager+Admin balances, all dates)
      - Three grids (Admins, Managers, Agents) with TOTAL balances (all dates), sorted DESC
      - Each item can be withdrawn from.

    FAST: single aggregation builds all role totals; no per-user loops.
    """
    scope = _ensure_executive_or_redirect()
    if not isinstance(scope, tuple):
        return scope
    exec_id, exec_doc = scope

    today = _today_str()

    # One pass to get all balances per role (desc sorted already)
    grouped = _group_totals_for_roles(["admin", "manager", "agent"])

    # Split into role groups (already sorted desc)
    def _fmt_row(r: Dict[str, Any]) -> Dict[str, Any]:
        total = float(r.get("total", 0.0))
        return {
            "_id": r["user_id"],
            "name": r.get("name") or "User",
            "phone": r.get("phone", ""),
            "role": r["role"],
            "available": f"{total:,.2f}",
            "available_num": total,
        }

    admins   = [_fmt_row(r) for r in grouped if r["role"] == "admin"   and r["user_id"] != exec_id]
    managers = [_fmt_row(r) for r in grouped if r["role"] == "manager"]
    agents   = [_fmt_row(r) for r in grouped if r["role"] == "agent"]

    # Cards (executive close total + unclose total across admins/managers/agents)
    close_total   = _sum_ledger_all_dates(exec_id)  # one quick agg
    unclose_total = float(sum(r["available_num"] for r in admins + managers + agents))

    return render_template(
        "executive_sales_close.html",
        executive_name=exec_doc.get("name", "Executive"),
        today=today,
        close_total=f"{close_total:,.2f}",
        unclose_total=f"{unclose_total:,.2f}",
        admins=admins,
        managers=managers,
        agents=agents
    )

@executive_sales_close_bp.route("/withdraw", methods=["POST"])
def executive_withdraw():
    """
    POST: target_id, amount, note (optional)
      - Try to debit TODAY's doc; if insufficient, debit the most recent doc with enough balance.
      - Credit the EXECUTIVE'S TODAY doc.
      - Returns refreshed numbers for target (TOTAL), unclose total, and executive close total.
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

    # 1) Debit TODAY or most recent doc
    today = _today_str()
    officer = {"id": exec_id, "name": exec_doc.get("name", "")}

    debited_date: Optional[str] = None
    if _attempt_debit_single_doc(str(target_id), today, amount, officer, note):
        debited_date = today
    else:
        debited_date = _attempt_debit_latest_doc_with_funds(str(target_id), amount, officer, note)
        if not debited_date:
            total_all = _sum_ledger_all_dates(str(target_id))
            msg = f"Insufficient balance. Target total across all days: GHS {total_all:,.2f}"
            return (jsonify(ok=False, message=msg, available=f"{total_all:,.2f}"), 409) if _is_ajax(request) else (msg, 409)

    # 2) Credit EXECUTIVE TODAY doc
    now_utc = datetime.utcnow()
    exec_filter = {"agent_id": exec_id, "date": today}
    exec_update = {
        "$setOnInsert": {"agent_id": exec_id, "manager_id": exec_id, "date": today, "created_at": now_utc},
        "$inc": {"total_amount": amount, "count": 1},
        "$set": {"updated_at": now_utc, "last_payment_at": now_utc}
    }
    sales_close_col.update_one(exec_filter, exec_update, upsert=True)

    # 3) Recompute refreshed totals (ALL dates) — minimal roundtrips
    target_total  = _sum_ledger_all_dates(str(target_id))  # one agg
    grouped       = _group_totals_for_roles(["admin", "manager", "agent"])  # one agg for unclose
    unclose_total = float(sum(float(r.get("total", 0.0)) for r in grouped))
    close_total   = _sum_ledger_all_dates(exec_id)  # one agg

    payload = {
        "ok": True,
        "message": f"Withdrew GHS {amount:,.2f} from {tgt_role} and credited executive account.",
        "target_id": str(target_id),
        "target_role": tgt_role,
        "available": f"{target_total:,.2f}",     # target TOTAL (all dates)
        "unclose_total": f"{unclose_total:,.2f}",
        "close_total": f"{close_total:,.2f}",
        "debited_date": debited_date
    }
    return jsonify(payload) if _is_ajax(request) else (
        f"OK. Target total now {payload['available']}, "
        f"Unclose Total {payload['unclose_total']}, "
        f"Close Total {payload['close_total']} (debited {debited_date})."
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

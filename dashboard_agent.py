# routes/agent_dashboard.py
from flask import Blueprint, render_template, request, redirect
from flask_login import login_required, current_user
from bson import ObjectId
import random
import string
from urllib.parse import quote
from datetime import datetime, timedelta, timezone, date

from db import db

# Collections
users_collection         = db["users"]
tasks_collection         = db["tasks"]
targets_collection       = db["targets"]
payments_collection      = db["payments"]
customers_collection     = db["customers"]
agent_commissions_coll   = db["agent_commissions"]   # GLOBAL commission %
sales_close_collection   = db["sales_close"]         # <-- agent “account money”
whatsapp_posts_col       = db["whatsapp_posts"]

agent_dashboard_bp = Blueprint('dashboard_agent', __name__)

# ----------------------------
# Helpers
# ----------------------------
def _today_utc_date() -> date:
    return datetime.now(timezone.utc).date()

def _period_window(duration_type: str):
    today = _today_utc_date()
    if duration_type == "daily":
        return today, today
    if duration_type == "weekly":
        start = today - timedelta(days=today.weekday())
        return start, start + timedelta(days=6)
    if duration_type == "monthly":
        start = today.replace(day=1)
        # last day of month
        end = (start.replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)
        return start, end
    if duration_type == "yearly":
        return today.replace(month=1, day=1), today.replace(month=12, day=31)
    return today, today

def _coerce_date_string(d) -> str:
    if isinstance(d, datetime):
        d = d.date()
    if hasattr(d, "isoformat"):
        return d.isoformat()
    return str(d)

def _target_window_strings(tgt) -> tuple[str, str]:
    duration_type = tgt.get("duration_type", "daily")
    start = tgt.get("start_date")
    end   = tgt.get("end_date")
    if start and end:
        return _coerce_date_string(start), _coerce_date_string(end)
    s, e = _period_window(duration_type)
    return _coerce_date_string(s), _coerce_date_string(e)

def _get_agent_allocation_for_target(target: dict, agent_id_str: str):
    """
    Allocation percentages for this agent in this target.
    Looks inside 'agent_allocations' (new) and falls back to 'agents_distribution' (old absolute quotas).
    """
    # New schema
    for rec in target.get("agent_allocations", []):
        if str(rec.get("agent_id")) == agent_id_str:
            return {
                "product_pct": float(rec.get("product_pct") or 0),
                "cash_pct": float(rec.get("cash_pct") or 0),
                "customer_pct": float(rec.get("customer_pct") or 0),
                "agent_name": rec.get("agent_name"),
                "image_url": rec.get("image_url"),
            }

    # Old schema (absolute quotas; infer %)
    for rec in target.get("agents_distribution", []):
        if str(rec.get("agent_id")) == agent_id_str:
            pt  = int(target.get("product_target") or 0)
            ct  = float(target.get("cash_target") or 0)
            cut = int(target.get("customer_target") or 0)
            return {
                "product_pct": (float(rec.get("product_target") or 0) / pt * 100) if pt else 0.0,
                "cash_pct": (float(rec.get("cash_target") or 0) / ct * 100) if ct else 0.0,
                "customer_pct": (float(rec.get("customer_target") or 0) / cut * 100) if cut else 0.0,
                "agent_name": rec.get("agent_name"),
                "image_url": None,
            }

    return {"product_pct": 0.0, "cash_pct": 0.0, "customer_pct": 0.0, "agent_name": None, "image_url": None}

def _round_int(n: float) -> int:
    try:
        return int(round(float(n)))
    except Exception:
        return 0

def _get_global_commission_pct() -> float:
    """
    Reads ONE global product_commission_pct from agent_commissions (scope='GLOBAL').
    Applies to all agents unless you add per-agent overrides later.
    """
    doc = agent_commissions_coll.find_one({"scope": "GLOBAL"})
    try:
        return float(doc.get("product_commission_pct", 0.0)) if doc else 0.0
    except Exception:
        return 0.0

# ----------------------------
# New: Fast sums for account money & today's collections
# ----------------------------
def _agent_unclosed_total_all_dates(agent_id_str: str) -> float:
    """
    Agent “account money” = sum of ALL sales_close.total_amount for this agent (all dates).
    This is the money still with the agent (not yet withdrawn by manager/admin/executive).
    """
    pipeline = [
        {"$match": {"agent_id": agent_id_str}},
        {"$group": {"_id": None, "sum_amount": {"$sum": {"$toDouble": "$total_amount"}}}},
    ]
    agg = list(sales_close_collection.aggregate(pipeline))
    if not agg:
        return 0.0
    try:
        return float(agg[0].get("sum_amount", 0.0))
    except Exception:
        return 0.0

def _agent_today_collections_total(agent_id_str: str) -> float:
    """
    Sum of TODAY's payments for this agent from payments collection (exclude withdrawals).
    Assumes payments.date is a 'YYYY-MM-DD' string.
    """
    today_str = _today_utc_date().isoformat()

    # Some installs store agent_id as string; others as ObjectId. Match both robustly.
    try:
        agent_oid = ObjectId(agent_id_str)
    except Exception:
        agent_oid = None

    match = {
        "payment_type": {"$ne": "WITHDRAWAL"},
        "date": today_str,
        "$or": [{"agent_id": agent_id_str}] + ([{"agent_id": agent_oid}] if agent_oid else [])
    }

    pipeline = [
        {"$match": match},
        {"$group": {"_id": None, "total": {"$sum": {"$toDouble": "$amount"}}}},
    ]
    agg = list(payments_collection.aggregate(pipeline))
    if not agg:
        return 0.0
    try:
        return float(agg[0].get("total", 0.0))
    except Exception:
        return 0.0

def _ensure_whatsapp_indexes():
    try:
        whatsapp_posts_col.create_index([("agent_id", 1), ("date", 1)], unique=True)
    except Exception:
        pass

_ensure_whatsapp_indexes()

# ----------------------------
# Core calc
# ----------------------------
def calculate_agent_targets(agent_id_str: str, commission_pct: float):
    """
    Build list of targets that include this agent, compute quotas from allocations,
    achievements, and commission on CASH SALES. Commission is calculated on
    the surplus cash above cash_quota (never negative).
    """
    assigned = list(targets_collection.find({
        "$or": [
            {"agent_allocations.agent_id": agent_id_str},
            {"agents_distribution.agent_id": agent_id_str}
        ]
    }).sort("created_at", -1))

    results = []
    total_commission_amount = 0.0
    total_surplus_cash = 0.0

    # support mixed agent_id types in customers: ObjectId vs str
    agent_oid = None
    try:
        agent_oid = ObjectId(agent_id_str)
    except Exception:
        pass

    for tgt in assigned:
        duration_type = tgt.get("duration_type", "daily")
        start_str, end_str = _target_window_strings(tgt)

        # Allocation (percentages for this agent)
        alloc = _get_agent_allocation_for_target(tgt, agent_id_str)
        prod_pct = float(alloc["product_pct"])
        cash_pct = float(alloc["cash_pct"])
        cust_pct = float(alloc["customer_pct"])

        # Manager totals
        pt  = int(tgt.get("product_target") or 0)      # units
        ct  = float(tgt.get("cash_target") or 0.0)     # GH₵
        cut = int(tgt.get("customer_target") or 0)     # customers

        # Agent quotas
        product_quota   = _round_int(pt * (prod_pct / 100.0))
        cash_quota      = round(ct * (cash_pct / 100.0), 2)
        customer_quota  = _round_int(cut * (cust_pct / 100.0))

        # --------------------
        # Achievements (THIS agent in window)
        # --------------------

        # CASH: include only sales (exclude withdrawals)
        pay_sum = list(payments_collection.aggregate([
            {"$match": {
                "agent_id": agent_id_str,
                "payment_type": {"$ne": "WITHDRAWAL"},
                "date": {"$gte": start_str, "$lte": end_str}
            }},
            {"$group": {"_id": None, "total": {"$sum": "$amount"}}}
        ]))
        cash_achieved = float(pay_sum[0]["total"]) if pay_sum else 0.0

        # PRODUCTS & CUSTOMERS in one pipeline (robust to ID type)
        cust_pipeline = [
            {"$match": {
                "$or": (
                    [{"agent_id": agent_oid}] if agent_oid else []
                ) + [{"agent_id": agent_id_str}],
                "purchases.purchase_date": {"$gte": start_str, "$lte": end_str}
            }},
            {"$unwind": "$purchases"},
            {"$match": {
                "purchases.purchase_date": {"$gte": start_str, "$lte": end_str},
                "$or": [
                    {"purchases.agent_id": agent_id_str},
                    {"purchases.agent_id": {"$exists": False}}
                ]
            }},
            {"$group": {
                "_id": None,
                "units": {"$sum": {"$ifNull": ["$purchases.product.quantity", 1]}},
                "customers": {"$addToSet": "$_id"}
            }}
        ]
        prod_cust = list(customers_collection.aggregate(cust_pipeline))
        product_achieved = int(prod_cust[0]["units"]) if prod_cust else 0
        customer_achieved = len(prod_cust[0]["customers"]) if prod_cust else 0

        # Percentages vs quotas
        product_pct  = round((product_achieved / product_quota * 100) if product_quota else 0.0, 2)
        payment_pct  = round((cash_achieved   / cash_quota * 100) if cash_quota else 0.0, 2)
        customer_pct = round((customer_achieved / customer_quota * 100) if customer_quota else 0.0, 2)

        active_metrics = sum(1 for x in [product_quota, cash_quota, customer_quota] if x)
        overall = round((product_pct + payment_pct + customer_pct) / (active_metrics or 1), 2)

        # --------------------
        # COMMISSION (Cash-based)
        # --------------------
        surplus_cash = max(0.0, cash_achieved - (cash_quota or 0.0))
        commission_amount = round(surplus_cash * (commission_pct / 100.0), 2)

        total_commission_amount += commission_amount
        total_surplus_cash += surplus_cash

        results.append({
            "target_id": str(tgt.get("_id")),
            "title": tgt.get("title"),
            "duration": duration_type,
            "start_date": start_str,
            "end_date": end_str,

            # quotas
            "product_quota": product_quota,
            "cash_quota": cash_quota,
            "customer_quota": customer_quota,

            # achievements
            "product_achieved": product_achieved,
            "cash_achieved": round(cash_achieved, 2),
            "customer_achieved": customer_achieved,

            # pct
            "product_pct": product_pct,
            "payment_pct": payment_pct,
            "customer_pct": customer_pct,
            "overall": overall,

            # commission (cash-based)
            "commission_pct": commission_pct,        # %
            "surplus_cash": round(surplus_cash, 2),  # GH₵
            "commission_amount": commission_amount   # GH₵
        })

    summary = {
        "commission_pct": commission_pct,
        "total_surplus_cash": round(total_surplus_cash, 2),
        "total_commission": round(total_commission_amount, 2)
    }
    return results, summary


@agent_dashboard_bp.route('/dashboard/agent')
@login_required
def agent_dashboard():
    if current_user.role != 'agent':
        return "Unauthorized", 403

    agent_id_str = str(current_user.id)
    today_str = _today_utc_date().isoformat()

    # user record
    try:
        user_data = users_collection.find_one({'_id': ObjectId(agent_id_str)})
    except Exception:
        user_data = users_collection.find_one({'_id': agent_id_str})
    if not user_data:
        return "User not found", 404

    # pending tasks badge
    pending_tasks_count = tasks_collection.count_documents({
        'target_type': 'agent',
        'status': 'pending',
        '$or': [
            {'user_id': ObjectId(agent_id_str)},
            {'user_id': 'all'}
        ]
    })
    user_data['has_pending_tasks'] = pending_tasks_count > 0
    user_data['pending_task_count'] = pending_tasks_count

    # Commission %
    global_commission_pct = _get_global_commission_pct()

    # agent targets/progress + commission
    agent_targets, commission_summary = calculate_agent_targets(agent_id_str, global_commission_pct)

    # ----------------------------
    # NEW: money metrics for header
    # ----------------------------
    account_money_total = _agent_unclosed_total_all_dates(agent_id_str)      # from sales_close (all dates)
    today_collections   = _agent_today_collections_total(agent_id_str)       # from payments (today only)

    return render_template(
        'dashboard_agent.html',
        user=user_data,
        agent_targets=agent_targets,
        commission_summary=commission_summary,
        # header metrics
        today=today_str,
        account_money_total=f"{account_money_total:,.2f}",
        today_collections=f"{today_collections:,.2f}"
    )


@agent_dashboard_bp.route("/dashboard/agent/post-whatsapp", methods=["GET"])
@login_required
def post_today_collections_whatsapp():
    if current_user.role != "agent":
        return "Unauthorized", 403

    agent_id_str = str(current_user.id)
    today_str = _today_utc_date().isoformat()

    try:
        user_data = users_collection.find_one({'_id': ObjectId(agent_id_str)})
    except Exception:
        user_data = users_collection.find_one({'_id': agent_id_str})

    agent_name = (user_data or {}).get("name") or "Agent"
    today_collections = _agent_today_collections_total(agent_id_str)

    existing = whatsapp_posts_col.find_one({"agent_id": agent_id_str, "date": today_str})
    if existing:
        post_id = existing.get("post_id") or ""
    else:
        suffix = ''.join(random.choices(string.ascii_uppercase + string.digits, k=6))
        post_id = f"WAP-{today_str.replace('-','')}-{suffix}"
        doc = {
            "post_id": post_id,
            "agent_id": agent_id_str,
            "agent_name": agent_name,
            "date": today_str,
            "today_collections": float(today_collections),
            "created_at": datetime.now(timezone.utc),
            "source": "dashboard_button",
            "status": "posted",
            "user_agent": request.headers.get("User-Agent"),
            "ip": request.headers.get("X-Forwarded-For", request.remote_addr),
        }
        whatsapp_posts_col.insert_one(doc)

    message = (
        f"Agent Name: {agent_name}, Sales: GH₵ {today_collections:,.2f}, "
        f"Date: {today_str}, ID: {post_id}"
    )
    whatsapp_url = "https://wa.me/?text=" + quote(message)
    return redirect(whatsapp_url)

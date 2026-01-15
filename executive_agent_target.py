from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify
from bson.objectid import ObjectId
from datetime import datetime, timedelta, timezone, date
from db import db
import re

executive_agent_target_bp = Blueprint(
    "executive_agent_target",
    __name__,
    url_prefix="/executive/agents/targets"
)

# Collections
targets_collection           = db["targets"]
users_collection             = db["users"]
payments_collection          = db["payments"]
customers_collection         = db["customers"]
agent_commissions_collection = db["agent_commissions"]  # GLOBAL + per-agent overrides


# ------------------------------------------------------------
# Indexes (run once; safe to leave enabled)
# ------------------------------------------------------------
def ensure_indexes():
    """
    Call this once at startup (e.g., in app factory) to ensure indexes exist.
    These match the queries below and massively improve speed.
    """
    # Users: fast lookups for managers/agents
    users_collection.create_index([("role", 1), ("name", 1)])
    users_collection.create_index([("role", 1), ("branch", 1)])
    users_collection.create_index([("_id", 1)])

    # Payments: filter by agent_id + date range
    payments_collection.create_index([("agent_id", 1), ("date", 1)])  # 'date' should be a datetime

    # Customers: filter by agent at root, and by purchases.* after unwind
    customers_collection.create_index([("agent_id", 1)])  # agent assigned to customer
    customers_collection.create_index([("purchases.purchase_date", 1)])
    customers_collection.create_index([("purchases.agent_id", 1)])

    # Targets: by nested allocations and created_at ordering
    targets_collection.create_index([("agent_allocations.agent_id", 1), ("created_at", -1)])
    targets_collection.create_index([("allocations.agent_id", 1), ("created_at", -1)])

    # Commissions: quick GLOBAL + per-agent override
    agent_commissions_collection.create_index([("scope", 1)], unique=True, sparse=True)
    agent_commissions_collection.create_index([("agent_id", 1)], unique=True, sparse=True)

# Uncomment if you want to auto-create indexes on import
# ensure_indexes()


# ------------------------------------------------------------
# Helpers (dates)
# ------------------------------------------------------------
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
        end = (start.replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)
        return start, end
    if duration_type == "yearly":
        return today.replace(month=1, day=1), today.replace(month=12, day=31)
    return today, today

def _as_naive_dt(d) -> datetime:
    """
    Accepts date/datetime/ISO 'YYYY-MM-DD' string and returns a naive (UTC) datetime.
    """
    if isinstance(d, datetime):
        return d.replace(tzinfo=None)
    if isinstance(d, date):
        return datetime(d.year, d.month, d.day)
    if isinstance(d, str):
        # Expect "YYYY-MM-DD" (adjust here if your format differs)
        y, m, dd = map(int, d.split("-")[:3])
        return datetime(y, m, dd)
    # Fallback to now to avoid None errors (you can raise instead)
    now = datetime.utcnow()
    return datetime(now.year, now.month, now.day)

def _target_window_datetimes(tgt) -> tuple[datetime, datetime]:
    """
    Returns [start_dt, end_dt] as naive datetimes covering full days so Mongo can use date indexes.
    """
    duration_type = tgt.get("duration_type", "daily")
    start = tgt.get("start_date")
    end   = tgt.get("end_date")

    if start and end:
        s = _as_naive_dt(start)
        e = _as_naive_dt(end)
    else:
        s_raw, e_raw = _period_window(duration_type)
        s = _as_naive_dt(s_raw)
        e = _as_naive_dt(e_raw)

    # Normalize to full-day inclusive window
    s = datetime(s.year, s.month, s.day)
    e = datetime(e.year, e.month, e.day) + timedelta(days=1) - timedelta(milliseconds=1)
    return s, e


# ------------------------------------------------------------
# Commission helpers
# ------------------------------------------------------------
def _get_global_commission_pct() -> float:
    doc = agent_commissions_collection.find_one({"scope": "GLOBAL"})
    return float(doc.get("product_commission_pct", 0.0)) if doc else 0.0

def _set_global_commission_pct(pct: float):
    agent_commissions_collection.update_one(
        {"scope": "GLOBAL"},
        {"$set": {"product_commission_pct": pct, "updated_at": datetime.utcnow()}},
        upsert=True
    )

def _get_agent_commission_pct(agent_oid: ObjectId) -> float:
    """Return per-agent override if it exists; otherwise GLOBAL."""
    per = agent_commissions_collection.find_one({"agent_id": agent_oid})
    if per and per.get("product_commission_pct") is not None:
        return float(per.get("product_commission_pct", 0.0))
    return _get_global_commission_pct()


# ------------------------------------------------------------
# Managers / Agents lookups
# ------------------------------------------------------------
def _get_managers_list():
    managers = list(
        users_collection.find(
            {"role": "manager"},
            {"_id": 1, "name": 1, "branch": 1, "image_url": 1}
        ).sort("name", 1)
    )
    return [{
        "manager_id": str(m["_id"]),
        "name": m.get("name") or "Manager",
        "branch": m.get("branch") or "",
        "image_url": m.get("image_url") or ""
    } for m in managers]

def _resolve_manager_ids_by_name_like(name_like: str):
    regex = re.compile(re.escape(name_like), re.IGNORECASE)
    ids = users_collection.find(
        {"role": "manager", "name": {"$regex": regex}},
        {"_id": 1}
    )
    return [m["_id"] for m in ids]

def _get_all_agents(manager_id: str | None = None,
                    branch: str | None = None,
                    manager_name_like: str | None = None):
    query = {"role": "agent"}

    if branch:
        query["branch"] = branch

    manager_ids = None
    if manager_id:
        try:
            manager_ids = [ObjectId(manager_id)]
        except Exception:
            manager_ids = []

    if manager_name_like:
        by_name = _resolve_manager_ids_by_name_like(manager_name_like)
        manager_ids = (manager_ids or []) + by_name if manager_ids is not None else by_name

    if manager_ids is not None:
        if len(manager_ids) == 0:
            return []
        query["manager_id"] = {"$in": manager_ids}

    agents = list(
        users_collection.find(
            query,
            {"_id": 1, "name": 1, "image_url": 1, "manager_id": 1, "branch": 1}
        ).sort("name", 1)
    )

    m_ids = {a.get("manager_id") for a in agents if a.get("manager_id")}
    managers = {}
    if m_ids:
        for m in users_collection.find({"_id": {"$in": list(m_ids)}}, {"_id": 1, "name": 1}):
            managers[str(m["_id"])] = m.get("name")

    result = []
    for a in agents:
        agent_oid = a["_id"]
        result.append({
            "agent_id": str(agent_oid),
            "name": a.get("name", "Unnamed"),
            "image_url": a.get("image_url"),
            "branch": a.get("branch") or "",
            "manager_id": str(a.get("manager_id")) if a.get("manager_id") else None,
            "manager_name": managers.get(str(a.get("manager_id")), "—"),
            "commission_pct": _get_agent_commission_pct(agent_oid)
        })
    return result


# ------------------------------------------------------------
# Agent targets + progress (optimized)
# ------------------------------------------------------------
def _agent_targets_with_progress(agent_oid: ObjectId):
    """
    For a given agent, find all targets where this agent appears in allocations and
    compute target quotas + achievements within the target's date window.

    Performance notes:
      - Uses datetime ranges so (agent_id, date) and (purchases.purchase_date) indexes are used.
      - Matches customer agent at root BEFORE unwinding purchases.
    """
    agent_id_str = str(agent_oid)

    assigned_targets = list(
        targets_collection.find({
            "$or": [
                {"agent_allocations.agent_id": agent_id_str},
                {"allocations.agent_id": agent_id_str}
            ]
        }).sort("created_at", -1)
    )

    rows = []
    commission_pct = _get_agent_commission_pct(agent_oid)

    for tgt in assigned_targets:
        title         = tgt.get("title", "Untitled")
        duration_type = tgt.get("duration_type", "daily")
        start_dt, end_dt = _target_window_datetimes(tgt)

        # Target totals
        pt  = int(tgt.get("product_target") or 0)      # units
        ct  = float(tgt.get("cash_target") or 0.0)     # currency
        cut = int(tgt.get("customer_target") or 0)     # distinct customers

        # Allocation for this agent
        allocs = tgt.get("agent_allocations") or tgt.get("allocations") or []
        alloc = next((a for a in allocs if a.get("agent_id") == agent_id_str), None)

        product_pct_alloc  = float(alloc.get("product_pct", 0.0)) if alloc else 0.0
        cash_pct_alloc     = float(alloc.get("cash_pct", 0.0))    if alloc else 0.0
        customer_pct_alloc = float(alloc.get("customer_pct", 0.0))if alloc else 0.0

        # Quotas for this agent
        product_quota  = int(round(pt  * (product_pct_alloc  / 100.0))) if pt  else 0
        cash_quota     = round(ct * (cash_pct_alloc / 100.0), 2)        if ct  else 0.0
        customer_quota = int(round(cut * (customer_pct_alloc / 100.0))) if cut else 0

        # ---- Achieved (per agent) ----
        # CASH: payments per agent within window (index: agent_id + date)
        pay_sum = list(payments_collection.aggregate([
            {"$match": {
                "agent_id": agent_id_str,
                "date": {"$gte": start_dt, "$lte": end_dt}
            }},
            {"$group": {"_id": None, "total": {"$sum": "$amount"}}}
        ]))
        cash_achieved = float(pay_sum[0]["total"]) if pay_sum else 0.0

        # PRODUCTS & CUSTOMERS from customers.purchases
        # Match agent at root first -> unwind -> filter purchases by date and (optionally) by item-level agent
        prod_agg = list(customers_collection.aggregate([
            {"$match": {
                "$or": [
                    {"agent_id": agent_oid},     # preferred: consistent ObjectId at root
                    {"agent_id": agent_id_str},  # fallback: if some docs stored as str
                ]
            }},
            {"$unwind": "$purchases"},
            {"$match": {
                "purchases.purchase_date": {"$gte": start_dt, "$lte": end_dt},
                "$or": [
                    {"purchases.agent_id": agent_id_str},            # item attributed to this agent
                    {"purchases.agent_id": {"$exists": False}}       # or legacy items without item-level agent
                ]
            }},
            {"$group": {
                "_id": None,
                "units": {
                    "$sum": {
                        "$ifNull": ["$purchases.product.quantity", 1]
                    }
                },
                "customers": {"$addToSet": "$_id"}
            }}
        ]))
        product_units      = int(prod_agg[0]["units"]) if prod_agg else 0
        customer_achieved  = len(prod_agg[0]["customers"]) if prod_agg else 0

        # ---- Pcts ----
        product_pct  = round((product_units  / product_quota  * 100) if product_quota  else 0.0, 2)
        payment_pct  = round((cash_achieved  / cash_quota     * 100) if cash_quota     else 0.0, 2)
        customer_pct = round((customer_achieved / customer_quota * 100) if customer_quota else 0.0, 2)

        # Weighted average across metrics that actually have quotas
        parts = []
        if product_quota:  parts.append(product_pct)
        if cash_quota:     parts.append(payment_pct)
        if customer_quota: parts.append(customer_pct)
        overall = round(sum(parts) / len(parts), 2) if parts else 0.0

        estimated_commission = round(cash_achieved * (commission_pct / 100.0), 2)

        rows.append({
            "target_id": str(tgt.get("_id")),
            "title": title,
            "duration": duration_type,
            "start_date": start_dt.date().isoformat(),
            "end_date": end_dt.date().isoformat(),
            "product_quota": product_quota,
            "cash_quota": cash_quota,
            "customer_quota": customer_quota,
            "product_achieved": product_units,
            "cash_achieved": round(cash_achieved, 2),
            "customer_achieved": customer_achieved,
            "product_pct": product_pct,
            "payment_pct": payment_pct,
            "customer_pct": customer_pct,
            "overall": overall,
            "commission_pct": commission_pct,
            "estimated_commission": estimated_commission
        })

    return rows


# ------------------------------------------------------------
# Routes (PUBLIC — no login required)
# ------------------------------------------------------------
@executive_agent_target_bp.route("/", methods=["GET"])
def executive_agent_targets_home():
    """
    Main page:
      - Filters: manager_id, branch, manager_name (substring), agent_id (to drill into one agent)
      - Shows manager list (for a dropdown), agents list (filtered), and optional selected agent targets.
    """
    manager_id   = request.args.get("manager_id") or None
    branch       = request.args.get("branch") or None
    manager_name = request.args.get("manager_name") or None
    agent_id     = request.args.get("agent_id") or None

    managers = _get_managers_list()
    agents   = _get_all_agents(manager_id=manager_id, branch=branch, manager_name_like=manager_name)

    selected_agent = None
    agent_targets  = []
    if agent_id:
        try:
            agent_oid = ObjectId(agent_id)
            selected_agent = users_collection.find_one(
                {"_id": agent_oid}, {"name": 1, "image_url": 1, "branch": 1}
            )
            if selected_agent:
                agent_targets = _agent_targets_with_progress(agent_oid)
        except Exception:
            flash("Invalid agent id.", "warning")

    global_commission_pct = _get_global_commission_pct()

    return render_template(
        "executive_agent_target.html",
        managers=managers,
        agents=agents,
        selected_agent=selected_agent,
        agent_id=agent_id,
        agent_targets=agent_targets,
        filters={"manager_id": manager_id, "branch": branch, "manager_name": manager_name},
        global_commission_pct=global_commission_pct
    )

@executive_agent_target_bp.route("/agents", methods=["GET"])
def executive_agents_for_manager_json():
    """
    JSON helper: /agents?manager_id=...&branch=...&manager_name=...
    Returns agents filtered; useful for dynamic dropdowns.
    """
    manager_id   = request.args.get("manager_id") or None
    branch       = request.args.get("branch") or None
    manager_name = request.args.get("manager_name") or None

    agents = _get_all_agents(manager_id=manager_id, branch=branch, manager_name_like=manager_name)
    return jsonify({"agents": agents})

# ---------------- Commission endpoints ----------------
@executive_agent_target_bp.route("/commission/global", methods=["POST"])
def executive_set_global_commission():
    """
    Set ONE commission % for all agents (GLOBAL). This value will be used
    for every agent unless an explicit per-agent override exists.
    """
    pct_str = request.form.get("product_commission_pct", "0")
    try:
        pct = float(pct_str)
        if pct < 0: pct = 0.0
        if pct > 100: pct = 100.0
    except ValueError:
        pct = 0.0

    _set_global_commission_pct(pct)
    flash("Global commission updated.", "success")
    qs = {
        "manager_id": request.args.get("manager_id") or "",
        "branch": request.args.get("branch") or "",
        "manager_name": request.args.get("manager_name") or "",
        "agent_id": request.args.get("agent_id") or ""
    }
    qs = {k: v for k, v in qs.items() if v}
    return redirect(url_for("executive_agent_target.executive_agent_targets_home", **qs))

@executive_agent_target_bp.route("/commission/agent", methods=["POST"])
def executive_set_agent_override_commission():
    """
    Optional override setter (kept for flexibility).
    If you truly want ONE commission only, you can hide this in the UI.
    """
    agent_id = request.form.get("agent_id")
    pct_str  = request.form.get("product_commission_pct", "0")
    try:
        agent_oid = ObjectId(agent_id)
    except Exception:
        flash("Invalid agent id.", "danger")
        return redirect(url_for("executive_agent_target.executive_agent_targets_home"))

    try:
        pct = float(pct_str)
        if pct < 0: pct = 0.0
        if pct > 100: pct = 100.0
    except ValueError:
        pct = 0.0

    agent_commissions_collection.update_one(
        {"agent_id": agent_oid},
        {"$set": {"product_commission_pct": pct, "updated_at": datetime.utcnow()}},
        upsert=True
    )
    flash("Agent commission override updated.", "success")
    return redirect(url_for("executive_agent_target.executive_agent_targets_home", agent_id=str(agent_oid)))

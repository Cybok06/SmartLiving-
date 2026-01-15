from flask import Blueprint, render_template, session, redirect, url_for, flash, request
from bson.objectid import ObjectId
from datetime import datetime, timedelta, timezone, date
from db import db

manager_target_bp = Blueprint('manager_target', __name__, url_prefix='/manager/targets')

# Collections
targets_collection   = db["targets"]
users_collection     = db["users"]
payments_collection  = db["payments"]
customers_collection = db["customers"]   # used for product + customer metrics

# ----------------------------
# Helpers
# ----------------------------
def _today_utc_date() -> date:
    return datetime.now(timezone.utc).date()

def _period_window(duration_type: str):
    """Fallback window if a target has no stored start/end."""
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

def _coerce_date_string(d) -> str:
    """
    Return 'YYYY-MM-DD' string (your stored date format).
    Accepts datetime.date, datetime, or string.
    """
    if isinstance(d, datetime):
        d = d.date()
    if hasattr(d, "isoformat"):
        return d.isoformat()
    return str(d)

def _target_window_strings(tgt) -> tuple[str, str]:
    """Use target's own start/end if present; else compute from duration_type."""
    duration_type = tgt.get("duration_type", "daily")
    start = tgt.get("start_date")
    end   = tgt.get("end_date")
    if start and end:
        return _coerce_date_string(start), _coerce_date_string(end)
    s, e = _period_window(duration_type)
    return _coerce_date_string(s), _coerce_date_string(e)

def _agent_ids_for_manager(manager_oid: ObjectId):
    """
    Returns (agent_obj_ids, agent_str_ids, agent_docs_map)
    - payments.agent_id is a STRING → use agent_str_ids there
    - customers.agent_id is typically ObjectId → use agent_obj_ids there (if you decide to scope by agent)
    """
    agents = list(users_collection.find(
        {"role": "agent", "manager_id": manager_oid},
        {"_id": 1, "name": 1, "image_url": 1}
    ))
    agent_obj_ids = [a["_id"] for a in agents]
    agent_str_ids = [str(a["_id"]) for a in agents]
    agent_map = {str(a["_id"]): {"_id": a["_id"], "name": a.get("name") or "Agent", "image_url": a.get("image_url") or ""} for a in agents}
    return agent_obj_ids, agent_str_ids, agent_map

# ----------------------------
# Calculations (manager-level + per-agent)
# ----------------------------
def calculate_target_results(manager_id):
    # targets.manager_id saved as string in your set_target flow
    assigned_targets = list(
        targets_collection.find({"manager_id": str(manager_id)}).sort("created_at", -1)
    )

    manager_oid = ObjectId(manager_id)
    agent_obj_ids, agent_str_ids, agent_map = _agent_ids_for_manager(manager_oid)
    results = []

    for tgt in assigned_targets:
        duration_type = tgt.get("duration_type", "daily")
        start_str, end_str = _target_window_strings(tgt)

        # --- MANAGER TOTALS -------------------------
        # CASH (payments)
        pay_sum = list(payments_collection.aggregate([
            {"$match": {
                "agent_id": {"$in": agent_str_ids},
                "date": {"$gte": start_str, "$lte": end_str},
            }},
            {"$group": {"_id": None, "total": {"$sum": "$amount"}}}
        ]))
        cash_achieved = float(pay_sum[0]["total"]) if pay_sum else 0.0

        # PRODUCTS & CUSTOMERS (from customers.purchases)
        prod_agg = list(customers_collection.aggregate([
            {"$match": {
                "manager_id": manager_oid,
                "purchases.purchase_date": {"$gte": start_str, "$lte": end_str}
            }},
            {"$unwind": "$purchases"},
            {"$match": {"purchases.purchase_date": {"$gte": start_str, "$lte": end_str}}},
            {"$group": {
                "_id": None,
                "units": {"$sum": {"$ifNull": ["$purchases.product.quantity", 1]}},
                "customers": {"$addToSet": "$_id"}
            }}
        ]))
        product_units_total = int(prod_agg[0]["units"]) if prod_agg else 0
        customers_total     = len(prod_agg[0]["customers"]) if prod_agg else 0

        # Targets
        pt  = int(tgt.get("product_target") or 0)   # units
        ct  = float(tgt.get("cash_target") or 0.0)  # GH₵
        cut = int(tgt.get("customer_target") or 0)  # distinct customers

        product_pct_mgr  = round((product_units_total / pt * 100) if pt  else 0.0, 2)
        payment_pct_mgr  = round((cash_achieved       / ct * 100) if ct  else 0.0, 2)
        customer_pct_mgr = round((customers_total     / cut * 100) if cut else 0.0, 2)
        overall_mgr      = round((product_pct_mgr + payment_pct_mgr + customer_pct_mgr) / (3 if (pt or ct or cut) else 1), 2)

        # --- PER-AGENT ALLOCATION & PROGRESS -------
        allocations = tgt.get("agent_allocations", []) or []
        # map allocations by agent_id (string)
        alloc_map = {str(a.get("agent_id")): a for a in allocations}

        agents_progress = []

        # We compute per-agent achievements in the same window
        # CASH per agent
        cash_by_agent = list(payments_collection.aggregate([
            {"$match": {
                "agent_id": {"$in": agent_str_ids},
                "date": {"$gte": start_str, "$lte": end_str},
            }},
            {"$group": {"_id": "$agent_id", "cash": {"$sum": "$amount"}}}
        ]))
        cash_by_agent_map = {row["_id"]: float(row["cash"]) for row in cash_by_agent}  # key: agent_id (string)

        # PRODUCTS & CUSTOMERS per agent (from customers.purchases)
        # Some of your customers docs may NOT store agent_id inside purchases; many store at root.
        # We'll attribute by customer.root agent_id when present (ObjectId).
        prod_by_agent = list(customers_collection.aggregate([
            {"$match": {
                "manager_id": manager_oid,
                "purchases.purchase_date": {"$gte": start_str, "$lte": end_str}
            }},
            {"$unwind": "$purchases"},
            {"$match": {"purchases.purchase_date": {"$gte": start_str, "$lte": end_str}}},
            {"$group": {
                "_id": "$agent_id",  # EXPECTED to be ObjectId at the customer root
                "units": {"$sum": {"$ifNull": ["$purchases.product.quantity", 1]}},
                "customers": {"$addToSet": "$_id"}
            }}
        ]))
        # Normalize agent key to string to match UI and allocations
        prod_by_agent_map = {}
        cust_count_by_agent_map = {}
        for row in prod_by_agent:
            key = str(row["_id"]) if row["_id"] is not None else None
            if key is None:
                # if agent_id missing on some customers, skip attribution
                continue
            prod_by_agent_map[key] = int(row.get("units", 0))
            cust_count_by_agent_map[key] = len(row.get("customers", []))

        # Build progress for each known agent under the manager (even if no allocation yet)
        for a_str_id, meta in agent_map.items():
            alloc = alloc_map.get(a_str_id, {
                "agent_id": a_str_id,
                "agent_name": meta["name"],
                "product_pct": 0.0,
                "cash_pct": 0.0,
                "customer_pct": 0.0
            })

            # quotas
            product_quota  = int(round(pt  * (float(alloc.get("product_pct", 0))  / 100.0))) if pt  else 0
            cash_quota     = float(ct * (float(alloc.get("cash_pct", 0))     / 100.0)) if ct  else 0.0
            customer_quota = int(round(cut * (float(alloc.get("customer_pct", 0)) / 100.0))) if cut else 0

            # achieved
            agent_units  = prod_by_agent_map.get(a_str_id, 0)
            agent_cash   = cash_by_agent_map.get(a_str_id, 0.0)
            agent_custom = cust_count_by_agent_map.get(a_str_id, 0)

            # pct vs quota (guard 0)
            agent_prod_pct  = round((agent_units  / product_quota  * 100) if product_quota  else 0.0, 2)
            agent_cash_pct  = round((agent_cash   / cash_quota     * 100) if cash_quota     else 0.0, 2)
            agent_cust_pct  = round((agent_custom / customer_quota * 100) if customer_quota else 0.0, 2)
            agent_overall   = round((agent_prod_pct + agent_cash_pct + agent_cust_pct) / (3 if (product_quota or cash_quota or customer_quota) else 1), 2)

            agents_progress.append({
                "agent_id": a_str_id,
                "agent_name": meta["name"],
                "image_url": meta["image_url"],
                # allocation (percentages)
                "alloc_product_pct": float(alloc.get("product_pct", 0.0)),
                "alloc_cash_pct": float(alloc.get("cash_pct", 0.0)),
                "alloc_customer_pct": float(alloc.get("customer_pct", 0.0)),
                # quotas (absolute numbers)
                "product_quota": product_quota,
                "cash_quota": round(cash_quota, 2),
                "customer_quota": customer_quota,
                # achieved
                "product_achieved": int(agent_units),
                "cash_achieved": round(float(agent_cash), 2),
                "customer_achieved": int(agent_custom),
                # progress
                "product_pct": agent_prod_pct,
                "payment_pct": agent_cash_pct,
                "customer_pct": agent_cust_pct,
                "overall": agent_overall
            })

        results.append({
            # manager-level card
            "title": tgt.get("title"),
            "duration": duration_type,
            "start_date": start_str,
            "end_date": end_str,
            "product_target": pt,
            "product_achieved": product_units_total,
            "product_pct": product_pct_mgr,
            "cash_target": ct,
            "cash_achieved": round(cash_achieved, 2),
            "payment_pct": payment_pct_mgr,
            "customer_target": cut,
            "customer_achieved": customers_total,
            "customer_pct": customer_pct_mgr,
            "overall": overall_mgr,
            # per-agent details for UI
            "target_id": str(tgt.get("_id")),
            "agents": agents_progress
        })

    return results

# ----------------------------
# Routes
# ----------------------------
@manager_target_bp.route('/')
def manager_targets():
    if 'manager_id' not in session:
        flash("Access denied. Please log in as a manager.", "danger")
        return redirect(url_for('login.login'))

    manager_id = session['manager_id']
    manager = users_collection.find_one({"_id": ObjectId(manager_id)})

    if not manager:
        flash("Manager not found.", "error")
        return redirect(url_for('login.login'))

    results = calculate_target_results(manager_id)
    return render_template("manager_target.html", manager=manager, results=results)

# ----- NEW: Distribution form (GET) -----
@manager_target_bp.route('/distribute/<target_id>', methods=['GET'])
def distribute_target_get(target_id):
    if 'manager_id' not in session:
        flash("Access denied. Please log in as a manager.", "danger")
        return redirect(url_for('login.login'))

    manager_id = session['manager_id']
    manager_oid = ObjectId(manager_id)

    tgt = targets_collection.find_one({"_id": ObjectId(target_id), "manager_id": str(manager_id)})
    if not tgt:
        flash("Target not found for this manager.", "warning")
        return redirect(url_for('manager_target.manager_targets'))

    # Agents under this manager
    _, _, agent_map = _agent_ids_for_manager(manager_oid)
    # Existing allocations if any
    allocs = tgt.get("agent_allocations", []) or []
    alloc_by_id = {str(a.get("agent_id")): a for a in allocs}

    # Build view model for the form
    form_agents = []
    for a_id, meta in agent_map.items():
        existing = alloc_by_id.get(a_id, {})
        form_agents.append({
            "agent_id": a_id,
            "agent_name": meta["name"],
            "image_url": meta["image_url"],
            "product_pct": existing.get("product_pct", 0.0),
            "cash_pct": existing.get("cash_pct", 0.0),
            "customer_pct": existing.get("customer_pct", 0.0),
        })

    start_str, end_str = _target_window_strings(tgt)

    return render_template(
        "manager_target_distribute.html",
        target_id=target_id,
        title=tgt.get("title", "Target"),
        duration=tgt.get("duration_type", ""),
        start_date=start_str,
        end_date=end_str,
        product_target=int(tgt.get("product_target") or 0),
        cash_target=float(tgt.get("cash_target") or 0.0),
        customer_target=int(tgt.get("customer_target") or 0),
        agents=form_agents
    )

# ----- NEW: Distribution save (POST) -----
@manager_target_bp.route('/distribute/<target_id>', methods=['POST'])
def distribute_target_post(target_id):
    if 'manager_id' not in session:
        flash("Access denied. Please log in as a manager.", "danger")
        return redirect(url_for('login.login'))

    manager_id = session['manager_id']

    tgt = targets_collection.find_one({"_id": ObjectId(target_id), "manager_id": str(manager_id)})
    if not tgt:
        flash("Target not found for this manager.", "warning")
        return redirect(url_for('manager_target.manager_targets'))

    # Pull arrays from the form (names: agent_id[], product_pct[], cash_pct[], customer_pct[])
    agent_ids       = request.form.getlist("agent_id[]")
    product_pcts    = request.form.getlist("product_pct[]")
    cash_pcts       = request.form.getlist("cash_pct[]")
    customer_pcts   = request.form.getlist("customer_pct[]")

    if not agent_ids:
        flash("Select at least one agent to allocate.", "warning")
        return redirect(url_for('manager_target.distribute_target_get', target_id=target_id))

    # Normalize/validate
    allocs = []
    total_product_pct  = 0.0
    total_cash_pct     = 0.0
    total_customer_pct = 0.0

    for i, a_id in enumerate(agent_ids):
        try:
            p_prod = float(product_pcts[i]) if i < len(product_pcts) else 0.0
            p_cash = float(cash_pcts[i]) if i < len(cash_pcts) else 0.0
            p_cust = float(customer_pcts[i]) if i < len(customer_pcts) else 0.0
        except Exception:
            p_prod, p_cash, p_cust = 0.0, 0.0, 0.0

        # clamp to [0,100]
        p_prod = max(0.0, min(100.0, p_prod))
        p_cash = max(0.0, min(100.0, p_cash))
        p_cust = max(0.0, min(100.0, p_cust))

        # keep names for UI
        agent_doc = users_collection.find_one({"_id": ObjectId(a_id)}, {"name": 1})
        agent_name = agent_doc.get("name") if agent_doc else "Agent"

        allocs.append({
            "agent_id": a_id,              # string form to match payments
            "agent_name": agent_name,
            "product_pct": p_prod,
            "cash_pct": p_cash,
            "customer_pct": p_cust
        })
        total_product_pct  += p_prod
        total_cash_pct     += p_cash
        total_customer_pct += p_cust

    # Validate totals (allow small float error)
    def _near_100(x): return abs(x - 100.0) <= 0.5 or x == 0.0  # allow 0 if you don't want to allocate that metric yet
    if not (_near_100(total_product_pct) and _near_100(total_cash_pct) and _near_100(total_customer_pct)):
        flash("Allocation totals must be 100% (or 0% if not allocating a metric yet).", "warning")
        return redirect(url_for('manager_target.distribute_target_get', target_id=target_id))

    # Save back into target
    targets_collection.update_one(
        {"_id": ObjectId(target_id)},
        {"$set": {
            "agent_allocations": allocs,
            "alloc_updated_at": datetime.utcnow()
        }}
    )

    flash("Agent allocations saved.", "success")
    return redirect(url_for('manager_target.manager_targets'))

from flask import Blueprint, render_template, session, redirect, url_for, flash
from bson.objectid import ObjectId
from datetime import datetime, timedelta
from db import db

manager_dashboard_bp = Blueprint('manager_dashboard', __name__, url_prefix='/manager')

# MongoDB collections
targets_collection   = db["targets"]
users_collection     = db["users"]
customers_collection = db["customers"]
payments_collection  = db["payments"]

# ----------------------------
# Monthly Date Range (as strings to match your stored dates)
# ----------------------------
def get_monthly_range():
    today = datetime.utcnow().date()
    start = today.replace(day=1)
    end = (start.replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)
    return start.isoformat(), end.isoformat()

# ----------------------------
# Helpers
# ----------------------------
def _agent_string_ids_under_manager(manager_id: ObjectId) -> list[str]:
    """payments.agent_id is a STRING in your DB; cast users._id -> str for matching."""
    agents = users_collection.find({"role": "agent", "manager_id": ObjectId(manager_id)}, {"_id": 1})
    return [str(a["_id"]) for a in agents]

# ----------------------------
# Calculate monthly target progress (CURRENT month only)
# ----------------------------
def calculate_monthly_targets(manager_id):
    # only monthly targets
    monthly_targets = list(
        targets_collection.find({"manager_id": str(manager_id), "duration_type": "monthly"})
                          .sort("created_at", -1)
    )

    if not monthly_targets:
        return []

    start_str, end_str = get_monthly_range()
    agent_id_list = _agent_string_ids_under_manager(ObjectId(manager_id))
    results = []

    # Pre-compute CASH for the whole month once (same window for all monthly targets)
    pay_sum = list(payments_collection.aggregate([
        {"$match": {
            "agent_id": {"$in": agent_id_list},
            "payment_type": {"$ne": "WITHDRAWAL"},
            "date": {"$gte": start_str, "$lte": end_str}
        }},
        {"$group": {"_id": None, "total": {"$sum": "$amount"}}}
    ]))
    month_cash = float(pay_sum[0]["total"]) if pay_sum else 0.0

    # Pre-compute PRODUCT UNITS + DISTINCT CUSTOMERS for the month (from customers.purchases)
    prod_agg = list(customers_collection.aggregate([
        {"$match": {
            "manager_id": ObjectId(manager_id),
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
    month_units = int(prod_agg[0]["units"]) if prod_agg else 0
    month_customers = len(prod_agg[0]["customers"]) if prod_agg else 0

    for target in monthly_targets:
        # Targets (assumptions: product = units, cash = GH₵, customer = distinct purchasers)
        pt  = int(target.get("product_target") or 0)
        ct  = float(target.get("cash_target") or 0)
        cut = int(target.get("customer_target") or 0)

        product_pct  = round((month_units     / pt * 100) if pt  else 0.0, 2)
        payment_pct  = round((month_cash      / ct * 100) if ct  else 0.0, 2)
        customer_pct = round((month_customers / cut * 100) if cut else 0.0, 2)
        overall      = round((product_pct + payment_pct + customer_pct) / (3 if (pt or ct or cut) else 1), 2)

        results.append({
            "title": target.get("title"),
            "duration": "monthly",
            # show the current month window on the card
            "start_date": start_str,
            "end_date": end_str,

            "product_target": pt,
            "product_achieved": month_units,
            "product_pct": product_pct,

            "cash_target": ct,
            "cash_achieved": round(month_cash, 2),
            "payment_pct": payment_pct,

            "customer_target": cut,
            "customer_achieved": month_customers,
            "customer_pct": customer_pct,

            "overall": overall
        })

    return results

# ----------------------------
# Manager Dashboard Route
# ----------------------------
@manager_dashboard_bp.route('/dashboard')
def manager_dashboard_view():
    if 'manager_id' not in session:
        flash("Access denied. Please log in as a manager.", "danger")
        return redirect(url_for('login.login'))

    manager_id = session['manager_id']
    manager = users_collection.find_one({"_id": ObjectId(manager_id)})

    if not manager:
        flash("Manager not found.", "error")
        return redirect(url_for('login.login'))

    # Monthly target progress (current month)
    results = calculate_monthly_targets(manager_id)

    # --- Today stats + attendance ---
    today = datetime.utcnow().date()
    today_str = today.isoformat()

    total_today_payment = 0.0
    attendance_data = []
    attended_customers_set = set()

    agents = users_collection.find({"manager_id": ObjectId(manager_id), "role": "agent"}, {"_id": 1, "name": 1, "image_url": 1})

    for agent in agents:
        agent_id = str(agent["_id"])
        # count all non-WITHDRAWAL payments for attendance and sum amounts
        agent_payments = payments_collection.find({
            "agent_id": agent_id,
            "date": today_str,
            "payment_type": {"$ne": "WITHDRAWAL"}  # SUSU, PAYMENT, PRODUCT, etc.
        })

        agent_payment_count = 0
        for p in agent_payments:
            total_today_payment += float(p.get("amount", 0))
            agent_payment_count += 1
            cid = p.get("customer_id")
            if cid:
                attended_customers_set.add(str(cid))

        attendance_data.append({
            "name": agent.get("name", "Agent"),
            "image_url": agent.get("image_url", "https://via.placeholder.com/80"),
            "payment_count": agent_payment_count,
            "status": "Worked" if agent_payment_count >= 1 else "Absent"
        })

    attended_customers_count = len(attended_customers_set)
    total_customer_count = customers_collection.count_documents({"manager_id": ObjectId(manager_id)})

    return render_template(
        "manager_dashboard.html",
        manager_name=manager.get("name", "Manager"),
        results=results,
        today_total_payment=round(total_today_payment, 2),
        attended_customers_count=attended_customers_count,
        total_customer_count=total_customer_count,
        attendance_data=attendance_data,
        today=today_str
    )

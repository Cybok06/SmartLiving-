# api_smartliving.py
from flask import Blueprint, request, jsonify
from bson import ObjectId
from datetime import datetime
import os

from db import db

api_bp = Blueprint("api", __name__, url_prefix="/api")

# --- collections ---
customers_col   = db["customers"]
users_col       = db["users"]
payments_col    = db["payments"]
sales_close_col = db["sales_close"]

# --- simple API key protection (for ChatGPT Actions / backend calls) ---
API_KEY = os.environ.get("SMARTLIVING_API_KEY", "sliving_4yH7G9pQz@2025")


def _require_api_key():
    """
    Basic header check.
    Send:  X-API-Key: <SMARTLIVING_API_KEY>
    """
    sent = request.headers.get("X-API-Key")
    if not sent or sent != API_KEY:
        return jsonify({"error": "Unauthorized"}), 401
    return None


def _safe_object_id(raw):
    try:
        return ObjectId(raw)
    except Exception:
        return None


# ----------------------------------------------------------------------
# 0. Health check (for testing from ChatGPT)
# ----------------------------------------------------------------------
@api_bp.route("/health", methods=["GET"])
def api_health():
    return jsonify({
        "ok": True,
        "service": "SmartLiving API",
        "time_utc": datetime.utcnow().isoformat() + "Z"
    })
    

# ----------------------------------------------------------------------
# 1. CUSTOMER ENDPOINTS
#    - By ID
#    - By phone
#    - Search
# ----------------------------------------------------------------------
def _compute_customer_financials(cust_doc):
    """
    Given a customer document, compute total_debt, total_paid, etc. using
    'purchases' & 'payments' collections (similar logic to your executive view).
    """
    cust_id = cust_doc["_id"]

    purchases = cust_doc.get("purchases", [])
    # total_debt = sum of total for all products
    total_debt = 0
    for p in purchases:
        prod = p.get("product", {})
        try:
            total_debt += int(prod.get("total", 0))
        except Exception:
            pass

    # payments: deposits vs withdrawals
    all_payments = list(payments_col.find({
        "customer_id": cust_id
    }))

    deposits = sum(
        float(p.get("amount", 0.0))
        for p in all_payments
        if p.get("payment_type") != "WITHDRAWAL"
    )
    withdrawn_amount = sum(
        float(p.get("amount", 0.0))
        for p in all_payments
        if p.get("payment_type") == "WITHDRAWAL"
    )

    total_paid = round(deposits - withdrawn_amount, 2)
    amount_left = round(total_debt - total_paid, 2)

    # last payment info
    last_payment = None
    if all_payments:
        all_payments_sorted = sorted(
            all_payments,
            key=lambda x: x.get("created_at", datetime.min),
            reverse=True
        )
        last_payment = all_payments_sorted[0]

    last_payment_info = None
    if last_payment:
        last_payment_info = {
            "amount": float(last_payment.get("amount", 0.0)),
            "date": last_payment.get("date"),
            "time": last_payment.get("time"),
            "method": last_payment.get("method"),
            "payment_type": last_payment.get("payment_type"),
        }

    penalties = cust_doc.get("penalties", [])
    total_penalty = round(sum(float(p.get("amount", 0.0)) for p in penalties), 2)

    return {
        "total_debt": float(total_debt),
        "deposits": float(deposits),
        "withdrawn_amount": float(withdrawn_amount),
        "total_paid": float(total_paid),
        "amount_left": float(amount_left),
        "total_penalty": float(total_penalty),
        "last_payment": last_payment_info,
    }


@api_bp.route("/customers/<customer_id>", methods=["GET"])
def api_get_customer_by_id():
    """
    Get full customer snapshot (basic info + financial summary + purchases).
    """
    guard = _require_api_key()
    if guard:
        return guard

    customer_id = request.view_args.get("customer_id")
    oid = _safe_object_id(customer_id)
    if not oid:
        return jsonify({"error": "Invalid customer_id"}), 400

    customer = customers_col.find_one({"_id": oid})
    if not customer:
        return jsonify({"error": "Customer not found"}), 404

    fin = _compute_customer_financials(customer)

    # basic fields
    base = {
        "customer_id": str(customer["_id"]),
        "name": customer.get("name"),
        "phone_number": customer.get("phone_number"),
        "location": customer.get("location"),
        "occupation": customer.get("occupation"),
        "comment": customer.get("comment"),
        "image_url": customer.get("image_url"),
        "agent_id": customer.get("agent_id"),
        "manager_id": str(customer.get("manager_id")) if customer.get("manager_id") else None,
        "date_registered": customer.get("date_registered").isoformat()
            if isinstance(customer.get("date_registered"), datetime)
            else customer.get("date_registered"),
        "coordinates": customer.get("coordinates"),
    }

    # purchases overview
    purchases_out = []
    for p in customer.get("purchases", []):
        prod = p.get("product", {})
        purchases_out.append({
            "product_name": prod.get("name"),
            "total": float(prod.get("total", 0) or 0),
            "purchase_date": p.get("purchase_date"),
            "end_date": p.get("end_date"),
        })

    penalties_out = [
        {"amount": float(x.get("amount", 0.0)), "reason": x.get("reason")}
        for x in customer.get("penalties", [])
    ]

    return jsonify({
        "ok": True,
        "customer": base,
        "financials": fin,
        "purchases": purchases_out,
        "penalties": penalties_out,
    })


@api_bp.route("/customers/by-phone", methods=["GET"])
def api_get_customer_by_phone():
    """
    /api/customers/by-phone?phone=0598342192
    Returns first match + financial summary.
    """
    guard = _require_api_key()
    if guard:
        return guard

    phone = request.args.get("phone", "").strip()
    if not phone:
        return jsonify({"error": "phone query parameter is required"}), 400

    customer = customers_col.find_one({"phone_number": phone})
    if not customer:
        return jsonify({"error": "Customer not found"}), 404

    fin = _compute_customer_financials(customer)

    return jsonify({
        "ok": True,
        "customer_id": str(customer["_id"]),
        "name": customer.get("name"),
        "phone_number": customer.get("phone_number"),
        "location": customer.get("location"),
        "financials": fin,
    })


@api_bp.route("/customers/search", methods=["GET"])
def api_search_customers():
    """
    /api/customers/search?q=ama
    Simple search by name or phone (case.insensitive).
    """
    guard = _require_api_key()
    if guard:
        return guard

    q = request.args.get("q", "").strip()
    if not q:
        return jsonify({"error": "q query parameter is required"}), 400

    regex = {"$regex": q, "$options": "i"}
    cursor = customers_col.find(
        {"$or": [{"name": regex}, {"phone_number": regex}]},
        {"name": 1, "phone_number": 1, "location": 1, "agent_id": 1, "manager_id": 1}
    ).limit(50)

    results = []
    for c in cursor:
        results.append({
            "customer_id": str(c["_id"]),
            "name": c.get("name"),
            "phone_number": c.get("phone_number"),
            "location": c.get("location"),
            "agent_id": c.get("agent_id"),
            "manager_id": str(c.get("manager_id")) if c.get("manager_id") else None,
        })

    return jsonify({"ok": True, "results": results})
    

# ----------------------------------------------------------------------
# 2. PAYMENTS ENDPOINTS
#    - Payments by customer
#    - Agent daily summary
# ----------------------------------------------------------------------
@api_bp.route("/customers/<customer_id>/payments", methods=["GET"])
def api_get_payments_for_customer(customer_id):
    """
    /api/customers/<id>/payments?start=YYYY-MM-DD&end=YYYY-MM-DD
    Returns list of all payments for a customer, optional date-range.
    """
    guard = _require_api_key()
    if guard:
        return guard

    oid = _safe_object_id(customer_id)
    if not oid:
        return jsonify({"error": "Invalid customer_id"}), 400

    start = request.args.get("start")
    end   = request.args.get("end")

    q = {"customer_id": oid}
    if start and end:
        q["date"] = {"$gte": start, "$lte": end}
    elif start:
        q["date"] = {"$gte": start}
    elif end:
        q["date"] = {"$lte": end}

    cursor = payments_col.find(q).sort("created_at", 1)

    payments_out = []
    total_paid = 0.0
    for p in cursor:
        amt = float(p.get("amount", 0.0))
        if p.get("payment_type") != "WITHDRAWAL":
            total_paid += amt

        payments_out.append({
            "amount": amt,
            "date": p.get("date"),
            "time": p.get("time"),
            "method": p.get("method"),
            "payment_type": p.get("payment_type"),
            "product_name": p.get("product_name"),
            "product_index": p.get("product_index"),
        })

    return jsonify({
        "ok": True,
        "customer_id": customer_id,
        "payments": payments_out,
        "total_paid_excluding_withdrawals": total_paid,
    })


@api_bp.route("/agents/<agent_id>/summary", methods=["GET"])
def api_agent_summary(agent_id):
    """
    1. Validate agent
    2. Sum all payments (excluding withdrawals)
    3. Give today's totals + lifetime totals + customer count
    """
    guard = _require_api_key()
    if guard:
        return guard

    # find agent doc (ObjectId or string)
    try:
        agent_doc = users_col.find_one({"_id": ObjectId(agent_id), "role": "agent"})
    except Exception:
        agent_doc = users_col.find_one({"_id": agent_id, "role": "agent"})

    if not agent_doc:
        return jsonify({"error": "Agent not found"}), 404

    agent_id_str = str(agent_doc["_id"])
    today_str = datetime.utcnow().strftime("%Y-%m-%d")

    # customers under this agent
    customers = list(customers_col.find({"agent_id": agent_id_str}, {"_id": 1}))
    customer_ids = [c["_id"] for c in customers]

    # lifetime payments
    lifetime_q = {
        "agent_id": agent_id_str,
        "payment_type": {"$ne": "WITHDRAWAL"}
    }
    lifetime_cursor = payments_col.find(lifetime_q)
    lifetime_total = 0.0
    lifetime_count = 0
    for p in lifetime_cursor:
        lifetime_total += float(p.get("amount", 0.0))
        lifetime_count += 1

    # today payments
    today_q = {
        "agent_id": agent_id_str,
        "date": today_str,
        "payment_type": {"$ne": "WITHDRAWAL"}
    }
    today_cursor = payments_col.find(today_q)
    today_total = 0.0
    today_count = 0
    for p in today_cursor:
        today_total += float(p.get("amount", 0.0))
        today_count += 1

    return jsonify({
        "ok": True,
        "agent": {
            "agent_id": agent_id_str,
            "name": agent_doc.get("name") or agent_doc.get("username"),
            "phone": agent_doc.get("phone"),
            "branch": agent_doc.get("branch"),
            "manager_id": str(agent_doc.get("manager_id")) if agent_doc.get("manager_id") else None,
        },
        "customers_count": len(customer_ids),
        "payments": {
            "lifetime_total": lifetime_total,
            "lifetime_count": lifetime_count,
            "today_total": today_total,
            "today_count": today_count,
            "today_date": today_str,
        }
    })


# ----------------------------------------------------------------------
# 3. MANAGER / SALES CLOSE DASHBOARD SUMMARY
#    This is a simplified JSON version of your manager dashboard.
# ----------------------------------------------------------------------
def _agents_under_manager(manager_id_str):
    """ Agents stored with manager_id as ObjectId or string. """
    try:
        m_oid = ObjectId(manager_id_str)
    except Exception:
        m_oid = manager_id_str

    agents = list(
        users_col.find(
            {"role": "agent", "manager_id": m_oid},
            {"_id": 1, "name": 1, "phone": 1}
        )
    )
    agent_ids = [str(a["_id"]) for a in agents]
    return agents, agent_ids


def _sales_close_sum_for_agents(agent_ids):
    """ Sum total_amount across all dates for given agent_ids (unclosed) """
    if not agent_ids:
        return 0.0

    pipeline = [
        {"$match": {"agent_id": {"$in": agent_ids}}},
        {
            "$group": {
                "_id": None,
                "sum_amount": {
                    "$sum": {"$toDouble": {"$ifNull": ["$total_amount", 0]}}
                }
            }
        }
    ]
    agg = list(sales_close_col.aggregate(pipeline))
    if not agg:
        return 0.0
    try:
        return float(agg[0].get("sum_amount", 0.0))
    except Exception:
        return 0.0


@api_bp.route("/managers/<manager_id>/summary", methods=["GET"])
def api_manager_summary(manager_id):
    """
    Simple manager-level summary based mainly on:
      - agents under this manager
      - unclosed total across agents (from sales_close.total_amount)
      - manager's own 'available' in sales_close where agent_id == manager_id
      - lifetime & today's total collections from payments (via agents)
    """
    guard = _require_api_key()
    if guard:
        return guard

    # manager doc
    try:
        mgr_doc = users_col.find_one({"_id": ObjectId(manager_id)})
    except Exception:
        mgr_doc = users_col.find_one({"_id": manager_id})

    if not mgr_doc:
        return jsonify({"error": "Manager not found"}), 404

    if (mgr_doc.get("role") or "").lower() not in ("manager", "admin", "executive"):
        return jsonify({"error": "User is not a manager/admin/executive"}), 400

    manager_id_str = str(mgr_doc["_id"])
    today_str = datetime.utcnow().strftime("%Y-%m-%d")

    # agents + their ids
    agents, agent_ids = _agents_under_manager(manager_id_str)

    # lifetime collections from payments for these agents
    pay_q = {
        "agent_id": {"$in": agent_ids},
        "payment_type": {"$ne": "WITHDRAWAL"}
    }
    lifetime_total = 0.0
    lifetime_count = 0
    for p in payments_col.find(pay_q):
        lifetime_total += float(p.get("amount", 0.0))
        lifetime_count += 1

    # today collections
    today_q = {
        "agent_id": {"$in": agent_ids},
        "date": today_str,
        "payment_type": {"$ne": "WITHDRAWAL"}
    }
    today_total = 0.0
    today_count = 0
    for p in payments_col.find(today_q):
        today_total += float(p.get("amount", 0.0))
        today_count += 1

    # unclose total across all agents (sales_close.total_amount)
    unclose_total_agents = _sales_close_sum_for_agents(agent_ids)

    # manager's own available (sales_close docs where agent_id == manager_id_str)
    close_available_manager = _sales_close_sum_for_agents([manager_id_str])

    # build agent list with unclosed per agent
    agent_cards = []
    for a in agents:
        a_id_str = str(a["_id"])
        bal = _sales_close_sum_for_agents([a_id_str])
        agent_cards.append({
            "agent_id": a_id_str,
            "name": a.get("name"),
            "phone": a.get("phone"),
            "unclosed_balance": bal,
        })

    # sort biggest first
    agent_cards.sort(key=lambda x: x["unclosed_balance"], reverse=True)

    return jsonify({
        "ok": True,
        "manager": {
            "manager_id": manager_id_str,
            "name": mgr_doc.get("name") or mgr_doc.get("username"),
            "phone": mgr_doc.get("phone"),
            "role": mgr_doc.get("role"),
            "branch": mgr_doc.get("branch"),
        },
        "agents_count": len(agent_ids),
        "collections": {
            "lifetime_total": lifetime_total,
            "lifetime_count": lifetime_count,
            "today_total": today_total,
            "today_count": today_count,
            "today_date": today_str,
        },
        "balances": {
            "unclose_total_agents": unclose_total_agents,
            "manager_close_available": close_available_manager,
        },
        "agents": agent_cards,
    })

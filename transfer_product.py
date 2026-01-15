# routes/transfer_product.py
from flask import Blueprint, render_template, request, jsonify, abort, session
from bson import ObjectId
from datetime import datetime
from db import db

transfer_product_bp = Blueprint("transfer_product", __name__, template_folder="templates")

customers_col        = db["customers"]
payments_col         = db["payments"]
users_col            = db["users"]
stopped_products_col = db["stopped_products"]    # archive stopped purchases
transfers_col        = db["product_transfers"]   # audit log

# ---------- helpers: ids / roles / scope (session-only) ----------

def _oid(v):
    try:
        return ObjectId(str(v))
    except Exception:
        return None

def _digits_only(s: str) -> str:
    return "".join(ch for ch in (s or "") if ch.isdigit())

def _session_actor():
    """
    Resolve the current actor purely from Flask session (no Flask-Login).
    Priority order: agent, manager, inventory, admin, executive.
    Returns (actor_doc or None, role_lc or "").
    """
    key_role_map = [
        ("agent_id", "agent"),
        ("manager_id", "manager"),
        ("inventory_id", "inventory"),
        ("admin_id", "admin"),
        ("executive_id", "executive"),
    ]
    for key, role in key_role_map:
        uid = session.get(key)
        if uid:
            oid = _oid(uid)
            if not oid:
                break
            doc = users_col.find_one({"_id": oid})
            if doc:
                return doc, role
    return None, ""

def _scoped_filter(base=None, branch=None):
    """
    Merge caller's filter with role/branch scope using $and (never overwrite).
    Tolerate agent_id/manager_id stored as string or ObjectId.
    """
    base = dict(base or {})
    actor, role = _session_actor()

    scope_clauses = []
    if role == "agent":
        aid_str = str(actor["_id"])
        scope_clauses.append({"$or": [{"agent_id": aid_str}, {"agent_id": actor["_id"]}]})
    elif role == "manager":
        mid = actor["_id"]
        scope_clauses.append({"$or": [{"manager_id": mid}, {"manager_id": str(mid)}]})
    else:
        if branch:
            scope_clauses.append({"branch": branch})

    if scope_clauses:
        base = {"$and": [base] + scope_clauses}

    return base, actor, role

def _ensure_customer_in_scope(customer_id: str):
    """Return customer if inside scope built from session; else 403/404."""
    cid = _oid(customer_id)
    if not cid:
        abort(400, description="Invalid customer id.")
    filt, _actor, _role = _scoped_filter({"_id": cid})
    customer = customers_col.find_one(filt)
    if not customer:
        abort(403, description="Unauthorized or customer not found.")
    return customer

def _sum_paid_for_product(customer_oid: ObjectId, product_index: int) -> float:
    """
    Sum ALL payments for this customer's selected product, regardless of payment_type.
    Also tolerate product_index stored as int or string in legacy rows.
    """
    idx = int(product_index)
    query = {
        "customer_id": customer_oid,
        "$or": [
            {"product_index": idx},
            {"product_index": str(idx)}
        ]
    }
    total = 0.0
    for p in payments_col.find(query, {"amount": 1}):
        try:
            total += float(p.get("amount", 0) or 0)
        except Exception:
            pass
    return round(total, 2)

# ---------- routes (no @login_required anywhere) ----------

@transfer_product_bp.route("/transfer_product", methods=["GET"])
def transfer_product_page():
    # Basic page shell; data loaded via AJAX search
    return render_template("transfer_product.html")

@transfer_product_bp.route("/transfer_product/search", methods=["GET"])
def transfer_product_search():
    """
    Inventory-wide search (session-scoped if session exists).
    Works even without session so the Inventory page is always usable.
    Supports ?q= and ?limit=&page=&branch=
    """
    raw_q = (request.args.get("q") or "").strip()
    if not raw_q:
        return jsonify(ok=True, results=[])

    # normalize phone
    q_digits = _digits_only(raw_q)
    phone_regex = q_digits if q_digits else raw_q

    try:
        limit = max(1, min(int(request.args.get("limit", 8)), 25))
    except Exception:
        limit = 8
    try:
        page = max(1, int(request.args.get("page", 1)))
    except Exception:
        page = 1
    skip = (page - 1) * limit

    branch = (request.args.get("branch") or "").strip() or None

    # Role-aware / anonymous filter
    base_filter = {
        "$or": [
            {"name": {"$regex": raw_q, "$options": "i"}},
            {"phone_number": {"$regex": phone_regex}},
        ]
    }
    filt, _actor, _role = _scoped_filter(base_filter, branch=branch)

    projection = {"name": 1, "phone_number": 1, "image_url": 1, "purchases": 1}
    cursor = customers_col.find(filt, projection).skip(skip).limit(limit)

    results = []
    for c in cursor:
        cid = c["_id"]
        purchases = c.get("purchases", []) or []
        enriched = []
        for idx, pur in enumerate(purchases):
            prod = (pur or {}).get("product", {}) or {}
            pname = prod.get("name", "Unnamed Product")
            ptotal = float(prod.get("total", 0) or 0)
            paid = _sum_paid_for_product(cid, idx)
            enriched.append({
                "index": idx,
                "name": pname,
                "total": round(ptotal, 2),
                "paid": paid,
                "outstanding": round(max(ptotal - paid, 0.0), 2),
                "status": prod.get("status", ""),
                "purchase_type": (pur or {}).get("purchase_type", ""),
                "purchase_date": (pur or {}).get("purchase_date", ""),
            })

        results.append({
            "customer_id": str(cid),
            "name": c.get("name", "Unknown"),
            "phone_number": c.get("phone_number", ""),
            "image_url": c.get("image_url", ""),
            "purchases": enriched
        })

    return jsonify(ok=True, results=results, page=page, limit=limit)

@transfer_product_bp.route("/transfer_product/execute", methods=["POST"])
def transfer_product_execute():
    """
    Transfer 2/3 of total paid on FROM product to TO product for the same customer,
    archive the FROM purchase to stopped_products, audit the action,
    and DELETE all FROM-product payments. (No sales_close updates.)
    """
    actor, actor_role = _session_actor()
    if not actor:
        return jsonify(ok=False, message="Unauthorized: please sign in to perform transfers."), 401

    data = request.get_json(silent=True) or {}
    customer_id = data.get("customer_id")
    from_index  = data.get("from_index")
    to_index    = data.get("to_index")
    note        = (data.get("note") or "").strip()

    if customer_id is None or from_index is None or to_index is None:
        return jsonify(ok=False, message="Missing required fields."), 400
    try:
        from_index = int(from_index)
        to_index = int(to_index)
    except Exception:
        return jsonify(ok=False, message="Invalid product selection."), 400
    if from_index == to_index:
        return jsonify(ok=False, message="FROM and TO products must be different."), 400

    # Validate + scoped access (session-scoped)
    customer = _ensure_customer_in_scope(customer_id)
    purchases = customer.get("purchases", []) or []
    if from_index < 0 or from_index >= len(purchases) or to_index < 0 or to_index >= len(purchases):
        return jsonify(ok=False, message="Selected product not found for this customer."), 404

    from_purchase = purchases[from_index] or {}
    to_purchase   = purchases[to_index] or {}
    from_prod     = (from_purchase.get("product") or {})
    to_prod       = (to_purchase.get("product") or {})

    cust_oid   = customer["_id"]
    actor_id   = str(actor["_id"])
    manager_id = customer.get("manager_id") or actor.get("manager_id") or ""

    # Compute totals + transfer amount (2/3 of total paid on FROM)
    total_paid_from = _sum_paid_for_product(cust_oid, from_index)
    transfer_amount = round((2.0 / 3.0) * total_paid_from, 2)

    now_utc = datetime.utcnow()
    today_str = now_utc.strftime("%Y-%m-%d")
    time_str  = now_utc.strftime("%H:%M:%S")

    # 1) Apply transfer to the LATEST payment on TO (or create if none)
    #    Ignore payment_type, match product_index as int or string, and sort by created_at/date/time
    latest_to = payments_col.find_one(
        {
            "customer_id": cust_oid,
            "$or": [{"product_index": to_index}, {"product_index": str(to_index)}]
        },
        sort=[("created_at", -1), ("date", -1), ("time", -1)]
    )

    created_new_payment = False
    if latest_to:
        payments_col.update_one(
            {"_id": latest_to["_id"]},
            {
                "$inc": {"amount": transfer_amount},
                "$set": {"updated_at": now_utc},
                "$push": {"transfer_notes": {
                    "from_product_index": from_index,
                    "from_product_name": from_prod.get("name", "Unknown"),
                    "amount_added": transfer_amount,
                    "at": now_utc,
                    "by_user": actor_id,
                    "by_role": actor_role,
                    "note": note
                }}
            }
        )
    else:
        # Create a fresh payment row on TO if none exists
        to_total = float(to_prod.get("total", 0) or 0)
        payments_col.insert_one({
            "customer_id": cust_oid,
            "agent_id": customer.get("agent_id", actor_id),  # prefer customer's agent_id; fallback to actor
            "manager_id": manager_id,
            "method": "TRANSFER",
            "amount": transfer_amount,
            "date": today_str,
            "time": time_str,
            "payment_type": "PRODUCT",  # keep for new rows (legacy reads are tolerant)
            "product_index": to_index,
            "product_name": to_prod.get("name", "Unnamed Product"),
            "product_total": to_total,
            "created_at": now_utc,
            "meta": {"reason": "transfer_from_other_product", "from_index": from_index, "note": note}
        })
        created_new_payment = True

    # 2) DELETE all payments for the FROM product (ignore payment_type; match int or string)
    from_match = {
        "customer_id": cust_oid,
        "$or": [{"product_index": from_index}, {"product_index": str(from_index)}]
    }
    from_payments = list(payments_col.find(
        from_match,
        {"_id": 1, "amount": 1, "date": 1, "agent_id": 1}
    ))
    deleted_count = len(from_payments)
    deleted_total = round(sum(float(p.get("amount", 0) or 0) for p in from_payments), 2)

    if deleted_count:
        payments_col.delete_many(from_match)

    # 3) Move FROM purchase out of customer's purchases[] -> stopped_products
    stopped_doc = {
        "customer_id": cust_oid,
        "customer_name": customer.get("name", ""),
        "customer_phone": customer.get("phone_number", ""),
        "agent_id": customer.get("agent_id", ""),
        "manager_id": manager_id,
        "stopped_at": now_utc,
        "stopped_reason": "transfer_to_other_product",
        "from_product_index": from_index,
        "from_product": from_prod,
        "original_purchase": from_purchase,
        "transfer": {
            "to_product_index": to_index,
            "to_product": to_prod,
            "total_paid_from_before": total_paid_from,
            "transfer_amount": transfer_amount,
            "note": note
        },
        "deleted_from_payments": {
            "count": int(deleted_count),
            "total_amount": float(deleted_total)
        },
        "by_user": actor_id,
        "by_role": actor_role,
    }
    stopped_products_col.insert_one(stopped_doc)

    # rebuild purchases array without the stopped item
    new_purchases = [p for i, p in enumerate(purchases) if i != from_index]
    customers_col.update_one({"_id": cust_oid}, {"$set": {"purchases": new_purchases, "updated_at": now_utc}})

    # 4) Audit
    transfers_col.insert_one({
        "customer_id": cust_oid,
        "agent_id": customer.get("agent_id", actor_id),
        "manager_id": manager_id,
        "at": now_utc,
        "action": "transfer_product_balance",
        "by_user": actor_id,
        "by_role": actor_role,
        "payload": {
            "from_index": from_index,
            "from_product_name": from_prod.get("name"),
            "to_index": to_index,
            "to_product_name": to_prod.get("name"),
            "total_paid_from": total_paid_from,
            "transfer_amount": transfer_amount,
            "created_new_payment": created_new_payment,
            "deleted_from_payments": {
                "count": int(deleted_count),
                "total_amount": float(deleted_total)
            },
            "note": note
        }
    })

    return jsonify(
        ok=True,
        message="Transfer completed. Stopped product payments deleted.",
        data={
            "transfer_amount": transfer_amount,
            "from_product_name": from_prod.get("name"),
            "to_product_name": to_prod.get("name"),
            "created_new_payment": created_new_payment,
            "deleted_payments_count": int(deleted_count),
            "deleted_payments_total": float(deleted_total)
        }
    )

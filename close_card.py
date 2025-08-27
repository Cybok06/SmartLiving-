# routes/close_card.py
from flask import Blueprint, render_template, request, jsonify, abort, session
from bson import ObjectId
from datetime import datetime
from db import db

close_card_bp = Blueprint("close_card", __name__, template_folder="templates")

customers_col           = db["customers"]
payments_col            = db["payments"]
users_col               = db["users"]
products_col            = db["products"]            # product catalog
stopped_customers_col   = db["stopped_customers"]   # archive whole customer on close
card_closures_col       = db["card_closures"]       # audit log for closures (not transfers)

# ---------- helpers: ids / roles / scope (session-only) ----------

def _oid(v):
    try:
        return ObjectId(str(v))
    except Exception:
        return None

def _session_actor():
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
    base = dict(base or {})
    actor, role = _session_actor()
    if role == "agent":
        base["agent_id"] = str(actor["_id"])
    elif role == "manager":
        base["manager_id"] = str(actor["_id"])
    else:
        if branch:
            base["branch"] = branch
    return base, actor, role

def _ensure_customer_in_scope(customer_id: str):
    cid = _oid(customer_id)
    if not cid:
        abort(400, description="Invalid customer id.")
    filt, _actor, _role = _scoped_filter({"_id": cid})
    customer = customers_col.find_one(filt)
    if not customer:
        abort(403, description="Unauthorized or customer not found.")
    return customer

def _sum_paid_for_product(customer_oid: ObjectId, product_index: int) -> float:
    total = 0.0
    for p in payments_col.find(
        {"customer_id": customer_oid, "payment_type": "PRODUCT", "product_index": int(product_index)},
        {"amount": 1}
    ):
        try:
            total += float(p.get("amount", 0) or 0)
        except Exception:
            pass
    return round(total, 2)

# ---------- routes ----------

@close_card_bp.route("/close_card", methods=["GET"])
def close_card_page():
    # Page shell; data via AJAX
    return render_template("close_card.html")

@close_card_bp.route("/close_card/search_customers", methods=["GET"])
def close_card_search_customers():
    q = (request.args.get("q") or "").strip()
    if not q:
        return jsonify(ok=True, results=[])

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

    filt, _actor, _role = _scoped_filter({
        "$or": [
            {"name": {"$regex": q, "$options": "i"}},
            {"phone_number": {"$regex": q, "$options": "i"}},
        ]
    }, branch=branch)

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

@close_card_bp.route("/close_card/suggest_products", methods=["GET"])
def close_card_suggest_products():
    """
    Given customer_id & product_index, compute:
      - total_paid on selected product
      - two_thirds = 2/3 * total_paid
      - one_third  = 1/3 * total_paid (forfeited)
    Return a list of products that two_thirds can fully purchase.
    Optional filters: ?category=&limit=
    """
    customer_id = request.args.get("customer_id", "").strip()
    product_index = request.args.get("product_index", "").strip()
    if not customer_id or product_index == "":
        return jsonify(ok=False, message="Missing fields."), 400

    customer = _ensure_customer_in_scope(customer_id)
    try:
        pidx = int(product_index)
    except Exception:
        return jsonify(ok=False, message="Invalid product index."), 400

    purchases = customer.get("purchases", []) or []
    if pidx < 0 or pidx >= len(purchases):
        return jsonify(ok=False, message="Product not found for this customer."), 404

    total_paid = _sum_paid_for_product(customer["_id"], pidx)
    two_thirds = round((2.0/3.0) * total_paid, 2)
    one_third  = round((1.0/3.0) * total_paid, 2)

    category = (request.args.get("category") or "").strip() or None
    try:
        limit = max(1, min(int(request.args.get("limit", 12)), 50))
    except Exception:
        limit = 12

    # Find products where (cash_price <= budget) OR (price <= budget)
    price_filter = {"$or": [{"cash_price": {"$lte": two_thirds}}, {"price": {"$lte": two_thirds}}]}
    base_filter = dict(price_filter)
    if category:
        base_filter["category"] = category

    # Pull a decent window and sort in Python by effective price desc (closest to budget)
    docs = list(products_col.find(base_filter, {"name": 1, "price": 1, "cash_price": 1, "image_url": 1, "category": 1}).limit(100))
    def eff_price(d):
        v = d.get("cash_price")
        if v is None:
            v = d.get("price")
        try:
            return float(v or 0.0)
        except Exception:
            return 0.0
    docs.sort(key=lambda d: eff_price(d), reverse=True)
    docs = docs[:limit]

    results = [{
        "_id": str(d["_id"]),
        "name": d.get("name", "Unnamed"),
        "price": float((d.get("cash_price") if d.get("cash_price") is not None else d.get("price")) or 0.0),
        "image_url": d.get("image_url", ""),
        "category": d.get("category", "")
    } for d in docs]

    return jsonify(ok=True, budget=two_thirds, forfeited=one_third, total_paid=total_paid, suggestions=results)

@close_card_bp.route("/close_card/execute", methods=["POST"])
def close_card_execute():
    """
    Close a card for a customer:
      - compute 2/3 of total paid on the selected product
      - (only suggest product; no new purchase is created here)
      - DELETE ALL payments of that customer (any product/type)
      - MOVE the entire customer doc to `stopped_customers`
      - AUDIT to `card_closures`
    """
    actor, actor_role = _session_actor()
    if not actor:
        return jsonify(ok=False, message="Unauthorized: please sign in to close cards."), 401

    data = request.get_json(silent=True) or {}
    customer_id = data.get("customer_id")
    product_index = data.get("product_index")
    note = (data.get("note") or "").strip()
    target_product_id = data.get("target_product_id")  # optional: which suggested product you picked

    if customer_id is None or product_index is None:
        return jsonify(ok=False, message="Missing required fields."), 400
    try:
        pidx = int(product_index)
    except Exception:
        return jsonify(ok=False, message="Invalid product selection."), 400

    # Fetch customer & compute totals
    customer = _ensure_customer_in_scope(customer_id)
    cust_oid = customer["_id"]
    purchases = customer.get("purchases", []) or []
    if pidx < 0 or pidx >= len(purchases):
        return jsonify(ok=False, message="Selected product not found."), 404

    from_purchase = purchases[pidx] or {}
    from_prod = (from_purchase.get("product") or {})

    total_paid = _sum_paid_for_product(cust_oid, pidx)
    two_thirds = round((2.0/3.0) * total_paid, 2)
    one_third  = round((1.0/3.0) * total_paid, 2)

    # Optional: resolve the chosen target product for logging
    target_product = None
    if target_product_id:
        t_oid = _oid(target_product_id)
        if t_oid:
            target_product = products_col.find_one({"_id": t_oid}, {"name": 1, "price": 1, "cash_price": 1, "category": 1, "image_url": 1})

    # 1) Delete ALL payments for this customer
    all_payments = list(payments_col.find({"customer_id": cust_oid}, {"_id": 1, "amount": 1, "date": 1, "payment_type": 1}))
    deleted_count = len(all_payments)
    deleted_total = round(sum(float(p.get("amount", 0) or 0) for p in all_payments), 2)
    if deleted_count:
        payments_col.delete_many({"customer_id": cust_oid})

    # 2) Move entire customer doc to stopped_customers
    now_utc = datetime.utcnow()
    stopped_doc = {
        "closed_at": now_utc,
        "closed_reason": "card_stop_no_transfer",
        "by_user": str(actor["_id"]),
        "by_role": actor_role,
        "note": note,
        "selected_product_index": pidx,
        "selected_product": from_prod,
        "totals": {
            "total_paid_selected_product": total_paid,
            "two_thirds_budget": two_thirds,
            "one_third_forfeited": one_third
        },
        "deleted_payments": {
            "count": int(deleted_count),
            "total_amount": float(deleted_total)
        },
        "target_product": ({
            "product_id": str(target_product["_id"]),
            "name": target_product.get("name"),
            "price": float((target_product.get("cash_price") if target_product.get("cash_price") is not None else target_product.get("price")) or 0.0),
            "category": target_product.get("category"),
            "image_url": target_product.get("image_url", "")
        } if target_product else None),
        "customer_snapshot": customer  # full customer document snapshot
    }
    stopped_customers_col.insert_one(stopped_doc)

    # 3) Remove from customers
    customers_col.delete_one({"_id": cust_oid})

    # 4) Audit log
    card_closures_col.insert_one({
        "customer_id": cust_oid,
        "at": now_utc,
        "action": "close_card",
        "by_user": str(actor["_id"]),
        "by_role": actor_role,
        "payload": {
            "selected_product_index": pidx,
            "selected_product_name": from_prod.get("name"),
            "two_thirds_budget": two_thirds,
            "one_third_forfeited": one_third,
            "deleted_payments_count": int(deleted_count),
            "deleted_payments_total": float(deleted_total),
            "target_product_id": (str(target_product["_id"]) if target_product else None),
            "note": note
        }
    })

    return jsonify(ok=True, message="Card closed. Customer archived and all payments deleted.",
                   data={
                       "two_thirds_budget": two_thirds,
                       "one_third_forfeited": one_third,
                       "deleted_payments_count": int(deleted_count),
                       "deleted_payments_total": float(deleted_total),
                       "target_product_id": (str(target_product["_id"]) if target_product else None)
                   })

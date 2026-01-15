# routes/close_card.py
from flask import Blueprint, render_template, request, jsonify, abort, session
from bson import ObjectId
from datetime import datetime
from db import db

close_card_bp = Blueprint("close_card", __name__, template_folder="templates")

customers_col                  = db["customers"]
payments_col                   = db["payments"]
users_col                      = db["users"]
inventory_col                  = db["inventory"]  # inventory catalog
card_closures_col              = db["card_closures"]       # audit log for closures (not transfers)
inventory_products_outflow_col = db["inventory_products_outflow"]

# ---------- helpers: ids / roles / scope (session-only) ----------

def _oid(v):
    try:
        return ObjectId(str(v))
    except Exception:
        return None

def _digits_only(s: str) -> str:
    return "".join(ch for ch in (s or "") if ch.isdigit())

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
    """
    Merge the caller's filter with role/branch scope using $and so we never
    overwrite the existing text/phone $or filter.
    Also tolerate agent_id/manager_id stored as string or ObjectId.
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

# ---------- routes ----------

@close_card_bp.route("/close_card", methods=["GET"])
def close_card_page():
    # Page shell; data via AJAX
    return render_template("close_card.html")

@close_card_bp.route("/close_card/search_customers", methods=["GET"])
def close_card_search_customers():
    raw_q = (request.args.get("q") or "").strip()
    if not raw_q:
        return jsonify(ok=True, results=[])

    # Normalize phone digits when the query looks like a phone
    q_digits = _digits_only(raw_q)

    # Base filter: name matches OR phone matches (digits if present, else raw)
    phone_regex = q_digits if q_digits else raw_q
    base_filter = {
        "$or": [
            {"name": {"$regex": raw_q, "$options": "i"}},
            {"phone_number": {"$regex": phone_regex}}
        ]
    }

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

@close_card_bp.route("/close_card/suggest_products", methods=["GET"])
def close_card_suggest_products():
    """
    Given customer_id & product_index, compute:
      - total_paid on selected product (ALL payments; no payment_type filter)
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

    # Optional override when user manually adjusts kept amount
    budget_override = request.args.get("budget")
    if budget_override is not None:
        try:
            budget_override = float(budget_override)
        except Exception:
            budget_override = None
    if budget_override is not None:
        if budget_override < 0:
            budget_override = 0
        if total_paid > 0 and budget_override > (total_paid + 0.01):
            budget_override = total_paid
        two_thirds = round(budget_override, 2)
        one_third = round(max(total_paid - two_thirds, 0.0), 2)

    category = (request.args.get("category") or "").strip() or None
    try:
        limit = max(1, min(int(request.args.get("limit", 20)), 50))
    except Exception:
        limit = 20
    try:
        page = max(1, int(request.args.get("page", 1)))
    except Exception:
        page = 1

    # Find inventory by scope and filter by budget in Python to tolerate string prices
    filters = []
    if category:
        filters.append({"category": category})
    manager_id = customer.get("manager_id")
    if manager_id:
        mid = _oid(manager_id) or manager_id
        filters.append({"$or": [{"manager_id": mid}, {"manager_id": str(mid)}]})

    base_filter = filters[0] if len(filters) == 1 else ({"$and": filters} if filters else {})

    # Pull a decent window and sort in Python by effective price desc (closest to budget)
    docs = list(inventory_col.find(
        base_filter,
        {"name": 1, "price": 1, "selling_price": 1, "image_url": 1, "category": 1, "qty": 1}
    ))

    def _to_float(v):
        try:
            return float(v)
        except Exception:
            return None

    def price_candidates(d):
        vals = []
        sp = _to_float(d.get("selling_price"))
        lp = _to_float(d.get("price"))
        if sp is not None:
            vals.append(sp)
        if lp is not None:
            vals.append(lp)
        return vals

    def eff_price(d):
        candidates = price_candidates(d)
        return max(candidates) if candidates else 0.0

    def display_price(d, budget):
        sp = _to_float(d.get("selling_price"))
        lp = _to_float(d.get("price"))
        if sp is not None and sp <= budget:
            return sp
        if lp is not None and lp <= budget:
            return lp
        if sp is not None:
            return sp
        if lp is not None:
            return lp
        return 0.0

    # Filter by budget (<= any of selling_price/price) and sort by effective price desc (closest to budget)
    docs = [d for d in docs if any(v <= two_thirds for v in price_candidates(d))]
    docs.sort(key=lambda d: eff_price(d), reverse=True)
    total = len(docs)
    total_pages = max(1, (total + limit - 1) // limit)
    if page > total_pages:
        page = total_pages
    start = (page - 1) * limit
    end = start + limit
    docs = docs[start:end]
    docs = docs[:limit]

    results = [{
        "_id": str(d["_id"]),
        "name": d.get("name", "Unnamed"),
        "price": float(display_price(d, two_thirds)),
        "qty": d.get("qty", 0),
        "image_url": d.get("image_url", ""),
        "category": d.get("category", "")
    } for d in docs]

    return jsonify(
        ok=True,
        budget=two_thirds,
        forfeited=one_third,
        total_paid=total_paid,
        suggestions=results,
        page=page,
        limit=limit,
        total=total,
        total_pages=total_pages
    )

@close_card_bp.route("/close_card/execute", methods=["POST"])
def close_card_execute():
    """
    Close a card for a customer:
      - compute 2/3 of total paid on the selected product (ALL payments; no payment_type filter)
      - (only suggest product; no new purchase is created here)
      - keep customer + payments intact
      - mark the selected purchase as closed (leave other purchases untouched)
      - set customer status to closed
      - AUDIT to `card_closures`
      - if a replacement product is chosen, log it in inventory_products_outflow
    """
    actor, actor_role = _session_actor()
    if not actor:
        return jsonify(ok=False, message="Unauthorized: please sign in to close cards."), 401

    data = request.get_json(silent=True) or {}
    customer_id = data.get("customer_id")
    product_index = data.get("product_index")
    note = (data.get("note") or "").strip()
    target_product_id = data.get("target_product_id")  # optional: single product pick
    target_product_ids = data.get("target_product_ids")  # optional: list of picks
    target_products_payload = data.get("target_products")  # optional: list of {id, qty}
    kept_amount = data.get("two_thirds_budget")
    forfeited_amount = data.get("one_third_forfeited")

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

    now_utc = datetime.utcnow()

    # Validate kept/forfeited amounts (fallback to defaults if missing)
    try:
        kept_amount = float(kept_amount) if kept_amount is not None else two_thirds
    except Exception:
        kept_amount = two_thirds
    try:
        forfeited_amount = float(forfeited_amount) if forfeited_amount is not None else one_third
    except Exception:
        forfeited_amount = one_third

    if kept_amount < 0 or forfeited_amount < 0:
        return jsonify(ok=False, message="Kept and forfeited amounts must be non-negative."), 400
    if total_paid > 0 and (kept_amount + forfeited_amount) > (total_paid + 0.01):
        return jsonify(ok=False, message="Kept + forfeited cannot exceed total paid on this card."), 400

    # Prevent double-close
    if (from_prod or {}).get("status") == "closed":
        return jsonify(ok=False, message="This product is already closed."), 400

    # Optional: resolve the chosen target product for logging
    target_products = []
    if target_products_payload:
        for item in target_products_payload:
            if not isinstance(item, dict):
                continue
            pid = item.get("id")
            qty = item.get("qty", 1)
            try:
                qty = int(qty)
            except Exception:
                qty = 1
            if qty < 1:
                qty = 1
            t_oid = _oid(pid)
            if not t_oid:
                continue
            prod = inventory_col.find_one({"_id": t_oid})
            if prod:
                target_products.append({"product": prod, "qty": qty})
    elif target_product_ids:
        for pid in target_product_ids:
            t_oid = _oid(pid)
            if not t_oid:
                continue
            prod = inventory_col.find_one({"_id": t_oid})
            if prod:
                target_products.append({"product": prod, "qty": 1})
    elif target_product_id:
        t_oid = _oid(target_product_id)
        if t_oid:
            prod = inventory_col.find_one({"_id": t_oid})
            if prod:
                target_products.append({"product": prod, "qty": 1})

    # 1) Mark the purchase as closed and close the customer
    customers_col.update_one(
        {"_id": cust_oid},
        {"$set": {
            "status": "closed",
            "status_updated_at": now_utc,
            f"purchases.{pidx}.product.status": "closed",
            f"purchases.{pidx}.status": "closed",
            f"purchases.{pidx}.closed_at": now_utc,
            f"purchases.{pidx}.closed_by": str(actor["_id"]),
            f"purchases.{pidx}.closed_by_role": actor_role,
            f"purchases.{pidx}.closed_note": note
        }}
    )

    # 2) Log inventory outflow if a replacement product was picked
    for entry in target_products:
        target_product = entry["product"]
        qty = entry.get("qty", 1)
        inventory_products_outflow_col.insert_one({
            "created_at": now_utc,
            "source": "close_card",
            "customer_id": cust_oid,
            "customer_name": customer.get("name"),
            "customer_phone": customer.get("phone_number"),
            "closed_product_index": pidx,
            "closed_product": from_prod,
            "budget": {
                "total_paid_selected_product": total_paid,
                "kept_amount": kept_amount,
                "forfeited_amount": forfeited_amount
            },
            "selected_product_id": str(target_product.get("_id")),
            "selected_product": target_product,
            "selected_qty": int(qty),
            "selected_total_price": float((qty or 0) * float((target_product.get("selling_price") or target_product.get("price") or 0) or 0)),
            "by_user": str(actor["_id"]),
            "by_role": actor_role
        })

    # 3) Audit log
    card_closures_col.insert_one({
        "customer_id": cust_oid,
        "at": now_utc,
        "action": "close_card",
        "by_user": str(actor["_id"]),
        "by_role": actor_role,
        "payload": {
            "selected_product_index": pidx,
            "selected_product_name": from_prod.get("name"),
            "kept_amount": float(kept_amount),
            "forfeited_amount": float(forfeited_amount),
            "target_products": [{
                "id": str(p["product"]["_id"]),
                "qty": int(p.get("qty", 1))
            } for p in target_products],
            "note": note
        }
    })

    return jsonify(
        ok=True,
        message="Card closed. Customer and payments retained.",
        data={
            "counted_for_two_thirds": float(total_paid),  # explicit, for clarity
            "two_thirds_budget": float(kept_amount),
            "one_third_forfeited": float(forfeited_amount),
            "target_products": [{
                "id": str(p["product"]["_id"]),
                "qty": int(p.get("qty", 1))
            } for p in target_products]
        }
    )

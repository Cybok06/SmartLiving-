# routes/executive_pricing.py
from __future__ import annotations
from flask import Blueprint, render_template, request, jsonify
from bson import ObjectId
from typing import Any, Dict, List
from datetime import datetime

from db import db

customers_col = db["customers"]
users_col = db["users"]
products_col = db["products"]
payments_col = db["payments"]
price_logs_col = db["price_change_logs"]   # NEW

executive_pricing_bp = Blueprint("executive_pricing", __name__, url_prefix="/exec/pricing")

def _oid(x: str):
    try: return ObjectId(x)
    except Exception: return None

# ---------------- Page ----------------
@executive_pricing_bp.route("/", methods=["GET"])
def pricing_page():
    managers = list(
        users_col.find({"role": "manager"}, {"name": 1, "branch": 1})
                .sort([("branch", 1), ("name", 1)])
    )
    return render_template("executive_pricing.html", managers=managers)

# ------------- Products for branch (with price shown) -------------
@executive_pricing_bp.route("/products", methods=["GET"])
def products_for_branch():
    manager_id = (request.args.get("manager_id") or "").strip()
    mo = _oid(manager_id)
    if not mo:
        return jsonify({"ok": False, "error": "invalid manager_id"}), 400

    prods = list(
        products_col.find({"manager_id": mo}, {"name": 1, "image_url": 1, "price": 1, "cash_price": 1})
                    .sort([("name", 1)])
    )
    out: List[Dict[str, Any]] = []
    for p in prods:
        out.append({
            "_id": str(p["_id"]),
            "name": p.get("name"),
            "image_url": p.get("image_url"),
            "price": p.get("price"),
            "cash_price": p.get("cash_price"),
        })
    return jsonify({"ok": True, "products": out})

# ---------------- Stats (branch required) ----------------
@executive_pricing_bp.route("/stats", methods=["GET"])
def stats():
    product_id = (request.args.get("product_id") or "").strip()
    manager_id = (request.args.get("manager_id") or "").strip()
    if not product_id:
        return jsonify({"ok": False, "error": "product_id required"}), 400
    mo = _oid(manager_id)
    if not mo:
        return jsonify({"ok": False, "error": "manager_id required"}), 400

    # Get selected product to fetch its name for payment join
    prod = products_col.find_one({"_id": ObjectId(product_id)}, {"name": 1, "price": 1, "cash_price": 1})
    if not prod:
        return jsonify({"ok": False, "error": "product not found"}), 404
    product_name = prod.get("name", "")

    # Customers in branch having this product in purchases
    match: Dict[str, Any] = {
        "manager_id": mo,
        "purchases.product._id": product_id
    }

    # Total customers (branch+product)
    total_customers = customers_col.count_documents(match)

    # Pull customers (trim) and keep only selected product purchases in payload
    customers = list(customers_col.find(
        match,
        {"name": 1, "phone_number": 1, "location": 1, "agent_id": 1, "purchases": 1}
    ).limit(1000))

    cust_ids = []
    per_customer_purchase_total: Dict[ObjectId, float] = {}

    for c in customers:
        cid = c["_id"]
        cust_ids.append(cid)
        filtered = []
        for p in (c.get("purchases") or []):
            prod_obj = p.get("product") or {}
            if prod_obj.get("_id") == product_id:
                # compute total if missing
                qty = prod_obj.get("quantity", 1) or 1
                price = prod_obj.get("price", 0) or 0
                total = prod_obj.get("total", None)
                if total is None:
                    total = price * qty
                    prod_obj["total"] = total
                filtered.append(p)
                # If multiple purchases of same named product exist, keep the largest total as target
                cur = per_customer_purchase_total.get(cid, 0.0)
                if float(total) > float(cur):
                    per_customer_purchase_total[cid] = float(total)
        c["purchases"] = filtered

    # Payments by these customers for this product name (branch-scoped)
    pay_match: Dict[str, Any] = {
        "manager_id": mo,
        "customer_id": {"$in": cust_ids},
        "product_name": product_name
    }
    pays = list(payments_col.aggregate([
        {"$match": pay_match},
        {"$group": {
            "_id": "$customer_id",
            "paid": {"$sum": "$amount"},
            "txns": {"$sum": 1}
        }}
    ]))

    paid_map: Dict[ObjectId, Dict[str, Any]] = {p["_id"]: {"paid": float(p["paid"]), "txns": int(p["txns"])} for p in pays}

    # Decorate customers with paid info and band
    def band_for_ratio(ratio: float) -> str:
        if ratio < 0.33: return "low"
        if ratio < 0.66: return "avg"
        return "high"

    bands_count = {"low": 0, "avg": 0, "high": 0}
    enriched_customers: List[Dict[str, Any]] = []
    for c in customers:
        cid = c["_id"]
        pc_total = float(per_customer_purchase_total.get(cid, 0.0))
        pm = paid_map.get(cid, {"paid": 0.0, "txns": 0})
        paid = float(pm["paid"])
        ratio = (paid / pc_total) if pc_total > 0 else 0.0
        band = band_for_ratio(ratio)
        bands_count[band] += 1

        # Convert ids to string for JSON
        enriched_customers.append({
            "_id": str(cid),
            "name": c.get("name"),
            "phone_number": c.get("phone_number"),
            "location": c.get("location"),
            "purchase_total": pc_total,
            "paid": round(paid, 2),
            "paid_ratio": round(ratio, 4),
            "band": band,
            "purchases": c.get("purchases", [])
        })

    # Branch label
    mgr = users_col.find_one({"_id": mo}, {"branch": 1, "name": 1})
    branch = (mgr or {}).get("branch") or "Unknown"
    manager_name = (mgr or {}).get("name") or "Unknown"

    return jsonify({
        "ok": True,
        "branch": branch,
        "manager_name": manager_name,
        "count": total_customers,
        "product": {
            "_id": str(prod["_id"]),
            "name": product_name,
            "price": prod.get("price"),
            "cash_price": prod.get("cash_price")
        },
        "bands": bands_count,              # distribution counts
        "customers": enriched_customers    # each has purchase_total, paid, paid_ratio, band
    })

# ---------------- Logs (recent) ----------------
@executive_pricing_bp.route("/logs", methods=["GET"])
def logs():
    product_id = (request.args.get("product_id") or "").strip()
    manager_id = (request.args.get("manager_id") or "").strip()
    limit = int(request.args.get("limit", 20))
    q: Dict[str, Any] = {}
    if product_id:
        q["product_id"] = product_id
    if manager_id:
        mo = _oid(manager_id)
        if mo: q["manager_id"] = mo

    items = list(
        price_logs_col.find(q).sort([("ts", -1)]).limit(max(1, min(limit, 100)))
    )
    for it in items:
        it["_id"] = str(it["_id"])
        if isinstance(it.get("manager_id"), ObjectId):
            it["manager_id"] = str(it["manager_id"])
    return jsonify({"ok": True, "logs": items})

# --------------- Bulk update (with logging) ---------------
@executive_pricing_bp.route("/update", methods=["POST"])
def bulk_update():
    data = request.get_json(silent=True) or {}
    product_id = (data.get("product_id") or "").strip()
    manager_id = (data.get("manager_id") or "").strip()
    mode = (data.get("mode") or "set").strip().lower()  # "set" | "percent"
    value = data.get("value")

    if not product_id:
        return jsonify({"ok": False, "error": "product_id required"}), 400
    mo = _oid(manager_id)
    if not mo:
        return jsonify({"ok": False, "error": "manager_id (branch) required"}), 400
    if mode not in ("set", "percent"):
        return jsonify({"ok": False, "error": "mode must be 'set' or 'percent'"}), 400
    try:
        num = float(value)
    except Exception:
        return jsonify({"ok": False, "error": "value must be numeric"}), 400

    # Fetch product doc for logging
    prod = products_col.find_one({"_id": ObjectId(product_id)}, {"name": 1, "price": 1})
    prod_name = (prod or {}).get("name", "")

    base_match = {
        "manager_id": mo,
        "purchases.product._id": product_id
    }

    # Collect a quick sample of old prices for logging (not all docs for performance)
    sample = list(customers_col.aggregate([
        {"$match": base_match},
        {"$unwind": "$purchases"},
        {"$match": {"purchases.product._id": product_id}},
        {"$limit": 10},
        {"$project": {"_id": 0, "old_price": "$purchases.product.price"}}
    ]))
    old_prices = [s.get("old_price") for s in sample if s.get("old_price") is not None]

    if mode == "set":
        new_price = int(round(num))
        update_pipeline = [{
            "$set": {
                "purchases": {
                    "$map": {
                        "input": "$purchases",
                        "as": "p",
                        "in": {
                            "$cond": [
                                {"$eq": ["$$p.product._id", product_id]},
                                {"$mergeObjects": [
                                    "$$p",
                                    {"product": {"$mergeObjects": [
                                        "$$p.product",
                                        {
                                            "price": new_price,
                                            "total": {"$multiply": [new_price, {"$ifNull": ["$$p.product.quantity", 1]}]}
                                        }
                                    ]}}
                                ]},
                                "$$p"
                            ]
                        }
                    }
                }
            }
        }]
        applied_value = new_price
    else:
        factor = 1 - (num / 100.0)
        update_pipeline = [{
            "$set": {
                "purchases": {
                    "$map": {
                        "input": "$purchases",
                        "as": "p",
                        "in": {
                            "$cond": [
                                {"$eq": ["$$p.product._id", product_id]},
                                {"$mergeObjects": [
                                    "$$p",
                                    {"product": {"$mergeObjects": [
                                        "$$p.product",
                                        {
                                            "price": {"$toInt": {"$round": [{"$multiply": ["$$p.product.price", factor]}, 0]}},
                                            "total": {"$toInt": {"$round": [{
                                                "$multiply": [
                                                    {"$multiply": ["$$p.product.price", factor]},
                                                    {"$ifNull": ["$$p.product.quantity", 1]}
                                                ]}, 0]}}
                                        }
                                    ]}}
                                ]},
                                "$$p"
                            ]
                        }
                    }
                }
            }
        }]
        applied_value = num  # percent value

    res = customers_col.update_many(base_match, update_pipeline)

    # Write log (summary-level)
    log_doc = {
        "ts": datetime.utcnow(),
        "manager_id": mo,
        "product_id": product_id,
        "product_name": prod_name,
        "mode": mode,                  # "set" | "percent"
        "value": applied_value,        # number (price or %)
        "matched": res.matched_count,
        "modified": res.modified_count,
        "old_price_samples": old_prices,  # optional, quick peek
        "actor": request.headers.get("X-Forwarded-For", request.remote_addr) or "public"
    }
    price_logs_col.insert_one(log_doc)

    return jsonify({
        "ok": True,
        "matched": res.matched_count,
        "modified": res.modified_count
    })

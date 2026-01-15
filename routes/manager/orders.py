# orders.py  (manager_orders_bp)
from flask import Blueprint, render_template, request, jsonify, abort, session
from bson import ObjectId
from datetime import datetime, timedelta
from uuid import uuid4
from db import db
from services.activity_audit import audit_action

manager_orders_bp = Blueprint(
    "manager_orders",
    __name__,
    template_folder="../../templates/manager",
    url_prefix="/manager/orders"
)

orders_col       = db["orders"]
catalog_col      = db["catalog_items"]
order_events_col = db["order_events"]
users_col        = db["users"]
inventory_col    = db["inventory"]   # manager inventory (saved with manager_id)

# --------- indexes ----------
try:
    inventory_col.create_index([("manager_id", 1), ("name", 1)], background=True)
    inventory_col.create_index([("manager_id", 1), ("qty", 1)], background=True)
    inventory_col.create_index([("manager_id", 1), ("sku", 1)], background=True)
    inventory_col.create_index([("manager_id", 1), ("code", 1)], background=True)
    orders_col.create_index([("manager_id", 1), ("updated_at", -1)], background=True)
except Exception:
    pass


def _oid(v):
    try:
        return ObjectId(str(v))
    except Exception:
        return None


def _require_manager():
    mid = session.get("manager_id")
    if not mid:
        abort(401, "Sign in as manager.")
    oid = _oid(mid)
    mgr = users_col.find_one({"_id": oid, "role": "manager"})
    if not mgr:
        abort(403, "Unauthorized.")
    return mgr


def _iso_timestamp(value):
    if isinstance(value, datetime):
        return value.isoformat()
    return value


def _manager_inventory_filter(mgr):
    # Your add_inventory.py always saves with manager_id
    return {
        "manager_id": mgr["_id"],
        "qty": {"$gt": 0},
        # "is_expired": {"$ne": True},  # uncomment if you want to hide expired
    }


def _map_inventory_product(doc):
    return {
        "_id": str(doc["_id"]),
        "name": doc.get("name") or doc.get("product_name") or "",
        "qty_available": int(doc.get("qty", 0) or 0),
        "image_url": doc.get("image_url"),
        "sku": doc.get("sku") or doc.get("code") or None,
        "tag": doc.get("branch") or doc.get("location") or (doc.get("source") or "Inventory"),
    }


@manager_orders_bp.route("/products", methods=["GET"])
def products_search():
    """
    Products for creating orders are fetched from LOGGED-IN MANAGER inventory.
    Supports:
      - q empty => returns top items
      - q provided => regex search on name/product_name/sku/code
    """
    mgr = _require_manager()
    q = (request.args.get("q") or "").strip()

    base = _manager_inventory_filter(mgr)

    try:
        limit = min(80, max(10, int(request.args.get("limit", 30))))
    except Exception:
        limit = 30

    projection = {
        "name": 1,
        "product_name": 1,
        "qty": 1,
        "image_url": 1,
        "sku": 1,
        "code": 1,
        "branch": 1,
        "location": 1,
        "source": 1,
        "is_expired": 1,
        "expiring_soon": 1,
    }

    if q:
        search_filters = [
            {"name": {"$regex": q, "$options": "i"}},
            {"product_name": {"$regex": q, "$options": "i"}},
            {"sku": {"$regex": q, "$options": "i"}},
            {"code": {"$regex": q, "$options": "i"}},
        ]
        match = {"$and": [base, {"$or": search_filters}]}
    else:
        match = base

    docs = list(
        inventory_col.find(match, projection)
        .sort([("name", 1), ("_id", 1)])
        .limit(limit)
    )
    return jsonify(ok=True, results=[_map_inventory_product(d) for d in docs])


@manager_orders_bp.route("/products_prefetch", methods=["GET"])
def products_prefetch():
    mgr = _require_manager()
    try:
        limit = min(500, max(50, int(request.args.get("limit", 250))))
    except Exception:
        limit = 250
    try:
        skip = max(0, int(request.args.get("skip", 0)))
    except Exception:
        skip = 0

    projection = {
        "name": 1,
        "product_name": 1,
        "qty": 1,
        "image_url": 1,
        "sku": 1,
        "code": 1,
        "branch": 1,
        "location": 1,
        "source": 1,
        "is_expired": 1,
        "expiring_soon": 1,
    }

    base_filter = _manager_inventory_filter(mgr)
    total = inventory_col.count_documents(base_filter)

    docs = list(
        inventory_col.find(base_filter, projection)
        .sort([("name", 1), ("_id", 1)])
        .skip(skip)
        .limit(limit)
    )
    return jsonify(
        ok=True,
        results=[_map_inventory_product(d) for d in docs],
        total=total,
        skip=skip,
        limit=limit,
    )


@manager_orders_bp.route("/", methods=["GET"])
def orders_page():
    _require_manager()
    return render_template("orders_list.html")


@manager_orders_bp.route("/list", methods=["GET"])
def list_orders():
    mgr = _require_manager()

    status    = (request.args.get("status") or "").strip().lower() or None
    date_from = (request.args.get("date_from") or "").strip() or None
    date_to   = (request.args.get("date_to") or "").strip() or None
    sort      = (request.args.get("sort") or "desc").lower()

    q = {"manager_id": mgr["_id"]}
    if status:
        q["status"] = status

    if date_from or date_to:
        dr = {}
        if date_from:
            try:
                dr["$gte"] = datetime.strptime(date_from, "%Y-%m-%d")
            except Exception:
                pass
        if date_to:
            try:
                dr["$lt"] = datetime.strptime(date_to, "%Y-%m-%d") + timedelta(days=1)
            except Exception:
                pass
        if dr:
            q["updated_at"] = dr

    sort_spec = [("updated_at", 1 if sort == "asc" else -1)]
    docs = list(orders_col.find(q).sort(sort_spec).limit(300))

    res = []
    for d in docs:
        items = []
        for it in d.get("items", []) or []:
            qty = int(it.get("qty", 0) or 0)
            delivered_qty = int(it.get("delivered_qty", 0) or 0)
            remaining_qty = max(0, qty - delivered_qty)
            items.append({
                "line_id": it.get("line_id"),
                "product_id": str(it.get("product_id")) if it.get("product_id") else None,
                "name": it.get("name"),
                "sku": it.get("sku") or it.get("code"),
                "qty": qty,
                "delivered_qty": delivered_qty,
                "remaining_qty": remaining_qty,
                "status": it.get("status"),
                "expected_date": it.get("expected_date"),
                "notes": it.get("notes"),
            })

        res.append({
            "_id": str(d["_id"]),
            "status": d.get("status", "open"),
            "notes": d.get("notes", ""),
            "created_at": _iso_timestamp(d.get("created_at")),
            "updated_at": _iso_timestamp(d.get("updated_at")),
            "items": items,
        })

    return jsonify(ok=True, results=res)


@manager_orders_bp.route("/catalog", methods=["GET"])
def catalog_suggest():
    _require_manager()
    q = (request.args.get("q") or "").strip()
    if not q:
        return jsonify(ok=True, results=[])
    results = list(catalog_col.find({"name": {"$regex": q, "$options": "i"}}).limit(20))
    return jsonify(ok=True, results=[r["name"] for r in results])


@manager_orders_bp.route("/create", methods=["GET"])
def create_page():
    _require_manager()
    return render_template("order_create.html")


@manager_orders_bp.route("/create", methods=["POST"])
@audit_action("order.created", "Created Order", entity_type="order")
def create_order():
    mgr = _require_manager()
    data = request.get_json(silent=True) or {}
    items = data.get("items") or []
    manual_items = data.get("manual_items") or []
    notes = (data.get("notes") or "").strip()
    branch = (data.get("branch") or mgr.get("branch") or "").strip()

    if not items and not manual_items:
        return jsonify(ok=False, message="Add at least one item."), 400

    shaped = []
    for raw in items:
        product_id = (raw.get("product_id") or "").strip()
        try:
            qty = int(raw.get("qty") or 0)
        except Exception:
            qty = 0

        exp = (raw.get("expected_date") or "").strip()
        line_notes = (raw.get("notes") or "").strip()

        if not product_id or qty <= 0:
            return jsonify(ok=False, message="Each line must include a product and quantity."), 400

        pid = _oid(product_id)
        if not pid:
            return jsonify(ok=False, message="Invalid product reference provided."), 400

        inv_doc = inventory_col.find_one(
            {"_id": pid, "manager_id": mgr["_id"]},
            {"name": 1, "qty": 1, "sku": 1, "code": 1}
        )
        if not inv_doc:
            return jsonify(ok=False, message="Selected product was not found in your inventory."), 400

        shaped.append({
            "line_id": str(uuid4()),
            "product_id": pid,
            "name": inv_doc.get("name"),
            "sku": inv_doc.get("sku") or inv_doc.get("code"),
            "qty": qty,
            "delivered_qty": 0,
            "remaining_qty": qty,
            "status": "pending",
            "expected_date": exp or None,
            "delivered_at": None,
            "postponements": [],
            "notes": line_notes,
        })

    manual_shaped = []
    for raw in manual_items:
        name = (raw.get("name") or "").strip()
        try:
            qty = int(raw.get("qty") or 0)
        except Exception:
            qty = 0
        line_notes = (raw.get("notes") or "").strip()

        if not name:
            return jsonify(ok=False, message="Manual item name is required."), 400
        if qty <= 0:
            return jsonify(ok=False, message="Manual item quantity must be at least 1."), 400

        manual_shaped.append({
            "line_id": str(uuid4()),
            "name": name,
            "qty": qty,
            "notes": line_notes,
        })

    now = datetime.utcnow()
    doc = {
        "manager_id": mgr["_id"],
        "branch": branch,
        "status": "open",
        "notes": notes,
        "items": shaped,
        "manual_items": manual_shaped,
        "created_at": now,
        "updated_at": now
    }
    ins = orders_col.insert_one(doc)

    event_items = []
    for line in shaped:
        event_items.append({
            "line_id": line.get("line_id"),
            "product_id": str(line.get("product_id")) if line.get("product_id") else None,
            "name": line.get("name"),
            "qty": line.get("qty"),
            "expected_date": line.get("expected_date"),
            "notes": line.get("notes"),
        })

    manual_event_items = []
    for line in manual_shaped:
        manual_event_items.append({
            "line_id": line.get("line_id"),
            "name": line.get("name"),
            "qty": line.get("qty"),
            "notes": line.get("notes"),
        })

    try:
        order_events_col.insert_one({
            "order_id": ins.inserted_id,
            "type": "create",
            "by": str(mgr["_id"]),
            "role": "manager",
            "payload": {
                "branch": branch,
                "notes": notes,
                "items": event_items,
                "manual_items": manual_event_items,
            },
            "at": now
        })
    except Exception:
        pass

    return jsonify(ok=True, order_id=str(ins.inserted_id))


@manager_orders_bp.route("/debug/products_count", methods=["GET"])
def debug_products_count():
    mgr = _require_manager()
    total = inventory_col.count_documents({"manager_id": mgr["_id"]})
    available = inventory_col.count_documents(_manager_inventory_filter(mgr))
    sample = list(inventory_col.find(_manager_inventory_filter(mgr), {"name": 1, "qty": 1, "image_url": 1}).limit(5))
    for s in sample:
        s["_id"] = str(s["_id"])
    return jsonify(ok=True, manager_total=total, manager_available=available, sample=sample)

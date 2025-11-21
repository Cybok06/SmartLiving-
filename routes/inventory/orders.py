from flask import Blueprint, render_template, request, jsonify, abort, session
from bson import ObjectId
from datetime import datetime, timedelta
from db import db

inventory_orders_bp = Blueprint(
    "inventory_orders",
    __name__,
    template_folder="../../templates/inventory",
    url_prefix="/inventory/orders"
)

orders_col       = db["orders"]
order_events_col = db["order_events"]
users_col        = db["users"]

def _oid(v):
    try:
        return ObjectId(str(v))
    except Exception:
        return None

def _require_inventory():
    uid = session.get("inventory_id") or session.get("admin_id") or session.get("executive_id")
    if not uid:
        abort(401, "Sign in as inventory/admin/executive.")
    user = users_col.find_one({"_id": _oid(uid)})
    if not user:
        abort(403, "Unauthorized.")
    return user

@inventory_orders_bp.route("/", methods=["GET"])
def inv_orders_page():
    _require_inventory()
    return render_template("orders_inbox.html")

# ---------- META (branches, managers) ----------
@inventory_orders_bp.route("/meta", methods=["GET"])
def inv_orders_meta():
    _require_inventory()
    branches = users_col.distinct("branch", {"role": "manager"})
    mgrs = list(users_col.find({"role": "manager"}, {"_id": 1, "name": 1, "branch": 1}).limit(500))
    return jsonify(
        ok=True,
        branches=[b for b in branches if b],
        managers=[{"_id": str(m["_id"]), "name": m.get("name",""), "branch": m.get("branch","")} for m in mgrs]
    )

# ---------- LIST with filters + stats ----------
@inventory_orders_bp.route("/list", methods=["GET"])
def inv_orders_list():
    _require_inventory()

    status     = (request.args.get("status") or "").strip().lower() or None
    branch     = (request.args.get("branch") or "").strip() or None
    manager_id = (request.args.get("manager_id") or "").strip() or None
    date_from  = (request.args.get("date_from") or "").strip() or None
    date_to    = (request.args.get("date_to") or "").strip() or None
    sort       = (request.args.get("sort") or "desc").lower()

    q = {}
    if status: q["status"] = status
    if branch: q["branch"] = branch
    if manager_id:
        mid = _oid(manager_id)
        if mid: q["manager_id"] = mid
    if date_from or date_to:
        dr = {}
        if date_from:
            try: dr["$gte"] = datetime.strptime(date_from, "%Y-%m-%d")
            except: pass
        if date_to:
            try: dr["$lt"]  = datetime.strptime(date_to, "%Y-%m-%d") + timedelta(days=1)
            except: pass
        if dr: q["updated_at"] = dr

    sort_key = [("updated_at", 1 if sort == "asc" else -1)]
    docs = list(orders_col.find(q).sort(sort_key).limit(500))

    outstanding = 0
    postponed   = 0

    out = []
    managers_index = {str(u["_id"]): u.get("name") for u in users_col.find({"role":"manager"},{"_id":1,"name":1})}

    for d in docs:
        shaped_items = []
        items = d.get("items", []) or []
        for it in items:
            qty = int(it.get("qty", 0) or 0)
            deliv = int(it.get("delivered_qty", 0) or 0)
            if qty - deliv > 0 and it.get("status") != "delivered":
                outstanding += 1
            if (it.get("status") or "") == "postponed":
                postponed += 1
            shaped_items.append({
                "line_id": it.get("line_id"),
                "name": it.get("name"),
                "qty": qty,
                "delivered_qty": deliv,
                "status": it.get("status"),
                "expected_date": it.get("expected_date"),
                "postponements": it.get("postponements", [])
            })

        out.append({
            "_id": str(d["_id"]),
            "manager_id": str(d["manager_id"]),
            "manager_name": managers_index.get(str(d["manager_id"]), None),
            "branch": d.get("branch"),
            "status": d.get("status"),
            "notes": d.get("notes", ""),
            "created_at": d.get("created_at"),
            "updated_at": d.get("updated_at"),
            "items": shaped_items
        })

    return jsonify(ok=True, results=out, stats={"outstanding_lines": outstanding, "postponed_lines": postponed})

# ---------- Last deliveries for a branch ----------
@inventory_orders_bp.route("/last_deliveries", methods=["GET"])
def last_deliveries():
    _require_inventory()
    branch     = (request.args.get("branch") or "").strip()
    if not branch:
        return jsonify(ok=True, results=[])
    date_from  = (request.args.get("date_from") or "").strip() or None
    date_to    = (request.args.get("date_to") or "").strip() or None

    order_ids = [d["_id"] for d in orders_col.find({"branch": branch}, {"_id":1}).limit(3000)]

    f = {"order_id": {"$in": order_ids}, "type": "deliver_line"}
    if date_from or date_to:
        dr = {}
        if date_from:
            try: dr["$gte"] = datetime.strptime(date_from, "%Y-%m-%d")
            except: pass
        if date_to:
            try: dr["$lt"]  = datetime.strptime(date_to, "%Y-%m-%d") + timedelta(days=1)
            except: pass
        if dr: f["at"] = dr

    evs = list(order_events_col.find(f).sort([("at",-1)]).limit(50))
    results = []
    managers_index = {str(u["_id"]): u.get("name") for u in users_col.find({"role":"manager"},{"_id":1,"name":1})}
    for e in evs:
        o = orders_col.find_one({"_id": e["order_id"]}, {"manager_id":1})
        mid = str(o["manager_id"]) if o else None
        results.append({
            "order_id": str(e["order_id"]),
            "manager_id": mid,
            "manager_name": managers_index.get(mid),
            "item_name": (e.get("payload") or {}).get("item_name") or "-",
            "qty": (e.get("payload") or {}).get("qty", 0),
            "at": e.get("at")
        })
    return jsonify(ok=True, results=results)

def _recompute_order_status(order):
    total = len(order.get("items",[]))
    delivered = sum(1 for it in order["items"] if it.get("status") == "delivered")
    if delivered == 0:
        return "open"
    if delivered < total:
        return "partially_delivered"
    return "closed"

# ---------- Actions ----------
@inventory_orders_bp.route("/line/deliver", methods=["POST"])
def deliver_line():
    user = _require_inventory()
    data = request.get_json(silent=True) or {}
    order_id = data.get("order_id")
    line_id  = data.get("line_id")
    qty      = int(data.get("qty") or 0)
    if not order_id or not line_id or qty <= 0:
        abort(400, "order_id, line_id, qty > 0 required")

    order = orders_col.find_one({"_id": _oid(order_id)})
    if not order:
        abort(404, "Order not found")

    items = order.get("items", [])
    line_item = None
    for it in items:
        if it.get("line_id") == line_id:
            remaining = int(it.get("qty",0)) - int(it.get("delivered_qty",0))
            if remaining <= 0:
                abort(400, "Line already fully delivered")
            deliver_now = min(qty, remaining)
            it["delivered_qty"] = int(it.get("delivered_qty",0)) + deliver_now
            it["status"] = "delivered" if it["delivered_qty"] >= it["qty"] else "pending"
            if it["status"] == "delivered":
                it["delivered_at"] = datetime.utcnow()
            it.setdefault("postponements", [])
            line_item = it
            break
    if not line_item:
        abort(404, "Line not found")

    sta = _recompute_order_status({"items": items})
    orders_col.update_one({"_id": order["_id"]}, {"$set": {"items": items, "status": sta, "updated_at": datetime.utcnow()}})

    order_events_col.insert_one({
        "order_id": order["_id"],
        "type": "deliver_line",
        "payload": {"line_id": line_id, "qty": qty, "item_name": line_item.get("name")},
        "by": str(user["_id"]),
        "role": "inventory",
        "at": datetime.utcnow()
    })

    return jsonify(ok=True, order_status=sta)

@inventory_orders_bp.route("/line/postpone", methods=["POST"])
def postpone_line():
    user = _require_inventory()
    data = request.get_json(silent=True) or {}
    order_id = data.get("order_id")
    line_id  = data.get("line_id")
    to_date  = (data.get("to_date") or "").strip()
    reason   = (data.get("reason") or "").strip()

    if not order_id or not line_id or not to_date:
        abort(400, "order_id, line_id, to_date required")

    order = orders_col.find_one({"_id": _oid(order_id)})
    if not order:
        abort(404, "Order not found")

    items = order.get("items", [])
    found = False
    for it in items:
        if it.get("line_id") == line_id:
            from_date = it.get("expected_date")
            it["expected_date"] = to_date
            it.setdefault("postponements", []).append({
                "from": from_date, "to": to_date, "reason": reason,
                "at": datetime.utcnow(), "by": str(user["_id"])
            })
            if it.get("status") != "delivered":
                it["status"] = "postponed"
            found = True
            break
    if not found:
        abort(404, "Line not found")

    sta = _recompute_order_status({"items": items})
    orders_col.update_one({"_id": order["_id"]}, {"$set": {"items": items, "status": sta, "updated_at": datetime.utcnow()}})

    order_events_col.insert_one({
        "order_id": order["_id"],
        "type": "postpone_line",
        "payload": {"line_id": line_id, "to_date": to_date, "reason": reason},
        "by": str(user["_id"]),
        "role": "inventory",
        "at": datetime.utcnow()
    })

    return jsonify(ok=True, order_status=sta)

# ---------- HISTORY (Closed orders, paginated) ----------
@inventory_orders_bp.route("/history", methods=["GET"])
def orders_history():
    _require_inventory()

    branch     = (request.args.get("branch") or "").strip() or None
    manager_id = (request.args.get("manager_id") or "").strip() or None
    date_from  = (request.args.get("date_from") or "").strip() or None
    date_to    = (request.args.get("date_to") or "").strip() or None
    sort       = (request.args.get("sort") or "desc").lower()
    try:
        page      = max(1, int(request.args.get("page", 1)))
    except Exception:
        page = 1
    try:
        page_size = min(100, max(1, int(request.args.get("page_size", 20))))
    except Exception:
        page_size = 20

    q = {"status": "closed"}
    if branch: q["branch"] = branch
    if manager_id:
        mid = _oid(manager_id)
        if mid: q["manager_id"] = mid

    if date_from or date_to:
        dr = {}
        if date_from:
            try: dr["$gte"] = datetime.strptime(date_from, "%Y-%m-%d")
            except: pass
        if date_to:
            try: dr["$lt"]  = datetime.strptime(date_to, "%Y-%m-%d") + timedelta(days=1)
            except: pass
        if dr: q["updated_at"] = dr  # closed time is reflected in updated_at

    sort_key = [("updated_at", 1 if sort == "asc" else -1)]
    total = orders_col.count_documents(q)
    skip = (page - 1) * page_size

    docs = list(orders_col.find(q).sort(sort_key).skip(skip).limit(page_size))

    managers_index = {str(u["_id"]): u.get("name") for u in users_col.find({"role":"manager"},{"_id":1,"name":1})}

    results = []
    for d in docs:
        results.append({
            "_id": str(d["_id"]),
            "branch": d.get("branch"),
            "manager_id": str(d.get("manager_id")) if d.get("manager_id") else None,
            "manager_name": managers_index.get(str(d.get("manager_id")), None),
            "closed_at": d.get("updated_at"),
            "items": [{
                "name": it.get("name"),
                "qty": int(it.get("qty", 0) or 0),
                "delivered_qty": int(it.get("delivered_qty", 0) or 0)
            } for it in (d.get("items") or [])]
        })

    pages = (total + page_size - 1) // page_size
    return jsonify(ok=True, results=results, page=page, pages=pages, total=total, page_size=page_size)

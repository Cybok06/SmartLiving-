from flask import Blueprint, render_template, request, jsonify, abort, session
from bson import ObjectId
from datetime import datetime, timedelta
from uuid import uuid4
from db import db

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

def _save_catalog_name(name: str):
    n = (name or "").strip()
    if not n:
        return
    try:
        catalog_col.update_one(
            {"name": n},
            {"$setOnInsert": {"name": n, "created_at": datetime.utcnow()}},
            upsert=True
        )
    except Exception:
        pass

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
            # filter by last update time; change to "created_at" if that's preferred
            q["updated_at"] = dr

    sort_spec = [("updated_at", 1 if sort == "asc" else -1)]

    docs = list(orders_col.find(q).sort(sort_spec).limit(300))
    res = []
    for d in docs:
        res.append({
            "_id": str(d["_id"]),
            "status": d.get("status", "open"),
            "notes": d.get("notes", ""),
            "created_at": d.get("created_at"),
            "updated_at": d.get("updated_at"),
            "items": [{
                "line_id": it.get("line_id"),
                "name": it.get("name"),
                "qty": it.get("qty"),
                "delivered_qty": it.get("delivered_qty", 0),
                "status": it.get("status"),
                "expected_date": it.get("expected_date"),
            } for it in d.get("items", [])]
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
def create_order():
    mgr = _require_manager()
    data = request.get_json(silent=True) or {}
    items = data.get("items") or []
    notes = (data.get("notes") or "").strip()
    branch = (data.get("branch") or mgr.get("branch") or "").strip()

    if not items:
        return jsonify(ok=False, message="Add at least one item."), 400

    shaped = []
    for raw in items:
        name = (raw.get("name") or "").strip()
        try:
            qty = int(raw.get("qty") or 0)
        except Exception:
            qty = 0
        exp = (raw.get("expected_date") or "").strip()
        if not name or qty <= 0:
            return jsonify(ok=False, message="Invalid item in list."), 400
        shaped.append({
            "line_id": str(uuid4()),
            "name": name,
            "qty": qty,
            "delivered_qty": 0,
            "status": "pending",
            "expected_date": exp or None,
            "delivered_at": None,
            "postponements": [],
            "notes": (raw.get("notes") or "").strip()
        })
        _save_catalog_name(name)

    now = datetime.utcnow()
    doc = {
        "manager_id": mgr["_id"],
        "branch": branch,
        "status": "open",
        "notes": notes,
        "items": shaped,
        "created_at": now,
        "updated_at": now
    }
    ins = orders_col.insert_one(doc)

    order_events_col.insert_one({
        "order_id": ins.inserted_id,
        "type": "create",
        "by": str(mgr["_id"]),
        "role": "manager",
        "payload": {"branch": branch, "notes": notes, "items": shaped},
        "at": now
    })

    return jsonify(ok=True, order_id=str(ins.inserted_id))

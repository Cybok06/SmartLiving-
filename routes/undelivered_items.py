from flask import Blueprint, render_template, request, jsonify, redirect, url_for, session
from bson import ObjectId
from datetime import datetime, timedelta
from db import db
from services.activity_audit import audit_action

undelivered_items_bp = Blueprint("undelivered_items", __name__, template_folder="templates")

customers_col = db["customers"]
users_col = db["users"]
products_col = db["products"]
inventory_col = db["inventory"]
undelivered_items_col = db["undelivered_items"]


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


def _resolve_product_def(purchase, manager_id):
    product = (purchase or {}).get("product", {}) or {}
    cf_image_id = product.get("cf_image_id")
    name = product.get("name")
    if not manager_id:
        return None
    if cf_image_id:
        doc = products_col.find_one(
            {"manager_id": manager_id, "cf_image_id": cf_image_id},
            sort=[("created_at", -1)]
        )
        if doc:
            return doc
    if name:
        return products_col.find_one(
            {"manager_id": manager_id, "name": name},
            sort=[("created_at", -1)]
        )
    return None


def _inv_match(comp_id, manager_id):
    if manager_id:
        return {"_id": comp_id, "$or": [{"manager_id": manager_id}, {"manager_id": str(manager_id)}]}
    return {"_id": comp_id}


def _build_component_catalog(product_def, purchase_qty, manager_id):
    items = []
    if not product_def:
        return items
    for comp in (product_def.get("components") or []):
        comp_id = comp.get("_id")
        if not comp_id:
            continue
        try:
            required_qty = int(comp.get("quantity", 1)) * int(purchase_qty or 1)
        except Exception:
            required_qty = int(purchase_qty or 1)
        inv = inventory_col.find_one(_inv_match(comp_id, manager_id), {"name": 1, "image_url": 1})
        items.append({
            "inventory_id": comp_id,
            "name": (inv or {}).get("name") or "Unknown item",
            "required_qty": required_qty,
            "image_url": (inv or {}).get("image_url"),
            "source": "component"
        })
    return items


def _safe_json_value(value):
    if isinstance(value, ObjectId):
        return str(value)
    if isinstance(value, dict):
        return {k: _safe_json_value(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_safe_json_value(v) for v in value]
    return value


def _due_flags(expected_date_str):
    if not expected_date_str:
        return False, False
    try:
        d = datetime.strptime(expected_date_str, "%Y-%m-%d").date()
    except Exception:
        return False, False
    today = datetime.utcnow().date()
    is_overdue = d < today
    is_due_soon = (d - today).days <= 2 and d >= today
    return is_due_soon, is_overdue


def _scoped_filter(base=None, branch=None, manager_id=None):
    base = dict(base or {})
    actor, role = _session_actor()
    scope_clauses = []

    if role == "agent":
        aid = str(actor["_id"])
        scope_clauses.append({"$or": [{"agent_id": aid}, {"agent_id": actor["_id"]}]})
    elif role == "manager":
        mid = actor["_id"]
        scope_clauses.append({"$or": [{"manager_id": mid}, {"manager_id": str(mid)}]})
    else:
        if manager_id:
            scope_clauses.append({"$or": [{"manager_id": manager_id}, {"manager_id": str(manager_id)}]})
        elif branch:
            scope_clauses.append({"manager_branch": branch})

    if scope_clauses:
        base = {"$and": [base] + scope_clauses}

    return base, actor, role


@undelivered_items_bp.route("/customer/<customer_id>/undelivered/<int:product_index>", methods=["POST"])
@audit_action("undelivered.created", "Created Undelivered Item", entity_type="undelivered", entity_id_from="customer_id")
def create_undelivered_record(customer_id, product_index):
    actor, role = _session_actor()
    if role != "agent":
        return jsonify(ok=False, message="Unauthorized"), 403

    agent_id = str(actor["_id"])
    cust_oid = _oid(customer_id)
    if not cust_oid:
        return jsonify(ok=False, message="Invalid customer."), 400

    customer = customers_col.find_one({"_id": cust_oid, "agent_id": agent_id})
    if not customer:
        return jsonify(ok=False, message="Unauthorized or customer not found."), 403

    purchases = customer.get("purchases", []) or []
    if product_index < 0 or product_index >= len(purchases):
        return jsonify(ok=False, message="Invalid product selection."), 404

    purchase = purchases[product_index]
    product_info = (purchase or {}).get("product", {}) or {}
    status = (product_info.get("status") or "").lower()
    if status not in ("completed", "packaged"):
        return jsonify(ok=False, message="Product not completed."), 400

    payload = request.get_json(silent=True) or request.form
    expected_delivery_date = (payload.get("expected_delivery_date") or "").strip()
    note = (payload.get("note") or "").strip()
    if not expected_delivery_date:
        return jsonify(ok=False, message="Expected delivery date is required."), 400

    manager_id = actor.get("manager_id")
    if manager_id and not isinstance(manager_id, ObjectId):
        manager_id = _oid(manager_id)
    manager_doc = users_col.find_one({"_id": manager_id}, {"branch": 1}) if manager_id else None

    purchase_qty = int(product_info.get("quantity") or 1)
    product_def = _resolve_product_def(purchase, manager_id)
    catalog = _build_component_catalog(product_def, purchase_qty, manager_id)

    components_payload = payload.get("components") or []
    manual_payload = payload.get("manual_items") or []
    selected_items = []

    # Normalize components payload
    if isinstance(components_payload, list):
        for row in components_payload:
            if not isinstance(row, dict):
                continue
            inv_id = row.get("inventory_id")
            if not inv_id:
                continue
            comp_id = inv_id if isinstance(inv_id, ObjectId) else _oid(inv_id) or inv_id
            required_qty = int(row.get("required_qty") or 0)
            undelivered_qty = int(row.get("undelivered_qty") or required_qty or 0)
            name = row.get("name")
            image_url = row.get("image_url")
            if not name:
                match = next((c for c in catalog if str(c.get("inventory_id")) == str(comp_id)), {})
                name = match.get("name")
                image_url = image_url or match.get("image_url")
                if not required_qty:
                    required_qty = int(match.get("required_qty") or 0)
            selected_items.append({
                "inventory_id": comp_id,
                "name": name or "Unknown item",
                "required_qty": required_qty,
                "undelivered_qty": undelivered_qty,
                "image_url": image_url,
                "source": "component"
            })

    # Manual items
    if isinstance(manual_payload, list):
        for row in manual_payload:
            if not isinstance(row, dict):
                continue
            name = (row.get("name") or "").strip()
            if not name:
                continue
            try:
                qty = int(row.get("undelivered_qty") or row.get("required_qty") or 0)
            except Exception:
                qty = 0
            if qty <= 0:
                continue
            selected_items.append({
                "inventory_id": None,
                "name": name,
                "required_qty": qty,
                "undelivered_qty": qty,
                "image_url": None,
                "source": "manual"
            })

    now_utc = datetime.utcnow()
    base_doc = {
        "created_at": now_utc,
        "updated_at": now_utc,
        "status": "pending",
        "customer_id": cust_oid,
        "customer_name": customer.get("name"),
        "customer_phone": customer.get("phone_number"),
        "agent_id": agent_id,
        "agent_name": actor.get("name"),
        "agent_branch": actor.get("branch"),
        "manager_id": manager_id,
        "manager_branch": (manager_doc or {}).get("branch"),
        "product_index": int(product_index),
        "packaged_product": product_info,
        "expected_delivery_date": expected_delivery_date,
        "note": note,
        "undelivered_items": selected_items
    }

    existing = undelivered_items_col.find_one({
        "customer_id": cust_oid,
        "product_index": int(product_index),
        "status": "pending"
    })
    if existing:
        base_doc["created_at"] = existing.get("created_at", now_utc)
        undelivered_items_col.update_one(
            {"_id": existing["_id"]},
            {"$set": base_doc}
        )
        return jsonify(ok=True, message="Undelivered items updated.")

    undelivered_items_col.insert_one(base_doc)
    return jsonify(ok=True, message="Undelivered items logged.")


@undelivered_items_bp.route("/undelivered_items", methods=["GET"])
def undelivered_items_page():
    actor, role = _session_actor()
    if role not in ("manager", "admin", "inventory", "executive"):
        return redirect(url_for("login.login"))

    branches = []
    if role in ("admin", "inventory", "executive"):
        branches = sorted([b for b in users_col.distinct("branch", {"role": "manager"}) if b])
    return render_template("undelivered_items.html", role=role, branches=branches)


@undelivered_items_bp.route("/undelivered_items/data", methods=["GET"])
def undelivered_items_data():
    status = (request.args.get("status") or "pending").strip().lower()
    date_from = (request.args.get("from") or "").strip()
    date_to = (request.args.get("to") or "").strip()
    manager_id_param = (request.args.get("manager_id") or "").strip()
    branch = (request.args.get("branch") or "").strip()

    base = {}
    if status in ("pending", "delivered"):
        base["status"] = status

    if date_from or date_to:
        dtflt = {}
        if date_from:
            try:
                dtflt["$gte"] = datetime.strptime(date_from, "%Y-%m-%d")
            except Exception:
                pass
        if date_to:
            try:
                dtflt["$lte"] = datetime.strptime(date_to, "%Y-%m-%d") + timedelta(days=1)
            except Exception:
                pass
        if dtflt:
            base["created_at"] = dtflt

    manager_oid = _oid(manager_id_param) if manager_id_param else None
    flt, _actor, _role = _scoped_filter(base, branch=branch, manager_id=manager_oid)

    rows = list(undelivered_items_col.find(flt).sort("created_at", -1))
    out = []
    for r in rows:
        expected = r.get("expected_delivery_date")
        is_due_soon, is_overdue = _due_flags(expected)
        created_at = r.get("created_at")
        created_str = created_at.isoformat() if hasattr(created_at, "isoformat") else None
        out.append({
            "id": str(r.get("_id")),
            "created_at": created_str,
            "expected_delivery_date": expected,
            "status": r.get("status"),
            "customer_name": r.get("customer_name"),
            "agent_name": r.get("agent_name"),
            "agent_branch": r.get("agent_branch"),
            "product_name": (r.get("packaged_product") or {}).get("name"),
            "undelivered_items": _safe_json_value(r.get("undelivered_items") or []),
            "is_due_soon": is_due_soon,
            "is_overdue": is_overdue
        })
    return jsonify(ok=True, rows=out)


@undelivered_items_bp.route("/undelivered_items/<record_id>/mark_delivered", methods=["POST"])
@audit_action("undelivered.delivered", "Marked Undelivered Delivered", entity_type="undelivered", entity_id_from="record_id")
def mark_undelivered_delivered(record_id):
    actor, role = _session_actor()
    if role not in ("agent", "manager", "admin", "inventory", "executive"):
        return jsonify(ok=False, message="Unauthorized"), 403

    rec_oid = _oid(record_id)
    if not rec_oid:
        return jsonify(ok=False, message="Invalid record."), 400

    filt = {"_id": rec_oid, "status": "pending"}
    if role == "agent":
        filt["$or"] = [{"agent_id": str(actor["_id"])}, {"agent_id": actor["_id"]}]

    rec = undelivered_items_col.find_one(filt)
    if not rec:
        return jsonify(ok=False, message="Record not found or unauthorized."), 404

    now_utc = datetime.utcnow()
    undelivered_items_col.update_one(
        {"_id": rec_oid},
        {"$set": {
            "status": "delivered",
            "delivered_at": now_utc,
            "delivered_by": str(actor["_id"]),
            "delivered_by_role": role,
            "updated_at": now_utc
        }}
    )
    return jsonify(ok=True, message="Marked delivered.")


@undelivered_items_bp.route("/undelivered_items/count_due", methods=["GET"])
def undelivered_items_count_due():
    base = {"status": "pending"}
    flt, _actor, _role = _scoped_filter(base)

    rows = list(undelivered_items_col.find(flt, {"expected_delivery_date": 1}))
    due_soon = 0
    overdue = 0
    for r in rows:
        expected = r.get("expected_delivery_date")
        is_due_soon, is_overdue = _due_flags(expected)
        if is_due_soon:
            due_soon += 1
        if is_overdue:
            overdue += 1
    return jsonify(ok=True, pending_total=len(rows), due_soon=due_soon, overdue=overdue)

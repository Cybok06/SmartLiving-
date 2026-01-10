from datetime import datetime

from flask import Blueprint, render_template, session, redirect, url_for, request, flash, jsonify
from bson.objectid import ObjectId
from pymongo import ReturnDocument

from db import db

manager_inventory_bp = Blueprint('manager_inventory', __name__)

# Collections
inventory_col = db.inventory
users_col = db.users
history_col = db.inventory_history
settings_col = db.inventory_settings

DEFAULT_REORDER_LEVEL = 20


def _ensure_settings_indexes():
    try:
        settings_col.create_index([("manager_id", 1)])
        settings_col.create_index([("updated_at", -1)])
    except Exception:
        pass


_ensure_settings_indexes()


def _is_ajax_request():
    return (
        request.headers.get("X-Requested-With") == "XMLHttpRequest"
        or "application/json" in request.headers.get("Accept", "")
    )


def _safe_int(val, default=0):
    try:
        return int(val)
    except (TypeError, ValueError):
        return default


def get_reorder_level(manager_id):
    """
    Resolve reorder level for a manager.
    Priority: manager-specific -> global (manager_id=None) -> DEFAULT_REORDER_LEVEL.
    """
    doc = None
    if manager_id:
        doc = settings_col.find_one(
            {"manager_id": manager_id},
            sort=[("updated_at", -1)]
        )
    if not doc:
        doc = settings_col.find_one(
            {"manager_id": None},
            sort=[("updated_at", -1)]
        )
    level = doc.get("reorder_level") if doc else None
    level_int = _safe_int(level, DEFAULT_REORDER_LEVEL)
    if level_int < 0:
        level_int = DEFAULT_REORDER_LEVEL
    return level_int


def _get_manager_oid():
    if 'manager_id' not in session:
        return None
    try:
        return ObjectId(session['manager_id'])
    except Exception:
        return None


@manager_inventory_bp.route('/manager/inventory')
def view_manager_inventory():
    """
    Manager inventory view:
      - Only products for the logged-in manager
      - Optional category filter via ?category=<name>
      - Results sorted by newest first
    """
    manager_id = _get_manager_oid()
    if not manager_id:
        return redirect(url_for('login.login'))

    selected_category = request.args.get('category', 'all')

    query_filter = {"manager_id": manager_id}
    if selected_category and selected_category != "all":
        query_filter["category"] = selected_category

    # Fetch inventory for this manager (filtered by category if provided)
    inventory_items = list(
        inventory_col.find(query_filter).sort("created_at", -1)
    )

    # Build category list for filter dropdown (distinct categories for this manager)
    raw_categories = inventory_col.distinct("category", {"manager_id": manager_id})
    categories = sorted([c for c in raw_categories if c])

    managers = list(
        users_col.find(
            {"role": "manager", "_id": {"$ne": manager_id}},
            {"name": 1, "branch": 1}
        ).sort("name", 1)
    )

    reorder_level = get_reorder_level(manager_id)
    low_stock_ids = []
    for item in inventory_items:
        qty_val = _safe_int(item.get("qty"), 0)
        if qty_val <= reorder_level:
            low_stock_ids.append(item.get("_id"))

    return render_template(
        'manager_inventory.html',
        inventory_items=inventory_items,
        categories=categories,
        selected_category=selected_category,
        managers=managers,
        reorder_level=reorder_level,
        low_stock_count=len(low_stock_ids),
        low_stock_ids=low_stock_ids
    )


@manager_inventory_bp.route('/manager/inventory/settings')
def manager_inventory_settings():
    manager_id = _get_manager_oid()
    if not manager_id:
        return redirect(url_for('login.login'))

    reorder_level = get_reorder_level(manager_id)
    return jsonify(ok=True, reorder_level=reorder_level)


@manager_inventory_bp.route('/manager/inventory/settings/reorder-level', methods=['POST'])
def manager_inventory_settings_reorder():
    manager_id = _get_manager_oid()
    if not manager_id:
        return jsonify(ok=False, message="Session expired. Please login again."), 401

    level_raw = request.form.get("reorder_level")
    if level_raw is None and request.is_json:
        payload = request.get_json(silent=True) or {}
        level_raw = payload.get("reorder_level")

    try:
        level = int(level_raw)
    except (TypeError, ValueError):
        return jsonify(ok=False, message="Reorder level must be an integer."), 400

    if level < 0:
        return jsonify(ok=False, message="Reorder level must be 0 or greater."), 400

    manager_doc = users_col.find_one(
        {"_id": manager_id},
        {"name": 1, "branch": 1}
    ) or {}

    settings_col.update_one(
        {"manager_id": manager_id},
        {
            "$set": {
                "manager_id": manager_id,
                "branch_name": manager_doc.get("branch"),
                "reorder_level": level,
                "updated_by": session.get("manager_name") or str(manager_id),
                "updated_at": datetime.utcnow(),
            }
        },
        upsert=True
    )

    return jsonify(ok=True, reorder_level=level)


@manager_inventory_bp.route('/manager/inventory/low-stocks')
def manager_low_stocks():
    manager_id = _get_manager_oid()
    if not manager_id:
        return redirect(url_for('login.login'))

    reorder_level = get_reorder_level(manager_id)
    low_stock_query = {
        "manager_id": manager_id,
        "$or": [
            {"qty": {"$lte": reorder_level}},
            {"qty": None},
            {"qty": {"$exists": False}},
        ],
    }
    items = list(inventory_col.find(low_stock_query))
    items.sort(key=lambda x: (_safe_int(x.get("qty"), 0), (x.get("name") or "")))

    managers = list(
        users_col.find(
            {"role": "manager", "_id": {"$ne": manager_id}},
            {"name": 1, "branch": 1}
        ).sort("name", 1)
    )

    low_stock_ids = [item.get("_id") for item in items]

    return render_template(
        'manager_inventory.html',
        inventory_items=items,
        categories=[],
        selected_category="all",
        managers=managers,
        reorder_level=reorder_level,
        low_stock_count=len(items),
        low_stock_ids=low_stock_ids,
        page_title="Low Stock Products",
        page_subtitle=f"Qty at or below {reorder_level} across your branch.",
        low_stock_only=True
    )


@manager_inventory_bp.route('/manager/inventory/low-stocks/count')
def manager_low_stocks_count():
    manager_id = _get_manager_oid()
    if not manager_id:
        return jsonify(ok=False, message="Session expired. Please login again."), 401

    reorder_level = get_reorder_level(manager_id)
    low_stock_query = {
        "manager_id": manager_id,
        "$or": [
            {"qty": {"$lte": reorder_level}},
            {"qty": None},
            {"qty": {"$exists": False}},
        ],
    }
    count = inventory_col.count_documents(low_stock_query)

    return jsonify(ok=True, reorder_level=reorder_level, low_stock_count=count)


@manager_inventory_bp.route('/manager/inventory/transfer', methods=['POST'])
def transfer_manager_inventory():
    is_ajax = _is_ajax_request()
    if 'manager_id' not in session:
        if is_ajax:
            return jsonify({"success": False, "message": "Session expired. Please login again."}), 401
        return redirect(url_for('login.login'))

    item_id = request.form.get('item_id', '').strip()
    to_manager_id = request.form.get('to_manager_id', '').strip()
    transfer_qty_raw = request.form.get('transfer_qty', '').strip()

    if not item_id or not to_manager_id or not transfer_qty_raw:
        if is_ajax:
            return jsonify({"success": False, "message": "Please fill all transfer fields."}), 400
        flash("Please fill all transfer fields.", "danger")
        return redirect(url_for('manager_inventory.view_manager_inventory'))

    try:
        transfer_qty = int(transfer_qty_raw)
    except ValueError:
        if is_ajax:
            return jsonify({"success": False, "message": "Transfer quantity must be a valid integer."}), 400
        flash("Transfer quantity must be a valid integer.", "danger")
        return redirect(url_for('manager_inventory.view_manager_inventory'))

    if transfer_qty <= 0:
        if is_ajax:
            return jsonify({"success": False, "message": "Transfer quantity must be greater than zero."}), 400
        flash("Transfer quantity must be greater than zero.", "danger")
        return redirect(url_for('manager_inventory.view_manager_inventory'))

    try:
        source_item_id = ObjectId(item_id)
        destination_manager_id = ObjectId(to_manager_id)
        source_manager_id = ObjectId(session['manager_id'])
    except Exception:
        if is_ajax:
            return jsonify({"success": False, "message": "Invalid transfer data provided."}), 400
        flash("Invalid transfer data provided.", "danger")
        return redirect(url_for('manager_inventory.view_manager_inventory'))

    if destination_manager_id == source_manager_id:
        if is_ajax:
            return jsonify({"success": False, "message": "Destination manager must be different from the source manager."}), 400
        flash("Destination manager must be different from the source manager.", "danger")
        return redirect(url_for('manager_inventory.view_manager_inventory'))

    source_item = inventory_col.find_one(
        {"_id": source_item_id, "manager_id": source_manager_id}
    )
    if not source_item:
        if is_ajax:
            return jsonify({"success": False, "message": "Source product not found for this manager."}), 404
        flash("Source product not found for this manager.", "danger")
        return redirect(url_for('manager_inventory.view_manager_inventory'))

    destination_manager = users_col.find_one(
        {"_id": destination_manager_id, "role": "manager"}
    )
    if not destination_manager:
        if is_ajax:
            return jsonify({"success": False, "message": "Destination manager not found."}), 404
        flash("Destination manager not found.", "danger")
        return redirect(url_for('manager_inventory.view_manager_inventory'))

    source_manager = users_col.find_one(
        {"_id": source_manager_id, "role": "manager"}
    )

    now = datetime.utcnow()

    updated_source = inventory_col.find_one_and_update(
        {
            "_id": source_item_id,
            "manager_id": source_manager_id,
            "qty": {"$gte": transfer_qty}
        },
        {"$inc": {"qty": -transfer_qty}, "$set": {"updated_at": now}},
        return_document=ReturnDocument.AFTER
    )

    if not updated_source:
        if is_ajax:
            return jsonify({"success": False, "message": "Insufficient stock to complete transfer."}), 409
        flash("Insufficient stock to complete transfer.", "danger")
        return redirect(url_for('manager_inventory.view_manager_inventory'))

    source_after_qty = updated_source.get("qty", 0)
    source_before_qty = source_after_qty + transfer_qty
    inventory_col.update_one(
        {"_id": source_item_id},
        {"$set": {"is_out_of_stock": source_after_qty == 0, "updated_at": now}}
    )

    dest_before_qty = 0
    dest_after_qty = transfer_qty

    try:
        existing_dest = inventory_col.find_one(
            {"manager_id": destination_manager_id, "name": source_item.get("name")}
        )
        if existing_dest:
            dest_before_qty = existing_dest.get("qty", 0)
            updated_dest = inventory_col.find_one_and_update(
                {"_id": existing_dest["_id"]},
                {"$inc": {"qty": transfer_qty}, "$set": {"updated_at": now, "is_out_of_stock": False}},
                return_document=ReturnDocument.AFTER
            )
            dest_after_qty = updated_dest.get("qty", dest_before_qty + transfer_qty)
        else:
            new_doc = dict(source_item)
            new_doc.pop("_id", None)
            new_doc["manager_id"] = destination_manager_id
            new_doc["qty"] = transfer_qty
            new_doc["is_out_of_stock"] = False
            new_doc["created_at"] = now
            new_doc["updated_at"] = now
            inventory_col.insert_one(new_doc)
    except Exception:
        inventory_col.update_one(
            {"_id": source_item_id},
            {"$inc": {"qty": transfer_qty}, "$set": {"updated_at": now}}
        )
        if is_ajax:
            return jsonify({"success": False, "message": "Transfer failed. Please try again."}), 500
        flash("Transfer failed. Please try again.", "danger")
        return redirect(url_for('manager_inventory.view_manager_inventory'))

    source_manager_name = source_manager.get("name") if source_manager else "Unknown"
    source_manager_branch = source_manager.get("branch") if source_manager else ""
    dest_manager_name = destination_manager.get("name") or "Unknown"
    dest_manager_branch = destination_manager.get("branch") or ""

    history_col.insert_one({
        "log_type": "TRANSFER",
        "product_name": source_item.get("name", ""),
        "from_manager_id": source_manager_id,
        "from_manager_name": source_manager_name,
        "from_branch": source_manager_branch,
        "to_manager_id": destination_manager_id,
        "to_manager_name": dest_manager_name,
        "to_branch": dest_manager_branch,
        "qty_moved": transfer_qty,
        "source_before_qty": source_before_qty,
        "source_after_qty": source_after_qty,
        "dest_before_qty": dest_before_qty,
        "dest_after_qty": dest_after_qty,
        "created_at": now,
        "created_by_manager_id": source_manager_id,
        "created_by_name": source_manager_name
    })

    if is_ajax:
        return jsonify({
            "success": True,
            "message": "Transfer successful",
            "payload": {
                "item_id": str(source_item_id),
                "product_name": source_item.get("name", ""),
                "transfer_qty": transfer_qty,
                "source_before_qty": source_before_qty,
                "source_after_qty": source_after_qty,
                "dest_manager_id": str(destination_manager_id),
                "dest_manager_name": dest_manager_name,
                "dest_before_qty": dest_before_qty,
                "dest_after_qty": dest_after_qty
            }
        })

    flash("Transfer completed successfully.", "success")
    return redirect(url_for('manager_inventory.view_manager_inventory'))


@manager_inventory_bp.route('/manager/inventory/transfer_history')
def transfer_history():
    if 'manager_id' not in session:
        return jsonify({"success": False, "message": "Session expired. Please login again."}), 401

    try:
        manager_id = ObjectId(session['manager_id'])
    except Exception:
        return jsonify({"success": False, "message": "Invalid session."}), 400

    logs = list(
        history_col.find(
            {
                "log_type": "TRANSFER",
                "$or": [
                    {"from_manager_id": manager_id},
                    {"to_manager_id": manager_id}
                ]
            }
        ).sort("created_at", -1)
    )

    payload = []
    for log in logs:
        direction = "OUT" if log.get("from_manager_id") == manager_id else "IN"
        payload.append({
            "log_type": log.get("log_type", ""),
            "product_name": log.get("product_name", ""),
            "direction": direction,
            "qty_moved": log.get("qty_moved", 0),
            "from_manager_name": log.get("from_manager_name", ""),
            "from_branch": log.get("from_branch", ""),
            "to_manager_name": log.get("to_manager_name", ""),
            "to_branch": log.get("to_branch", ""),
            "source_before_qty": log.get("source_before_qty", 0),
            "source_after_qty": log.get("source_after_qty", 0),
            "dest_before_qty": log.get("dest_before_qty", 0),
            "dest_after_qty": log.get("dest_after_qty", 0),
            "created_at": log.get("created_at").isoformat() if log.get("created_at") else ""
        })

    return jsonify(payload)

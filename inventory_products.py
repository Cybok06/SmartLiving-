from flask import Blueprint, render_template, request, redirect, url_for, flash, session, jsonify
from bson.objectid import ObjectId, InvalidId
from typing import Any, Dict, List
import threading
import re, os
from datetime import datetime, timedelta
from werkzeug.utils import secure_filename
from db import db

inventory_products_bp = Blueprint('inventory_products', __name__)

inventory_col       = db.inventory
users_col           = db.users
inventory_logs_col  = db.inventory_logs
deleted_col         = db.deleted
stock_closings_col = db.stock_closings
stock_closing_lines_col = db.stock_closing_lines
inventory_closings_col = db.inventory_closings
closing_jobs_col = db.closing_jobs
settings_col = db.inventory_settings

DEFAULT_REORDER_LEVEL = 20

UPLOAD_FOLDER = os.path.join(os.getcwd(), 'uploads')
ALLOWED_EXTENSIONS = {'png', 'jpg', 'jpeg', 'gif'}
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

# -----------------------------
# Closing indexes
# -----------------------------
def _ensure_closing_indexes():
    try:
        inventory_closings_col.create_index([("year", 1), ("manager_id", 1)])
        inventory_closings_col.create_index([("year", 1), ("branch_name", 1)])
        inventory_closings_col.create_index([("status", 1)])
        closing_jobs_col.create_index([("created_at", -1)])
        stock_closings_col.create_index([("closing_year", 1)])
        stock_closings_col.create_index([("status", 1)])
        stock_closing_lines_col.create_index([("closing_year", 1), ("manager_id", 1)])
    except Exception:
        pass

_ensure_closing_indexes()


def _ensure_settings_indexes():
    try:
        settings_col.create_index([("manager_id", 1)])
        settings_col.create_index([("updated_at", -1)])
    except Exception:
        pass


_ensure_settings_indexes()

# -----------------------------
# Helpers
# -----------------------------
def allowed_file(filename: str) -> bool:
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def safe_float(val):
    if val is None:
        return None
    s = str(val).strip()
    if s == "":
        return None
    try:
        return float(s)
    except Exception:
        return None

def safe_int(val):
    if val is None:
        return None
    s = str(val).strip()
    if s == "":
        return None
    try:
        return int(s)
    except Exception:
        return None

def get_reorder_level(manager_id=None):
    """
    Resolve reorder level:
    manager-specific -> global (manager_id=None) -> DEFAULT_REORDER_LEVEL.
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
    try:
        level_int = int(level)
    except (TypeError, ValueError):
        level_int = DEFAULT_REORDER_LEVEL
    if level_int < 0:
        level_int = DEFAULT_REORDER_LEVEL
    return level_int

def money2(v):
    if v is None:
        return None
    try:
        return round(float(v), 2)
    except Exception:
        return None

def to_oid(val):
    try:
        return ObjectId(val)
    except (InvalidId, TypeError):
        return None

def parse_date_yyyy_mm_dd(val: str):
    """Parse YYYY-MM-DD to datetime at midnight. Return None if empty/invalid."""
    if val is None:
        return None
    s = str(val).strip()
    if s == "":
        return None
    try:
        return datetime.strptime(s, "%Y-%m-%d")
    except Exception:
        return None

def fmt_date_yyyy_mm_dd(dtval):
    if not dtval:
        return ""
    if isinstance(dtval, datetime):
        return dtval.strftime("%Y-%m-%d")
    try:
        return str(dtval)[:10]
    except Exception:
        return ""

def expiry_meta(expiry_dt: datetime | None):
    """Return flags + days_to_expiry based on UTC day boundary."""
    today0 = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    if not expiry_dt:
        return {
            "expiry_date": None,
            "expiry_date_str": None,
            "days_to_expiry": None,
            "is_expired": False,
            "expiring_soon_7": False,
            "expiring_soon_30": False,
            "expiry_priority": 4,  # no expiry -> last
        }

    exp0 = expiry_dt.replace(hour=0, minute=0, second=0, microsecond=0)
    days = (exp0 - today0).days

    is_expired = days < 0
    exp7  = (0 <= days <= 7)
    exp30 = (0 <= days <= 30)

    # priority order:
    # 0 expired, 1 expiring <=7, 2 expiring <=30, 3 valid, 4 no expiry
    if is_expired:
        pr = 0
    elif exp7:
        pr = 1
    elif exp30:
        pr = 2
    else:
        pr = 3

    return {
        "expiry_date": expiry_dt,
        "expiry_date_str": fmt_date_yyyy_mm_dd(expiry_dt),
        "days_to_expiry": days,
        "is_expired": is_expired,
        "expiring_soon_7": exp7,
        "expiring_soon_30": exp30,
        "expiry_priority": pr,
    }


def _inventory_role_ok() -> bool:
    return bool(
        session.get("admin_id")
        or session.get("executive_id")
        or session.get("inventory_id")
        or session.get("role") == "inventory"
    )


def _fetch_inventory_list(manager_query: str, branch_query: str, product_query: str,
                          low_stock_filter: bool, limit: int, offset: int):
    reorder_level = get_reorder_level(None)
    if limit <= 0:
        limit = 50
    if offset < 0:
        offset = 0

    pipeline = [
        {"$lookup": {"from": "users", "localField": "manager_id", "foreignField": "_id", "as": "manager"}},
        {"$unwind": "$manager"},
    ]

    filters = {}
    if manager_query:
        filters["manager.name"] = {"$regex": re.escape(manager_query), "$options": "i"}
    if branch_query:
        filters["manager.branch"] = {"$regex": re.escape(branch_query), "$options": "i"}
    if product_query:
        filters["name"] = {"$regex": re.escape(product_query), "$options": "i"}

    if filters:
        pipeline.append({"$match": filters})

    if low_stock_filter:
        pipeline.append({
            "$match": {
                "$or": [
                    {"qty": {"$lte": reorder_level}},
                    {"qty": None},
                    {"qty": {"$exists": False}},
                ]
            }
        })

    total_cursor = inventory_col.aggregate(pipeline + [{"$count": "count"}])
    total_count = next(total_cursor, {}).get("count", 0)

    page_pipeline = pipeline + [
        {"$sort": {
            "is_expired": -1,
            "expiring_soon_7": -1,
            "expiring_soon_30": -1,
            "expiry_date": 1,
            "manager.branch": 1,
            "name": 1
        }},
        {"$skip": offset},
        {"$limit": limit}
    ]
    raw_inventory = list(inventory_col.aggregate(page_pipeline))

    for it in raw_inventory:
        exp_dt = it.get("expiry_date")
        exp_str = it.get("expiry_date_str") or fmt_date_yyyy_mm_dd(exp_dt) or None
        exp_dt2 = parse_date_yyyy_mm_dd(exp_str) if (not exp_dt and exp_str) else exp_dt
        meta = expiry_meta(exp_dt2)

        it["expiry_date"] = meta["expiry_date"]
        it["expiry_date_str"] = meta["expiry_date_str"]
        it["days_to_expiry"] = meta["days_to_expiry"]
        it["is_expired"] = meta["is_expired"]
        it["expiring_soon_7"] = meta["expiring_soon_7"]
        it["expiring_soon_30"] = meta["expiring_soon_30"]
        it["expiry_priority"] = meta["expiry_priority"]

    group_products = not manager_query and not branch_query
    if group_products:
        grouped = {}
        for item in raw_inventory:
            key = (
                item.get("name"),
                item.get("price"),
                item.get("selling_price"),
                item.get("cost_price"),
                item.get("description"),
                item.get("image_url")
            )
            if key not in grouped:
                grouped[key] = {
                    "_id": item["_id"],
                    "name": item.get("name"),
                    "price": item.get("price"),
                    "selling_price": item.get("selling_price"),
                    "cost_price": item.get("cost_price"),
                    "margin": item.get("margin"),
                    "description": item.get("description"),
                    "image_url": item.get("image_url"),
                    "qty": item.get("qty") or 0,
                    "manager": {"name": "Multiple", "branch": "All Branches"},
                    "expiry_date": item.get("expiry_date"),
                    "expiry_date_str": item.get("expiry_date_str"),
                    "days_to_expiry": item.get("days_to_expiry"),
                    "is_expired": item.get("is_expired", False),
                    "expiring_soon_7": item.get("expiring_soon_7", False),
                    "expiring_soon_30": item.get("expiring_soon_30", False),
                    "expiry_priority": item.get("expiry_priority", 4),
                }
            else:
                grouped[key]["qty"] += (item.get("qty") or 0)

                cur_dt = grouped[key].get("expiry_date")
                new_dt = item.get("expiry_date")
                if (new_dt and (not cur_dt or new_dt < cur_dt)):
                    meta = expiry_meta(new_dt)
                    grouped[key]["expiry_date"] = meta["expiry_date"]
                    grouped[key]["expiry_date_str"] = meta["expiry_date_str"]
                    grouped[key]["days_to_expiry"] = meta["days_to_expiry"]
                    grouped[key]["is_expired"] = meta["is_expired"]
                    grouped[key]["expiring_soon_7"] = meta["expiring_soon_7"]
                    grouped[key]["expiring_soon_30"] = meta["expiring_soon_30"]
                    grouped[key]["expiry_priority"] = meta["expiry_priority"]

        inventory = list(grouped.values())

        inventory.sort(key=lambda x: (
            x.get("expiry_priority", 4),
            x.get("expiry_date") or datetime(9999, 12, 31),
            x.get("name") or ""
        ))
    else:
        inventory = raw_inventory

    if group_products:
        group_stage = {
            "$group": {
                "_id": {
                    "sku": {"$ifNull": ["$sku", ""]},
                    "name_key": {"$toLower": {"$ifNull": ["$name", ""]}},
                    "name": {"$ifNull": ["$name", "Unnamed"]},
                    "description": {"$ifNull": ["$description", ""]},
                    "image_url": {"$ifNull": ["$image_url", ""]},
                    "price": {"$ifNull": ["$price", 0]},
                    "selling_price": {"$ifNull": ["$selling_price", 0]},
                    "cost_price": {"$ifNull": ["$cost_price", 0]},
                },
                "anchor_id": {"$first": "$_id"},
                "qty": {"$sum": {"$ifNull": ["$qty", 0]}},
                "margin": {"$avg": {"$ifNull": ["$margin", 0]}},
                "expiry_date": {"$min": "$expiry_date"},
                "expiry_date_str": {"$first": "$expiry_date_str"},
                "is_expired": {"$max": {"$ifNull": ["$is_expired", False]}},
                "expiring_soon_7": {"$max": {"$ifNull": ["$expiring_soon_7", False]}},
                "expiring_soon_30": {"$max": {"$ifNull": ["$expiring_soon_30", False]}},
                "expiry_priority": {"$min": {"$ifNull": ["$expiry_priority", 4]}},
            }
        }
        grouped_pipeline = list(pipeline) + [group_stage]
        if low_stock_filter:
            grouped_pipeline.append({"$match": {"qty": {"$lte": reorder_level}}})
        total_cursor = inventory_col.aggregate(grouped_pipeline + [{"$count": "count"}])
        total_count = next(total_cursor, {}).get("count", 0)
        page_pipeline = grouped_pipeline + [
            {"$sort": {
                "expiry_priority": 1,
                "expiry_date": 1,
                "_id.name": 1,
            }},
            {"$skip": offset},
            {"$limit": limit}
        ]
        grouped_raw = list(inventory_col.aggregate(page_pipeline))
        inventory = []
        for doc in grouped_raw:
            payload = doc.get("_id", {})
            exp_dt = doc.get("expiry_date")
            exp_str = doc.get("expiry_date_str") or fmt_date_yyyy_mm_dd(exp_dt) or None
            exp_dt2 = parse_date_yyyy_mm_dd(exp_str) if (not exp_dt and exp_str) else exp_dt
            meta = expiry_meta(exp_dt2)
            inventory.append({
                "_id": str(doc.get("anchor_id") or payload.get("sku") or payload.get("name_key") or ""),
                "name": payload.get("name"),
                "description": payload.get("description"),
                "image_url": payload.get("image_url"),
                "price": payload.get("price"),
                "selling_price": payload.get("selling_price"),
                "cost_price": payload.get("cost_price"),
                "margin": doc.get("margin"),
                "qty": doc.get("qty", 0),
                "manager": {"name": "Multiple", "branch": "All Branches"},
                "expiry_date": meta["expiry_date"],
                "expiry_date_str": meta["expiry_date_str"],
                "days_to_expiry": meta["days_to_expiry"],
                "is_expired": meta["is_expired"],
                "expiring_soon_7": meta["expiring_soon_7"],
                "expiring_soon_30": meta["expiring_soon_30"],
                "expiry_priority": meta["expiry_priority"],
            })

    return inventory, total_count, group_products, reorder_level


def _serialize_item_for_card(item: Dict[str, Any], reorder_level: int, grouped: bool):
    qty_val = item.get("qty") or 0
    manager = item.get("manager") or {}
    return {
        "_id": str(item.get("_id") or ""),
        "name": item.get("name") or "",
        "description": item.get("description") or "",
        "image_url": item.get("image_url") or "",
        "qty": qty_val,
        "price": item.get("price"),
        "selling_price": item.get("selling_price"),
        "cost_price": item.get("cost_price"),
        "margin": item.get("margin"),
        "manager_name": manager.get("name") or "",
        "branch_name": manager.get("branch") or "",
        "is_low_stock": qty_val <= reorder_level,
        "expiry_date_str": item.get("expiry_date_str") or "",
        "is_expired": bool(item.get("is_expired")),
        "expiring_soon_7": bool(item.get("expiring_soon_7")),
        "expiring_soon_30": bool(item.get("expiring_soon_30")),
        "grouped": bool(grouped),
    }

# -----------------------------
# Stock Close/Open helpers
# -----------------------------
STOCK_PASSCODE = "3625"


def _passcode_ok(val: str | None) -> bool:
    return (val or "").strip() == STOCK_PASSCODE


def _stock_log(col, doc_id, message: str, level: str = "info"):
    col.update_one(
        {"_id": doc_id},
        {"$push": {"logs": {"ts": datetime.utcnow(), "level": level, "message": message}}},
    )


def _process_stock_closing(closing_id, year: int, user_id: str, user_name: str):
    try:
        stock_closings_col.update_one({"_id": closing_id}, {"$set": {"step": "validating"}})
        _stock_log(stock_closings_col, closing_id, f"Starting closing for {year}", "info")

        stock_closings_col.update_one({"_id": closing_id}, {"$set": {"step": "fetching"}})
        items = list(
            inventory_col.find(
                {},
                {
                    "manager_id": 1,
                    "name": 1,
                    "qty": 1,
                    "cost_price": 1,
                    "initial_price": 1,
                    "selling_price": 1,
                    "price": 1,
                    "image_url": 1,
                },
            )
        )
        items_total = len(items)
        manager_ids = {str(it.get("manager_id")) for it in items if it.get("manager_id")}
        managers_total = len(manager_ids)
        stock_closings_col.update_one(
            {"_id": closing_id},
            {"$set": {"items_count": items_total, "managers_count": managers_total}},
        )

        stock_closings_col.update_one({"_id": closing_id}, {"$set": {"step": "processing"}})
        manager_cache: Dict[str, Dict[str, Any]] = {}
        for m in users_col.find({"role": "manager"}, {"name": 1, "branch": 1, "branch_id": 1}):
            manager_cache[str(m["_id"])] = m

        total_cost = 0.0
        total_selling = 0.0
        items_done = 0
        managers_done = 0

        items_by_manager: Dict[str, List[Dict[str, Any]]] = {}
        for it in items:
            mid = str(it.get("manager_id") or "")
            items_by_manager.setdefault(mid, []).append(it)

        branch_totals: Dict[str, Dict[str, Any]] = {}
        branch_manager_ids: Dict[str, set[str]] = {}

        for mid, mgr_items in items_by_manager.items():
            mgr_doc = manager_cache.get(mid, {})
            manager_name = mgr_doc.get("name", "Unknown")
            branch_name = mgr_doc.get("branch", "")
            branch_id = mgr_doc.get("branch_id", "")
            current = f"{manager_name} ({branch_name})"
            stock_closings_col.update_one(
                {"_id": closing_id},
                {"$set": {"current": current, "current_manager": manager_name, "current_branch": branch_name}},
            )

            line_docs = []
            for it in mgr_items:
                qty = safe_float(it.get("qty")) or 0.0
                cost_price = safe_float(it.get("cost_price"))
                if cost_price is None:
                    cost_price = safe_float(it.get("initial_price")) or 0.0
                selling_price = safe_float(it.get("selling_price"))
                if selling_price is None:
                    selling_price = safe_float(it.get("price")) or 0.0
                closing_cost_value = qty * cost_price
                closing_selling_value = qty * selling_price
                total_cost += closing_cost_value
                total_selling += closing_selling_value
                items_done += 1

                line_docs.append(
                    {
                        "closing_year": year,
                        "manager_id": it.get("manager_id"),
                        "manager_name": manager_name,
                        "branch_id": branch_id,
                        "branch_name": branch_name,
                        "inventory_item_id": it.get("_id"),
                        "name": it.get("name", ""),
                        "qty": qty,
                        "cost_price": cost_price,
                        "selling_price": selling_price,
                        "closing_cost_value": closing_cost_value,
                        "closing_selling_value": closing_selling_value,
                        "image_url": it.get("image_url", ""),
                        "created_at": datetime.utcnow(),
                    }
                )

                branch_key = f"{branch_name}|{branch_id}"
                if branch_key not in branch_totals:
                    branch_totals[branch_key] = {
                        "branch_name": branch_name or "Unknown",
                        "branch_id": branch_id or "",
                        "managers_count": 0,
                        "items_count": 0,
                        "total_qty": 0.0,
                        "closing_cost_value": 0.0,
                        "closing_selling_value": 0.0,
                    }
                    branch_manager_ids[branch_key] = set()
                branch_totals[branch_key]["items_count"] += 1
                branch_totals[branch_key]["total_qty"] += qty
                branch_totals[branch_key]["closing_cost_value"] += closing_cost_value
                branch_totals[branch_key]["closing_selling_value"] += closing_selling_value
                branch_manager_ids[branch_key].add(mid)

            if line_docs:
                stock_closing_lines_col.insert_many(line_docs)

            managers_done += 1
            percent = int((items_done / items_total) * 100) if items_total else 100
            branches_total = len(branch_totals)
            stock_closings_col.update_one(
                {"_id": closing_id},
                {
                    "$set": {
                        "items_processed": items_done,
                        "managers_processed": managers_done,
                        "percent": percent,
                        "branches_total": branches_total,
                    }
                },
            )
            _stock_log(stock_closings_col, closing_id, f"Processed {current}", "info")

        stock_closings_col.update_one({"_id": closing_id}, {"$set": {"step": "saving"}})
        branch_totals_list = []
        for key, data in branch_totals.items():
            data["managers_count"] = len(branch_manager_ids.get(key, set()))
            branch_totals_list.append(data)
        stock_closings_col.update_one(
            {"_id": closing_id},
            {
                "$set": {
                    "status": "completed",
                    "closed_at": datetime.utcnow(),
                    "total_closing_cost_value": total_cost,
                    "total_closing_selling_value": total_selling,
                    "items_processed": items_done,
                    "managers_processed": managers_done,
                    "percent": 100,
                    "step": "finalizing",
                    "current": "",
                    "branch_totals": branch_totals_list,
                    "branches_done": len(branch_totals_list),
                }
            },
        )
        _stock_log(stock_closings_col, closing_id, "Closing completed", "ok")
    except Exception as e:
        stock_closings_col.update_one(
            {"_id": closing_id},
            {"$set": {"status": "failed", "error": str(e), "step": "finalizing"}},
        )
        _stock_log(stock_closings_col, closing_id, f"Closing failed: {e}", "error")

# -----------------------------
# Closing helpers
# -----------------------------
def _closing_targets(scope: str, scope_id: str | None) -> list[dict]:
    query = {"role": "manager"}
    if scope == "branch" and scope_id:
        query["branch"] = scope_id
    elif scope == "manager" and scope_id:
        oid = to_oid(scope_id)
        if oid:
            query["_id"] = oid
        else:
            query["_id"] = scope_id
    return list(users_col.find(query, {"name": 1, "branch": 1, "branch_id": 1}))


def _job_log(job_id, message: str, level: str = "info"):
    closing_jobs_col.update_one(
        {"_id": job_id},
        {"$push": {"logs": {"ts": datetime.utcnow(), "level": level, "message": message}}},
    )


def _process_closing_job(job_id, year: int, targets: list[dict]):
    total = len(targets)
    done = skipped = failed = 0
    for manager in targets:
        manager_id = manager.get("_id")
        manager_name = manager.get("name", "")
        branch_name = manager.get("branch", "") or ""
        branch_id = manager.get("branch_id") or ""
        current = f"{manager_name} ({branch_name})"
        closing_jobs_col.update_one({"_id": job_id}, {"$set": {"current": current}})
        try:
            exists = inventory_closings_col.find_one(
                {"year": year, "manager_id": manager_id, "status": "Closed"}
            )
            if exists:
                skipped += 1
                _job_log(job_id, f"Skipped {current} (already closed)", "skip")
            else:
                items = list(
                    inventory_col.find(
                        {"manager_id": manager_id},
                        {"name": 1, "qty": 1, "cost_price": 1},
                    )
                )
                item_rows = []
                total_cost_value = 0.0
                total_qty = 0.0
                for it in items:
                    qty = safe_float(it.get("qty")) or 0.0
                    cost = safe_float(it.get("cost_price")) or 0.0
                    line_total = qty * cost
                    total_cost_value += line_total
                    total_qty += qty
                    item_rows.append(
                        {
                            "product_id": it.get("_id"),
                            "name": it.get("name", ""),
                            "qty": qty,
                            "cost_price": cost,
                            "line_cost_value": line_total,
                        }
                    )
                inventory_closings_col.insert_one(
                    {
                        "year": year,
                        "manager_id": manager_id,
                        "manager_name": manager_name,
                        "branch_name": branch_name,
                        "branch_id": branch_id,
                        "status": "Closed",
                        "created_at": datetime.utcnow(),
                        "items": item_rows,
                        "items_count": len(item_rows),
                        "total_qty": total_qty,
                        "total_cost_value": total_cost_value,
                    }
                )
                done += 1
                _job_log(job_id, f"Closed {current}: GHS {total_cost_value:.2f}", "ok")
        except Exception as e:
            failed += 1
            _job_log(job_id, f"Failed {current}: {e}", "error")

        processed = done + skipped + failed
        percent = int((processed / total) * 100) if total else 100
        closing_jobs_col.update_one(
            {"_id": job_id},
            {
                "$set": {
                    "done": done,
                    "skipped": skipped,
                    "failed": failed,
                    "percent": percent,
                }
            },
        )

    closing_jobs_col.update_one(
        {"_id": job_id},
        {"$set": {"status": "done", "percent": 100, "current": ""}},
    )

# -----------------------------
# Route: Change Image for Product
# -----------------------------
@inventory_products_bp.route('/inventory_products/change_image/<item_id>', methods=['POST'])
def change_inventory_image(item_id):
    admin_username = session.get('username', 'Unknown')
    oid = to_oid(item_id)
    if not oid:
        flash("❌ Invalid item ID.", "danger")
        return redirect(url_for('inventory_products.inventory_products'))

    file = request.files.get('image')
    if not file or not allowed_file(file.filename):
        flash("❌ Invalid image file. Only PNG, JPG, JPEG, and GIF allowed.", "danger")
        return redirect(url_for('inventory_products.inventory_products'))

    item = inventory_col.find_one({"_id": oid})
    if not item:
        flash("❌ Item not found.", "danger")
        return redirect(url_for('inventory_products.inventory_products'))

    filename = secure_filename(file.filename)
    base, ext = os.path.splitext(filename)
    save_path = os.path.join(UPLOAD_FOLDER, filename)
    counter = 1
    while os.path.exists(save_path):
        filename = f"{base}_{counter}{ext}"
        save_path = os.path.join(UPLOAD_FOLDER, filename)
        counter += 1
    file.save(save_path)

    image_url = f"/uploads/{filename}"

    name_to_match = item.get('name')
    legacy_price  = item.get('price')
    selling_price = item.get('selling_price')

    or_terms = []
    if legacy_price is not None:
        or_terms.append({"price": legacy_price})
    if selling_price is not None:
        or_terms.append({"selling_price": selling_price})

    q = {"name": name_to_match}
    if or_terms:
        q["$or"] = or_terms

    matched_items = list(inventory_col.find(q))

    now = datetime.utcnow()
    for product in matched_items:
        inventory_col.update_one(
            {'_id': product['_id']},
            {'$set': {'image_url': image_url, 'updated_at': now}}
        )
        inventory_logs_col.insert_one({
            'product_id': product['_id'],
            'product_name': product.get('name'),
            'action': 'image_update',
            'log_type': 'image_update',
            'old_image_url': product.get('image_url'),
            'new_image_url': image_url,
            'updated_by': admin_username,
            'updated_at': now
        })

    flash(f"✅ Image updated for {len(matched_items)} matching products.", "success")
    return redirect(url_for('inventory_products.inventory_products'))


@inventory_products_bp.route('/inventory/settings/reorder-level', methods=['POST'])
def inventory_reorder_level():
    if not (session.get("admin_id") or session.get("executive_id") or session.get("inventory_id") or session.get("role") == "inventory"):
        return jsonify(ok=False, message="Unauthorized"), 403

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

    updated_by = session.get("username") or session.get("inventory_name") or "Unknown"
    settings_col.update_one(
        {"manager_id": None},
        {
            "$set": {
                "manager_id": None,
                "reorder_level": level,
                "updated_by": updated_by,
                "updated_at": datetime.utcnow(),
            }
        },
        upsert=True
    )

    return jsonify(ok=True, reorder_level=level)


@inventory_products_bp.route("/inventory/settings/reset-qty", methods=["POST"])
def inventory_reset_qty():
    if not (session.get("admin_id") or session.get("executive_id") or session.get("inventory_id") or session.get("role") == "inventory"):
        return jsonify(ok=False, message="Unauthorized"), 403

    payload = request.get_json(silent=True) or {}
    branches = payload.get("branches") or []
    confirm_text = (payload.get("confirm_text") or "").strip().upper()
    passcode = (payload.get("passcode") or "").strip()

    if not isinstance(branches, list) or not branches:
        return jsonify(ok=False, message="Select at least one branch."), 400
    if confirm_text != "RESET":
        return jsonify(ok=False, message="Confirmation text must be RESET."), 400
    if passcode != STOCK_PASSCODE:
        return jsonify(ok=False, message="Invalid passcode."), 400

    valid_branches = list(users_col.distinct("branch", {"role": "manager", "branch": {"$in": branches}}))
    if not valid_branches:
        return jsonify(ok=False, message="No valid branches found."), 400

    manager_ids = list(users_col.distinct("_id", {"role": "manager", "branch": {"$in": valid_branches}}))
    if not manager_ids:
        return jsonify(ok=False, message="No managers found for selected branches."), 400

    now = datetime.utcnow()
    updated_by = session.get("username") or session.get("inventory_name") or "Unknown"

    affected = list(
        inventory_col.find(
            {"manager_id": {"$in": manager_ids}},
            {"_id": 1, "name": 1, "qty": 1, "manager_id": 1},
        )
    )
    result = inventory_col.update_many(
        {"manager_id": {"$in": manager_ids}},
        {"$set": {"qty": 0, "updated_at": now}},
    )

    if affected:
        manager_branch_map = {
            str(u["_id"]): u.get("branch")
            for u in users_col.find({"_id": {"$in": manager_ids}}, {"branch": 1})
        }
        log_docs = []
        for row in affected:
            mid = row.get("manager_id")
            log_docs.append(
                {
                    "product_id": row.get("_id"),
                    "product_name": row.get("name", ""),
                    "action": "reset_qty",
                    "log_type": "reset_qty",
                    "branch": manager_branch_map.get(str(mid), ""),
                    "manager_id": mid,
                    "old_qty": row.get("qty"),
                    "new_qty": 0,
                    "updated_by": updated_by,
                    "updated_at": now,
                    "meta": {"branches": valid_branches, "reason": "settings_reset"},
                }
            )
        if log_docs:
            inventory_logs_col.insert_many(log_docs)

    inventory_logs_col.insert_one(
        {
            "action": "reset_qty_bulk",
            "log_type": "reset_qty",
            "branches": valid_branches,
            "matched_count": int(result.matched_count),
            "modified_count": int(result.modified_count),
            "updated_by": updated_by,
            "updated_at": now,
        }
    )

    return jsonify(
        ok=True,
        branches=valid_branches,
        matched_count=int(result.matched_count),
        modified_count=int(result.modified_count),
    )


@inventory_products_bp.route('/inventory/low-stocks/count')
def inventory_low_stocks_count():
    if not (session.get("admin_id") or session.get("executive_id") or session.get("inventory_id") or session.get("role") == "inventory"):
        return jsonify(ok=False, message="Unauthorized"), 403

    reorder_level = get_reorder_level(None)
    low_stock_query = {
        "$or": [
            {"qty": {"$lte": reorder_level}},
            {"qty": None},
            {"qty": {"$exists": False}},
        ],
    }
    count = inventory_col.count_documents(low_stock_query)
    return jsonify(ok=True, reorder_level=reorder_level, low_stock_count=count)

# -----------------------------
# JSON APIs (AJAX)
# -----------------------------
@inventory_products_bp.route('/inventory_products/api/list', methods=['GET'])
def inventory_products_api_list():
    if not _inventory_role_ok():
        return jsonify(ok=False, message="Unauthorized"), 403

    manager_query = (request.args.get('manager') or '').strip()
    branch_query = (request.args.get('branch') or '').strip()
    product_query = (request.args.get('product') or '').strip()
    low_stock_filter = (request.args.get('low_stock') or '').strip().lower() in ("1", "true", "yes", "on")
    limit = safe_int(request.args.get('limit')) or 50
    offset = safe_int(request.args.get('offset')) or 0

    inventory, total_count, group_products, reorder_level = _fetch_inventory_list(
        manager_query, branch_query, product_query, low_stock_filter, limit, offset
    )
    items = [_serialize_item_for_card(item, reorder_level, group_products) for item in inventory]
    has_more = (offset + limit) < total_count

    return jsonify(
        ok=True,
        items=items,
        total_count=total_count,
        offset=offset,
        limit=limit,
        has_more=has_more,
        reorder_level=reorder_level,
        group_products=group_products,
        active_filters={
            "manager": manager_query,
            "branch": branch_query,
            "product": product_query,
            "low_stock": low_stock_filter,
        }
    )


@inventory_products_bp.route('/inventory_products/api/update', methods=['POST'])
def inventory_products_api_update():
    if not _inventory_role_ok():
        return jsonify(ok=False, message="Unauthorized"), 403

    payload = request.get_json(silent=True) or {}
    item_id = payload.get('item_id')
    branches = payload.get('branches') or []
    admin_username = session.get('username', 'Unknown')

    if not branches:
        return jsonify(ok=False, message="Please select at least one branch."), 400

    anchor_oid = to_oid(item_id)
    if not anchor_oid:
        return jsonify(ok=False, message="Invalid item ID."), 400

    anchor_item = next(inventory_col.aggregate([
        {"$lookup": {"from": "users", "localField": "manager_id", "foreignField": "_id", "as": "manager"}},
        {"$unwind": "$manager"},
        {"$match": {"_id": anchor_oid}}
    ]), None)
    if not anchor_item:
        return jsonify(ok=False, message="Item not found."), 404

    name_to_match = anchor_item['name']
    matched_items = list(inventory_col.aggregate([
        {"$lookup": {"from": "users", "localField": "manager_id", "foreignField": "_id", "as": "manager"}},
        {"$unwind": "$manager"},
        {"$match": {"name": name_to_match, "manager.branch": {"$in": branches}}}
    ]))

    try:
        new_name = (payload.get('name') or "").strip()
        new_price = safe_float(payload.get('price'))
        new_qty = safe_int(payload.get('qty'))

        new_cost_price = safe_float(payload.get('cost_price'))
        new_selling_price = safe_float(payload.get('selling_price'))

        expiry_str = (payload.get('expiry_date') or '').strip()
        expiry_dt = parse_date_yyyy_mm_dd(expiry_str)

        if not new_name or new_price is None or new_qty is None:
            return jsonify(ok=False, message="Provide valid name, legacy price, and quantity."), 400

        updated_count = 0
        now = datetime.utcnow()

        for product in matched_items:
            updates = {
                'name': new_name,
                'price': money2(new_price),
                'qty': new_qty,
                'updated_at': now
            }

            old_cost = product.get('cost_price')
            old_sell = product.get('selling_price')
            old_margin = product.get('margin')

            old_exp_dt = product.get('expiry_date')
            old_exp_str = product.get('expiry_date_str') or fmt_date_yyyy_mm_dd(old_exp_dt)

            if new_cost_price is not None:
                updates['cost_price'] = money2(new_cost_price)
            if new_selling_price is not None:
                updates['selling_price'] = money2(new_selling_price)

            if ('cost_price' in updates) or ('selling_price' in updates):
                c = updates.get('cost_price', old_cost)
                s = updates.get('selling_price', old_sell)
                if c is not None and s is not None:
                    updates['margin'] = money2(s - c)

            if expiry_str == "":
                updates['expiry_date'] = None
                updates['expiry_date_str'] = None
                updates['is_expired'] = False
                updates['expiring_soon_7'] = False
                updates['expiring_soon_30'] = False
            else:
                if not expiry_dt:
                    return jsonify(ok=False, message="Expiry date is invalid. Use YYYY-MM-DD."), 400

                meta = expiry_meta(expiry_dt)
                updates['expiry_date'] = meta['expiry_date']
                updates['expiry_date_str'] = meta['expiry_date_str']
                updates['is_expired'] = meta['is_expired']
                updates['expiring_soon_7'] = meta['expiring_soon_7']
                updates['expiring_soon_30'] = meta['expiring_soon_30']

            inventory_col.update_one({'_id': product['_id']}, {'$set': updates})
            updated_count += 1

            new_exp_str = updates.get('expiry_date_str', None)
            log_doc = {
                'product_id': product['_id'],
                'product_name': new_name,
                'log_type': 'update',
                'old_name': product.get('name'),
                'new_name': new_name,
                'old_price': product.get('price'),
                'new_price': money2(new_price),
                'old_qty': product.get('qty'),
                'new_qty': new_qty,

                'old_cost_price': old_cost,
                'new_cost_price': updates.get('cost_price', old_cost),
                'old_selling_price': old_sell,
                'new_selling_price': updates.get('selling_price', old_sell),
                'old_margin': old_margin,
                'new_margin': updates.get('margin', old_margin),

                'old_expiry_date': old_exp_str or None,
                'new_expiry_date': new_exp_str,

                'updated_by': admin_username,
                'action': 'update',
                'updated_at': now
            }
            inventory_logs_col.insert_one(log_doc)

        updated_anchor = next(inventory_col.aggregate([
            {"$lookup": {"from": "users", "localField": "manager_id", "foreignField": "_id", "as": "manager"}},
            {"$unwind": "$manager"},
            {"$match": {"_id": anchor_oid}}
        ]), None)
        if updated_anchor:
            exp_dt = updated_anchor.get("expiry_date")
            exp_str = updated_anchor.get("expiry_date_str") or fmt_date_yyyy_mm_dd(exp_dt) or None
            exp_dt2 = parse_date_yyyy_mm_dd(exp_str) if (not exp_dt and exp_str) else exp_dt
            meta = expiry_meta(exp_dt2)
            updated_anchor["expiry_date"] = meta["expiry_date"]
            updated_anchor["expiry_date_str"] = meta["expiry_date_str"]
            updated_anchor["is_expired"] = meta["is_expired"]
            updated_anchor["expiring_soon_7"] = meta["expiring_soon_7"]
            updated_anchor["expiring_soon_30"] = meta["expiring_soon_30"]

        reorder_level = get_reorder_level(None)
        return jsonify(
            ok=True,
            updated_count=updated_count,
            updated_item=_serialize_item_for_card(updated_anchor, reorder_level, False) if updated_anchor else None
        )
    except Exception as exc:
        return jsonify(ok=False, message=str(exc)), 500


@inventory_products_bp.route('/inventory_products/api/delete', methods=['POST'])
def inventory_products_api_delete():
    if not _inventory_role_ok():
        return jsonify(ok=False, message="Unauthorized"), 403

    payload = request.get_json(silent=True) or {}
    item_id = payload.get('item_id')
    branches = payload.get('branches') or []
    admin_username = session.get('username', 'Unknown')

    if not branches:
        return jsonify(ok=False, message="Please select at least one branch."), 400

    anchor_oid = to_oid(item_id)
    if not anchor_oid:
        return jsonify(ok=False, message="Invalid item ID."), 400

    anchor_item = next(inventory_col.aggregate([
        {"$lookup": {"from": "users", "localField": "manager_id", "foreignField": "_id", "as": "manager"}},
        {"$unwind": "$manager"},
        {"$match": {"_id": anchor_oid}}
    ]), None)
    if not anchor_item:
        return jsonify(ok=False, message="Item not found."), 404

    name_to_match = anchor_item['name']
    matched_items = list(inventory_col.aggregate([
        {"$lookup": {"from": "users", "localField": "manager_id", "foreignField": "_id", "as": "manager"}},
        {"$unwind": "$manager"},
        {"$match": {"name": name_to_match, "manager.branch": {"$in": branches}}}
    ]))

    deleted_count = 0
    now = datetime.utcnow()
    for product in matched_items:
        inventory_logs_col.insert_one({
            'product_id': product['_id'],
            'product_name': product.get('name'),
            'price': product.get('price'),
            'qty': product.get('qty'),
            'cost_price': product.get('cost_price'),
            'selling_price': product.get('selling_price'),
            'margin': product.get('margin'),
            'expiry_date': product.get('expiry_date_str') or fmt_date_yyyy_mm_dd(product.get('expiry_date')) or None,
            'deleted_by': admin_username,
            'action': 'delete',
            'log_type': 'delete',
            'deleted_at': now,
            'updated_at': now
        })
        deleted_col.insert_one({
            'deleted_item': product,
            'deleted_by': admin_username,
            'deleted_at': now
        })
        inventory_col.delete_one({'_id': product['_id']})
        deleted_count += 1

    return jsonify(ok=True, deleted_count=deleted_count)


@inventory_products_bp.route('/inventory_products/api/transfer', methods=['POST'])
def inventory_products_api_transfer():
    if not _inventory_role_ok():
        return jsonify(ok=False, message="Unauthorized"), 403

    payload = request.get_json(silent=True) or {}
    item_id = payload.get('item_id')
    transfer_qty = safe_int(payload.get('transfer_qty'))
    to_branch = (payload.get('to_branch') or '').strip()
    admin_username = session.get('username', 'Unknown')

    src_oid = to_oid(item_id)
    if not src_oid:
        return jsonify(ok=False, message="Invalid item ID for transfer."), 400

    if not transfer_qty or transfer_qty <= 0:
        return jsonify(ok=False, message="Enter a valid transfer quantity (> 0)."), 400

    if not to_branch:
        return jsonify(ok=False, message="Please select a destination branch."), 400

    src_item = next(inventory_col.aggregate([
        {"$lookup": {"from": "users", "localField": "manager_id", "foreignField": "_id", "as": "manager"}},
        {"$unwind": "$manager"},
        {"$match": {"_id": src_oid}}
    ]), None)
    if not src_item:
        return jsonify(ok=False, message="Source item not found for transfer."), 404

    from_branch = src_item["manager"]["branch"]
    if to_branch == from_branch:
        return jsonify(ok=False, message="Destination branch cannot be the same as source branch."), 400

    src_current_qty = src_item.get("qty") or 0
    if transfer_qty > src_current_qty:
        return jsonify(
            ok=False,
            message=f"Cannot transfer {transfer_qty}. Only {src_current_qty} available in {from_branch}."
        ), 400

    dest_item = next(inventory_col.aggregate([
        {"$lookup": {"from": "users", "localField": "manager_id", "foreignField": "_id", "as": "manager"}},
        {"$unwind": "$manager"},
        {"$match": {"name": src_item["name"], "manager.branch": to_branch}}
    ]), None)
    if not dest_item:
        return jsonify(
            ok=False,
            message=f"No matching product found in destination branch '{to_branch}'."
        ), 404

    dest_current_qty = dest_item.get("qty") or 0
    now = datetime.utcnow()
    new_src_qty = src_current_qty - transfer_qty
    new_dest_qty = dest_current_qty + transfer_qty

    inventory_col.update_one({"_id": src_item["_id"]}, {"$set": {"qty": new_src_qty, "updated_at": now}})
    inventory_col.update_one({"_id": dest_item["_id"]}, {"$set": {"qty": new_dest_qty, "updated_at": now}})

    inventory_logs_col.insert_one({
        "product_id": src_item["_id"],
        "product_name": src_item.get("name"),
        "action": "transfer_out",
        "log_type": "transfer",
        "from_branch": from_branch,
        "to_branch": to_branch,
        "qty_moved": transfer_qty,
        "old_qty": src_current_qty,
        "new_qty": new_src_qty,
        "updated_by": admin_username,
        "updated_at": now
    })
    inventory_logs_col.insert_one({
        "product_id": dest_item["_id"],
        "product_name": dest_item.get("name"),
        "action": "transfer_in",
        "log_type": "transfer",
        "from_branch": from_branch,
        "to_branch": to_branch,
        "qty_moved": transfer_qty,
        "old_qty": dest_current_qty,
        "new_qty": new_dest_qty,
        "updated_by": admin_username,
        "updated_at": now
    })

    return jsonify(
        ok=True,
        from_branch=from_branch,
        to_branch=to_branch,
        new_src_qty=new_src_qty,
        new_dest_qty=new_dest_qty,
        product_name=src_item.get("name")
    )


@inventory_products_bp.route('/inventory_products/api/change-image/<item_id>', methods=['POST'])
def inventory_products_api_change_image(item_id):
    if not _inventory_role_ok():
        return jsonify(ok=False, message="Unauthorized"), 403

    admin_username = session.get('username', 'Unknown')
    oid = to_oid(item_id)
    if not oid:
        return jsonify(ok=False, message="Invalid item ID."), 400

    file = request.files.get('image')
    if not file or not allowed_file(file.filename):
        return jsonify(ok=False, message="Invalid image file. Only PNG, JPG, JPEG, and GIF allowed."), 400

    item = inventory_col.find_one({"_id": oid})
    if not item:
        return jsonify(ok=False, message="Item not found."), 404

    filename = secure_filename(file.filename)
    base, ext = os.path.splitext(filename)
    save_path = os.path.join(UPLOAD_FOLDER, filename)
    counter = 1
    while os.path.exists(save_path):
        filename = f"{base}_{counter}{ext}"
        save_path = os.path.join(UPLOAD_FOLDER, filename)
        counter += 1
    file.save(save_path)

    image_url = f"/uploads/{filename}"

    name_to_match = item.get('name')
    legacy_price = item.get('price')
    selling_price = item.get('selling_price')

    or_terms = []
    if legacy_price is not None:
        or_terms.append({"price": legacy_price})
    if selling_price is not None:
        or_terms.append({"selling_price": selling_price})

    q = {"name": name_to_match}
    if or_terms:
        q["$or"] = or_terms

    matched_items = list(inventory_col.find(q))

    now = datetime.utcnow()
    for product in matched_items:
        inventory_col.update_one(
            {'_id': product['_id']},
            {'$set': {'image_url': image_url, 'updated_at': now}}
        )
        inventory_logs_col.insert_one({
            'product_id': product['_id'],
            'product_name': product.get('name'),
            'action': 'image_update',
            'log_type': 'image_update',
            'old_image_url': product.get('image_url'),
            'new_image_url': image_url,
            'updated_by': admin_username,
            'updated_at': now
        })

    return jsonify(ok=True, image_url=image_url, updated_count=len(matched_items))

# -----------------------------
# Route: Inventory Management
# -----------------------------
@inventory_products_bp.route('/inventory_products', methods=['GET', 'POST'])
def inventory_products():
    if request.method == 'POST':
        action         = request.form.get('action')
        item_id        = request.form.get('item_id')
        admin_username = session.get('username', 'Unknown')

        # ---------- TRANSFER ----------
        if action == 'transfer':
            transfer_qty = safe_int(request.form.get('transfer_qty'))
            to_branch    = (request.form.get('to_branch') or '').strip()

            src_oid = to_oid(item_id)
            if not src_oid:
                flash("❌ Invalid item ID for transfer.", "danger")
                return redirect(url_for('inventory_products.inventory_products'))

            if not transfer_qty or transfer_qty <= 0:
                flash("❌ Enter a valid transfer quantity (> 0).", "danger")
                return redirect(url_for('inventory_products.inventory_products'))

            if not to_branch:
                flash("❌ Please select a destination branch.", "danger")
                return redirect(url_for('inventory_products.inventory_products'))

            src_item = next(inventory_col.aggregate([
                {"$lookup": {"from":"users","localField":"manager_id","foreignField":"_id","as":"manager"}},
                {"$unwind":"$manager"},
                {"$match":{"_id":src_oid}}
            ]), None)

            if not src_item:
                flash("❌ Source item not found for transfer.", "danger")
                return redirect(url_for('inventory_products.inventory_products'))

            from_branch = src_item["manager"]["branch"]
            if to_branch == from_branch:
                flash("❌ Destination branch cannot be the same as source branch.", "danger")
                return redirect(url_for('inventory_products.inventory_products'))

            src_current_qty = src_item.get("qty") or 0
            if transfer_qty > src_current_qty:
                flash(f"❌ Cannot transfer {transfer_qty}. Only {src_current_qty} available in {from_branch}.", "danger")
                return redirect(url_for('inventory_products.inventory_products'))

            dest_item = next(inventory_col.aggregate([
                {"$lookup": {"from":"users","localField":"manager_id","foreignField":"_id","as":"manager"}},
                {"$unwind":"$manager"},
                {"$match":{"name":src_item["name"], "manager.branch":to_branch}}
            ]), None)

            if not dest_item:
                flash(f"❌ No matching product found in destination branch '{to_branch}'.", "danger")
                return redirect(url_for('inventory_products.inventory_products'))

            dest_current_qty = dest_item.get("qty") or 0

            now = datetime.utcnow()
            new_src_qty  = src_current_qty - transfer_qty
            new_dest_qty = dest_current_qty + transfer_qty

            inventory_col.update_one({"_id": src_item["_id"]}, {"$set": {"qty": new_src_qty, "updated_at": now}})
            inventory_col.update_one({"_id": dest_item["_id"]}, {"$set": {"qty": new_dest_qty, "updated_at": now}})

            inventory_logs_col.insert_one({
                "product_id": src_item["_id"],
                "product_name": src_item.get("name"),
                "action": "transfer_out",
                "log_type": "transfer",
                "from_branch": from_branch,
                "to_branch": to_branch,
                "qty_moved": transfer_qty,
                "old_qty": src_current_qty,
                "new_qty": new_src_qty,
                "updated_by": admin_username,
                "updated_at": now
            })
            inventory_logs_col.insert_one({
                "product_id": dest_item["_id"],
                "product_name": dest_item.get("name"),
                "action": "transfer_in",
                "log_type": "transfer",
                "from_branch": from_branch,
                "to_branch": to_branch,
                "qty_moved": transfer_qty,
                "old_qty": dest_current_qty,
                "new_qty": new_dest_qty,
                "updated_by": admin_username,
                "updated_at": now
            })

            flash(f"🔁 Transferred {transfer_qty} unit(s) of '{src_item['name']}' from {from_branch} to {to_branch}.", "success")
            return redirect(url_for('inventory_products.inventory_products'))

        # ---------- UPDATE / DELETE ----------
        selected_branches = request.form.getlist('branches')
        if not selected_branches:
            flash("❌ Please select at least one branch.", "danger")
            return redirect(url_for('inventory_products.inventory_products'))

        anchor_oid = to_oid(item_id)
        if not anchor_oid:
            flash("❌ Invalid item ID.", "danger")
            return redirect(url_for('inventory_products.inventory_products'))

        anchor_item = next(inventory_col.aggregate([
            {"$lookup": {"from":"users","localField":"manager_id","foreignField":"_id","as":"manager"}},
            {"$unwind":"$manager"},
            {"$match":{"_id":anchor_oid}}
        ]), None)

        if not anchor_item:
            flash("❌ Item not found.", "danger")
            return redirect(url_for('inventory_products.inventory_products'))

        name_to_match = anchor_item['name']

        matched_items = list(inventory_col.aggregate([
            {"$lookup": {"from":"users","localField":"manager_id","foreignField":"_id","as":"manager"}},
            {"$unwind":"$manager"},
            {"$match":{"name":name_to_match, "manager.branch":{"$in": selected_branches}}}
        ]))

        if action == 'update':
            try:
                new_name  = (request.form.get('name') or "").strip()
                new_price = safe_float(request.form.get('price'))
                new_qty   = safe_int(request.form.get('qty'))

                new_cost_price    = safe_float(request.form.get('cost_price'))
                new_selling_price = safe_float(request.form.get('selling_price'))

                # ✅ NEW: expiry from edit modal
                expiry_str = (request.form.get('expiry_date') or '').strip()
                expiry_dt  = parse_date_yyyy_mm_dd(expiry_str)

                if not new_name or new_price is None or new_qty is None:
                    flash("❌ Provide valid name, legacy price, and quantity.", "danger")
                    return redirect(url_for('inventory_products.inventory_products'))

                updated_count = 0
                now = datetime.utcnow()

                for product in matched_items:
                    updates = {
                        'name': new_name,
                        'price': money2(new_price),
                        'qty': new_qty,
                        'updated_at': now
                    }

                    old_cost   = product.get('cost_price')
                    old_sell   = product.get('selling_price')
                    old_margin = product.get('margin')

                    # old expiry
                    old_exp_dt  = product.get('expiry_date')
                    old_exp_str = product.get('expiry_date_str') or fmt_date_yyyy_mm_dd(old_exp_dt)

                    # modern pricing partial updates
                    if new_cost_price is not None:
                        updates['cost_price'] = money2(new_cost_price)
                    if new_selling_price is not None:
                        updates['selling_price'] = money2(new_selling_price)

                    if ('cost_price' in updates) or ('selling_price' in updates):
                        c = updates.get('cost_price', old_cost)
                        s = updates.get('selling_price', old_sell)
                        if c is not None and s is not None:
                            updates['margin'] = money2(s - c)

                    # ✅ expiry updates (we always allow setting/clearing)
                    # If empty input, clear expiry
                    if expiry_str == "":
                        updates['expiry_date'] = None
                        updates['expiry_date_str'] = None
                        updates['is_expired'] = False
                        updates['expiring_soon_7'] = False
                        updates['expiring_soon_30'] = False
                    else:
                        if not expiry_dt:
                            flash("❌ Expiry date is invalid. Use YYYY-MM-DD.", "danger")
                            return redirect(url_for('inventory_products.inventory_products'))

                        meta = expiry_meta(expiry_dt)
                        updates['expiry_date'] = meta['expiry_date']
                        updates['expiry_date_str'] = meta['expiry_date_str']
                        updates['is_expired'] = meta['is_expired']
                        updates['expiring_soon_7'] = meta['expiring_soon_7']
                        updates['expiring_soon_30'] = meta['expiring_soon_30']

                    inventory_col.update_one({'_id': product['_id']}, {'$set': updates})
                    updated_count += 1

                    # logs
                    new_exp_str = updates.get('expiry_date_str', None)
                    log_doc = {
                        'product_id': product['_id'],
                        'product_name': new_name,
                        'log_type': 'update',
                        'old_name': product.get('name'),
                        'new_name': new_name,
                        'old_price': product.get('price'),
                        'new_price': money2(new_price),
                        'old_qty': product.get('qty'),
                        'new_qty': new_qty,

                        'old_cost_price': old_cost,
                        'new_cost_price': updates.get('cost_price', old_cost),
                        'old_selling_price': old_sell,
                        'new_selling_price': updates.get('selling_price', old_sell),
                        'old_margin': old_margin,
                        'new_margin': updates.get('margin', old_margin),

                        # ✅ expiry log fields
                        'old_expiry_date': old_exp_str or None,
                        'new_expiry_date': new_exp_str,

                        'updated_by': admin_username,
                        'action': 'update',
                        'updated_at': now
                    }
                    inventory_logs_col.insert_one(log_doc)

                flash(f"✅ Product updated across {updated_count} selected branch item(s).", "success")

            except Exception as e:
                flash(f"❌ Error: {str(e)}", "danger")

        elif action == 'delete':
            deleted_count = 0
            now = datetime.utcnow()
            for product in matched_items:
                inventory_logs_col.insert_one({
                    'product_id': product['_id'],
                    'product_name': product.get('name'),
                    'price': product.get('price'),
                    'qty': product.get('qty'),
                    'cost_price': product.get('cost_price'),
                    'selling_price': product.get('selling_price'),
                    'margin': product.get('margin'),

                    # ✅ keep expiry in delete log
                    'expiry_date': product.get('expiry_date_str') or fmt_date_yyyy_mm_dd(product.get('expiry_date')) or None,

                    'deleted_by': admin_username,
                    'action': 'delete',
                    'log_type': 'delete',
                    'deleted_at': now,
                    'updated_at': now
                })
                deleted_col.insert_one({
                    'deleted_item': product,
                    'deleted_by': admin_username,
                    'deleted_at': now
                })
                inventory_col.delete_one({'_id': product['_id']})
                deleted_count += 1

            flash(f"🗑️ Product deleted across {deleted_count} selected branch item(s).", "success")

        else:
            flash("❌ Unknown action.", "danger")

        return redirect(url_for('inventory_products.inventory_products'))

    # -----------------------------
    # GET logic (Sorting: expiry first)
    # -----------------------------
    manager_query = (request.args.get('manager') or '').strip()
    branch_query  = (request.args.get('branch') or '').strip()
    product_query = (request.args.get('product') or '').strip()
    low_stock_filter = (request.args.get('low_stock') or '').strip().lower() in ("1", "true", "yes", "on")
    limit         = safe_int(request.args.get('limit')) or 50
    offset        = safe_int(request.args.get('offset')) or 0
    if limit <= 0:
        limit = 50
    if offset < 0:
        offset = 0
    current_page  = (offset // limit) + 1
    inventory, total_count, group_products, reorder_level = _fetch_inventory_list(
        manager_query, branch_query, product_query, low_stock_filter, limit, offset
    )

    manager_names = sorted(users_col.distinct("name", {"role": "manager"}))
    branch_names  = sorted(users_col.distinct("branch", {"role": "manager"}))

    can_close_stock = bool(
        session.get("admin_id")
        or session.get("executive_id")
        or session.get("inventory_id")
        or session.get("role") == "inventory"
    )
    return render_template(
        'inventory_products.html',
        inventory=inventory,
        managers=manager_names,
        branches=branch_names,
        total_count=total_count,
        offset=offset,
        limit=limit,
        current_page=current_page,
        can_close_stock=can_close_stock,
        reorder_level=reorder_level,
        low_stock_filter=low_stock_filter
    )


@inventory_products_bp.route('/inventory_products/distribution', methods=['GET'])
def inventory_products_distribution():
    name = (request.args.get('name') or '').strip()
    if not name:
        return jsonify(ok=False, message="Product name required"), 400
    price = safe_float(request.args.get("price"))
    selling_price = safe_float(request.args.get("selling_price"))
    cost_price = safe_float(request.args.get("cost_price"))
    description = (request.args.get("description") or "").strip()

    match_stage = {
        "name": {"$regex": f"^{re.escape(name)}$", "$options": "i"}
    }
    if price is not None:
        match_stage["price"] = price
    if selling_price is not None:
        match_stage["selling_price"] = selling_price
    if cost_price is not None:
        match_stage["cost_price"] = cost_price
    if description:
        match_stage["description"] = description
    pipeline = [
        {"$match": match_stage},
        {
            "$lookup": {
                "from": "users",
                "localField": "manager_id",
                "foreignField": "_id",
                "as": "manager",
            }
        },
        {
            "$unwind": {
                "path": "$manager",
                "preserveNullAndEmptyArrays": True,
            }
        },
        {
            "$group": {
                "_id": {
                    "branch": {
                        "$ifNull": [
                            {"$ifNull": ["$manager.branch", "$branch"]},
                            "Warehouse",
                        ]
                    }
                },
                "qty": {"$sum": {"$ifNull": ["$qty", 0]}},
                "managers": {"$addToSet": {"$ifNull": ["$manager.name", "Warehouse"]}},
            }
        },
        {"$sort": {"_id.branch": 1}},
    ]

    rows = list(inventory_col.aggregate(pipeline))
    distribution = []
    total_qty = 0
    for row in rows:
        branch_name = row["_id"].get("branch") if isinstance(row["_id"], dict) else row["_id"]
        qty = row.get("qty") or 0
        total_qty += qty
        managers = sorted(filter(None, row.get("managers") or []))
        distribution.append(
            {
                "branch_name": branch_name or "Unknown",
                "qty": qty,
                "manager_names": managers or ["Unknown"],
            }
        )

    return jsonify(ok=True, total_qty=total_qty, distribution=distribution)


@inventory_products_bp.route('/inventory_products/distribution/update-qty', methods=['POST'])
def inventory_products_distribution_update_qty():
    if not (session.get("admin_id") or session.get("executive_id") or session.get("inventory_id") or session.get("role") == "inventory"):
        return jsonify(ok=False, message="Unauthorized"), 403

    payload = request.get_json(silent=True) or {}
    product = payload.get("product") or {}
    updates = payload.get("updates") or []
    if not product or not updates:
        return jsonify(ok=False, message="Product and updates are required."), 400

    name = (product.get("name") or "").strip()
    if not name:
        return jsonify(ok=False, message="Product name required."), 400
    price = safe_float(product.get("price"))
    selling_price = safe_float(product.get("selling_price"))
    cost_price = safe_float(product.get("cost_price"))
    description = (product.get("description") or "").strip()

    match_stage = {
        "name": {"$regex": f"^{re.escape(name)}$", "$options": "i"}
    }
    if price is not None:
        match_stage["price"] = price
    if selling_price is not None:
        match_stage["selling_price"] = selling_price
    if cost_price is not None:
        match_stage["cost_price"] = cost_price
    if description:
        match_stage["description"] = description

    product_filters = {"name": name}
    if price is not None:
        product_filters["price"] = price
    if selling_price is not None:
        product_filters["selling_price"] = selling_price
    if cost_price is not None:
        product_filters["cost_price"] = cost_price
    if description:
        product_filters["description"] = description

    now = datetime.utcnow()
    updated_by = session.get("username") or session.get("inventory_name") or "Unknown"
    updated = []
    errors = []

    for row in updates:
        if not isinstance(row, dict):
            errors.append({"branch_name": "Unknown", "message": "Invalid update payload."})
            continue
        branch_name = (row.get("branch_name") or "").strip()
        new_qty = safe_int(row.get("new_qty"))

        if not branch_name:
            errors.append({"branch_name": "Unknown", "message": "Branch name is required."})
            continue
        if new_qty is None or new_qty < 0:
            errors.append({"branch_name": branch_name, "message": "Quantity must be an integer >= 0."})
            continue

        manager_ids = list(users_col.distinct("_id", {"role": "manager", "branch": branch_name}))
        if not manager_ids:
            errors.append({"branch_name": branch_name, "message": "No managers found for this branch."})
            continue

        item = inventory_col.find_one({**match_stage, "manager_id": {"$in": manager_ids}})
        if not item:
            errors.append({"branch_name": branch_name, "message": "Branch does not have this product."})
            continue

        old_qty = item.get("qty")
        if old_qty is None:
            old_qty = 0

        if old_qty != new_qty:
            inventory_col.update_one(
                {"_id": item["_id"]},
                {"$set": {"qty": new_qty, "updated_at": now}},
            )
            inventory_logs_col.insert_one(
                {
                    "product_id": item["_id"],
                    "product_name": item.get("name"),
                    "action": "update_qty_distribution",
                    "log_type": "update",
                    "branch": branch_name,
                    "manager_id": item.get("manager_id"),
                    "old_qty": old_qty,
                    "new_qty": new_qty,
                    "updated_by": updated_by,
                    "updated_at": now,
                    "meta": {
                        "source": "qty_distribution_modal",
                        "product_filters": product_filters,
                    },
                }
            )

        updated.append({"branch_name": branch_name, "old_qty": old_qty, "new_qty": new_qty})

    if not updated:
        return jsonify(ok=False, message="No updates applied.", errors=errors), 400

    return jsonify(ok=True, updated=updated, errors=errors)


# -----------------------------
# Stock Closing (Year End)
# -----------------------------
@inventory_products_bp.route("/inventory/stock/close", methods=["POST"])
def stock_close():
    if not (session.get("admin_id") or session.get("executive_id") or session.get("inventory_id") or session.get("role") == "inventory"):
        return jsonify(ok=False, message="Unauthorized"), 403

    payload = request.get_json(silent=True) or {}
    year_raw = payload.get("year")
    passcode = payload.get("passcode")
    if not _passcode_ok(passcode):
        return jsonify(ok=False, message="Invalid passcode"), 400

    try:
        year = int(year_raw)
    except Exception:
        return jsonify(ok=False, message="Invalid year"), 400

    existing = stock_closings_col.find_one(
        {"closing_year": year, "status": {"$in": ["processing", "completed"]}}
    )
    if existing:
        return jsonify(ok=False, message="Stock closing already exists for this year"), 400

    user_id = session.get("admin_id") or session.get("executive_id") or ""
    user_name = session.get("username", "Unknown")

    doc = {
        "closing_year": year,
        "closed_at": None,
        "closed_by_user_id": str(user_id),
        "closed_by_name": user_name,
        "status": "processing",
        "branch_scope": "ALL",
        "managers_count": 0,
        "items_count": 0,
        "total_closing_cost_value": 0.0,
        "total_closing_selling_value": 0.0,
        "error": None,
        "step": "validating",
        "managers_processed": 0,
        "items_processed": 0,
        "percent": 0,
        "current": "",
        "logs": [],
    }
    closing_id = stock_closings_col.insert_one(doc).inserted_id

    thread = threading.Thread(
        target=_process_stock_closing,
        args=(closing_id, year, str(user_id), user_name),
        daemon=True,
    )
    thread.start()

    return jsonify(ok=True, closing_id=str(closing_id))


@inventory_products_bp.route("/inventory/stock/close/status/<closing_id>", methods=["GET"])
def stock_close_status(closing_id):
    oid = to_oid(closing_id)
    if not oid:
        return jsonify(ok=False, message="Invalid id"), 400
    doc = stock_closings_col.find_one({"_id": oid})
    if not doc:
        return jsonify(ok=False, message="Not found"), 404

    logs = doc.get("logs", [])
    formatted_logs = []
    for row in logs[-200:]:
        ts = row.get("ts")
        ts_str = ts.strftime("%H:%M:%S") if isinstance(ts, datetime) else ""
        formatted_logs.append({"ts": ts_str, "level": row.get("level", ""), "message": row.get("message", "")})

    return jsonify(
        ok=True,
        status=doc.get("status"),
        step=doc.get("step", ""),
        percent=doc.get("percent", 0),
        managers_total=doc.get("managers_count", 0),
        managers_done=doc.get("managers_processed", 0),
        items_total=doc.get("items_count", 0),
        items_done=doc.get("items_processed", 0),
        branches_total=doc.get("branches_total", 0),
        branches_done=doc.get("branches_done", 0),
        current_branch=doc.get("current_branch", ""),
        current_manager=doc.get("current_manager", ""),
        branch_totals_live=doc.get("branch_totals", []) or [],
        current=doc.get("current", ""),
        logs=formatted_logs,
    )


# -----------------------------
# Inventory Closing: Start Job
# -----------------------------
@inventory_products_bp.route("/inventory/closing/start", methods=["POST"])
def start_inventory_closing():
    if not (session.get("admin_id") or session.get("executive_id")):
        return jsonify(ok=False, message="Unauthorized"), 403

    payload = request.get_json(silent=True) or {}
    year_raw = payload.get("year")
    scope = (payload.get("scope") or "all").strip().lower()
    scope_id = (payload.get("id") or "").strip() or None

    try:
        year = int(year_raw) if year_raw else datetime.utcnow().year
    except Exception:
        year = datetime.utcnow().year

    if scope not in ("all", "branch", "manager"):
        return jsonify(ok=False, message="Invalid scope"), 400

    targets = _closing_targets(scope, scope_id)
    total_targets = len(targets)
    if total_targets == 0:
        return jsonify(ok=False, message="No closing targets found"), 400

    job_doc = {
        "year": year,
        "scope": scope,
        "scope_id": scope_id,
        "status": "running",
        "created_at": datetime.utcnow(),
        "total": total_targets,
        "done": 0,
        "skipped": 0,
        "failed": 0,
        "percent": 0,
        "current": "",
        "logs": [],
    }
    job_id = closing_jobs_col.insert_one(job_doc).inserted_id

    thread = threading.Thread(target=_process_closing_job, args=(job_id, year, targets), daemon=True)
    thread.start()

    return jsonify(ok=True, job_id=str(job_id), total_targets=total_targets)


# -----------------------------
# Inventory Closing: Job Status
# -----------------------------
@inventory_products_bp.route("/inventory/closing/job/<job_id>", methods=["GET"])
def inventory_closing_job(job_id):
    oid = to_oid(job_id)
    if not oid:
        return jsonify(ok=False, message="Invalid job id"), 400
    doc = closing_jobs_col.find_one({"_id": oid})
    if not doc:
        return jsonify(ok=False, message="Job not found"), 404

    logs = doc.get("logs", [])
    formatted_logs = []
    for row in logs[-200:]:
        ts = row.get("ts")
        if isinstance(ts, datetime):
            ts_str = ts.strftime("%H:%M:%S")
        else:
            ts_str = ""
        formatted_logs.append(
            {
                "ts": ts_str,
                "level": row.get("level", "info"),
                "message": row.get("message", ""),
            }
        )

    return jsonify(
        ok=True,
        status=doc.get("status"),
        percent=doc.get("percent", 0),
        done=doc.get("done", 0),
        total=doc.get("total", 0),
        skipped=doc.get("skipped", 0),
        failed=doc.get("failed", 0),
        current=doc.get("current", ""),
        logs=formatted_logs,
    )

# -----------------------------
# Route: Inventory Logs History
# -----------------------------
@inventory_products_bp.route('/inventory_history/<item_id>')
def inventory_history(item_id):
    oid = to_oid(item_id)
    if not oid:
        return jsonify([])

    view = (request.args.get('view') or '').strip().lower()

    query = {'product_id': oid}
    if view == 'transfer':
        query['log_type'] = 'transfer'
    elif view == 'updates':
        query['log_type'] = 'update'
    elif view == 'deletes':
        query['log_type'] = 'delete'
    elif view == 'images':
        query['log_type'] = 'image_update'

    logs = list(inventory_logs_col.find(query).sort('updated_at', -1))
    for log in logs:
        log['_id'] = str(log['_id'])
        log['product_id'] = str(log.get('product_id', ''))
        ts = log.get('updated_at') or log.get('deleted_at')
        if ts:
            if isinstance(ts, datetime):
                log['updated_at'] = ts.strftime('%Y-%m-%d %H:%M:%S')
            else:
                log['updated_at'] = str(ts)
        else:
            log['updated_at'] = ""
    return jsonify(logs)

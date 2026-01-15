from __future__ import annotations

from flask import Blueprint, render_template, request, jsonify, abort, session, Response
from werkzeug.exceptions import HTTPException
from bson import ObjectId
from datetime import datetime, timedelta
from typing import Any, Dict, List, Tuple
from pymongo import ReturnDocument
from db import db
from services.activity_audit import audit_action

import io
import requests
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
from reportlab.lib.pagesizes import A4, landscape
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib.utils import ImageReader


inventory_orders_bp = Blueprint(
    "inventory_orders",
    __name__,
    template_folder="../../templates/inventory",
    url_prefix="/inventory/orders"
)

orders_col       = db["orders"]
order_events_col = db["order_events"]
users_col        = db["users"]
inventory_col    = db["inventory"]

WAREHOUSE_LOCATION_TAGS = ("warehouse", "Warehouse")

try:
    inventory_col.create_index([("stock_scope", 1), ("name", 1)], background=True)
    inventory_col.create_index([("manager_id", 1), ("name", 1)], background=True)
    orders_col.create_index([("manager_id", 1), ("updated_at", -1)], background=True)
    orders_col.create_index([("updated_at", -1)], background=True)
except Exception:
    pass

LOGO_URL = "https://res.cloudinary.com/dljpgzbto/image/upload/v1743993646/company-logo_dksb23.jpg"


def _oid(v):
    try:
        return ObjectId(str(v))
    except Exception:
        return None


def _iso_timestamp(value):
    if isinstance(value, datetime):
        return value.isoformat()
    return value


def _warehouse_base_filter():
    return {
        "$and": [
            {
                "$or": [
                    {"stock_scope": "warehouse"},
                    {"manager_id": {"$exists": False}},
                    {"manager_id": None},
                    {"location": {"$in": WAREHOUSE_LOCATION_TAGS}},
                    {"branch": {"$in": WAREHOUSE_LOCATION_TAGS}},
                ]
            },
            {"$nor": [{"manager_id": {"$type": "objectId"}}]},
        ]
    }


class WarehouseStockError(Exception):
    """Raised when warehouse stock deduction fails."""
    pass


class ManagerCreditError(Exception):
    """Raised when manager inventory credit cannot be applied."""
    pass


def _require_inventory():
    uid = session.get("inventory_id") or session.get("admin_id") or session.get("executive_id")
    if not uid:
        abort(401, "Sign in as inventory/admin/executive.")
    user = users_col.find_one({"_id": _oid(uid)})
    if not user:
        abort(403, "Unauthorized.")
    return user


def _can_force_deliver(user: Dict[str, Any]) -> bool:
    return (user or {}).get("role") in ("inventory", "admin", "executive")


@inventory_orders_bp.route("/", methods=["GET"])
def inv_orders_page():
    _require_inventory()
    return render_template("orders_inbox.html")


@inventory_orders_bp.errorhandler(HTTPException)
def handle_inventory_http_error(err):
    return jsonify(ok=False, message=err.description), err.code


@inventory_orders_bp.errorhandler(Exception)
def handle_inventory_unexpected_error(err):
    return jsonify(ok=False, message="Internal server error"), 500


# ------------------ SIMPLE GROUPED API (for the new UI) ------------------

def _order_items_summary(order_doc: Dict[str, Any]) -> Tuple[int, int]:
    """(items_count, remaining_total) including manual_items."""
    items = order_doc.get("items") or []
    manual_items = order_doc.get("manual_items") or []
    remaining_total = 0
    for it in items:
        qty = int(it.get("qty", 0) or 0)
        deliv = int(it.get("delivered_qty", 0) or 0)
        remaining_total += max(0, qty - deliv)
    for it in manual_items:
        qty = int(it.get("qty", 0) or 0)
        deliv = int(it.get("delivered_qty", 0) or 0)
        remaining_total += max(0, qty - deliv)
    return len(items) + len(manual_items), remaining_total


def _matches_search(order_doc: Dict[str, Any], q: str | None) -> bool:
    qq = (q or "").strip().lower()
    if not qq:
        return True
    if qq in str(order_doc.get("_id", "")).lower():
        return True
    if qq in (order_doc.get("branch", "") or "").lower():
        return True
    for it in (order_doc.get("items") or []):
        if qq in (it.get("name", "") or "").lower():
            return True
    for it in (order_doc.get("manual_items") or []):
        if qq in (it.get("name", "") or "").lower():
            return True
    return False


@inventory_orders_bp.route("/counts", methods=["GET"])
def inv_orders_counts():
    _require_inventory()
    docs = list(orders_col.find({}, {"items": 1}).limit(4000))
    undelivered = 0
    delivered = 0
    for d in docs:
        _, remaining_total = _order_items_summary(d)
        if remaining_total > 0:
            undelivered += 1
        else:
            delivered += 1
    return jsonify(ok=True, undelivered=undelivered, delivered=delivered)


@inventory_orders_bp.route("/orders", methods=["GET"])
def inv_orders_grouped_orders():
    _require_inventory()

    state = (request.args.get("state") or "undelivered").strip().lower()
    q = (request.args.get("q") or "").strip()

    # newest first
    docs = list(orders_col.find({}).sort([("updated_at", -1)]).limit(1200))

    managers_index = {
        str(u["_id"]): u.get("name")
        for u in users_col.find({"role": "manager"}, {"_id": 1, "name": 1})
    }

    results: List[Dict[str, Any]] = []

    for d in docs:
        if not _matches_search(d, q):
            continue

        items_count, remaining_total = _order_items_summary(d)
        is_delivered = remaining_total == 0

        if state == "delivered":
            if not is_delivered:
                continue
        else:
            if is_delivered:
                continue

        results.append(
            {
                "_id": str(d["_id"]),
                "branch": d.get("branch"),
                "manager_id": str(d.get("manager_id")) if d.get("manager_id") else None,
                "manager_name": managers_index.get(str(d.get("manager_id")), None) or "-",
                "status": d.get("status") or ("closed" if is_delivered else "open"),
                "items_count": items_count,
                "remaining_total": remaining_total,
                "created_at": _iso_timestamp(d.get("created_at")) or "",
                "updated_at": _iso_timestamp(d.get("updated_at")) or "",
            }
        )

    return jsonify(ok=True, results=results)


@inventory_orders_bp.route("/detail", methods=["GET"])
def inv_orders_detail():
    _require_inventory()
    order_id = (request.args.get("order_id") or "").strip()
    if not order_id:
        return jsonify(ok=False, message="order_id required"), 400

    order = orders_col.find_one({"_id": _oid(order_id)})
    if not order:
        return jsonify(ok=False, message="Order not found"), 404

    mid = str(order.get("manager_id")) if order.get("manager_id") else None
    mgr_name = None
    if mid:
        mgr = users_col.find_one({"_id": _oid(mid)}, {"name": 1})
        mgr_name = (mgr or {}).get("name")

    shaped_items = []
    items = order.get("items") or []
    manual_items = order.get("manual_items") or []

    product_ids = []
    for it in items:
        pid = _oid(it.get("product_id"))
        if pid:
            product_ids.append(pid)
    image_lookup = {}
    if product_ids:
        for p in inventory_col.find({"_id": {"$in": product_ids}}, {"image_url": 1}):
            image_lookup[str(p["_id"])] = p.get("image_url")

    for it in items:
        qty = int(it.get("qty", 0) or 0)
        deliv = int(it.get("delivered_qty", 0) or 0)
        remaining = max(0, qty - deliv)
        shaped_items.append(
            {
                "line_id": it.get("line_id"),
                "product_id": str(it.get("product_id")) if it.get("product_id") else None,
                "name": it.get("name"),
                "sku": it.get("sku") or it.get("code"),
                "qty": qty,
                "delivered_qty": deliv,
                "remaining_qty": remaining,
                "status": it.get("status"),
                "expected_date": it.get("expected_date"),
                "postponements": it.get("postponements", []),
                "notes": it.get("notes"),
                "image_url": image_lookup.get(str(it.get("product_id"))) if it.get("product_id") else None,
                "is_manual": False,
            }
        )

    for it in manual_items:
        qty = int(it.get("qty", 0) or 0)
        deliv = int(it.get("delivered_qty", 0) or 0)
        remaining = max(0, qty - deliv)
        shaped_items.append(
            {
                "line_id": it.get("line_id"),
                "product_id": None,
                "name": it.get("name"),
                "sku": None,
                "qty": qty,
                "delivered_qty": deliv,
                "remaining_qty": remaining,
                "status": it.get("status") or ("delivered" if remaining == 0 else "pending"),
                "expected_date": it.get("expected_date"),
                "postponements": it.get("postponements", []),
                "notes": it.get("notes"),
                "image_url": None,
                "is_manual": True,
            }
        )

    return jsonify(
        ok=True,
        order={
            "_id": str(order["_id"]),
            "manager_id": mid,
            "manager_name": mgr_name or "-",
            "branch": order.get("branch"),
            "status": order.get("status"),
            "notes": order.get("notes", ""),
            "created_at": _iso_timestamp(order.get("created_at")),
            "updated_at": _iso_timestamp(order.get("updated_at")),
            "items": shaped_items,
        },
    )


# ---------- META (branches, managers) ----------
@inventory_orders_bp.route("/meta", methods=["GET"])
def inv_orders_meta():
    _require_inventory()
    branches = users_col.distinct("branch", {"role": "manager"})
    mgrs = list(
        users_col.find(
            {"role": "manager"}, {"_id": 1, "name": 1, "branch": 1}
        ).limit(500)
    )
    return jsonify(
        ok=True,
        branches=[b for b in branches if b],
        managers=[
            {
                "_id": str(m["_id"]),
                "name": m.get("name", ""),
                "branch": m.get("branch", ""),
            }
            for m in mgrs
        ],
    )


# ------------------ EXISTING LINES API (kept, but you can ignore in UI) ------------------

def _query_orders_with_lines(
    *,
    status: str | None,
    branch: str | None,
    manager_id: str | None,
    date_from: str | None,
    date_to: str | None,
    sort: str,
    search_q: str | None,
    delivery_state: str | None,
    limit: int = 500,
) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    """
    Shared helper for /list and /export.
    Returns (results, stats) where results is a list of orders with item lines
    already shaped and filtered, and stats has outstanding_lines + postponed_lines.
    """

    q: Dict[str, Any] = {}
    if status:
        q["status"] = status
    if branch:
        q["branch"] = branch
    if manager_id:
        mid = _oid(manager_id)
        if mid:
            q["manager_id"] = mid

    # Date filters on updated_at
    if date_from or date_to:
        dr: Dict[str, Any] = {}
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

    sort_key = [("updated_at", 1 if sort == "asc" else -1)]
    docs = list(orders_col.find(q).sort(sort_key).limit(limit))

    # Simple in-Python search
    search_q = (search_q or "").strip().lower() or None
    if search_q:
        filtered_docs = []
        for d in docs:
            if search_q in str(d.get("_id", "")).lower():
                filtered_docs.append(d)
                continue
            if search_q in (d.get("branch", "") or "").lower():
                filtered_docs.append(d)
                continue
            found_item = False
            for it in d.get("items", []) or []:
                if search_q in (it.get("name", "") or "").lower():
                    found_item = True
                    break
            if found_item:
                filtered_docs.append(d)
        docs = filtered_docs

    managers_index = {
        str(u["_id"]): u.get("name")
        for u in users_col.find({"role": "manager"}, {"_id": 1, "name": 1})
    }

    outstanding = 0
    postponed = 0
    results: List[Dict[str, Any]] = []

    ds = (delivery_state or "").strip().lower()
    if ds not in ("undelivered", "delivered", "all"):
        ds = "all"

    for d in docs:
        shaped_items = []
        items = d.get("items", []) or []

        for it in items:
            qty = int(it.get("qty", 0) or 0)
            deliv = int(it.get("delivered_qty", 0) or 0)
            remaining = max(0, qty - deliv)
            line_status = (it.get("status") or "").lower()

            include = True
            if ds == "undelivered":
                include = remaining > 0
            elif ds == "delivered":
                include = remaining == 0

            if not include:
                continue

            if remaining > 0 and line_status != "delivered":
                outstanding += 1
            if line_status == "postponed":
                postponed += 1

            shaped_items.append(
                {
                    "line_id": it.get("line_id"),
                    "product_id": str(it.get("product_id")) if it.get("product_id") else None,
                    "name": it.get("name"),
                    "sku": it.get("sku") or it.get("code"),
                    "qty": qty,
                    "delivered_qty": deliv,
                    "remaining_qty": remaining,
                    "status": it.get("status"),
                    "expected_date": it.get("expected_date"),
                    "postponements": it.get("postponements", []),
                    "notes": it.get("notes"),
                }
            )

        if not shaped_items:
            continue

        results.append(
            {
                "_id": str(d["_id"]),
                "manager_id": str(d["manager_id"]) if d.get("manager_id") else None,
                "manager_name": managers_index.get(str(d.get("manager_id")), None),
                "branch": d.get("branch"),
                "status": d.get("status"),
                "notes": d.get("notes", ""),
                "created_at": _iso_timestamp(d.get("created_at")),
                "updated_at": _iso_timestamp(d.get("updated_at")),
                "items": shaped_items,
            }
        )

    stats = {"outstanding_lines": outstanding, "postponed_lines": postponed}
    return results, stats


@inventory_orders_bp.route("/list", methods=["GET"])
def inv_orders_list():
    _require_inventory()

    status     = (request.args.get("status") or "").strip().lower() or None
    branch     = (request.args.get("branch") or "").strip() or None
    manager_id = (request.args.get("manager_id") or "").strip() or None
    date_from  = (request.args.get("date_from") or "").strip() or None
    date_to    = (request.args.get("date_to") or "").strip() or None
    sort       = (request.args.get("sort") or "desc").lower()
    search_q   = (request.args.get("q") or "").strip() or None
    delivery_state = (request.args.get("delivery_state") or "").strip().lower() or None

    results, stats = _query_orders_with_lines(
        status=status,
        branch=branch,
        manager_id=manager_id,
        date_from=date_from,
        date_to=date_to,
        sort=sort,
        search_q=search_q,
        delivery_state=delivery_state,
        limit=500,
    )

    return jsonify(ok=True, results=results, stats=stats)


@inventory_orders_bp.route("/last_deliveries", methods=["GET"])
def last_deliveries():
    _require_inventory()
    branch = (request.args.get("branch") or "").strip()
    if not branch:
        return jsonify(ok=True, results=[])
    date_from = (request.args.get("date_from") or "").strip() or None
    date_to   = (request.args.get("date_to") or "").strip() or None

    order_ids = [d["_id"] for d in orders_col.find({"branch": branch}, {"_id": 1}).limit(3000)]

    f: Dict[str, Any] = {"order_id": {"$in": order_ids}, "type": "deliver_line"}
    if date_from or date_to:
        dr: Dict[str, Any] = {}
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
            f["at"] = dr

    evs = list(order_events_col.find(f).sort([("at", -1)]).limit(50))
    results = []
    managers_index = {
        str(u["_id"]): u.get("name")
        for u in users_col.find({"role": "manager"}, {"_id": 1, "name": 1})
    }
    for e in evs:
        o = orders_col.find_one({"_id": e["order_id"]}, {"manager_id": 1})
        mid = str(o["manager_id"]) if o else None
        results.append(
            {
                "order_id": str(e["order_id"]),
                "manager_id": mid,
                "manager_name": managers_index.get(mid),
                "item_name": (e.get("payload") or {}).get("item_name") or "-",
                "qty": (e.get("payload") or {}).get("qty", 0),
                "at": e.get("at"),
            }
        )
    return jsonify(ok=True, results=results)


def _recompute_order_status(order):
    items = order.get("items", []) or []
    manual_items = order.get("manual_items", []) or []
    total = len(items) + len(manual_items)
    delivered = sum(1 for it in items if it.get("status") == "delivered")
    delivered += sum(1 for it in manual_items if it.get("status") == "delivered")
    if delivered == 0:
        return "open"
    if delivered < total:
        return "partially_delivered"
    return "closed"


# ------------------ DELIVERY CORE (REUSED BY deliver_line + deliver_all) ------------------

def _deliver_line_core(*, user: Dict[str, Any], order_id: str, line_id: str, qty: int, force: bool) -> str:
    if not order_id or not line_id or qty <= 0:
        abort(400, "order_id, line_id, qty > 0 required")

    if force and not _can_force_deliver(user):
        abort(403, "Force delivery not permitted.")

    order = orders_col.find_one({"_id": _oid(order_id)})
    if not order:
        abort(404, "Order not found")

    items = order.get("items", []) or []
    manual_items = order.get("manual_items", []) or []
    line_item = None
    deliver_now = 0
    is_manual = False

    for it in items:
        if it.get("line_id") == line_id:
            remaining = int(it.get("qty", 0) or 0) - int(it.get("delivered_qty", 0) or 0)
            if remaining <= 0:
                abort(400, "Line already fully delivered")
            deliver_now = min(int(qty), remaining)
            line_item = it
            break

    if not line_item:
        for it in manual_items:
            if it.get("line_id") == line_id:
                remaining = int(it.get("qty", 0) or 0) - int(it.get("delivered_qty", 0) or 0)
                if remaining <= 0:
                    abort(400, "Line already fully delivered")
                deliver_now = min(int(qty), remaining)
                line_item = it
                is_manual = True
                break

    if not line_item:
        abort(404, "Line not found")

    if deliver_now <= 0:
        abort(400, "Requested quantity exceeds remaining line quantity")

    now = datetime.utcnow()

    # update line numbers
    line_item["delivered_qty"] = int(line_item.get("delivered_qty", 0) or 0) + deliver_now
    total_qty = int(line_item.get("qty", 0) or 0)
    line_item["status"] = "delivered" if line_item["delivered_qty"] >= total_qty else "pending"
    if line_item["status"] == "delivered":
        line_item["delivered_at"] = now
    line_item.setdefault("postponements", [])
    line_item["remaining_qty"] = max(0, total_qty - line_item["delivered_qty"])

    # inventory transfer: warehouse -> manager inventory (skip for manual items)
    warehouse_product_oid = _oid(line_item.get("product_id")) if line_item.get("product_id") else None
    manager_id = order.get("manager_id")
    branch = order.get("branch") or ""

    stock_snapshot = None
    if force and warehouse_product_oid:
        stock_snapshot = inventory_col.find_one(
            {"_id": warehouse_product_oid},
            {"qty": 1},
        )

    if warehouse_product_oid and not is_manual and not force:
        def _session_kwargs(sess):
            return {"session": sess} if sess else {}

        def _deduct_stock(sess):
            kwargs = _session_kwargs(sess)
            snapshot = inventory_col.find_one_and_update(
                {
                    "$and": [
                        _warehouse_base_filter(),
                        {"_id": warehouse_product_oid},
                        {"qty": {"$gte": deliver_now}},
                    ]
                },
                {"$inc": {"qty": -deliver_now}, "$set": {"updated_at": now}},
                return_document=ReturnDocument.AFTER,
                **kwargs,
            )
            if not snapshot:
                raise WarehouseStockError("Insufficient warehouse stock for that product.")
            if snapshot.get("qty", 0) == 0:
                inventory_col.update_one(
                    {"_id": warehouse_product_oid},
                    {"$set": {"is_out_of_stock": True, "updated_at": now}},
                    **kwargs,
                )
            return snapshot

        def _credit_manager_inventory(sess, snapshot):
            if not manager_id:
                return
            kwargs = _session_kwargs(sess)

            dest_filter = {
                "manager_id": manager_id,
                "warehouse_product_id": snapshot["_id"],
            }
            updates = {
                "$inc": {"qty": deliver_now},
                "$set": {
                    "updated_at": now,
                    "is_out_of_stock": False,
                    "stock_scope": "manager",
                    "branch": branch,
                },
            }

            updated = inventory_col.find_one_and_update(
                dest_filter,
                updates,
                return_document=ReturnDocument.AFTER,
                **kwargs,
            )
            if updated:
                return

            # fallback by name (legacy)
            fallback = inventory_col.find_one(
                {"manager_id": manager_id, "name": line_item.get("name")},
                **kwargs,
            )
            if fallback:
                inventory_col.update_one(
                    {"_id": fallback["_id"]},
                    {
                        "$inc": {"qty": deliver_now},
                        "$set": {
                            "updated_at": now,
                            "is_out_of_stock": False,
                            "stock_scope": "manager",
                            "branch": branch,
                        },
                    },
                    **kwargs,
                )
                return

            new_doc = {
                "manager_id": manager_id,
                "branch": branch,
                "stock_scope": "manager",
                "warehouse_product_id": snapshot["_id"],
                "name": line_item.get("name"),
                "sku": snapshot.get("sku") or snapshot.get("code"),
                "qty": deliver_now,
                "is_out_of_stock": False,
                "image_url": snapshot.get("image_url"),
                "description": snapshot.get("description"),
                "cost_price": snapshot.get("cost_price"),
                "selling_price": snapshot.get("selling_price"),
                "price": snapshot.get("price"),
                "source": "warehouse_transfer",
                "created_at": now,
                "updated_at": now,
            }
            inventory_col.insert_one(new_doc, **kwargs)

        def _run_transfer():
            def action(sess):
                snapshot = _deduct_stock(sess)
                try:
                    _credit_manager_inventory(sess, snapshot)
                except Exception as exc:
                    # rollback if no transaction session
                    if sess is None:
                        inventory_col.update_one(
                            {"_id": warehouse_product_oid},
                            {"$inc": {"qty": deliver_now}, "$set": {"updated_at": datetime.utcnow(), "is_out_of_stock": False}},
                        )
                    raise ManagerCreditError("Failed to credit manager inventory.") from exc
                return snapshot

            sess = None
            try:
                sess = db.client.start_session()
            except Exception:
                sess = None

            if sess:
                with sess:
                    with sess.start_transaction():
                        return action(sess)
            return action(None)

        try:
            _run_transfer()
        except WarehouseStockError as exc:
            abort(409, str(exc))
        except ManagerCreditError:
            abort(500, "Failed to credit manager inventory.")

    # update order status
    sta = _recompute_order_status({"items": items, "manual_items": manual_items})
    orders_col.update_one(
        {"_id": order["_id"]},
        {"$set": {"items": items, "manual_items": manual_items, "status": sta, "updated_at": now}},
    )

    # event log
    order_events_col.insert_one(
        {
            "order_id": order["_id"],
            "type": "deliver_line",
            "payload": {
                "line_id": line_id,
                "qty": deliver_now,
                "item_name": line_item.get("name"),
                "product_id": str(line_item.get("product_id")) if line_item.get("product_id") else None,
                "from": "warehouse" if not is_manual else "manual",
                "to_manager_id": str(manager_id) if manager_id else None,
                "is_manual": is_manual,
                "force": bool(force),
                "note": "forced delivery (warehouse stock bypassed)" if force else None,
                "warehouse_qty_before": (stock_snapshot or {}).get("qty") if force else None,
            },
            "by": str(user["_id"]),
            "role": "inventory",
            "at": now,
        }
    )

    return sta


# ---------- Actions ----------
@inventory_orders_bp.route("/line/deliver", methods=["POST"])
@audit_action("order.delivered", "Delivered Order Line", entity_type="order")
def deliver_line():
    user = _require_inventory()
    data = request.get_json(silent=True) or {}
    order_id = data.get("order_id")
    line_id = data.get("line_id")
    qty = int(data.get("qty") or 0)
    force = bool(data.get("force"))

    sta = _deliver_line_core(user=user, order_id=str(order_id), line_id=str(line_id), qty=qty, force=force)
    return jsonify(ok=True, order_status=sta)


@inventory_orders_bp.route("/order/deliver_all", methods=["POST"])
@audit_action("order.delivered_all", "Delivered Order", entity_type="order")
def deliver_all_remaining():
    user = _require_inventory()
    data = request.get_json(silent=True) or {}
    order_id = (data.get("order_id") or "").strip()
    force = bool(data.get("force"))
    if not order_id:
        abort(400, "order_id required")

    order = orders_col.find_one({"_id": _oid(order_id)})
    if not order:
        abort(404, "Order not found")

    did = 0
    last_status = order.get("status") or "open"
    for it in (order.get("items") or []):
        line_id = it.get("line_id")
        qty = int(it.get("qty", 0) or 0)
        deliv = int(it.get("delivered_qty", 0) or 0)
        remaining = max(0, qty - deliv)
        if remaining <= 0:
            continue
        last_status = _deliver_line_core(user=user, order_id=order_id, line_id=str(line_id), qty=remaining, force=force)
        did += 1

    for it in (order.get("manual_items") or []):
        line_id = it.get("line_id")
        qty = int(it.get("qty", 0) or 0)
        deliv = int(it.get("delivered_qty", 0) or 0)
        remaining = max(0, qty - deliv)
        if remaining <= 0:
            continue
        last_status = _deliver_line_core(user=user, order_id=order_id, line_id=str(line_id), qty=remaining, force=force)
        did += 1

    return jsonify(ok=True, delivered_lines=did, order_status=last_status)


@inventory_orders_bp.route("/line/postpone", methods=["POST"])
@audit_action("order.postponed", "Postponed Order Line", entity_type="order")
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

    items = order.get("items", []) or []
    found = False
    for it in items:
        if it.get("line_id") == line_id:
            from_date = it.get("expected_date")
            it["expected_date"] = to_date
            it.setdefault("postponements", []).append(
                {
                    "from": from_date,
                    "to": to_date,
                    "reason": reason,
                    "at": datetime.utcnow(),
                    "by": str(user["_id"]),
                }
            )
            if it.get("status") != "delivered":
                it["status"] = "postponed"
            found = True
            break
    if not found:
        abort(404, "Line not found")

    sta = _recompute_order_status({"items": items})
    orders_col.update_one(
        {"_id": order["_id"]},
        {"$set": {"items": items, "status": sta, "updated_at": datetime.utcnow()}},
    )

    order_events_col.insert_one(
        {
            "order_id": order["_id"],
            "type": "postpone_line",
            "payload": {"line_id": line_id, "to_date": to_date, "reason": reason},
            "by": str(user["_id"]),
            "role": "inventory",
            "at": datetime.utcnow(),
        }
    )

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
        page = max(1, int(request.args.get("page", 1)))
    except Exception:
        page = 1
    try:
        page_size = min(100, max(1, int(request.args.get("page_size", 20))))
    except Exception:
        page_size = 20

    q: Dict[str, Any] = {"status": "closed"}
    if branch:
        q["branch"] = branch
    if manager_id:
        mid = _oid(manager_id)
        if mid:
            q["manager_id"] = mid

    if date_from or date_to:
        dr: Dict[str, Any] = {}
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

    sort_key = [("updated_at", 1 if sort == "asc" else -1)]
    total = orders_col.count_documents(q)
    skip = (page - 1) * page_size

    docs = list(orders_col.find(q).sort(sort_key).skip(skip).limit(page_size))

    managers_index = {
        str(u["_id"]): u.get("name")
        for u in users_col.find({"role": "manager"}, {"_id": 1, "name": 1})
    }

    results: List[Dict[str, Any]] = []
    for d in docs:
        history_items = []
        for it in (d.get("items") or []):
            qty = int(it.get("qty", 0) or 0)
            delivered_qty = int(it.get("delivered_qty", 0) or 0)
            remaining_qty = max(0, qty - delivered_qty)
            history_items.append(
                {
                    "line_id": it.get("line_id"),
                    "product_id": str(it.get("product_id")) if it.get("product_id") else None,
                    "name": it.get("name"),
                    "sku": it.get("sku") or it.get("code"),
                    "qty": qty,
                    "delivered_qty": delivered_qty,
                    "remaining_qty": remaining_qty,
                    "expected_date": it.get("expected_date"),
                }
            )

        results.append(
            {
                "_id": str(d["_id"]),
                "branch": d.get("branch"),
                "manager_id": str(d.get("manager_id")) if d.get("manager_id") else None,
                "manager_name": managers_index.get(str(d.get("manager_id")), None),
                "closed_at": _iso_timestamp(d.get("updated_at")),
                "items": history_items,
            }
        )

    pages = (total + page_size - 1) // page_size
    return jsonify(
        ok=True,
        results=results,
        page=page,
        pages=pages,
        total=total,
        page_size=page_size,
    )


# ---------- PDF EXPORT (Undelivered lines) grouped by Branch ----------
def _build_undelivered_pdf_grouped_by_branch(orders: List[Dict[str, Any]]) -> bytes:
    """
    Landscape A4 PDF.
    Grouped by Branch (section header for each branch).
    Only undelivered lines included (remaining > 0).
    Watermarked with logo.
    """
    buf = io.BytesIO()
    doc = SimpleDocTemplate(
        buf,
        pagesize=landscape(A4),
        leftMargin=20,
        rightMargin=20,
        topMargin=70,
        bottomMargin=30,
    )

    styles = getSampleStyleSheet()
    elements: List[Any] = []

    elements.append(
        Paragraph(
            "Smart Living Emporium Plus – Undelivered Products (Inventory Pick List)",
            styles["Title"],
        )
    )
    elements.append(Spacer(1, 6))

    # sort by branch then manager then updated_at
    def _br(o): return (o.get("branch") or "").lower()
    def _mg(o): return (o.get("manager_name") or o.get("manager_id") or "").lower()
    orders_sorted = sorted(orders, key=lambda o: (_br(o), _mg(o), o.get("updated_at") or ""))

    data: List[List[Any]] = []
    header = ["#", "Manager", "Order ID", "Item", "Qty", "Delivered", "Remaining", "Expected Date"]

    def add_branch_row(branch: str):
        data.append([f"BRANCH: {branch}", "", "", "", "", "", "", ""])
        data.append(header)

    row_idx = 1
    current_branch = None

    for o in orders_sorted:
        branch = o.get("branch") or "-"
        manager = o.get("manager_name") or o.get("manager_id") or "-"

        # collect undelivered lines
        undel = []
        for line in (o.get("items") or []):
            remaining = int(line.get("remaining_qty", 0) or 0)
            if remaining > 0:
                undel.append((line, remaining))
        if not undel:
            continue

        if current_branch != branch:
            current_branch = branch
            add_branch_row(branch)

        for line, remaining in undel:
            data.append(
                [
                    row_idx,
                    manager,
                    o.get("_id"),
                    line.get("name") or "-",
                    int(line.get("qty", 0) or 0),
                    int(line.get("delivered_qty", 0) or 0),
                    remaining,
                    line.get("expected_date") or "",
                ]
            )
            row_idx += 1

    if row_idx == 1:
        data = [["", "", "", "No undelivered lines for selected filters.", "", "", "", ""]]

    table = Table(data, repeatRows=0)
    ts = TableStyle(
        [
            ("GRID", (0, 0), (-1, -1), 0.3, colors.grey),
            ("FONTSIZE", (0, 0), (-1, -1), 8),
            ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ]
    )

    # style branch rows + headers
    for i, row in enumerate(data):
        if row and isinstance(row[0], str) and row[0].startswith("BRANCH:"):
            ts.add("BACKGROUND", (0, i), (-1, i), colors.HexColor("#eef4ff"))
            ts.add("FONTNAME", (0, i), (-1, i), "Helvetica-Bold")
            ts.add("FONTSIZE", (0, i), (-1, i), 9)
            ts.add("SPAN", (0, i), (-1, i))
        elif row == header:
            ts.add("BACKGROUND", (0, i), (-1, i), colors.HexColor("#e5ecff"))
            ts.add("TEXTCOLOR", (0, i), (-1, i), colors.HexColor("#0b3b52"))
            ts.add("FONTNAME", (0, i), (-1, i), "Helvetica-Bold")
            ts.add("FONTSIZE", (0, i), (-1, i), 9)

    # zebra striping on data rows (not headers)
    zebra = 0
    for i, row in enumerate(data):
        if row == header or (row and isinstance(row[0], str) and row[0].startswith("BRANCH:")):
            continue
        if zebra % 2 == 1:
            ts.add("BACKGROUND", (0, i), (-1, i), colors.whitesmoke)
        zebra += 1

    table.setStyle(ts)
    elements.append(table)

    # fetch logo once
    logo_img = None
    try:
        resp = requests.get(LOGO_URL, timeout=5)
        resp.raise_for_status()
        logo_img = ImageReader(io.BytesIO(resp.content))
    except Exception:
        logo_img = None

    def _watermark(canvas, doc_obj):
        canvas.saveState()

        pw, ph = doc_obj.pagesize

        if logo_img:
            iw, ih = logo_img.getSize()
            max_w = 180.0
            scale = max_w / float(iw)
            w = max_w
            h = ih * scale
            x = (pw - w) / 2.0
            y = (ph - h) / 2.0
            try:
                canvas.setFillAlpha(0.08)
            except Exception:
                pass
            canvas.drawImage(logo_img, x, y, width=w, height=h, mask="auto")
            try:
                canvas.setFillAlpha(1)
            except Exception:
                pass

        canvas.setFont("Helvetica-Bold", 9)
        canvas.drawString(
            30,
            ph - 40,
            "Smart Living Emporium Plus – Undelivered Products (Inventory Pick List)",
        )
        canvas.setFont("Helvetica", 8)
        canvas.drawRightString(
            pw - 30,
            ph - 40,
            "Generated: " + datetime.now().strftime("%Y-%m-%d %H:%M"),
        )
        canvas.restoreState()

    doc.build(elements, onFirstPage=_watermark, onLaterPages=_watermark)

    pdf_bytes = buf.getvalue()
    buf.close()
    return pdf_bytes


@inventory_orders_bp.route("/export/undelivered.pdf", methods=["GET"])
def export_undelivered_pdf():
    """
    Always exports ONLY undelivered lines, grouped by Branch.
    Supports only q search from UI (optional).
    """
    _require_inventory()
    search_q = (request.args.get("q") or "").strip() or None

    results, _stats = _query_orders_with_lines(
        status=None,
        branch=None,
        manager_id=None,
        date_from=None,
        date_to=None,
        sort="asc",              # nicer for branch grouping
        search_q=search_q,
        delivery_state="undelivered",
        limit=3000,
    )

    pdf_bytes = _build_undelivered_pdf_grouped_by_branch(results)
    return Response(
        pdf_bytes,
        mimetype="application/pdf",
        headers={"Content-Disposition": "inline; filename=undelivered_orders_by_branch.pdf"},
    )

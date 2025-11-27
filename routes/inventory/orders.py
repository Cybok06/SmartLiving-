from __future__ import annotations

from flask import Blueprint, render_template, request, jsonify, abort, session, Response
from bson import ObjectId
from datetime import datetime, timedelta
from typing import Any, Dict, List, Tuple
from db import db

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

LOGO_URL = "https://res.cloudinary.com/dljpgzbto/image/upload/v1743993646/company-logo_dksb23.jpg"


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

    # Simple in-Python search for q (order id / branch / item name)
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

    # Normalize delivery_state
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

            # Filter per-line based on delivery_state
            include = True
            if ds == "undelivered":
                include = remaining > 0
            elif ds == "delivered":
                include = (remaining == 0)

            if not include:
                continue

            # KPI counts only on included lines
            if remaining > 0 and line_status != "delivered":
                outstanding += 1
            if line_status == "postponed":
                postponed += 1

            shaped_items.append(
                {
                    "line_id": it.get("line_id"),
                    "name": it.get("name"),
                    "qty": qty,
                    "delivered_qty": deliv,
                    "remaining_qty": remaining,
                    "status": it.get("status"),
                    "expected_date": it.get("expected_date"),
                    "postponements": it.get("postponements", []),
                }
            )

        # If this order has no lines after filtering, skip it
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
                "created_at": d.get("created_at"),
                "updated_at": d.get("updated_at"),
                "items": shaped_items,
            }
        )

    stats = {"outstanding_lines": outstanding, "postponed_lines": postponed}
    return results, stats


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


# ---------- Last deliveries for a branch ----------
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
    total = len(order.get("items", []))
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
            remaining = int(it.get("qty", 0)) - int(it.get("delivered_qty", 0))
            if remaining <= 0:
                abort(400, "Line already fully delivered")
            deliver_now = min(qty, remaining)
            it["delivered_qty"] = int(it.get("delivered_qty", 0)) + deliver_now
            it["status"] = "delivered" if it["delivered_qty"] >= it["qty"] else "pending"
            if it["status"] == "delivered":
                it["delivered_at"] = datetime.utcnow()
            it.setdefault("postponements", [])
            line_item = it
            break
    if not line_item:
        abort(404, "Line not found")

    sta = _recompute_order_status({"items": items})
    orders_col.update_one(
        {"_id": order["_id"]},
        {"$set": {"items": items, "status": sta, "updated_at": datetime.utcnow()}},
    )

    order_events_col.insert_one(
        {
            "order_id": order["_id"],
            "type": "deliver_line",
            "payload": {
                "line_id": line_id,
                "qty": qty,
                "item_name": line_item.get("name"),
            },
            "by": str(user["_id"]),
            "role": "inventory",
            "at": datetime.utcnow(),
        }
    )

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
        page      = max(1, int(request.args.get("page", 1)))
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
            q["updated_at"] = dr  # closed time is reflected in updated_at

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
        results.append(
            {
                "_id": str(d["_id"]),
                "branch": d.get("branch"),
                "manager_id": str(d.get("manager_id")) if d.get("manager_id") else None,
                "manager_name": managers_index.get(str(d.get("manager_id")), None),
                "closed_at": d.get("updated_at"),
                "items": [
                    {
                        "name": it.get("name"),
                        "qty": int(it.get("qty", 0) or 0),
                        "delivered_qty": int(it.get("delivered_qty", 0) or 0),
                    }
                    for it in (d.get("items") or [])
                ],
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


# ---------- PDF EXPORT (Undelivered lines) ----------
def _build_undelivered_pdf(orders: List[Dict[str, Any]]) -> bytes:
    """
    Create a landscape A4 PDF where each row is:
    #, Branch, Manager, Order ID, Item, Qty, Delivered, Remaining, Expected Date
    With Smart Living Emporium Plus logo as watermark.
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

    title = Paragraph(
        "Smart Living Emporium Plus – Undelivered Products (Inventory Pick List)",
        styles["Title"],
    )
    elements.append(title)
    elements.append(Spacer(1, 6))

    # Header + data rows
    data: List[List[Any]] = []
    header = [
        "#",
        "Branch",
        "Manager",
        "Order ID",
        "Item",
        "Qty",
        "Delivered",
        "Remaining",
        "Expected Date",
    ]
    data.append(header)

    row_idx = 1
    for o in orders:
        branch = o.get("branch") or "-"
        manager = o.get("manager_name") or o.get("manager_id") or "-"
        order_id = o.get("_id")
        for line in o.get("items", []):
            remaining = int(line.get("remaining_qty", 0) or 0)
            if remaining <= 0:
                continue  # only undelivered
            data.append(
                [
                    row_idx,
                    branch,
                    manager,
                    order_id,
                    line.get("name") or "-",
                    int(line.get("qty", 0) or 0),
                    int(line.get("delivered_qty", 0) or 0),
                    remaining,
                    line.get("expected_date") or "",
                ]
            )
            row_idx += 1

    # If no rows, still create a message
    if row_idx == 1:
        data.append(["", "", "", "", "No undelivered lines for selected filters.", "", "", "", ""])

    table = Table(data, repeatRows=1)
    table_style = TableStyle(
        [
            ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#e5ecff")),
            ("TEXTCOLOR", (0, 0), (-1, 0), colors.HexColor("#0b3b52")),
            ("ALIGN", (0, 0), (-1, 0), "CENTER"),
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ("FONTSIZE", (0, 0), (-1, 0), 9),
            ("BOTTOMPADDING", (0, 0), (-1, 0), 6),
            ("GRID", (0, 0), (-1, -1), 0.3, colors.grey),
            ("FONTSIZE", (0, 1), (-1, -1), 8),
            ("VALIGN", (0, 1), (-1, -1), "MIDDLE"),
        ]
    )

    # Zebra striping
    for i in range(1, len(data)):
        if i % 2 == 1:
            table_style.add("BACKGROUND", (0, i), (-1, i), colors.whitesmoke)

    table.setStyle(table_style)
    elements.append(table)

    # Try to fetch logo once
    logo_img = None
    try:
        resp = requests.get(LOGO_URL, timeout=5)
        resp.raise_for_status()
        logo_img = ImageReader(io.BytesIO(resp.content))
    except Exception:
        logo_img = None

    def _watermark(canvas, doc_obj):
        canvas.saveState()
        # Logo in center as light watermark
        if logo_img:
            pw, ph = doc_obj.pagesize
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

        # Header text at top
        pw, ph = doc_obj.pagesize
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

    doc.build(
        elements,
        onFirstPage=lambda c, d: _watermark(c, d),
        onLaterPages=lambda c, d: _watermark(c, d),
    )

    pdf_bytes = buf.getvalue()
    buf.close()
    return pdf_bytes


@inventory_orders_bp.route("/export/undelivered.pdf", methods=["GET"])
def export_undelivered_pdf():
    """
    Export current filters as a PDF of undelivered product lines
    in a clean, tabular layout. Used by the 'Undelivered PDF' button.
    """
    _require_inventory()

    status     = (request.args.get("status") or "").strip().lower() or None
    branch     = (request.args.get("branch") or "").strip() or None
    manager_id = (request.args.get("manager_id") or "").strip() or None
    date_from  = (request.args.get("date_from") or "").strip() or None
    date_to    = (request.args.get("date_to") or "").strip() or None
    sort       = (request.args.get("sort") or "desc").lower()
    search_q   = (request.args.get("q") or "").strip() or None

    # Regardless of what the UI sends, force undelivered lines for this pdf
    delivery_state = "undelivered"

    results, _stats = _query_orders_with_lines(
        status=status,
        branch=branch,
        manager_id=manager_id,
        date_from=date_from,
        date_to=date_to,
        sort=sort,
        search_q=search_q,
        delivery_state=delivery_state,
        limit=2000,  # allow more lines for export
    )

    pdf_bytes = _build_undelivered_pdf(results)
    return Response(
        pdf_bytes,
        mimetype="application/pdf",
        headers={
            "Content-Disposition": "inline; filename=undelivered_orders.pdf"
        },
    )

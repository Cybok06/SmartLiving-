# routes/executive_stock_entry.py
from __future__ import annotations

from flask import (
    Blueprint, render_template, request,
    jsonify, session, redirect, url_for
)
from bson.objectid import ObjectId
from datetime import datetime, timedelta
from typing import Tuple, Optional, Dict, Any, List

from db import db
from services.activity_audit import audit_action

executive_stock_entry_bp = Blueprint(
    "executive_stock_entry",
    __name__,
    url_prefix="/executive-stock"
)

users_col  = db["users"]
stock_col  = db["stock_entries"]   # new collection for stock purchases


# ----------------- Helpers -----------------
def _current_exec_session() -> Tuple[Optional[str], Optional[str]]:
    """
    Allow executive or admin to access this page.
    Returns (session_key, user_id) or (None, None).
    """
    if session.get("executive_id"):
        return "executive_id", session["executive_id"]
    if session.get("admin_id"):
        return "admin_id", session["admin_id"]
    return None, None


def _ensure_exec_or_redirect():
    """
    Return (user_id_str, user_doc) if valid executive/admin.
    Else redirect to login.
    """
    _, uid = _current_exec_session()
    if not uid:
        return redirect(url_for("login.login"))

    # Handle ObjectId and string id
    user = None
    try:
        user = users_col.find_one({"_id": ObjectId(uid)})
    except Exception:
        user = users_col.find_one({"_id": uid})

    if not user:
        return redirect(url_for("login.login"))

    role = (user.get("role") or "").lower()
    if role not in ("executive", "admin"):
        return redirect(url_for("login.login"))

    return str(user["_id"]), user


def _parse_date(date_str: Optional[str]) -> Optional[datetime]:
    if not date_str:
        return None
    try:
        # date-only input: YYYY-MM-DD
        return datetime.strptime(date_str, "%Y-%m-%d")
    except Exception:
        return None


def _build_match_from_request(args) -> Dict[str, Any]:
    """
    Build a MongoDB match filter from query args.
    Filters: start, end, name
    - If no start/end, default is last 30 days.
    """
    now = datetime.utcnow()

    start = _parse_date(args.get("start"))
    end   = _parse_date(args.get("end"))

    if not start and not end:
        # default last 30 days
        end = now
        start = now - timedelta(days=30)
    elif start and not end:
        # if only start → up to now
        end = now
    elif end and not start:
        # if only end → 30 days before end
        start = end - timedelta(days=30)

    # make end inclusive by going to next day
    end = end + timedelta(days=1)

    match: Dict[str, Any] = {
        "purchased_at": {"$gte": start, "$lt": end}
    }

    name = (args.get("name") or "").strip()
    if name:
        match["name"] = name

    return match


def _recent_item_names(limit: int = 15) -> List[str]:
    """
    Last N distinct item names for the datalist suggestions.
    """
    cursor = stock_col.find(
        {"name": {"$exists": True, "$ne": ""}},
        {"name": 1, "created_at": 1}
    ).sort("created_at", -1).limit(limit * 3)  # fetch a bit more, dedupe in Python

    seen = set()
    names: List[str] = []
    for doc in cursor:
        nm = (doc.get("name") or "").strip()
        if nm and nm not in seen:
            seen.add(nm)
            names.append(nm)
        if len(names) >= limit:
            break
    return names


# ----------------- Page -----------------
@executive_stock_entry_bp.route("/", methods=["GET"])
def stock_entry_page():
    """
    Executive Stock Entry dashboard:
      - Stock entry form
      - Summary KPIs
      - Charts (loaded via AJAX)
      - Recent entries table
    """
    scope = _ensure_exec_or_redirect()
    if not isinstance(scope, tuple):
        return scope
    exec_id, exec_doc = scope

    # Recent entries for default table (last 30 days)
    now = datetime.utcnow()
    start_30 = now - timedelta(days=30)
    recent_docs = list(
        stock_col.find(
            {"purchased_at": {"$gte": start_30, "$lt": now + timedelta(days=1)}}
        )
        .sort("purchased_at", -1)
        .limit(100)
    )

    rows = []
    total_30 = 0.0
    total_all_time = 0.0

    # All-time total
    agg_all = list(
        stock_col.aggregate([
            {"$group": {"_id": None, "sum_total": {"$sum": {"$ifNull": ["$total_cost", 0]}}}}
        ])
    )
    if agg_all:
        total_all_time = float(agg_all[0].get("sum_total", 0.0) or 0.0)

    for d in recent_docs:
        qty = float(d.get("quantity", 0) or 0)
        unit_price = float(d.get("unit_price", 0) or 0)
        total = float(d.get("total_cost", qty * unit_price) or 0)
        total_30 += total

        dt = d.get("purchased_at") or d.get("created_at")
        if isinstance(dt, datetime):
            date_str = dt.strftime("%Y-%m-%d")
            time_str = dt.strftime("%H:%M")
        else:
            date_str = ""
            time_str = ""

        rows.append({
            "_id": str(d["_id"]),
            "name": d.get("name", ""),
            "quantity": qty,
            "unit_price": unit_price,
            "total_cost": total,
            "description": d.get("description", ""),
            "date": date_str,
            "time": time_str,
        })

    recent_names = _recent_item_names()

    return render_template(
        "executive_stock_entry.html",
        executive_name=exec_doc.get("name", "Executive"),
        today=datetime.utcnow().strftime("%Y-%m-%d"),
        rows=rows,
        total_30=f"{total_30:,.2f}",
        total_all_time=f"{total_all_time:,.2f}",
        recent_names=recent_names,
    )


# ----------------- Add Entry -----------------
@executive_stock_entry_bp.route("/add", methods=["POST"])
@audit_action("stock_entry.created", "Created Stock Entry", entity_type="inventory")
def add_stock_entry():
    scope = _ensure_exec_or_redirect()
    if not isinstance(scope, tuple):
        return jsonify(ok=False, message="Please log in."), 401
    exec_id, _ = scope

    form = request.form
    name = (form.get("name") or "").strip()
    description = (form.get("description") or "").strip()
    qty_str = form.get("quantity") or "0"
    unit_price_str = form.get("unit_price") or "0"
    total_cost_str = form.get("total_cost") or ""

    date_str = form.get("date") or ""
    dt = _parse_date(date_str) or datetime.utcnow()

    try:
        quantity = float(qty_str)
    except Exception:
        quantity = 0.0

    try:
        unit_price = float(unit_price_str)
    except Exception:
        unit_price = 0.0

    if total_cost_str:
        try:
            total_cost = float(total_cost_str)
        except Exception:
            total_cost = quantity * unit_price
    else:
        total_cost = quantity * unit_price

    if not name:
        return jsonify(ok=False, message="Item name is required."), 400

    doc = {
        "name": name,
        "description": description,
        "quantity": quantity,
        "unit_price": unit_price,
        "total_cost": total_cost,
        "purchased_at": dt,
        "created_at": datetime.utcnow(),
        "created_by": exec_id,
    }

    stock_col.insert_one(doc)
    return jsonify(ok=True, message="Stock entry saved.")


# ----------------- List Entries (with filters) -----------------
@executive_stock_entry_bp.route("/list", methods=["GET"])
def list_stock_entries():
    scope = _ensure_exec_or_redirect()
    if not isinstance(scope, tuple):
        return jsonify(ok=False, message="Please log in."), 401

    match = _build_match_from_request(request.args)
    docs = list(
        stock_col.find(match)
        .sort("purchased_at", -1)
        .limit(300)
    )

    rows = []
    for d in docs:
        qty = float(d.get("quantity", 0) or 0)
        unit_price = float(d.get("unit_price", 0) or 0)
        total_cost = float(d.get("total_cost", qty * unit_price) or 0)

        dt = d.get("purchased_at") or d.get("created_at")
        if isinstance(dt, datetime):
            date_str = dt.strftime("%Y-%m-%d")
            time_str = dt.strftime("%H:%M")
        else:
            date_str = ""
            time_str = ""

        rows.append({
            "_id": str(d["_id"]),
            "name": d.get("name", ""),
            "quantity": qty,
            "unit_price": unit_price,
            "total_cost": f"{total_cost:,.2f}",
            "description": d.get("description", ""),
            "date": date_str,
            "time": time_str,
        })

    return jsonify(ok=True, rows=rows)


# ----------------- Stats (for charts) -----------------
@executive_stock_entry_bp.route("/stats", methods=["GET"])
def stock_stats():
    """
    kind=daily   → total per day (for line chart)
    kind=items   → top items by total amount (for bar chart)
    Filters: start, end, name (same as /list)
    """
    scope = _ensure_exec_or_redirect()
    if not isinstance(scope, tuple):
        return jsonify(ok=False, message="Please log in."), 401

    args = request.args
    kind = (args.get("kind") or "daily").lower()
    if kind not in ("daily", "items"):
        kind = "daily"

    match = _build_match_from_request(args)
    pipeline: List[Dict[str, Any]] = [{"$match": match}]

    if kind == "items":
        pipeline += [
            {
                "$group": {
                    "_id": "$name",
                    "sum_total": {"$sum": {"$ifNull": ["$total_cost", 0]}},
                }
            },
            {"$sort": {"sum_total": -1}},
            {"$limit": 10},
        ]
        agg = list(stock_col.aggregate(pipeline))
        labels = [a["_id"] for a in agg]
        values = [float(a["sum_total"] or 0) for a in agg]
        total = sum(values)
        return jsonify(ok=True, labels=labels, values=values, total=round(total, 2))

    # daily
    pipeline += [
        {
            "$group": {
                "_id": {
                    "$dateToString": {
                        "format": "%Y-%m-%d",
                        "date": "$purchased_at",
                    }
                },
                "sum_total": {"$sum": {"$ifNull": ["$total_cost", 0]}},
            }
        },
        {"$sort": {"_id": 1}},
    ]
    agg = list(stock_col.aggregate(pipeline))
    labels = [a["_id"] for a in agg]
    values = [float(a["sum_total"] or 0) for a in agg]
    total = sum(values)
    return jsonify(ok=True, labels=labels, values=values, total=round(total, 2))

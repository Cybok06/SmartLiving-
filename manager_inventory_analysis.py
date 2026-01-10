from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

from flask import Blueprint, render_template, session, redirect, url_for, request
from bson import ObjectId
from bson.errors import InvalidId

from db import db

manager_inventory_analysis_bp = Blueprint("manager_inventory_analysis", __name__)
inventory_col = db["inventory"]


def _oid(val: str) -> Optional[ObjectId]:
    try:
        return ObjectId(val)
    except (InvalidId, TypeError):
        return None


def _to_float(v) -> Optional[float]:
    try:
        if v is None or v == "":
            return None
        return float(v)
    except Exception:
        return None


def _days_between(a: datetime, b: datetime) -> int:
    return int((b - a).total_seconds() // 86400)


@manager_inventory_analysis_bp.route("/manager/inventory/analysis")
def manager_inventory_analysis():
    if "manager_id" not in session:
        return redirect(url_for("login.login"))

    manager_id = _oid(session.get("manager_id"))
    if not manager_id:
        return redirect(url_for("login.login"))

    # Filters (affect KPIs + charts only)
    q = (request.args.get("q") or "").strip().lower()
    stock = (request.args.get("stock") or "all").strip().lower()  # all|in|out|low

    try:
        low_threshold = int(request.args.get("low") or 5)
    except Exception:
        low_threshold = 5

    try:
        expiry_window = int(request.args.get("expiry_window") or 30)
    except Exception:
        expiry_window = 30

    items = list(inventory_col.find({"manager_id": manager_id}))

    today0 = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)

    rows: List[Dict[str, Any]] = []
    for it in items:
        name = (it.get("name") or "Unnamed").strip()
        qty = it.get("qty", 0) or 0

        expiry_dt = it.get("expiry_date")
        if isinstance(expiry_dt, str):
            expiry_dt = None

        is_out = bool(it.get("is_out_of_stock")) or (qty == 0)
        is_low = (qty > 0 and qty <= low_threshold)

        days_to_expiry = None
        is_expired = False
        expiring_soon = False
        if isinstance(expiry_dt, datetime):
            exp0 = expiry_dt.replace(hour=0, minute=0, second=0, microsecond=0)
            days_to_expiry = _days_between(today0, exp0)
            is_expired = days_to_expiry < 0
            expiring_soon = (0 <= days_to_expiry <= expiry_window)

        cost_price = _to_float(it.get("cost_price")) or _to_float(it.get("initial_price")) or 0.0
        selling_price = _to_float(it.get("selling_price")) or _to_float(it.get("price")) or 0.0
        margin = _to_float(it.get("margin"))
        if margin is None:
            margin = selling_price - cost_price

        rows.append({
            "name": name,
            "qty": qty,
            "is_out": is_out,
            "is_low": is_low,
            "is_expired": is_expired,
            "expiring_soon": expiring_soon,
            "days_to_expiry": days_to_expiry,
            "margin": margin,
        })

    # Apply search filter (charts + KPIs)
    if q:
        rows = [r for r in rows if q in r["name"].lower()]

    # Apply stock filter (charts + KPIs)
    if stock == "out":
        rows = [r for r in rows if r["is_out"]]
    elif stock == "in":
        rows = [r for r in rows if not r["is_out"]]
    elif stock == "low":
        rows = [r for r in rows if r["is_low"]]

    # KPIs
    total_skus = len(rows)
    total_qty = sum(r["qty"] for r in rows)
    out_of_stock = sum(1 for r in rows if r["is_out"])
    low_stock = sum(1 for r in rows if r["is_low"])
    expired = sum(1 for r in rows if r["is_expired"])
    expiring_soon = sum(1 for r in rows if r["expiring_soon"])

    # Margin KPIs
    positive_margin = sum(1 for r in rows if (r["margin"] is not None and r["margin"] > 0))
    negative_margin = sum(1 for r in rows if (r["margin"] is not None and r["margin"] < 0))
    break_even = sum(1 for r in rows if (r["margin"] is not None and r["margin"] == 0))

    # Top products by quantity (for chart)
    top = sorted(rows, key=lambda x: x["qty"], reverse=True)[:12]
    labels = [x["name"] for x in top]
    quantities = [x["qty"] for x in top]

    # Stock split
    in_count = sum(1 for r in rows if (not r["is_out"] and not r["is_low"]))
    stock_split_labels = ["In Stock", "Out of Stock", "Low Stock"]
    stock_split_values = [in_count, out_of_stock, low_stock]

    # Expiry split
    expiry_split_labels = ["Expired", "Expiring Soon", "Valid/No Expiry"]
    valid_count = total_skus - expired - expiring_soon
    expiry_split_values = [expired, expiring_soon, valid_count]

    # Margin split
    margin_split_labels = ["Positive Margin", "Negative Margin", "Break-even"]
    margin_split_values = [positive_margin, negative_margin, break_even]

    # Extra insights
    # (These are quick, helpful “executive summary” signals)
    insights = []
    if out_of_stock > 0:
        insights.append(f"{out_of_stock} SKU(s) are out of stock — consider restocking.")
    if low_stock > 0:
        insights.append(f"{low_stock} SKU(s) are low stock (≤ {low_threshold}).")
    if expired > 0:
        insights.append(f"{expired} SKU(s) are expired — remove/discount urgently.")
    if expiring_soon > 0:
        insights.append(f"{expiring_soon} SKU(s) expiring within {expiry_window} days — plan promotions.")
    if negative_margin > 0:
        insights.append(f"{negative_margin} SKU(s) have negative margin — check pricing/cost entry.")

    return render_template(
        "manager_inventory_analysis.html",
        summary={
            "total_skus": total_skus,
            "total_qty": total_qty,
            "out_of_stock": out_of_stock,
            "low_stock": low_stock,
            "expired": expired,
            "expiring_soon": expiring_soon,
            "positive_margin": positive_margin,
            "negative_margin": negative_margin,
            "break_even": break_even,
        },
        filters={
            "q": q,
            "stock": stock,
            "low_threshold": low_threshold,
            "expiry_window": expiry_window,
        },
        chart={
            "labels": labels,
            "quantities": quantities,
            "stock_split_labels": stock_split_labels,
            "stock_split_values": stock_split_values,
            "expiry_split_labels": expiry_split_labels,
            "expiry_split_values": expiry_split_values,
            "margin_split_labels": margin_split_labels,
            "margin_split_values": margin_split_values,
        },
        insights=insights,
    )

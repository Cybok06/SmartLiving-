from __future__ import annotations

from flask import (
    Blueprint,
    render_template,
    request,
    jsonify,
    session,
    redirect,
    url_for,
    Response,
)
from bson import ObjectId
from datetime import datetime, timedelta
from typing import Tuple, Optional, Dict, Any, List
import json

from db import db

executive_returns_outwards_bp = Blueprint(
    "executive_returns_outwards",
    __name__,
    url_prefix="/executive-returns-outwards",
    template_folder="../templates",
)

users_col = db["users"]
returns_outwards_col = db["returns_outwards"]
inventory_col = db["inventory"]


def _current_exec_session() -> Tuple[Optional[str], Optional[str]]:
    if session.get("executive_id"):
        return "executive_id", session["executive_id"]
    if session.get("admin_id"):
        return "admin_id", session["admin_id"]
    return None, None


def _ensure_exec_or_redirect():
    _, uid = _current_exec_session()
    if not uid:
        return redirect(url_for("login.login"))
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


def _parse_date(val: Optional[str]) -> Optional[datetime]:
    if not val:
        return None
    try:
        return datetime.strptime(val, "%Y-%m-%d")
    except Exception:
        return None


def _build_filters(args):
    start = _parse_date(args.get("start"))
    end = _parse_date(args.get("end"))
    status = (args.get("status") or "posted").lower()
    company = (args.get("company_name") or "").strip()
    if not start and not end:
        end = datetime.utcnow()
        start = end - timedelta(days=30)
    elif start and not end:
        end = datetime.utcnow()
    elif end and not start:
        start = end - timedelta(days=30)
    end = end + timedelta(days=1)
    filters = {
        "date_dt": {"$gte": start, "$lt": end},
    }
    if status and status != "all":
        filters["status"] = status
    if company:
        filters["company_name"] = {"$regex": company, "$options": "i"}
    return filters, company, status, start, end - timedelta(days=1)


def _recent_rets(filters):
    cursor = returns_outwards_col.find(filters).sort("date_dt", -1).limit(100)
    rows = []
    for doc in cursor:
        items = doc.get("items") or []
        rows.append({
            "_id": str(doc["_id"]),
            "date": doc.get("date") or (doc.get("date_dt").strftime("%Y-%m-%d") if isinstance(doc.get("date_dt"), datetime) else ""),
            "company_name": doc.get("company_name", ""),
            "reason": doc.get("reason", ""),
            "total_cost": float(doc.get("total_cost") or 0),
            "items_count": len(items),
            "created_by": doc.get("created_by", ""),
            "status": doc.get("status", "posted"),
            "items": items,
        })
    return rows


@executive_returns_outwards_bp.route("/", methods=["GET"])
def page():
    scope = _ensure_exec_or_redirect()
    if not isinstance(scope, tuple):
        return scope
    exec_id, exec_doc = scope
    filters, company, status, start, end = _build_filters(request.args)
    rows = _recent_rets(filters)
    products = list(
        inventory_col.find({}, {"_id": 1, "name": 1, "qty": 1, "cost_price": 1}).limit(500)
    )
    return render_template(
        "executive_returns_outwards.html",
        executive_name=exec_doc.get("name", "Executive"),
        rows=rows,
        start=start.strftime("%Y-%m-%d") if start else "",
        end=end.strftime("%Y-%m-%d") if end else "",
        company=company,
        status=status,
        today=datetime.utcnow().strftime("%Y-%m-%d"),
        products=products,
    )


@executive_returns_outwards_bp.route("/add", methods=["POST"])
def add():
    scope = _ensure_exec_or_redirect()
    if not isinstance(scope, tuple):
        return jsonify(ok=False, message="Please log in."), 401
    exec_id, exec_doc = scope
    data = request.get_json() or {}
    company_name = (data.get("company_name") or request.form.get("company_name") or "").strip()
    reason = (data.get("reason") or request.form.get("reason") or "").strip()
    status = (data.get("status") or request.form.get("status") or "posted").lower()
    date_str = (data.get("date") or request.form.get("date") or "")
    date_dt = _parse_date(date_str) or datetime.utcnow()
    items = data.get("items") or request.form.get("items")
    if isinstance(items, str):
        try:
            items = json.loads(items)
        except Exception:
            items = []
    if not isinstance(items, list):
        items = []
    if not company_name or not reason or not items:
        return jsonify(ok=False, message="Company, reason, and items are required."), 400
    total_cost = 0.0
    processed_items = []
    inventory_updates: List[Dict[str, Any]] = []
    for item in items:
        product_id = item.get("product_id")
        qty = float(item.get("qty") or 0)
        unit_cost = float(item.get("unit_cost") or 0)
        if not product_id or qty <= 0 or unit_cost < 0:
            continue
        try:
            prod_oid = ObjectId(product_id)
        except Exception:
            prod_oid = None
        if prod_oid:
            product = inventory_col.find_one({"_id": prod_oid})
            if not product:
                return jsonify(ok=False, message=f"Product {product_id} not found."), 400
            current_qty = float(product.get("qty") or 0)
            if qty > current_qty:
                return jsonify(ok=False, message=f"Insufficient stock for {product.get('name','')}. Received {qty}, available {current_qty}"), 400
            new_qty = max(current_qty - qty, 0)
            inventory_updates.append({"_id": prod_oid, "new_qty": new_qty})
        line_total = qty * unit_cost
        total_cost += line_total
        processed_items.append({
            "product_id": prod_oid or product_id,
            "product_name": item.get("product_name") or (product.get("name") if prod_oid else ""),
            "qty": qty,
            "unit_cost": unit_cost,
            "line_total": line_total,
        })
        if not prod_oid:
            continue
    branch_info = {}
    if exec_doc.get("branch"):
        branch_info["branch_name"] = exec_doc.get("branch", "")
    if exec_doc.get("branch_id"):
        branch_info["branch_id"] = exec_doc.get("branch_id")
    doc = {
        "date_dt": date_dt,
        "date": date_dt.strftime("%Y-%m-%d"),
        "time": date_dt.strftime("%H:%M"),
        "company_name": company_name,
        "reason": reason,
        "items": processed_items,
        "total_cost": total_cost,
        "status": status if status in ("posted", "draft") else "posted",
        "created_at": datetime.utcnow(),
        "created_by": exec_id,
        **branch_info,
    }
    returns_outwards_col.insert_one(doc)
    if doc["status"] == "posted":
        for upd in inventory_updates:
            inventory_col.update_one(
                {"_id": upd["_id"]},
                {"$set": {"qty": upd["new_qty"], "updated_at": datetime.utcnow()}}
            )
    return jsonify(ok=True, message="Returns outwards saved.")


@executive_returns_outwards_bp.route("/list", methods=["GET"])
def list_returns():
    scope = _ensure_exec_or_redirect()
    if not isinstance(scope, tuple):
        return jsonify(ok=False, message="Please log in."), 401
    filters, _, _, _, _ = _build_filters(request.args)
    rows = _recent_rets(filters)
    return jsonify(ok=True, rows=rows)


@executive_returns_outwards_bp.route("/stats", methods=["GET"])
def stats():
    scope = _ensure_exec_or_redirect()
    if not isinstance(scope, tuple):
        return jsonify(ok=False, message="Please log in."), 401
    filters, _, _, _, _ = _build_filters(request.args)
    filters["status"] = "posted"
    agg = list(
        returns_outwards_col.aggregate([
            {"$match": filters},
            {"$group": {"_id": None, "sum_total": {"$sum": {"$ifNull": ["$total_cost", 0]}}}}
        ])
    )
    total = float(agg[0].get("sum_total", 0) or 0) if agg else 0.0
    return jsonify(ok=True, total=round(total, 2))

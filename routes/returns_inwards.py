from __future__ import annotations

from datetime import datetime, date, time, timedelta
from typing import Any, Dict, List, Optional, Set
import json

from flask import (
    Blueprint,
    render_template,
    request,
    jsonify,
    redirect,
    url_for,
    flash,
    session,
)
from bson import ObjectId
from pymongo import ReturnDocument

from db import db

returns_inwards_bp = Blueprint("returns_inwards", __name__, template_folder="../templates")

users_col = db["users"]
customers_col = db["customers"]
products_col = db["products"]
inventory_col = db["inventory"]
returns_inwards_col = db["returns_inwards"]
counters_col = db["counters"]


def _ensure_indexes() -> None:
    try:
        returns_inwards_col.create_index([("return_date_dt", -1)])
        returns_inwards_col.create_index([("manager_id", 1)])
        returns_inwards_col.create_index([("status", 1)])
        returns_inwards_col.create_index([("customer_id", 1)])
        returns_inwards_col.create_index([("return_no", 1)], unique=True)
    except Exception:
        pass


_ensure_indexes()


def _get_current_role() -> tuple[Optional[str], Optional[str]]:
    if "admin_id" in session:
        return "admin", session["admin_id"]
    if "executive_id" in session:
        return "executive", session["executive_id"]
    if "manager_id" in session:
        return "manager", session["manager_id"]
    return None, None


def _safe_oid(val: Any) -> Optional[ObjectId]:
    if not val:
        return None
    try:
        return ObjectId(str(val))
    except Exception:
        return None


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        if v is None or v == "":
            return default
        return float(str(v).replace(",", "").strip())
    except Exception:
        return default


def _parse_date(s: str | None) -> Optional[datetime]:
    if not s:
        return None
    try:
        d = datetime.strptime(s, "%Y-%m-%d").date()
        return datetime.combine(d, time.min)
    except Exception:
        return None


def _month_range() -> tuple[str, str]:
    today = date.today()
    start = today.replace(day=1)
    if start.month == 12:
        end = date(start.year + 1, 1, 1) - timedelta(days=1)
    else:
        end = date(start.year, start.month + 1, 1) - timedelta(days=1)
    return start.isoformat(), end.isoformat()


def _manager_agent_ids(manager_id_str: str) -> Set[str]:
    agent_ids: Set[str] = set()
    mgr_oid = _safe_oid(manager_id_str)
    query: Dict[str, Any] = {"role": "agent"}
    if mgr_oid:
        query["$or"] = [{"manager_id": mgr_oid}, {"manager_id": manager_id_str}]
    else:
        query["manager_id"] = manager_id_str
    for ag in users_col.find(query, {"_id": 1}):
        agent_ids.add(str(ag["_id"]))
    return agent_ids


def _customer_belongs_to_manager(customer: Dict[str, Any], manager_id_str: str) -> bool:
    if not customer:
        return False
    cust_mgr = customer.get("manager_id")
    if cust_mgr is not None and str(cust_mgr) == str(manager_id_str):
        return True
    agent_id = customer.get("agent_id")
    if agent_id and str(agent_id) in _manager_agent_ids(manager_id_str):
        return True
    return False


def _next_return_no() -> str:
    year = datetime.utcnow().year
    key = f"returns_inwards:{year}"
    try:
        doc = counters_col.find_one_and_update(
            {"_id": key},
            {"$inc": {"seq": 1}, "$setOnInsert": {"year": year}},
            upsert=True,
            return_document=ReturnDocument.AFTER,
        )
        seq = int(doc.get("seq", 1))
        return f"RI-{year}-{seq:06d}"
    except Exception:
        ts = datetime.utcnow().strftime("%Y%m%d%H%M%S")
        return f"RI-{year}-{ts}"


def _extract_purchase_info(customer: Dict[str, Any], purchase_index: int) -> Optional[Dict[str, Any]]:
    purchases = customer.get("purchases") or []
    if purchase_index < 0 or purchase_index >= len(purchases):
        return None
    purchase = purchases[purchase_index] or {}
    product = purchase.get("product") or {}
    qty = _safe_float(product.get("quantity"), 1.0)
    total = product.get("total")
    if total is None:
        total = _safe_float(product.get("price")) * qty
    selling_total = _safe_float(total)
    product_id = product.get("_id") or product.get("product_id")
    return {
        "purchase": purchase,
        "product": product,
        "selling_total": selling_total,
        "qty": qty,
        "product_id": product_id,
    }


def _build_customer_scope_query(role: str, user_id: str) -> Dict[str, Any]:
    if role in ("admin", "executive"):
        return {}
    mgr_oid = _safe_oid(user_id)
    query: Dict[str, Any] = {"$or": [{"manager_id": user_id}]}
    if mgr_oid:
        query["$or"].append({"manager_id": mgr_oid})
    agent_ids = list(_manager_agent_ids(user_id))
    if agent_ids:
        query["$or"].append({"agent_id": {"$in": agent_ids}})
    return query


@returns_inwards_bp.get("/manager/returns-inwards")
def manager_returns_inwards_page():
    role, user_id = _get_current_role()
    if role not in ("manager", "admin", "executive") or not user_id:
        return redirect(url_for("login.login"))

    today_str = date.today().isoformat()

    return render_template(
        "manager_returns_inwards.html",
        role=role,
        today=today_str,
    )


@returns_inwards_bp.get("/manager/returns-inwards/search-customers")
def manager_returns_inwards_search_customers():
    role, user_id = _get_current_role()
    if role not in ("manager", "admin", "executive") or not user_id:
        return jsonify(ok=False, message="Unauthorized"), 401

    q = (request.args.get("q") or "").strip()
    if len(q) < 2:
        return jsonify(ok=True, customers=[])

    scope_query = _build_customer_scope_query(role, user_id)
    search_query = {
        "$or": [
            {"name": {"$regex": q, "$options": "i"}},
            {"phone_number": {"$regex": q, "$options": "i"}},
        ]
    }
    query = {"$and": [scope_query, search_query]} if scope_query else search_query

    customers = list(
        customers_col.find(query, {"name": 1, "phone_number": 1})
        .sort("name", 1)
        .limit(10)
    )
    results = [
        {"id": str(c["_id"]), "name": c.get("name", ""), "phone": c.get("phone_number", "")}
        for c in customers
    ]
    return jsonify(ok=True, customers=results)


@returns_inwards_bp.get("/manager/returns-inwards/customer/<customer_id>/purchases")
def manager_returns_inwards_customer_purchases(customer_id: str):
    role, user_id = _get_current_role()
    if role not in ("manager", "admin", "executive") or not user_id:
        return jsonify(ok=False, message="Unauthorized"), 401

    cust_oid = _safe_oid(customer_id)
    if not cust_oid:
        return jsonify(ok=False, message="Invalid customer id"), 400

    customer = customers_col.find_one(
        {"_id": cust_oid},
        {"purchases": 1, "agent_id": 1, "manager_id": 1},
    )
    if not customer:
        return jsonify(ok=False, message="Customer not found"), 404

    if role == "manager" and not _customer_belongs_to_manager(customer, user_id):
        return jsonify(ok=False, message="Not authorized for this customer"), 403

    purchases = customer.get("purchases") or []
    rows: List[Dict[str, Any]] = []
    for idx, p in enumerate(purchases):
        product = p.get("product") or {}
        qty = _safe_float(product.get("quantity"), 1.0)
        total = product.get("total")
        if total is None:
            total = _safe_float(product.get("price")) * qty
        rows.append(
            {
                "index": idx,
                "product_name": product.get("name") or "Unnamed Product",
                "total": _safe_float(total),
                "purchase_date": p.get("purchase_date") or "",
                "end_date": p.get("end_date") or "",
                "purchase_type": p.get("purchase_type") or "",
                "product_id": str(product.get("_id")) if product.get("_id") else "",
                "status": p.get("status") or "",
            }
        )

    return jsonify(ok=True, purchases=rows)


@returns_inwards_bp.get("/manager/returns-inwards/purchase/<customer_id>/<int:purchase_index>/components")
def manager_returns_inwards_purchase_components(customer_id: str, purchase_index: int):
    role, user_id = _get_current_role()
    if role not in ("manager", "admin", "executive") or not user_id:
        return jsonify(ok=False, message="Unauthorized"), 401

    cust_oid = _safe_oid(customer_id)
    if not cust_oid:
        return jsonify(ok=False, message="Invalid customer id"), 400

    customer = customers_col.find_one({"_id": cust_oid})
    if not customer:
        return jsonify(ok=False, message="Customer not found"), 404

    if role == "manager" and not _customer_belongs_to_manager(customer, user_id):
        return jsonify(ok=False, message="Not authorized for this customer"), 403

    purchase_info = _extract_purchase_info(customer, purchase_index)
    if not purchase_info:
        return jsonify(ok=False, message="Purchase not found"), 404

    product_id = purchase_info.get("product_id")
    if not product_id:
        return jsonify(ok=False, message="Purchased product id not found"), 400

    product_oid = _safe_oid(product_id)
    product = products_col.find_one({"_id": product_oid or product_id})
    if not product:
        return jsonify(ok=False, message="Bundle product not found"), 404

    components = product.get("components") or []
    rows: List[Dict[str, Any]] = []
    for comp in components:
        comp_id = comp.get("_id")
        bundle_qty = int(_safe_float(comp.get("quantity"), 0.0))
        comp_oid = _safe_oid(comp_id)
        inv_doc = inventory_col.find_one({"_id": comp_oid or comp_id}) if comp_id else None
        comp_name = (inv_doc or {}).get("name") or "Unknown item"
        comp_image_url = (inv_doc or {}).get("image_url") or ""
        comp_stock_qty = _safe_float((inv_doc or {}).get("qty"), 0.0)
        unit_price = (
            _safe_float((inv_doc or {}).get("selling_price"))
            or _safe_float((inv_doc or {}).get("price"))
        )
        unit_cost = _safe_float((inv_doc or {}).get("cost_price"))
        rows.append(
            {
                "component_id": str(comp_id) if comp_id else "",
                "component_name": comp_name,
                "component_image_url": comp_image_url,
                "component_stock_qty": comp_stock_qty,
                "bundle_qty": bundle_qty,
                "unit_price": unit_price,
                "unit_cost": unit_cost,
                "missing": inv_doc is None,
            }
        )
    rows.sort(key=lambda r: (r.get("component_name") or "").lower())

    return jsonify(
        ok=True,
        purchased_product_id=str(product.get("_id")),
        purchased_product_name=product.get("name", ""),
        purchased_product_total=_safe_float(purchase_info.get("selling_total")),
        components=rows,
    )


@returns_inwards_bp.post("/manager/returns-inwards/create")
def manager_returns_inwards_create():
    role, user_id = _get_current_role()
    if role not in ("manager", "admin", "executive") or not user_id:
        return redirect(url_for("login.login"))

    customer_id = (request.form.get("customer_id") or "").strip()
    purchase_index_raw = (request.form.get("purchase_index") or "").strip()
    return_date_str = (request.form.get("return_date") or "").strip()
    reason = (request.form.get("reason") or "").strip()
    notes = (request.form.get("notes") or "").strip()
    components_raw = request.form.get("components_payload") or "[]"

    if not customer_id or purchase_index_raw == "" or not reason:
        flash("Customer, purchase, and reason are required.", "warning")
        return redirect(url_for("returns_inwards.manager_returns_inwards_page"))

    cust_oid = _safe_oid(customer_id)
    if not cust_oid:
        flash("Invalid customer selected.", "warning")
        return redirect(url_for("returns_inwards.manager_returns_inwards_page"))

    try:
        purchase_index = int(purchase_index_raw)
    except Exception:
        flash("Invalid purchase selection.", "warning")
        return redirect(url_for("returns_inwards.manager_returns_inwards_page"))

    customer = customers_col.find_one({"_id": cust_oid})
    if not customer:
        flash("Customer not found.", "warning")
        return redirect(url_for("returns_inwards.manager_returns_inwards_page"))

    if role == "manager" and not _customer_belongs_to_manager(customer, user_id):
        flash("Not authorized for this customer.", "warning")
        return redirect(url_for("returns_inwards.manager_returns_inwards_page"))

    purchase_info = _extract_purchase_info(customer, purchase_index)
    if not purchase_info:
        flash("Selected purchase not found for this customer.", "warning")
        return redirect(url_for("returns_inwards.manager_returns_inwards_page"))

    dup_q = {
        "customer_id": cust_oid,
        "product_ref.purchase_index": purchase_index,
        "status": {"$ne": "Voided"},
    }
    if returns_inwards_col.count_documents(dup_q) > 0:
        flash("A return already exists for this purchase. Void it before recording a new one.", "warning")
        return redirect(url_for("returns_inwards.manager_returns_inwards_page"))

    return_dt = _parse_date(return_date_str) or datetime.combine(date.today(), time.min)
    return_date_str = return_dt.date().isoformat()

    selling_total = _safe_float(purchase_info["selling_total"], 0.0)

    manager_id = None
    manager_name = ""
    branch_id = None
    branch_name = ""

    if role == "manager":
        manager_id = user_id
    else:
        cust_mgr = customer.get("manager_id")
        if cust_mgr:
            manager_id = str(cust_mgr)

    if manager_id:
        mgr_doc = users_col.find_one({"_id": _safe_oid(manager_id)})
        if mgr_doc:
            manager_name = mgr_doc.get("name", "")
            branch_id = mgr_doc.get("branch_id") or mgr_doc.get("branch") or None
            branch_name = mgr_doc.get("branch_name") or mgr_doc.get("branch") or ""

    product = purchase_info["product"]
    purchase = purchase_info["purchase"]

    try:
        components_payload = json.loads(components_raw)
    except Exception:
        components_payload = []

    components_payload = components_payload if isinstance(components_payload, list) else []

    product_id = purchase_info.get("product_id")
    product_oid = _safe_oid(product_id)
    bundle_doc = products_col.find_one({"_id": product_oid or product_id}) if product_id else None
    bundle_components = (bundle_doc or {}).get("components") or []
    bundle_map: Dict[str, float] = {}
    for comp in bundle_components:
        comp_id = comp.get("_id")
        if comp_id is None:
            continue
        bundle_map[str(comp_id)] = _safe_float(comp.get("quantity"), 0.0)

    components_returned: List[Dict[str, Any]] = []
    sales_reduction_amount = 0.0
    returns_qty_total = 0.0

    for row in components_payload:
        comp_id_raw = (row or {}).get("component_id")
        returned_qty = _safe_float((row or {}).get("returned_qty"), 0.0)
        if returned_qty <= 0:
            continue
        if not comp_id_raw:
            continue
        bundle_qty = bundle_map.get(str(comp_id_raw), 0.0)
        if returned_qty > bundle_qty:
            flash("Returned quantity exceeds bundle quantity.", "warning")
            return redirect(url_for("returns_inwards.manager_returns_inwards_page"))

        comp_oid = _safe_oid(comp_id_raw)
        inv_doc = inventory_col.find_one({"_id": comp_oid or comp_id_raw}) if comp_id_raw else None
        comp_name = (inv_doc or {}).get("name") or "Unknown item"
        unit_price = (
            _safe_float((inv_doc or {}).get("selling_price"))
            or _safe_float((inv_doc or {}).get("price"))
        )
        unit_cost = _safe_float((inv_doc or {}).get("cost_price"))
        line_sales_value = _safe_float(returned_qty * unit_price)
        line_cost_value = _safe_float(returned_qty * unit_cost)

        components_returned.append(
            {
                "component_id": comp_oid or comp_id_raw,
                "component_name": comp_name,
                "bundle_qty": bundle_qty,
                "returned_qty": returned_qty,
                "unit_selling_price": unit_price,
                "unit_cost_price": unit_cost,
                "line_sales_value": line_sales_value,
                "line_cost_value": line_cost_value,
            }
        )
        sales_reduction_amount += line_sales_value
        returns_qty_total += returned_qty

    if not components_returned:
        flash("Select at least one component with returned quantity.", "warning")
        return redirect(url_for("returns_inwards.manager_returns_inwards_page"))

    returned_components_count = len(components_returned)

    doc = {
        "return_no": _next_return_no(),
        "created_at": datetime.utcnow(),
        "return_date": return_date_str,
        "return_date_dt": return_dt,
        "status": "Recorded",
        "reason": reason,
        "notes": notes,
        "manager_id": str(manager_id) if manager_id else "",
        "manager_name": manager_name,
        "branch_id": branch_id,
        "branch_name": branch_name,
        "recorded_by_user_id": str(user_id),
        "recorded_by_role": role,
        "customer_id": cust_oid,
        "customer_name": customer.get("name", ""),
        "customer_phone": customer.get("phone_number", ""),
        "purchase_ref": {
            "purchase_type": purchase.get("purchase_type") or "",
            "purchase_date": purchase.get("purchase_date") or "",
            "end_date": purchase.get("end_date") or "",
        },
        "product_return_ref": {
            "purchase_index": purchase_index,
            "purchased_product_id": str(product.get("_id")) if product.get("_id") else "",
            "purchased_product_name": product.get("name") or "Unnamed Product",
            "purchased_product_total": selling_total,
        },
        "components_returned": components_returned,
        "sales_reduction_amount": _safe_float(sales_reduction_amount, 0.0),
        "returns_qty_total": _safe_float(returns_qty_total, 0.0),
        "returned_components_count": returned_components_count,
    }

    returns_inwards_col.insert_one(doc)
    flash("Return inward recorded.", "success")
    return redirect(url_for("returns_inwards.manager_returns_inwards_page"))


@returns_inwards_bp.get("/accounting/returns-inwards")
def accounting_returns_inwards_list():
    role, user_id = _get_current_role()
    if role not in ("admin", "executive") or not user_id:
        return redirect(url_for("login.login"))

    from_str = (request.args.get("from") or "").strip()
    to_str = (request.args.get("to") or "").strip()
    status = (request.args.get("status") or "").strip()
    manager_id = (request.args.get("manager_id") or "").strip()
    branch = (request.args.get("branch") or "").strip()
    customer_q = (request.args.get("customer") or "").strip()
    component_q = (request.args.get("component") or "").strip()

    if not from_str and not to_str:
        from_str, to_str = _month_range()

    start_dt = _parse_date(from_str) if from_str else None
    end_dt = _parse_date(to_str) if to_str else None
    if end_dt:
        end_dt = end_dt.replace(hour=23, minute=59, second=59, microsecond=999999)

    query: Dict[str, Any] = {}
    if start_dt and end_dt:
        query["return_date_dt"] = {"$gte": start_dt, "$lte": end_dt}
    if status:
        query["status"] = status
    if manager_id:
        query["manager_id"] = manager_id
    if branch:
        query["$or"] = [{"branch_id": branch}, {"branch_name": branch}]
    if customer_q:
        query["$or"] = query.get("$or", []) + [
            {"customer_name": {"$regex": customer_q, "$options": "i"}},
            {"customer_phone": {"$regex": customer_q, "$options": "i"}},
        ]
    if component_q:
        query["components_returned.component_name"] = {"$regex": component_q, "$options": "i"}

    results_limit = 200
    returns = list(returns_inwards_col.find(query).sort("return_date_dt", -1).limit(results_limit))
    for r in returns:
        r["_id"] = str(r["_id"])
        r["sales_reduction_amount"] = _safe_float(r.get("sales_reduction_amount"))
        r["refund_amount"] = _safe_float(r.get("refund_amount"))
        r["return_date"] = r.get("return_date") or ""
        components = r.get("components_returned") or []
        if components:
            preview_parts = []
            qty_total = 0.0
            for c in components:
                name = c.get("component_name") or "Component"
                qty = _safe_float(c.get("returned_qty"))
                qty_total += qty
                preview_parts.append(f"{name} x{qty:g}")
            r["components_preview"] = ", ".join(preview_parts)
            r["returns_qty_total"] = _safe_float(r.get("returns_qty_total"), qty_total)
        else:
            r["components_preview"] = r.get("product_return_ref", {}).get("purchased_product_name", "")
            r["returns_qty_total"] = _safe_float(r.get("returns_qty_total"), 0.0)

    total_qty = sum(_safe_float(r.get("returns_qty_total")) for r in returns)
    total_value = sum(_safe_float(r.get("sales_reduction_amount")) for r in returns)
    metrics = {
        "count": len(returns),
        "total_qty": total_qty,
        "total_value": total_value,
    }

    managers = list(users_col.find({"role": "manager"}, {"name": 1}).sort("name", 1))
    manager_options = [{"id": str(m["_id"]), "name": m.get("name", "")} for m in managers]

    return render_template(
        "accounting/returns_inwards_list.html",
        returns=returns,
        metrics=metrics,
        results_limit=results_limit,
        from_str=from_str,
        to_str=to_str,
        status=status,
        manager_id=manager_id,
        branch=branch,
        customer_q=customer_q,
        component_q=component_q,
        manager_options=manager_options,
    )


@returns_inwards_bp.get("/manager/returns-inwards/history")
def manager_returns_inwards_history():
    role, user_id = _get_current_role()
    if role != "manager" or not user_id:
        return redirect(url_for("login.login"))

    from_str = (request.args.get("from") or "").strip()
    to_str = (request.args.get("to") or "").strip()
    status = (request.args.get("status") or "").strip()
    customer_q = (request.args.get("customer") or "").strip()

    if not from_str and not to_str:
        from_str, to_str = _month_range()

    start_dt = _parse_date(from_str) if from_str else None
    end_dt = _parse_date(to_str) if to_str else None
    if end_dt:
        end_dt = end_dt.replace(hour=23, minute=59, second=59, microsecond=999999)

    query: Dict[str, Any] = {"manager_id": str(user_id)}
    if start_dt and end_dt:
        query["return_date_dt"] = {"$gte": start_dt, "$lte": end_dt}
    if status:
        query["status"] = status
    if customer_q:
        query["$or"] = [
            {"customer_name": {"$regex": customer_q, "$options": "i"}},
            {"customer_phone": {"$regex": customer_q, "$options": "i"}},
        ]

    returns = list(returns_inwards_col.find(query).sort("return_date_dt", -1).limit(500))

    total_value = 0.0
    total_qty = 0.0
    component_counter: Dict[str, float] = {}

    for r in returns:
        r["_id"] = str(r["_id"])
        r["sales_reduction_amount"] = _safe_float(r.get("sales_reduction_amount"))
        r["return_date"] = r.get("return_date") or ""
        components = r.get("components_returned") or []
        qty_total = _safe_float(r.get("returns_qty_total"))
        if not qty_total:
            qty_total = sum(_safe_float(c.get("returned_qty")) for c in components)
        r["returns_qty_total"] = qty_total

        total_value += r["sales_reduction_amount"]
        total_qty += qty_total

        for c in components:
            name = c.get("component_name") or "Component"
            component_counter[name] = component_counter.get(name, 0.0) + _safe_float(c.get("returned_qty"))

    top_components = sorted(component_counter.items(), key=lambda kv: kv[1], reverse=True)[:5]
    top_components = [{"name": k, "qty": v} for k, v in top_components]

    metrics = {
        "count": len(returns),
        "total_qty": total_qty,
        "total_value": total_value,
        "top_components": top_components,
    }

    return render_template(
        "manager_returns_inwards_history.html",
        returns=returns,
        from_str=from_str,
        to_str=to_str,
        status=status,
        customer_q=customer_q,
        metrics=metrics,
    )


@returns_inwards_bp.post("/accounting/returns-inwards/<return_id>/approve")
def accounting_returns_inwards_approve(return_id: str):
    role, user_id = _get_current_role()
    if role not in ("admin", "executive") or not user_id:
        return redirect(url_for("login.login"))

    oid = _safe_oid(return_id)
    if not oid:
        flash("Invalid return id.", "warning")
        return redirect(url_for("returns_inwards.accounting_returns_inwards_list"))

    returns_inwards_col.update_one(
        {"_id": oid},
        {
            "$set": {
                "status": "Approved",
                "approved_by": str(user_id),
                "approved_at": datetime.utcnow(),
            }
        },
    )
    flash("Return inward approved.", "success")
    return redirect(url_for("returns_inwards.accounting_returns_inwards_list"))


@returns_inwards_bp.post("/accounting/returns-inwards/<return_id>/void")
def accounting_returns_inwards_void(return_id: str):
    role, user_id = _get_current_role()
    if role not in ("admin", "executive") or not user_id:
        return redirect(url_for("login.login"))

    void_reason = (request.form.get("void_reason") or "").strip()
    if not void_reason:
        flash("Void reason is required.", "warning")
        return redirect(url_for("returns_inwards.accounting_returns_inwards_list"))

    oid = _safe_oid(return_id)
    if not oid:
        flash("Invalid return id.", "warning")
        return redirect(url_for("returns_inwards.accounting_returns_inwards_list"))

    returns_inwards_col.update_one(
        {"_id": oid},
        {
            "$set": {
                "status": "Voided",
                "void_reason": void_reason,
                "voided_by": str(user_id),
                "voided_at": datetime.utcnow(),
            }
        },
    )
    flash("Return inward voided.", "success")
    return redirect(url_for("returns_inwards.accounting_returns_inwards_list"))

# executive_inventory_analytics.py
# -------------------------------------------------------------------
# SCHEMA + WORKFLOW MAP (repo-derived)
# Collections used:
# - users: {_id, role, name, branch, manager_id} for branch/agent/manager mapping.
# - inventory: {_id, name, qty, price, selling_price, cost_price, manager_id, updated_at}
#   Stock snapshot per manager (branch); created via add_inventory.
# - products: {_id, name, price, manager_id, category?} package definitions (no stock qty).
# - inventory_products_outflow: outflow audit logs for stock leaving inventory
#   sources include: instant_sale, close_card, Agent_deliveries, etc.
#   fields vary: created_at, selected_total_price, selected_product, selected_qty,
#   components_deducted[{inventory_id, required_qty, ...}], manager_id (sometimes).
# - instant_sales: {agent_id, manager_id, product{...}, purchase_date, payment_method}
#   Instant/cash sales; updates inventory qty and logs to outflow.
# - card_closures: {customer_id, at, payload{selected_product_name, kept_amount, ...}}
#   Card closure audit log (customer remains; used for closed-card metrics).
# - customers: {_id, name, phone_number, agent_id, manager_id, purchases[]}
#   Used to map card_closures to branch (manager_id).
# - stock_entries: {name, quantity, unit_price, total_cost, purchased_at, created_by}
#   Executive stock-in purchases (stock in).
# - orders, order_events: inventory delivery tracking (deliver_line events, branch in orders).
# - assigned_products: allocated/reserved stock per manager (assigned_total, sent_total).
#
# Workflow notes:
# - Stock-in: executive_stock_entry -> stock_entries.
# - Stock-out: instant_sales + close_card + Agent_deliveries -> inventory_products_outflow.
# - Branch/manager linkage: inventory.manager_id -> users(role=manager).branch.
# - Agent linkage: instant_sales.agent_id -> users(role=agent).
# - Deliveries: orders/items + order_events (deliver_line).
# -------------------------------------------------------------------
from __future__ import annotations

from flask import Blueprint, render_template, request, redirect, url_for, session
from bson import ObjectId
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple
from db import db

executive_inventory_analytics_bp = Blueprint("executive_inventory_analytics", __name__)

users_col = db["users"]
inventory_col = db["inventory"]
products_col = db["products"]
inventory_outflow_col = db["inventory_products_outflow"]
instant_sales_col = db["instant_sales"]
card_closures_col = db["card_closures"]
customers_col = db["customers"]
orders_col = db["orders"]
order_events_col = db["order_events"]
stock_entries_col = db["stock_entries"]
assigned_products_col = db["assigned_products"]

LOW_STOCK_THRESHOLD = 5
DEAD_STOCK_DAYS = 90


@executive_inventory_analytics_bp.app_template_filter("money_gh")
def money_gh(value):
    try:
        if value is None:
            return "GHS 0.00"
        return f"GHS {float(value):,.2f}"
    except Exception:
        return "GHS 0.00"


@executive_inventory_analytics_bp.app_template_filter("number_fmt")
def number_fmt(value):
    try:
        if value is None:
            return "0"
        return f"{int(round(float(value))):,}"
    except Exception:
        return "0"


def _oid(v) -> Optional[ObjectId]:
    try:
        return ObjectId(str(v))
    except Exception:
        return None


def _ensure_exec_or_admin():
    uid = session.get("executive_id") or session.get("admin_id")
    if not uid:
        return None
    user = users_col.find_one({"_id": _oid(uid)}) or users_col.find_one({"_id": uid})
    if not user:
        return None
    role = (user.get("role") or "").lower()
    if role not in ("executive", "admin"):
        return None
    return user


def _parse_date_range(args) -> Tuple[datetime, datetime, str]:
    preset = (args.get("range") or "30").strip()
    start_str = (args.get("start") or "").strip()
    end_str = (args.get("end") or "").strip()

    now = datetime.utcnow()
    end = now
    start = now - timedelta(days=30)

    if preset in ("7", "30", "90"):
        days = int(preset)
        end = now
        start = now - timedelta(days=days)
        label = f"Last {days} Days"
    else:
        label = "Custom"
        try:
            if start_str:
                start = datetime.strptime(start_str, "%Y-%m-%d")
            if end_str:
                end = datetime.strptime(end_str, "%Y-%m-%d")
        except Exception:
            start = now - timedelta(days=30)
            end = now
            label = "Last 30 Days"

    # normalize end to end-of-day
    end = end.replace(hour=23, minute=59, second=59, microsecond=0)
    return start, end, label


def _safe_float(v, default=0.0) -> float:
    try:
        return float(v)
    except Exception:
        return float(default)


def _build_manager_maps():
    managers = list(users_col.find({"role": "manager"}, {"_id": 1, "name": 1, "branch": 1}))
    manager_map = {}
    branches = set()
    for m in managers:
        mid = str(m["_id"])
        branch = (m.get("branch") or "").strip()
        manager_map[mid] = {"name": m.get("name", ""), "branch": branch}
        if branch:
            branches.add(branch)
    return manager_map, sorted(branches)


def _build_agent_list():
    agents = list(users_col.find({"role": "agent"}, {"_id": 1, "name": 1, "branch": 1}))
    out = []
    for a in agents:
        out.append({
            "id": str(a["_id"]),
            "name": a.get("name", ""),
            "branch": a.get("branch", ""),
        })
    out.sort(key=lambda x: (x["branch"], x["name"]))
    return out


def _inventory_match(branch: Optional[str], category: Optional[str], manager_map: Dict[str, Dict[str, str]]):
    match: Dict[str, Any] = {}
    if branch:
        manager_ids_str = [mid for mid, meta in manager_map.items() if meta.get("branch") == branch]
        manager_ids_oid = [ObjectId(mid) for mid in manager_ids_str if _oid(mid)]
        if manager_ids_str or manager_ids_oid:
            match["$or"] = [
                {"manager_id": {"$in": manager_ids_oid}},
                {"manager_id": {"$in": manager_ids_str}}
            ]
        else:
            match["manager_id"] = {"$in": []}
    if category:
        match["category"] = category
    return match


def _group_by_date(items: List[Tuple[datetime, float]]) -> Dict[str, float]:
    out: Dict[str, float] = {}
    for dt, val in items:
        if not isinstance(dt, datetime):
            continue
        key = dt.strftime("%Y-%m-%d")
        out[key] = out.get(key, 0.0) + float(val or 0.0)
    return out


@executive_inventory_analytics_bp.route("/executive/analytics/inventory", methods=["GET"])
def executive_inventory_analytics():
    user = _ensure_exec_or_admin()
    if not user:
        return redirect(url_for("login.login"))

    manager_map, branches = _build_manager_maps()
    agents = _build_agent_list()
    categories = [c for c in inventory_col.distinct("category") if c]

    branch = (request.args.get("branch") or "").strip()
    agent_id = (request.args.get("agent") or "").strip()
    category = (request.args.get("category") or "").strip()
    include_transfers = request.args.get("include_transfers") == "1"
    include_adjustments = request.args.get("include_adjustments") == "1"

    if category and category not in categories:
        category = ""

    start_dt, end_dt, range_label = _parse_date_range(request.args)

    inv_match = _inventory_match(branch or None, category or None, manager_map)
    inventory_docs = list(
        inventory_col.find(
            inv_match,
            {"name": 1, "qty": 1, "price": 1, "selling_price": 1, "cost_price": 1, "manager_id": 1}
        )
    )

    total_units = 0
    cost_value = 0.0
    selling_value = 0.0
    low_stock = 0
    out_of_stock = 0

    product_value_map: Dict[str, Dict[str, float]] = {}
    inv_by_id: Dict[str, Dict[str, Any]] = {}
    stock_value_by_branch: Dict[str, float] = {}
    top_products: Dict[str, float] = {}

    for doc in inventory_docs:
        qty = int(doc.get("qty", 0) or 0)
        cost = _safe_float(doc.get("cost_price"), _safe_float(doc.get("price"), 0.0))
        sell = _safe_float(doc.get("selling_price"), _safe_float(doc.get("price"), 0.0))

        total_units += qty
        cost_value += qty * cost
        selling_value += qty * sell

        if qty <= 0:
            out_of_stock += 1
        elif qty <= LOW_STOCK_THRESHOLD:
            low_stock += 1

        mid = str(doc.get("manager_id")) if doc.get("manager_id") else ""
        branch_name = manager_map.get(mid, {}).get("branch", "Unknown")
        stock_value_by_branch[branch_name] = stock_value_by_branch.get(branch_name, 0.0) + (qty * cost)

        pname = (doc.get("name") or "Unnamed").strip()
        top_products[pname] = top_products.get(pname, 0.0) + (qty * sell)

        inv_id = str(doc.get("_id"))
        inv_by_id[inv_id] = doc
        product_value_map[inv_id] = {"cost": cost, "sell": sell}

    gross_profit = selling_value - cost_value

    # Dead stock (no outflow in last DEAD_STOCK_DAYS)
    dead_stock_value = None
    dead_stock_count = None
    dead_stock_available = True
    try:
        window_start = datetime.utcnow() - timedelta(days=DEAD_STOCK_DAYS)
        outflow_docs = list(
            inventory_outflow_col.find(
                {"created_at": {"$gte": window_start}},
                {"selected_product_id": 1, "components_deducted.inventory_id": 1}
            )
        )
        moved_ids = set()
        for d in outflow_docs:
            sid = d.get("selected_product_id")
            if sid:
                moved_ids.add(str(sid))
            for comp in (d.get("components_deducted") or []):
                cid = comp.get("inventory_id")
                if cid:
                    moved_ids.add(str(cid))

        dead_stock_value = 0.0
        dead_stock_count = 0
        for doc in inventory_docs:
            if str(doc.get("_id")) in moved_ids:
                continue
            qty = int(doc.get("qty", 0) or 0)
            cost = _safe_float(doc.get("cost_price"), _safe_float(doc.get("price"), 0.0))
            dead_stock_value += qty * cost
            dead_stock_count += 1
    except Exception:
        dead_stock_available = False

    # Stock-out value (period)
    outflow_value = None
    outflow_available = True
    try:
        outflow_match = {"created_at": {"$gte": start_dt, "$lte": end_dt}}
        outflow_docs = list(
            inventory_outflow_col.find(
                outflow_match,
                {
                    "created_at": 1,
                    "selected_total_price": 1,
                    "selected_qty": 1,
                    "selected_product": 1,
                    "selected_product_id": 1,
                    "components_deducted.inventory_id": 1,
                    "components_deducted.required_qty": 1,
                }
            )
        )
        outflow_value = 0.0
        for d in outflow_docs:
            created_at = d.get("created_at")
            val = _safe_float(d.get("selected_total_price"), None)
            if val is None:
                sel = d.get("selected_product") or {}
                qty = _safe_float(d.get("selected_qty"), 0.0)
                if sel:
                    price = _safe_float(sel.get("selling_price"), _safe_float(sel.get("price"), 0.0))
                    val = qty * price
                else:
                    val = 0.0
            if val == 0.0:
                for comp in (d.get("components_deducted") or []):
                    cid = str(comp.get("inventory_id"))
                    rq = _safe_float(comp.get("required_qty"), 0.0)
                    val += rq * product_value_map.get(cid, {}).get("cost", 0.0)
            outflow_value += float(val or 0.0)
    except Exception:
        outflow_available = False

    # Stock In vs Stock Out (chart data)
    stock_in_out_chart = None
    stock_in_note = ""
    outflow_note = ""
    try:
        group_fmt = "%Y-%m-%d"
        if (end_dt - start_dt).days > 90:
            group_fmt = "%G-W%V"

        manager_ids_str = []
        manager_ids_oid = []
        if branch:
            manager_ids_str = [mid for mid, meta in manager_map.items() if meta.get("branch") == branch]
            manager_ids_oid = [ObjectId(mid) for mid in manager_ids_str if _oid(mid)]
            if not manager_ids_str and not manager_ids_oid:
                outflow_note = "Stock Out is company-level"

        out_match: Dict[str, Any] = {
            "created_at": {"$gte": start_dt, "$lte": end_dt}
        }
        if manager_ids_str or manager_ids_oid:
            out_match["$or"] = [
                {"manager_id": {"$in": manager_ids_oid}},
                {"manager_id": {"$in": manager_ids_str}}
            ]

        out_pipeline = [
            {"$match": out_match},
            {"$project": {
                "day": {"$dateToString": {"format": group_fmt, "date": "$created_at"}},
                "out_amount": {
                    "$switch": {
                        "branches": [
                            {
                                "case": {"$eq": ["$source", "close_card"]},
                                "then": {"$ifNull": ["$budget.forfeited_amount", 0]}
                            },
                            {
                                "case": {"$eq": ["$source", "Agent_deliveries"]},
                                "then": {
                                    "$ifNull": [
                                        "$packaged_product.total",
                                        {"$multiply": [
                                            {"$ifNull": ["$packaged_product.price", 0]},
                                            {"$ifNull": ["$package_qty", {"$ifNull": ["$packaged_product.quantity", 1]}]}
                                        ]}
                                    ]
                                }
                            }
                        ],
                        "default": {
                            "$ifNull": [
                                {"$multiply": [
                                    {"$ifNull": ["$selected_product.price", None]},
                                    {"$ifNull": ["$selected_product.qty", 1]}
                                ]},
                                {"$ifNull": ["$closed_product.total", {"$ifNull": ["$packaged_product.total", 0]}]}
                            ]
                        }
                    }
                }
            }},
            {"$group": {"_id": "$day", "total": {"$sum": "$out_amount"}}},
            {"$sort": {"_id": 1}}
        ]
        out_agg = list(inventory_outflow_col.aggregate(out_pipeline))
        out_map = {d["_id"]: float(d.get("total", 0) or 0) for d in out_agg}

        in_match = {
            "$or": [
                {"purchased_at": {"$gte": start_dt, "$lte": end_dt}},
                {"created_at": {"$gte": start_dt, "$lte": end_dt}}
            ]
        }
        if branch:
            stock_in_note = "Stock In is company-level"

        in_pipeline = [
            {"$match": in_match},
            {"$project": {
                "day": {"$dateToString": {"format": group_fmt, "date": {"$ifNull": ["$purchased_at", "$created_at"]}}},
                "in_amount": {"$ifNull": ["$total_cost", {"$multiply": [
                    {"$ifNull": ["$quantity", 0]},
                    {"$ifNull": ["$unit_price", 0]}
                ]}]}
            }},
            {"$group": {"_id": "$day", "total": {"$sum": "$in_amount"}}},
            {"$sort": {"_id": 1}}
        ]
        in_agg = list(stock_entries_col.aggregate(in_pipeline))
        in_map = {d["_id"]: float(d.get("total", 0) or 0) for d in in_agg}

        labels = sorted(set(list(in_map.keys()) + list(out_map.keys())))
        in_vals = [in_map.get(lbl, 0.0) for lbl in labels]
        out_vals = [out_map.get(lbl, 0.0) for lbl in labels]

        if labels:
            stock_in_out_chart = {"labels": labels, "in": in_vals, "out": out_vals}
    except Exception:
        stock_in_out_chart = None

    # Instant sales
    instant_available = True
    instant_sales = []
    try:
        inst_match: Dict[str, Any] = {}
        if agent_id:
            inst_match["agent_id"] = agent_id
        if branch:
            manager_ids = [mid for mid, meta in manager_map.items() if meta.get("branch") == branch]
            inst_match["manager_id"] = {"$in": manager_ids + [ObjectId(mid) for mid in manager_ids if _oid(mid)]}
        sales = list(instant_sales_col.find(inst_match))
        for s in sales:
            try:
                dt = datetime.strptime(s.get("purchase_date", ""), "%Y-%m-%d")
            except Exception:
                dt = None
            if not dt or not (start_dt <= dt <= end_dt):
                continue
            prod = s.get("product") or {}
            amount = _safe_float(prod.get("total"), _safe_float(prod.get("price"), 0.0))
            instant_sales.append({
                "date": dt,
                "product_name": prod.get("name", "Unnamed"),
                "amount": amount,
                "customer": s.get("customer_name", ""),
                "branch": manager_map.get(str(s.get("manager_id")), {}).get("branch", "Unknown")
            })
    except Exception:
        instant_available = False

    instant_count = len(instant_sales)
    instant_revenue = sum(s["amount"] for s in instant_sales)
    instant_by_branch: Dict[str, float] = {}
    instant_by_product: Dict[str, float] = {}
    instant_trend_items: List[Tuple[datetime, float]] = []
    for s in instant_sales:
        instant_by_branch[s["branch"]] = instant_by_branch.get(s["branch"], 0.0) + s["amount"]
        instant_by_product[s["product_name"]] = instant_by_product.get(s["product_name"], 0.0) + s["amount"]
        instant_trend_items.append((s["date"], s["amount"]))

    # Closed cards metrics
    closed_available = True
    closed_cards = []
    try:
        closures = list(card_closures_col.find(
            {"at": {"$gte": start_dt, "$lte": end_dt}},
            {"customer_id": 1, "at": 1, "payload": 1}
        ))
        customer_ids = [c.get("customer_id") for c in closures if c.get("customer_id")]
        customer_map = {
            c["_id"]: c for c in customers_col.find({"_id": {"$in": customer_ids}}, {"manager_id": 1})
        }
        for c in closures:
            cust = customer_map.get(c.get("customer_id"))
            mgr_id = cust.get("manager_id") if cust else None
            branch_name = manager_map.get(str(mgr_id), {}).get("branch", "Unknown")
            payload = c.get("payload") or {}
            closed_cards.append({
                "date": c.get("at"),
                "product_name": payload.get("selected_product_name", "Unknown"),
                "kept_amount": _safe_float(payload.get("kept_amount"), 0.0),
                "branch": branch_name
            })
    except Exception:
        closed_available = False

    closed_count = len(closed_cards)
    closed_value = sum(c["kept_amount"] for c in closed_cards)
    closed_by_branch: Dict[str, float] = {}
    closed_by_product: Dict[str, int] = {}
    closed_trend_items: List[Tuple[datetime, float]] = []
    for c in closed_cards:
        closed_by_branch[c["branch"]] = closed_by_branch.get(c["branch"], 0.0) + c["kept_amount"]
        closed_by_product[c["product_name"]] = closed_by_product.get(c["product_name"], 0) + 1
        if isinstance(c["date"], datetime):
            closed_trend_items.append((c["date"], 1))

    # Deliveries / field ops (order_events: deliver_line)
    deliveries_available = True
    deliveries = []
    try:
        evs = list(order_events_col.find(
            {"type": "deliver_line", "at": {"$gte": start_dt, "$lte": end_dt}},
            {"order_id": 1, "payload": 1, "at": 1}
        ))
        order_ids = list({e.get("order_id") for e in evs if e.get("order_id")})
        order_map = {o["_id"]: o for o in orders_col.find({"_id": {"$in": order_ids}}, {"branch": 1, "manager_id": 1})}
        for e in evs:
            o = order_map.get(e.get("order_id")) or {}
            deliveries.append({
                "date": e.get("at"),
                "branch": o.get("branch", "Unknown"),
                "item": (e.get("payload") or {}).get("item_name", ""),
                "qty": (e.get("payload") or {}).get("qty", 0),
                "manager_id": str(o.get("manager_id")) if o.get("manager_id") else None
            })
    except Exception:
        deliveries_available = False

    deliveries_count = len(deliveries)
    deliveries_by_branch: Dict[str, int] = {}
    deliveries_trend_items: List[Tuple[datetime, float]] = []
    for d in deliveries:
        deliveries_by_branch[d["branch"]] = deliveries_by_branch.get(d["branch"], 0) + 1
        if isinstance(d["date"], datetime):
            deliveries_trend_items.append((d["date"], 1))

    # Allocated / reserved stock value (assigned_products)
    allocated_available = True
    allocated_value = None
    try:
        assigned_docs = list(assigned_products_col.find({}, {"product_id": 1, "assigned_total": 1, "sent_total": 1}))
        product_ids = list({d.get("product_id") for d in assigned_docs if d.get("product_id")})
        price_map = {}
        for p in products_col.find({"_id": {"$in": [_oid(pid) for pid in product_ids if _oid(pid)]}}, {"price": 1}):
            price_map[str(p["_id"])] = _safe_float(p.get("price"), 0.0)
        total_alloc = 0.0
        for d in assigned_docs:
            assigned = int(d.get("assigned_total", 0) or 0)
            sent = int(d.get("sent_total", 0) or 0)
            remaining = max(0, assigned - sent)
            price = price_map.get(d.get("product_id"), 0.0)
            total_alloc += remaining * price
        allocated_value = total_alloc
    except Exception:
        allocated_available = False

    def _sorted_top(data: Dict[str, float], limit=10):
        return sorted(data.items(), key=lambda x: x[1], reverse=True)[:limit]

    kpis = [
        {"label": "Total Stock Value (Cost)", "value": cost_value, "unit": "GHS", "change": None},
        {"label": "Total Stock Value (Selling)", "value": selling_value, "unit": "GHS", "change": None},
        {"label": "Potential Gross Profit", "value": gross_profit, "unit": "GHS", "change": None},
        {"label": "Total Units in Stock", "value": total_units, "unit": "", "change": None},
        {"label": "Low Stock Items", "value": low_stock, "unit": "", "change": None},
        {"label": "Out of Stock Items", "value": out_of_stock, "unit": "", "change": None},
        {
            "label": "Dead Stock Value (90d)",
            "value": dead_stock_value,
            "unit": "GHS",
            "change": None,
            "available": dead_stock_available
        },
        {
            "label": "Allocated Stock Value",
            "value": allocated_value,
            "unit": "GHS",
            "change": None,
            "available": allocated_available
        },
        {
            "label": "Stock Out Value (Period)",
            "value": outflow_value,
            "unit": "GHS",
            "change": None,
            "available": outflow_available
        },
    ]

    charts = {
        "stock_by_branch": _sorted_top(stock_value_by_branch, 12),
        "top_products": _sorted_top(top_products, 10),
        "stock_in_out": stock_in_out_chart,
        "instant_trend": _group_by_date(instant_trend_items) if instant_available else None,
        "instant_by_branch": _sorted_top(instant_by_branch, 10) if instant_available else None,
        "closed_trend": _group_by_date(closed_trend_items) if closed_available else None,
        "closed_by_branch": _sorted_top(closed_by_branch, 10) if closed_available else None,
        "deliveries_trend": _group_by_date(deliveries_trend_items) if deliveries_available else None,
        "deliveries_by_branch": _sorted_top(deliveries_by_branch, 10) if deliveries_available else None,
    }

    tables = {
        "instant_recent": sorted(instant_sales, key=lambda x: x["date"], reverse=True)[:10],
        "deliveries_recent": sorted(deliveries, key=lambda x: x["date"] or datetime.min, reverse=True)[:10],
        "closed_by_branch": _sorted_top(closed_by_branch, 10),
    }

    insights = []
    if low_stock > 0:
        top_low = sorted(
            inventory_docs,
            key=lambda x: int(x.get("qty", 0) or 0)
        )[:5]
        insights.append("Stockout risk: " + ", ".join([(p.get("name") or "Unnamed") for p in top_low if p]))
    if dead_stock_available and dead_stock_value:
        insights.append(f"Dead stock lockup: GHS {dead_stock_value:,.2f}")
    if instant_by_branch:
        best_branch = _sorted_top(instant_by_branch, 1)[0][0]
        insights.append(f"Best branch for instant sales: {best_branch}")
    if closed_by_branch:
        best_close = _sorted_top(closed_by_branch, 1)[0][0]
        insights.append(f"Highest closures branch: {best_close}")
    if deliveries_by_branch:
        top_delivery = _sorted_top(deliveries_by_branch, 1)[0][0]
        insights.append(f"Top delivery branch: {top_delivery}")

    return render_template(
        "executive_inventory_analytics.html",
        branches=branches,
        agents=agents,
        categories=categories,
        filters={
            "branch": branch,
            "agent": agent_id,
            "category": category,
            "range": request.args.get("range", "30"),
            "start": request.args.get("start", ""),
            "end": request.args.get("end", ""),
            "include_transfers": include_transfers,
            "include_adjustments": include_adjustments
        },
        range_label=range_label,
        kpis=kpis,
        charts=charts,
        tables=tables,
        insights=insights,
        instant_count=instant_count,
        instant_revenue=instant_revenue,
        closed_count=closed_count,
        closed_value=closed_value,
        deliveries_count=deliveries_count,
        stock_in_note=stock_in_note,
        stock_out_note=outflow_note
    )

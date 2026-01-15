from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from bson import ObjectId
from flask import Blueprint, jsonify, render_template, request, session

from db import db
from login import get_current_identity, role_required

reports_insights_bp = Blueprint("reports_insights", __name__, url_prefix="/reports")

users_col = db["users"]
inventory_outflow_col = db["inventory_products_outflow"]
customers_col = db["customers"]
payments_col = db["payments"]
inventory_col = db["inventory"]


def _oid(val: Any) -> Optional[ObjectId]:
    try:
        return ObjectId(str(val))
    except Exception:
        return None


def _id_variants(val: Any) -> List[Any]:
    if not val:
        return []
    variants: List[Any] = []
    val_str = str(val)
    variants.append(val_str)
    oid = _oid(val)
    if oid:
        variants.append(oid)
    return variants


def _parse_date_range(args) -> Tuple[datetime, datetime, str]:
    preset = (args.get("range") or "30").strip()
    start_str = (args.get("start") or "").strip()
    end_str = (args.get("end") or "").strip()

    now = datetime.utcnow()
    start = now - timedelta(days=30)
    end = now
    label = "Last 30 days"

    if preset in {"7", "30", "90"}:
        days = int(preset)
        start = now - timedelta(days=days)
        end = now
        label = f"Last {days} days"
    elif start_str or end_str:
        label = "Custom range"
        try:
            if start_str:
                start = datetime.strptime(start_str, "%Y-%m-%d")
            if end_str:
                end = datetime.strptime(end_str, "%Y-%m-%d")
        except Exception:
            start = now - timedelta(days=30)
            end = now
            label = "Last 30 days"

    return start, end, label


def _list_managers() -> List[Dict[str, Any]]:
    managers = list(users_col.find({"role": "manager"}, {"name": 1, "branch": 1}))
    return [
        {"id": str(m.get("_id")), "name": m.get("name") or "Manager", "branch": m.get("branch") or "-"}
        for m in managers
    ]


def _list_branches() -> List[str]:
    branches = users_col.distinct("branch", {"role": {"$in": ["agent", "manager"]}})
    return sorted([b for b in branches if b])


def _agent_query(manager_variants: List[Any], branch: str | None) -> Dict[str, Any]:
    query: Dict[str, Any] = {"role": "agent"}
    if manager_variants:
        query["manager_id"] = {"$in": manager_variants}
    if branch:
        query["branch"] = branch
    return query


def _list_agents(manager_variants: List[Any], branch: str | None) -> List[Dict[str, Any]]:
    agents = list(users_col.find(_agent_query(manager_variants, branch), {"name": 1, "branch": 1}))
    return [
        {
            "id": str(a.get("_id")),
            "name": a.get("name") or a.get("username") or "Agent",
            "branch": a.get("branch") or "-",
        }
        for a in agents
    ]


def _agent_map(agent_ids: List[str]) -> Dict[str, Dict[str, Any]]:
    if not agent_ids:
        return {}
    oids = [oid for oid in (_oid(v) for v in agent_ids) if oid]
    query: Dict[str, Any] = {"role": "agent"}
    if oids:
        query["_id"] = {"$in": oids}
    docs = list(users_col.find(query, {"name": 1, "branch": 1}))
    return {str(d["_id"]): d for d in docs}


def _manager_ids_for_branch(branch: str) -> List[Any]:
    if not branch:
        return []
    managers = list(users_col.find({"role": "manager", "branch": branch}, {"_id": 1}))
    ids = []
    for m in managers:
        mid = m.get("_id")
        if not mid:
            continue
        ids.append(mid)
        ids.append(str(mid))
    return ids


def _as_float(val: Any) -> float:
    try:
        return float(val)
    except Exception:
        return 0.0


def _status_allows_liability(status_raw: Any) -> bool:
    status = (status_raw or "payment_ongoing").strip().lower()
    return status not in {"completed", "approved", "packaging", "delivering", "delivered", "closed"}


def _product_status_allows_liability(status_raw: Any) -> bool:
    status = (status_raw or "active").strip().lower()
    return status not in {"completed", "closed", "packaged", "delivered"}


@reports_insights_bp.route("/insights", methods=["GET"])
@role_required("executive", "admin", "manager")
def insights_page():
    ident = get_current_identity()
    role = ident.get("role")

    manager_id = session.get("manager_id") if role == "manager" else (request.args.get("manager_id") or "")
    manager_variants = _id_variants(manager_id)

    now = datetime.utcnow()
    default_start = (now - timedelta(days=30)).strftime("%Y-%m-%d")
    default_end = now.strftime("%Y-%m-%d")

    context = {
        "identity": ident,
        "managers": _list_managers(),
        "branches": _list_branches(),
        "agents": _list_agents(manager_variants, None),
        "default_start": default_start,
        "default_end": default_end,
        "default_range": "30",
        "manager_id": str(manager_id) if manager_id else "",
        "can_pick_manager": role in {"executive", "admin"},
    }
    return render_template("reports/insights.html", **context)


def _date_key(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d")


@reports_insights_bp.route("/insights/data", methods=["GET"])
@role_required("executive", "admin", "manager")
def insights_data():
    ident = get_current_identity()
    role = ident.get("role")

    manager_id = session.get("manager_id") if role == "manager" else (request.args.get("manager_id") or "")
    manager_variants = _id_variants(manager_id)
    branch = (request.args.get("branch") or "").strip()
    agent_id = (request.args.get("agent") or "").strip()

    start_dt, end_dt, range_label = _parse_date_range(request.args)
    end_exclusive = end_dt + timedelta(days=1)
    start_date_str = start_dt.strftime("%Y-%m-%d")
    end_date_str = end_dt.strftime("%Y-%m-%d")

    branch_agent_ids = [a["id"] for a in _list_agents(manager_variants, branch or None)] if branch else []
    agent_ids_for_filters = branch_agent_ids if branch else []

    # ---- Profit (inventory_products_outflow) ----
    outflow_conditions: List[Dict[str, Any]] = [
        {"created_at": {"$gte": start_dt, "$lt": end_exclusive}}
    ]

    if manager_variants:
        manager_agent_ids = [a["id"] for a in _list_agents(manager_variants, None)]
        manager_or: List[Dict[str, Any]] = [{"manager_id": {"$in": manager_variants}}]
        if manager_agent_ids:
            manager_or += [
                {"agent_id": {"$in": manager_agent_ids}},
                {"by_user": {"$in": manager_agent_ids}},
                {"agent.id": {"$in": manager_agent_ids}},
            ]
        outflow_conditions.append({"$or": manager_or})

    if branch:
        branch_or: List[Dict[str, Any]] = [{"agent_branch": branch}]
        if branch_agent_ids:
            branch_or += [
                {"agent_id": {"$in": branch_agent_ids}},
                {"by_user": {"$in": branch_agent_ids}},
                {"agent.id": {"$in": branch_agent_ids}},
            ]
        outflow_conditions.append({"$or": branch_or})

    if agent_id:
        outflow_conditions.append({
            "$or": [
                {"agent_id": agent_id},
                {"by_user": agent_id},
                {"agent.id": agent_id},
            ]
        })

    outflow_query = {"$and": outflow_conditions} if outflow_conditions else {}
    outflow_docs = list(
        inventory_outflow_col.find(
            outflow_query,
            {
                "total_profit": 1,
                "agent_id": 1,
                "agent_name": 1,
                "agent_branch": 1,
                "by_user": 1,
                "agent": 1,
            },
        )
    )

    profit_trend_map: Dict[str, float] = {}
    outflow_agent_ids: List[str] = []
    for d in outflow_docs:
        created = d.get("created_at")
        if isinstance(created, datetime):
            key = _date_key(created)
            profit_trend_map[key] = profit_trend_map.get(key, 0.0) + _as_float(d.get("total_profit"))

        agent_val = d.get("agent_id") or d.get("by_user") or (d.get("agent") or {}).get("id")
        if agent_val:
            outflow_agent_ids.append(str(agent_val))

    agent_map = _agent_map(list(set(outflow_agent_ids)))

    profit_total = 0.0
    profit_by_branch: Dict[str, float] = {}
    profit_by_agent: Dict[str, float] = {}

    for doc in outflow_docs:
        profit = _as_float(doc.get("total_profit"))
        if profit <= 0:
            continue
        profit_total += profit

        agent_val = doc.get("agent_id") or doc.get("by_user") or (doc.get("agent") or {}).get("id")
        agent_key = str(agent_val) if agent_val else "unknown"
        agent_doc = agent_map.get(agent_key) or {}
        branch_name = doc.get("agent_branch") or agent_doc.get("branch") or "Unknown"
        agent_name = doc.get("agent_name") or agent_doc.get("name") or "Unknown"

        profit_by_branch[branch_name] = profit_by_branch.get(branch_name, 0.0) + profit
        profit_by_agent[agent_name] = profit_by_agent.get(agent_name, 0.0) + profit

    profit_by_branch_rows = [
        {"label": k, "value": round(v, 2)}
        for k, v in sorted(profit_by_branch.items(), key=lambda kv: kv[1], reverse=True)
    ]
    profit_by_agent_rows = [
        {"label": k, "value": round(v, 2)}
        for k, v in sorted(profit_by_agent.items(), key=lambda kv: kv[1], reverse=True)
    ]

    # ---- Liability (customers + payments) ----
    customer_query: Dict[str, Any] = {}
    if manager_variants:
        customer_query["manager_id"] = {"$in": manager_variants}
    if agent_id:
        customer_query["agent_id"] = agent_id
    elif branch and branch_agent_ids:
        customer_query["agent_id"] = {"$in": branch_agent_ids}
    elif branch:
        customer_query["_id"] = {"$in": []}

    customers = list(
        customers_col.find(
            customer_query,
            {"name": 1, "phone_number": 1, "agent_id": 1, "purchases": 1, "status": 1},
        )
    )

    customer_ids = [c["_id"] for c in customers if c.get("_id")]
    payments_map: Dict[Tuple[str, str], float] = {}
    if customer_ids:
        pipeline = [
            {
                "$match": {
                    "customer_id": {"$in": customer_ids},
                    "$or": [
                        {"payment_type": "PRODUCT"},
                        {"payment_type": "WITHDRAWAL", "product_index": {"$ne": None}},
                    ],
                }
            },
            {
                "$group": {
                    "_id": {
                        "customer_id": "$customer_id",
                        "product_index": "$product_index",
                    },
                    "sum_product": {
                        "$sum": {
                            "$cond": [
                                {"$eq": ["$payment_type", "PRODUCT"]},
                                {"$toDouble": {"$ifNull": ["$amount", 0]}},
                                0,
                            ]
                        }
                    },
                    "sum_withdrawal": {
                        "$sum": {
                            "$cond": [
                                {"$eq": ["$payment_type", "WITHDRAWAL"]},
                                {"$toDouble": {"$ifNull": ["$amount", 0]}},
                                0,
                            ]
                        }
                    },
                }
            },
        ]
        for row in payments_col.aggregate(pipeline):
            cid = str(row["_id"]["customer_id"])
            idx = str(row["_id"].get("product_index"))
            paid = _as_float(row.get("sum_product")) - _as_float(row.get("sum_withdrawal"))
            payments_map[(cid, idx)] = paid

    last_payment_map: Dict[str, str] = {}
    if customer_ids:
        last_pipeline = [
            {
                "$match": {
                    "customer_id": {"$in": customer_ids},
                    "payment_type": "PRODUCT",
                }
            },
            {"$group": {"_id": "$customer_id", "last_date": {"$max": "$date"}}},
        ]
        for row in payments_col.aggregate(last_pipeline):
            last_payment_map[str(row["_id"])] = row.get("last_date") or ""

    liability_trend_map: Dict[str, float] = {}
    if customer_ids:
        payment_match: Dict[str, Any] = {
            "customer_id": {"$in": customer_ids},
            "$or": [
                {"payment_type": "PRODUCT"},
                {"payment_type": "WITHDRAWAL", "product_index": {"$ne": None}},
            ],
        }
        if manager_variants:
            payment_match["manager_id"] = {"$in": manager_variants}
        if agent_id:
            payment_match["agent_id"] = agent_id
        elif branch and branch_agent_ids:
            payment_match["agent_id"] = {"$in": branch_agent_ids}

        trend_pipeline = [
            {"$match": payment_match},
            {
                "$group": {
                    "_id": "$date",
                    "sum_product": {
                        "$sum": {
                            "$cond": [
                                {"$eq": ["$payment_type", "PRODUCT"]},
                                {"$toDouble": {"$ifNull": ["$amount", 0]}},
                                0,
                            ]
                        }
                    },
                    "sum_withdrawal": {
                        "$sum": {
                            "$cond": [
                                {"$eq": ["$payment_type", "WITHDRAWAL"]},
                                {"$toDouble": {"$ifNull": ["$amount", 0]}},
                                0,
                            ]
                        }
                    },
                }
            },
        ]
        for row in payments_col.aggregate(trend_pipeline):
            day = row.get("_id")
            if not day:
                continue
            net = _as_float(row.get("sum_product")) - _as_float(row.get("sum_withdrawal"))
            liability_trend_map[str(day)] = liability_trend_map.get(str(day), 0.0) + net

    agent_ids_for_customers = list({str(c.get("agent_id")) for c in customers if c.get("agent_id")})
    agent_map_for_customers = _agent_map(agent_ids_for_customers)

    liability_total = 0.0
    liability_count = 0
    liability_by_branch: Dict[str, float] = {}
    liability_by_agent: Dict[str, float] = {}
    top_owing: List[Dict[str, Any]] = []
    dormant_rows: List[Dict[str, Any]] = []

    cutoff = (datetime.utcnow() - timedelta(days=14)).strftime("%Y-%m-%d")

    for cust in customers:
        cust_id = str(cust["_id"])
        if not _status_allows_liability(cust.get("status")):
            continue

        total_outstanding = 0.0
        purchases = cust.get("purchases") or []
        for index, purchase in enumerate(purchases):
            product = purchase.get("product") or {}
            status = product.get("status") or purchase.get("status")
            if not _product_status_allows_liability(status):
                continue

            product_total = _as_float(product.get("total"))
            paid_key = (cust_id, str(index))
            paid = payments_map.get(paid_key, 0.0)
            amount_left = max(0.0, product_total - paid)
            if amount_left <= 0:
                continue
            total_outstanding += amount_left

        if total_outstanding <= 0:
            continue

        liability_total += total_outstanding
        liability_count += 1

        agent_key = str(cust.get("agent_id") or "")
        agent_doc = agent_map_for_customers.get(agent_key) or {}
        branch_name = agent_doc.get("branch") or "Unknown"
        agent_name = agent_doc.get("name") or "Unknown"

        liability_by_branch[branch_name] = liability_by_branch.get(branch_name, 0.0) + total_outstanding
        liability_by_agent[agent_name] = liability_by_agent.get(agent_name, 0.0) + total_outstanding

        top_owing.append({
            "name": cust.get("name") or "Customer",
            "phone": cust.get("phone_number") or "",
            "branch": branch_name,
            "agent": agent_name,
            "amount": round(total_outstanding, 2),
        })

        last_date = last_payment_map.get(cust_id) or ""
        if not last_date or last_date < cutoff:
            dormant_rows.append({
                "name": cust.get("name") or "Customer",
                "phone": cust.get("phone_number") or "",
                "branch": branch_name,
                "agent": agent_name,
                "last_payment": last_date or "N/A",
                "amount": round(total_outstanding, 2),
            })

    top_owing_rows = sorted(top_owing, key=lambda r: r["amount"], reverse=True)[:10]
    dormant_rows = sorted(dormant_rows, key=lambda r: r["last_payment"])[:10]

    liability_by_branch_rows = [
        {"label": k, "value": round(v, 2)}
        for k, v in sorted(liability_by_branch.items(), key=lambda kv: kv[1], reverse=True)
    ]
    liability_by_agent_rows = [
        {"label": k, "value": round(v, 2)}
        for k, v in sorted(liability_by_agent.items(), key=lambda kv: kv[1], reverse=True)
    ]

    # ---- Top paying customers in range ----
    top_paying_rows: List[Dict[str, Any]] = []
    if customer_ids:
        payment_match: Dict[str, Any] = {
            "customer_id": {"$in": customer_ids},
            "payment_type": "PRODUCT",
            "date": {"$gte": start_date_str, "$lte": end_date_str},
        }
        if manager_variants:
            payment_match["manager_id"] = {"$in": manager_variants}
        if agent_id:
            payment_match["agent_id"] = agent_id
        elif branch and branch_agent_ids:
            payment_match["agent_id"] = {"$in": branch_agent_ids}

        pay_pipeline = [
            {"$match": payment_match},
            {
                "$group": {
                    "_id": "$customer_id",
                    "total": {"$sum": {"$toDouble": {"$ifNull": ["$amount", 0]}}},
                }
            },
            {"$sort": {"total": -1}},
            {"$limit": 10},
        ]
        customer_by_id = {str(c["_id"]): c for c in customers}
        for row in payments_col.aggregate(pay_pipeline):
            cid = str(row["_id"])
            cust = customer_by_id.get(cid) or {}
            agent_key = str(cust.get("agent_id") or "")
            agent_doc = agent_map_for_customers.get(agent_key) or {}
            top_paying_rows.append({
                "name": cust.get("name") or "Customer",
                "phone": cust.get("phone_number") or "",
                "branch": agent_doc.get("branch") or "Unknown",
                "agent": agent_doc.get("name") or "Unknown",
                "amount": round(_as_float(row.get("total")), 2),
            })

    # ---- Stock cost value ----
    stock_query: Dict[str, Any] = {}
    if manager_variants:
        stock_query["manager_id"] = {"$in": manager_variants}
    if branch:
        manager_ids = _manager_ids_for_branch(branch)
        if manager_ids:
            if "manager_id" in stock_query:
                filtered = [mid for mid in manager_ids if mid in stock_query["manager_id"]["$in"]]
                stock_query["manager_id"] = {"$in": filtered} if filtered else {"$in": []}
            else:
                stock_query["manager_id"] = {"$in": manager_ids}
        else:
            stock_query["_id"] = {"$in": []}

    stock_docs = list(
        inventory_col.find(stock_query, {"qty": 1, "cost_price": 1, "initial_price": 1, "manager_id": 1})
    )
    stock_total = 0.0
    for doc in stock_docs:
        qty = _as_float(doc.get("qty"))
        unit_cost = _as_float(doc.get("cost_price"))
        if unit_cost <= 0:
            unit_cost = _as_float(doc.get("initial_price"))
        stock_total += qty * unit_cost

    trend_labels = sorted(set(list(profit_trend_map.keys()) + list(liability_trend_map.keys())))
    profit_trend = [round(profit_trend_map.get(day, 0.0), 2) for day in trend_labels]
    liability_trend = [round(liability_trend_map.get(day, 0.0), 2) for day in trend_labels]

    selected_agent_doc = None
    if agent_id:
        selected_agent_doc = users_col.find_one({"_id": _oid(agent_id)}, {"name": 1, "branch": 1})

    return jsonify(
        ok=True,
        range={
            "start": start_date_str,
            "end": end_date_str,
            "label": range_label,
        },
        meta={
            "branch_selected": bool(branch),
            "agent_selected": bool(agent_id),
        },
        filters={
            "agents": _list_agents(manager_variants, branch or None),
        },
        agent_focus={
            "id": agent_id or "",
            "name": (selected_agent_doc or {}).get("name") or "",
            "branch": (selected_agent_doc or {}).get("branch") or "",
        },
        summary={
            "profit_total": round(profit_total, 2),
            "liability_total": round(liability_total, 2),
            "liability_count": liability_count,
            "stock_cost_total": round(stock_total, 2),
        },
        tables={
            "profit_by_branch": profit_by_branch_rows,
            "profit_by_agent": profit_by_agent_rows,
            "liability_by_branch": liability_by_branch_rows,
            "liability_by_agent": liability_by_agent_rows,
            "top_owing": top_owing_rows,
            "top_paying": top_paying_rows,
            "dormant_customers": dormant_rows,
        },
        charts={
            "profit_by_branch": profit_by_branch_rows,
            "liability_by_branch": liability_by_branch_rows,
            "profit_by_agent": profit_by_agent_rows,
            "liability_by_agent": liability_by_agent_rows,
            "trend_labels": trend_labels,
            "profit_trend": profit_trend,
            "liability_trend": liability_trend,
        }
    )


try:
    inventory_outflow_col.create_index([("created_at", 1)])
    inventory_outflow_col.create_index([("manager_id", 1)])
    inventory_outflow_col.create_index([("agent_branch", 1)])
    inventory_outflow_col.create_index([("agent_id", 1)])
    payments_col.create_index([("customer_id", 1), ("date", 1)])
    payments_col.create_index([("agent_id", 1), ("date", 1)])
    customers_col.create_index([("agent_id", 1), ("manager_id", 1)])
except Exception:
    pass

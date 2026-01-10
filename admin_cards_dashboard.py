from flask import Blueprint, render_template, request, jsonify, redirect, url_for
from bson import ObjectId
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import re

from db import db
from card_sales_helper import sold_counts_by_name
from login import get_current_identity

admin_cards_dashboard_bp = Blueprint("admin_cards_dashboard", __name__, url_prefix="/admin/cards")

products_col = db["products"]
users_col = db["users"]
card_movements_col = db["card_movements"]
card_print_batches_col = db["card_print_batches"]
customers_col = db["customers"]
instant_sales_col = db["instant_sales"]


def _require_admin():
    ident = get_current_identity()
    if not ident.get("is_authenticated"):
        return redirect(url_for("login.login", next=request.path))
    if ident.get("role") != "admin":
        return "Forbidden", 403
    return None


def normalize_name(name: str) -> str:
    if not name:
        return ""
    s = str(name).strip().lower()
    s = re.sub(r"\s+", " ", s)
    return s


def _dedupe_products(products):
    unique_products = {}
    for p in products:
        key = normalize_name(p.get("name", ""))
        if not key:
            continue
        if key not in unique_products:
            unique_products[key] = p
            continue
        existing = unique_products[key]
        if not existing.get("image_url") and p.get("image_url"):
            unique_products[key] = p
        else:
            cur_dt = existing.get("created_at")
            new_dt = p.get("created_at")
            if new_dt and cur_dt and new_dt > cur_dt:
                unique_products[key] = p
    return unique_products


def _unique_product_list(products):
    unique_products = _dedupe_products(products)
    rows = []
    for key, p in unique_products.items():
        rows.append(
            {
                "product_key": key,
                "name": p.get("name", ""),
                "image_url": p.get("image_url", ""),
                "price": p.get("price", 0),
                "cash_price": p.get("cash_price", 0),
                "description": p.get("description", ""),
            }
        )
    rows.sort(key=lambda x: (x.get("name") or "").lower())
    return rows


def _unique_product_names(products):
    unique = {}
    for p in products:
        key = norm_name(p.get("name", ""))
        if not key:
            continue
        if key not in unique:
            unique[key] = p.get("name", "")
            continue
        if not unique[key] and p.get("name"):
            unique[key] = p.get("name", "")
    rows = [name for name in unique.values() if name]
    rows.sort(key=lambda x: x.lower())
    return rows


def norm_name(val: str) -> str:
    return " ".join((val or "").strip().lower().split())


def _name_regex(normed: str):
    if not normed:
        return None
    parts = normed.split()
    if not parts:
        return None
    pattern = r"^" + r"\s+".join(re.escape(p) for p in parts) + r"$"
    return {"$regex": pattern, "$options": "i"}


def _id_variants(val):
    if not val:
        return []
    if isinstance(val, ObjectId):
        return [val, str(val)]
    sval = str(val).strip()
    if ObjectId.is_valid(sval):
        return [ObjectId(sval), sval]
    return [sval]


def _month_bounds(month_str: str):
    try:
        start = datetime.strptime(month_str, "%Y-%m")
    except Exception:
        now = datetime.utcnow()
        start = datetime(now.year, now.month, 1)
    end = start + relativedelta(months=1)
    return start, end


def _safe_int(val, default=0):
    try:
        return int(val)
    except Exception:
        return default


def _movement_match(start, end, product_key=None, manager_id=None):
    match = {"created_at": {"$gte": start, "$lt": end}}
    if product_key:
        match["product_key"] = product_key
    if manager_id:
        variants = _id_variants(manager_id)
        if variants:
            match["$or"] = [
                {"from_id": {"$in": variants}},
                {"to_id": {"$in": variants}},
            ]
    return match


def _sum_qty(match_query):
    rows = list(
        card_movements_col.aggregate(
            [
                {"$match": match_query},
                {"$group": {"_id": None, "total": {"$sum": "$qty"}}},
            ]
        )
    )
    if not rows:
        return 0
    return _safe_int(rows[0].get("total", 0), 0)


def _days_in_month(dt: datetime):
    return (dt + relativedelta(months=1) - relativedelta(days=1)).day


def _trend_range(end: datetime, months: int):
    start = end - relativedelta(months=months)
    return start, end


def _month_labels(start: datetime, months: int):
    labels = []
    current = start
    for _ in range(months):
        labels.append(current.strftime("%Y-%m"))
        current = current + relativedelta(months=1)
    return labels


def _ensure_history_indexes():
    try:
        card_movements_col.create_index([("created_at", 1), ("from_id", 1), ("to_id", 1), ("product_name", 1)])
        customers_col.create_index([("agent_id", 1)])
        customers_col.create_index([("purchases.agent_id", 1)])
        customers_col.create_index([("manager_id", 1)])
        customers_col.create_index([("purchases.purchase_date", 1)])
        customers_col.create_index([("purchases.product.name", 1)])
        instant_sales_col.create_index([("agent_id", 1), ("purchase_date", 1)])
        instant_sales_col.create_index([("manager_id", 1)])
        instant_sales_col.create_index([("product.name", 1)])
    except Exception:
        pass


_ensure_history_indexes()


def _history_bounds(from_str: str, to_str: str):
    today = datetime.utcnow().date()
    start_default = today.replace(day=1)
    end_default = (start_default + relativedelta(months=1)) - timedelta(days=1)
    try:
        start_date = datetime.strptime(from_str, "%Y-%m-%d").date()
    except Exception:
        start_date = start_default
    try:
        end_date = datetime.strptime(to_str, "%Y-%m-%d").date()
    except Exception:
        end_date = end_default
    if end_date < start_date:
        start_date, end_date = end_date, start_date
    start_dt = datetime.combine(start_date, datetime.min.time())
    end_dt = datetime.combine(end_date + timedelta(days=1), datetime.min.time())
    return start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d"), start_dt, end_dt


def _id_str(val):
    if val is None:
        return ""
    return str(val)


def _agent_ids_for_manager(manager_id):
    if not manager_id:
        return []
    variants = _id_variants(manager_id)
    if not variants:
        return []
    agents = list(
        users_col.find(
            {"role": "agent", "manager_id": {"$in": variants}},
            {"_id": 1},
        )
    )
    return [str(a.get("_id")) for a in agents if a.get("_id")]


@admin_cards_dashboard_bp.route("/dashboard")
def admin_cards_dashboard():
    auth = _require_admin()
    if auth:
        return auth

    now = datetime.utcnow()
    default_month = now.strftime("%Y-%m")
    products = list(
        products_col.find(
            {},
            {"name": 1, "image_url": 1, "price": 1, "cash_price": 1, "description": 1, "created_at": 1},
        )
    )
    product_rows = _unique_product_list(products)
    managers = list(users_col.find({"role": "manager"}, {"name": 1, "branch": 1}).sort("name", 1))

    return render_template(
        "admin_cards_dashboard.html",
        default_month=default_month,
        products=product_rows,
        managers=managers,
    )


@admin_cards_dashboard_bp.route("/dashboard/data")
def admin_cards_dashboard_data():
    auth = _require_admin()
    if auth:
        return auth

    month = (request.args.get("month") or "").strip()
    manager_id = (request.args.get("manager_id") or "").strip() or None
    product_key = normalize_name(request.args.get("product_key")) or None
    trend_months = _safe_int(request.args.get("trend_months"), 12)
    if trend_months not in (12, 24):
        trend_months = 12

    start, end = _month_bounds(month)

    base_match = _movement_match(start, end, product_key, manager_id)

    printed_month = _sum_qty({**base_match, "from_type": "print"})
    distributed_month = _sum_qty({**base_match, "from_type": "manager", "to_type": "agent"})
    to_managers_month = _sum_qty({**base_match, "from_type": "admin", "to_type": "manager"})

    all_match = {"created_at": {"$lt": end}}
    if product_key:
        all_match["product_key"] = product_key
    if manager_id:
        variants = _id_variants(manager_id)
        if variants:
            all_match["$or"] = [
                {"from_id": {"$in": variants}},
                {"to_id": {"$in": variants}},
            ]

    printed_all = _sum_qty({**all_match, "from_type": "print"})
    distributed_all = _sum_qty({**all_match, "from_type": "manager", "to_type": "agent"})
    to_managers_all = _sum_qty({**all_match, "from_type": "admin", "to_type": "manager"})

    days_count = _days_in_month(start)
    day_labels = [str(i) for i in range(1, days_count + 1)]
    day_printed = [0] * days_count
    day_distributed = [0] * days_count

    daily_rows = list(
        card_movements_col.aggregate(
            [
                {"$match": base_match},
                {
                    "$group": {
                        "_id": {"day": {"$dayOfMonth": "$created_at"}},
                        "printed": {
                            "$sum": {
                                "$cond": [
                                    {"$eq": ["$from_type", "print"]},
                                    "$qty",
                                    0,
                                ]
                            }
                        },
                        "distributed": {
                            "$sum": {
                                "$cond": [
                                    {"$and": [
                                        {"$eq": ["$from_type", "manager"]},
                                        {"$eq": ["$to_type", "agent"]},
                                    ]},
                                    "$qty",
                                    0,
                                ]
                            }
                        },
                    }
                },
            ]
        )
    )

    for row in daily_rows:
        day_idx = _safe_int(row.get("_id", {}).get("day"), 0) - 1
        if 0 <= day_idx < days_count:
            day_printed[day_idx] = _safe_int(row.get("printed", 0), 0)
            day_distributed[day_idx] = _safe_int(row.get("distributed", 0), 0)

    day_left = [p - d for p, d in zip(day_printed, day_distributed)]

    trend_start, trend_end = _trend_range(end, trend_months)
    trend_match = {"created_at": {"$gte": trend_start, "$lt": trend_end}}
    if product_key:
        trend_match["product_key"] = product_key
    if manager_id:
        variants = _id_variants(manager_id)
        if variants:
            trend_match["$or"] = [
                {"from_id": {"$in": variants}},
                {"to_id": {"$in": variants}},
            ]

    trend_rows = list(
        card_movements_col.aggregate(
            [
                {"$match": trend_match},
                {
                    "$group": {
                        "_id": {
                            "year": {"$year": "$created_at"},
                            "month": {"$month": "$created_at"},
                        },
                        "printed": {
                            "$sum": {
                                "$cond": [
                                    {"$eq": ["$from_type", "print"]},
                                    "$qty",
                                    0,
                                ]
                            }
                        },
                        "distributed": {
                            "$sum": {
                                "$cond": [
                                    {"$and": [
                                        {"$eq": ["$from_type", "manager"]},
                                        {"$eq": ["$to_type", "agent"]},
                                    ]},
                                    "$qty",
                                    0,
                                ]
                            }
                        },
                    }
                },
            ]
        )
    )

    trend_map = {}
    for row in trend_rows:
        y = row.get("_id", {}).get("year")
        m = row.get("_id", {}).get("month")
        if not y or not m:
            continue
        key = f"{y:04d}-{m:02d}"
        trend_map[key] = {
            "printed": _safe_int(row.get("printed", 0), 0),
            "distributed": _safe_int(row.get("distributed", 0), 0),
        }

    trend_labels = _month_labels(trend_start, trend_months)
    trend_printed = [trend_map.get(label, {}).get("printed", 0) for label in trend_labels]
    trend_distributed = [trend_map.get(label, {}).get("distributed", 0) for label in trend_labels]

    top_rows = list(
        card_movements_col.aggregate(
            [
                {"$match": base_match},
                {
                    "$group": {
                        "_id": "$product_key",
                        "product_name": {"$first": "$product_name"},
                        "product_image_url": {"$first": "$product_image_url"},
                        "printed": {
                            "$sum": {
                                "$cond": [
                                    {"$eq": ["$from_type", "print"]},
                                    "$qty",
                                    0,
                                ]
                            }
                        },
                        "distributed": {
                            "$sum": {
                                "$cond": [
                                    {"$and": [
                                        {"$eq": ["$from_type", "manager"]},
                                        {"$eq": ["$to_type", "agent"]},
                                    ]},
                                    "$qty",
                                    0,
                                ]
                            }
                        },
                    }
                },
                {"$sort": {"distributed": -1, "printed": -1}},
                {"$limit": 10},
            ]
        )
    )

    top_products = []
    for row in top_rows:
        left = _safe_int(row.get("printed", 0), 0) - _safe_int(row.get("distributed", 0), 0)
        top_products.append(
            {
                "product_key": row.get("_id"),
                "name": row.get("product_name", ""),
                "image_url": row.get("product_image_url", ""),
                "printed": _safe_int(row.get("printed", 0), 0),
                "distributed": _safe_int(row.get("distributed", 0), 0),
                "left": left,
            }
        )

    recent_match = _movement_match(start, end, product_key, manager_id)
    recent_rows = list(card_movements_col.find(recent_match).sort("created_at", -1).limit(30))
    recent = []
    for row in recent_rows:
        recent.append(
            {
                "at": row.get("created_at").strftime("%Y-%m-%d %H:%M") if row.get("created_at") else "",
                "action": f"{row.get('from_type','')}?{row.get('to_type','')}",
                "qty": _safe_int(row.get("qty", 0), 0),
                "product": row.get("product_name", ""),
                "to": row.get("to_name") or row.get("to_id") or "",
                "from": row.get("from_name") or row.get("from_id") or "",
            }
        )

    return jsonify(
        {
            "month": start.strftime("%Y-%m"),
            "kpis": {
                "month_printed": printed_month,
                "month_distributed_agents": distributed_month,
                "month_left": printed_month - distributed_month,
                "month_to_managers": to_managers_month,
                "all_printed": printed_all,
                "all_distributed_agents": distributed_all,
                "all_left": printed_all - distributed_all,
                "all_to_managers": to_managers_all,
            },
            "monthly_daily": {
                "labels": day_labels,
                "printed": day_printed,
                "distributed_agents": day_distributed,
                "left": day_left,
            },
            "trend_months": {
                "labels": trend_labels,
                "printed": trend_printed,
                "distributed_agents": trend_distributed,
            },
            "top_products": top_products,
            "recent": recent,
        }
    )


@admin_cards_dashboard_bp.route("/history")
def admin_cards_history():
    auth = _require_admin()
    if auth:
        return auth

    default_from, default_to, _, _ = _history_bounds("", "")
    managers = list(users_col.find({"role": "manager"}, {"name": 1, "branch": 1}).sort("name", 1))
    agents = list(users_col.find({"role": "agent"}, {"name": 1, "manager_id": 1}).sort("name", 1))
    product_names = _unique_product_names(list(products_col.find({}, {"name": 1})))

    return render_template(
        "admin_card_history.html",
        default_from=default_from,
        default_to=default_to,
        managers=managers,
        agents=agents,
        products=product_names,
    )


@admin_cards_dashboard_bp.route("/history/data")
def admin_cards_history_data():
    auth = _require_admin()
    if auth:
        return auth

    from_str = (request.args.get("from") or "").strip()
    to_str = (request.args.get("to") or "").strip()
    manager_id = (request.args.get("manager_id") or "").strip() or None
    agent_id = (request.args.get("agent_id") or "").strip() or None
    product_name = (request.args.get("product_name") or "").strip()
    level = (request.args.get("level") or "all").strip().lower()

    from_str, to_str, start_dt, end_dt = _history_bounds(from_str, to_str)
    product_norm = norm_name(product_name)
    product_regex = _name_regex(product_norm)

    manager_variants = _id_variants(manager_id) if manager_id else []
    agent_variants = _id_variants(agent_id) if agent_id else []

    if agent_id:
        level = "agent"
    if level not in ("agent", "manager", "all"):
        level = "all"

    agent_ids = []
    if agent_variants:
        agent_ids = [str(v) for v in agent_variants]
    elif manager_id:
        agent_ids = _agent_ids_for_manager(manager_id)

    users = list(
        users_col.find(
            {"role": {"$in": ["manager", "agent"]}},
            {"name": 1, "branch": 1, "manager_id": 1, "role": 1},
        )
    )
    users_by_id = {}
    agent_manager_map = {}
    for u in users:
        uid = u.get("_id")
        if not uid:
            continue
        users_by_id[uid] = u
        users_by_id[str(uid)] = u
        if (u.get("role") or "").lower() == "agent":
            mgr = u.get("manager_id")
            if mgr:
                agent_manager_map[str(uid)] = str(mgr)

    def _max_dt(a, b):
        if not a:
            return b
        if not b:
            return a
        return max(a, b)

    def _build_map(rows, id_key):
        out = {}
        for row in rows:
            raw_name = row.get("_id", {}).get("product_name") or ""
            pnorm = norm_name(raw_name) or product_norm
            if not pnorm:
                continue
            holder_id = row.get("_id", {}).get(id_key)
            key = (_id_str(holder_id), pnorm)
            entry = out.setdefault(key, {"qty": 0, "last_at": None, "name": raw_name})
            entry["qty"] += _safe_int(row.get("total", 0), 0)
            entry["last_at"] = _max_dt(entry["last_at"], row.get("last_at"))
            if not entry["name"] and raw_name:
                entry["name"] = raw_name
        return out

    base_range = {"created_at": {"$gte": start_dt, "$lt": end_dt}}
    base_all = {"created_at": {"$lt": end_dt}}
    if product_regex:
        base_range["product_name"] = product_regex
        base_all["product_name"] = product_regex

    printed_range_match = {**base_range, "from_type": "admin", "to_type": "manager"}
    printed_all_match = {**base_all, "from_type": "admin", "to_type": "manager"}
    if manager_variants:
        printed_range_match["to_id"] = {"$in": manager_variants}
        printed_all_match["to_id"] = {"$in": manager_variants}

    distributed_range_match = {**base_range, "from_type": "manager", "to_type": "agent"}
    distributed_all_match = {**base_all, "from_type": "manager", "to_type": "agent"}
    if manager_variants:
        distributed_range_match["from_id"] = {"$in": manager_variants}
        distributed_all_match["from_id"] = {"$in": manager_variants}
    if agent_variants:
        distributed_range_match["to_id"] = {"$in": agent_variants}
        distributed_all_match["to_id"] = {"$in": agent_variants}

    printed_range_rows = list(
        card_movements_col.aggregate(
            [
                {"$match": printed_range_match},
                {
                    "$group": {
                        "_id": {"manager_id": "$to_id", "product_name": "$product_name"},
                        "total": {"$sum": "$qty"},
                        "last_at": {"$max": "$created_at"},
                    }
                },
            ]
        )
    )
    printed_all_rows = list(
        card_movements_col.aggregate(
            [
                {"$match": printed_all_match},
                {
                    "$group": {
                        "_id": {"manager_id": "$to_id", "product_name": "$product_name"},
                        "total": {"$sum": "$qty"},
                        "last_at": {"$max": "$created_at"},
                    }
                },
            ]
        )
    )
    distributed_range_rows = list(
        card_movements_col.aggregate(
            [
                {"$match": distributed_range_match},
                {
                    "$group": {
                        "_id": {"manager_id": "$from_id", "product_name": "$product_name"},
                        "total": {"$sum": "$qty"},
                        "last_at": {"$max": "$created_at"},
                    }
                },
            ]
        )
    )
    distributed_all_rows = list(
        card_movements_col.aggregate(
            [
                {"$match": distributed_all_match},
                {
                    "$group": {
                        "_id": {"manager_id": "$from_id", "product_name": "$product_name"},
                        "total": {"$sum": "$qty"},
                        "last_at": {"$max": "$created_at"},
                    }
                },
            ]
        )
    )
    given_range_rows = list(
        card_movements_col.aggregate(
            [
                {"$match": distributed_range_match},
                {
                    "$group": {
                        "_id": {"agent_id": "$to_id", "product_name": "$product_name"},
                        "total": {"$sum": "$qty"},
                        "last_at": {"$max": "$created_at"},
                    }
                },
            ]
        )
    )
    given_all_rows = list(
        card_movements_col.aggregate(
            [
                {"$match": distributed_all_match},
                {
                    "$group": {
                        "_id": {"agent_id": "$to_id", "product_name": "$product_name"},
                        "total": {"$sum": "$qty"},
                        "last_at": {"$max": "$created_at"},
                    }
                },
            ]
        )
    )

    printed_range = _build_map(printed_range_rows, "manager_id")
    printed_all = _build_map(printed_all_rows, "manager_id")
    distributed_range = _build_map(distributed_range_rows, "manager_id")
    distributed_all = _build_map(distributed_all_rows, "manager_id")
    given_range = _build_map(given_range_rows, "agent_id")
    given_all = _build_map(given_all_rows, "agent_id")

    def _sales_map(sales_payload):
        out = {}
        for key, entry in sales_payload.get("total", {}).items():
            agent_key, pnorm = key
            if not pnorm:
                continue
            out[(agent_key, pnorm)] = {
                "qty": _safe_int(entry.get("count", 0), 0),
                "last_at": entry.get("last_at"),
                "name": entry.get("name", ""),
            }
        return out

    sold_range_payload = sold_counts_by_name(
        customers_col,
        instant_sales_col,
        agent_id=agent_id,
        manager_id=manager_id,
        product_name=product_name,
        start_dt=start_dt,
        end_dt=end_dt,
        group_by_agent=True,
    )
    sold_all_payload = sold_counts_by_name(
        customers_col,
        instant_sales_col,
        agent_id=agent_id,
        manager_id=manager_id,
        product_name=product_name,
        start_dt=None,
        end_dt=end_dt,
        group_by_agent=True,
    )

    sold_range = _sales_map(sold_range_payload)
    sold_all = _sales_map(sold_all_payload)

    manager_sold_range = {}
    for (agent_key, pnorm), entry in sold_range.items():
        mgr_id = agent_manager_map.get(agent_key)
        if not mgr_id:
            continue
        key = (mgr_id, pnorm)
        current = manager_sold_range.setdefault(key, {"qty": 0, "last_at": None})
        current["qty"] += entry.get("qty", 0)
        current["last_at"] = _max_dt(current.get("last_at"), entry.get("last_at"))

    manager_left_map = {}
    manager_keys = set(printed_all) | set(distributed_all)
    for key in manager_keys:
        printed_qty = printed_all.get(key, {}).get("qty", 0)
        distributed_qty = distributed_all.get(key, {}).get("qty", 0)
        manager_left_map[key] = printed_qty - distributed_qty

    rows = []

    def _format_dt(val):
        if not val:
            return ""
        return val.strftime("%Y-%m-%d")

    if level != "agent":
        keys = set(printed_range) | set(distributed_range) | set(printed_all) | set(distributed_all)
        for key in keys:
            manager_key, pnorm = key
            manager_doc = users_by_id.get(manager_key)
            name = printed_range.get(key, {}).get("name") or distributed_range.get(key, {}).get("name") or product_name
            row = {
                "level": "manager",
                "branch": manager_doc.get("branch") if manager_doc else "",
                "manager_name": manager_doc.get("name") if manager_doc else "",
                "agent_name": "",
                "product_name": name or "",
                "printed_to_manager": printed_range.get(key, {}).get("qty", 0),
                "distributed_to_agents": distributed_range.get(key, {}).get("qty", 0),
                "given_to_agent": 0,
                "sold_total": manager_sold_range.get(key, {}).get("qty", 0),
                "left_manager": manager_left_map.get(key, 0),
                "left_agent": 0,
                "last_given_at": _format_dt(printed_range.get(key, {}).get("last_at")),
                "last_sold_at": _format_dt(manager_sold_range.get(key, {}).get("last_at")),
                "misuse": False,
            }
            rows.append(row)

    if level == "agent":
        keys = set(given_range) | set(given_all) | set(sold_range) | set(sold_all)
        for key in keys:
            agent_key, pnorm = key
            agent_doc = users_by_id.get(agent_key)
            mgr_id = agent_manager_map.get(agent_key)
            manager_doc = users_by_id.get(mgr_id) if mgr_id else None
            name = (
                given_range.get(key, {}).get("name")
                or sold_range.get(key, {}).get("name")
                or product_name
            )
            given_all_qty = given_all.get(key, {}).get("qty", 0)
            sold_all_qty = sold_all.get(key, {}).get("qty", 0)
            row = {
                "level": "agent",
                "branch": manager_doc.get("branch") if manager_doc else "",
                "manager_name": manager_doc.get("name") if manager_doc else "",
                "agent_name": agent_doc.get("name") if agent_doc else "",
                "product_name": name or "",
                "printed_to_manager": 0,
                "distributed_to_agents": 0,
                "given_to_agent": given_range.get(key, {}).get("qty", 0),
                "sold_total": sold_range.get(key, {}).get("qty", 0),
                "left_manager": manager_left_map.get((mgr_id, pnorm), 0),
                "left_agent": given_all_qty - sold_all_qty,
                "last_given_at": _format_dt(given_range.get(key, {}).get("last_at")),
                "last_sold_at": _format_dt(sold_range.get(key, {}).get("last_at")),
                "misuse": sold_all_qty > given_all_qty,
            }
            rows.append(row)

    rows.sort(
        key=lambda x: (
            (x.get("branch") or "").lower(),
            (x.get("manager_name") or "").lower(),
            (x.get("agent_name") or "").lower(),
            (x.get("product_name") or "").lower(),
        )
    )

    summary_printed = sum(item.get("printed_to_manager", 0) for item in rows)
    if level == "agent":
        summary_distributed = sum(item.get("given_to_agent", 0) for item in rows)
        summary_left = sum(item.get("left_agent", 0) for item in rows)
    else:
        summary_distributed = sum(item.get("distributed_to_agents", 0) for item in rows)
        summary_left = sum(item.get("left_manager", 0) for item in rows)
    summary_sold = sum(item.get("sold_total", 0) for item in rows)

    return jsonify(
        {
            "filters": {
                "from": from_str,
                "to": to_str,
                "manager_id": manager_id or "",
                "agent_id": agent_id or "",
                "product_name": product_name,
                "level": level,
            },
            "summary": {
                "printed": summary_printed,
                "distributed": summary_distributed,
                "sold": summary_sold,
                "left": summary_left,
            },
            "rows": rows,
        }
    )

from flask import Blueprint, render_template, jsonify, request
from bson import ObjectId
from db import db
from datetime import datetime, timedelta

executive_bp = Blueprint('executive_dashboard', __name__)

# Collections
customers_col = db["customers"]
payments_col = db["payments"]
leads_col = db["leads"]
users_col = db["users"]
manager_expenses_col = db["manager_expenses"]


def _safe_amount(val):
    try:
        return float(val)
    except Exception:
        return 0.0


def _id_variants(val):
    if val is None:
        return []
    if isinstance(val, ObjectId):
        return [val, str(val)]
    sval = str(val).strip()
    if ObjectId.is_valid(sval):
        return [ObjectId(sval), sval]
    return [sval]


def _ensure_exec_indexes():
    try:
        payments_col.create_index([("date", 1), ("payment_type", 1)])
        payments_col.create_index([("created_at", 1)])
        payments_col.create_index([("manager_id", 1), ("date", 1)])
        payments_col.create_index([("agent_id", 1), ("date", 1)])
        manager_expenses_col.create_index([("created_at", 1)])
        manager_expenses_col.create_index([("status", 1), ("created_at", 1)])
        manager_expenses_col.create_index([("category", 1), ("created_at", 1)])
    except Exception:
        pass


_ensure_exec_indexes()


def _today_range_utc(base_date):
    start = datetime(base_date.year, base_date.month, base_date.day)
    end = start + timedelta(days=1)
    return start, end


def _yesterday_range_utc(base_date):
    y = base_date - timedelta(days=1)
    return _today_range_utc(y)


def _week_range_utc(base_date):
    start = datetime(base_date.year, base_date.month, base_date.day) - timedelta(days=base_date.weekday())
    end = start + timedelta(days=7)
    return start, end


def _month_range_utc(base_date):
    start = datetime(base_date.year, base_date.month, 1)
    if base_date.month == 12:
        end = datetime(base_date.year + 1, 1, 1)
    else:
        end = datetime(base_date.year, base_date.month + 1, 1)
    return start, end


def _lookup_name_map(role, ids):
    variants = []
    for val in ids:
        variants.extend(_id_variants(val))
    if not variants:
        return {}
    docs = list(users_col.find({"role": role, "_id": {"$in": variants}}, {"name": 1}))
    name_map = {}
    for doc in docs:
        name = doc.get("name", "")
        if not name:
            continue
        key = str(doc.get("_id"))
        name_map[key] = name
    return name_map


def _lookup_manager_name(manager_id):
    for variant in _id_variants(manager_id):
        doc = users_col.find_one({"_id": variant}, {"name": 1})
        if doc and doc.get("name"):
            return doc.get("name")
    return ""


@executive_bp.route("/executive/dashboard")
def executive_dashboard():
    """
    Lightweight initial render:
    - Summary metrics only
    - Heavy chart data is loaded via AJAX
    """
    # Total customers (fast, approximate but OK for dashboard)
    total_customers = customers_col.estimated_document_count()

    # Total leads
    total_leads = leads_col.estimated_document_count()
    # TODO: confirm leads source if leads collection is not authoritative.

    # Total products sold
    sold_result = customers_col.aggregate([
        {"$match": {"purchases": {"$exists": True, "$ne": []}}},
        {"$group": {"_id": None, "total": {"$sum": {"$size": "$purchases"}}}}
    ])
    total_products_sold = next(sold_result, {}).get("total", 0)

    return render_template("executive_dashboard.html", data={
        "total_customers": total_customers,
        "total_products_sold": total_products_sold,
        "total_leads": total_leads
    })


@executive_bp.route("/executive/dashboard/charts")
def dashboard_charts():
    """
    Heavier aggregations for charts.
    Called asynchronously from the frontend so the main page loads faster.
    """
    # Top Products Sold
    top_products_cursor = customers_col.aggregate([
        {"$unwind": "$purchases"},
        {"$group": {"_id": "$purchases.product.name", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 5}
    ])
    top_products = {"labels": [], "values": []}
    for item in top_products_cursor:
        top_products["labels"].append(item["_id"] or "Unnamed")
        top_products["values"].append(item["count"])

    # Top Managers by Payments
    top_managers_cursor = payments_col.aggregate([
        {"$match": {"payment_type": {"$ne": "WITHDRAWAL"}}},
        {"$lookup": {
            "from": "users",
            "localField": "manager_id",
            "foreignField": "_id",
            "as": "manager"
        }},
        {"$unwind": "$manager"},
        {"$group": {
            "_id": {
                "manager_id": "$manager._id",
                "name": "$manager.name",
                "branch": "$manager.branch"
            },
            "total": {"$sum": "$amount"}
        }},
        {"$sort": {"total": -1}},
        {"$limit": 5}
    ])
    top_managers = {"labels": [], "values": []}
    for item in top_managers_cursor:
        label = f"{item['_id']['name']} ({item['_id']['branch']})"
        top_managers["labels"].append(label)
        top_managers["values"].append(round(item["total"], 2))

    return jsonify({
        "ok": True,
        "top_products": top_products,
        "top_managers": top_managers
    })


@executive_bp.route("/executive/dashboard/insights")
def dashboard_insights():
    range_key = (request.args.get("range") or "today").strip().lower()
    if range_key not in ("today", "week", "month"):
        range_key = "today"

    now = datetime.utcnow()
    today = now.date()
    today_str = today.strftime("%Y-%m-%d")
    yesterday = today - timedelta(days=1)
    yesterday_str = yesterday.strftime("%Y-%m-%d")

    start_today, end_today = _today_range_utc(today)
    start_yesterday, end_yesterday = _today_range_utc(yesterday)
    start_week, end_week = _week_range_utc(today)
    start_month, end_month = _month_range_utc(today)

    if range_key == "week":
        range_start = start_week.date().strftime("%Y-%m-%d")
        range_end = (end_week.date() - timedelta(days=1)).strftime("%Y-%m-%d")
    elif range_key == "month":
        range_start = start_month.date().strftime("%Y-%m-%d")
        range_end = today.strftime("%Y-%m-%d")
    else:
        range_start = today_str
        range_end = today_str

    def _sum_and_count(date_str):
        rows = list(
            payments_col.aggregate(
                [
                    {"$match": {"date": date_str, "payment_type": {"$ne": "WITHDRAWAL"}}},
                    {"$group": {"_id": None, "total": {"$sum": "$amount"}, "count": {"$sum": 1}}},
                ]
            )
        )
        if not rows:
            return 0.0, 0
        return round(_safe_amount(rows[0].get("total")), 2), int(rows[0].get("count", 0) or 0)

    sales_today, payments_count_today = _sum_and_count(today_str)
    sales_yesterday, _ = _sum_and_count(yesterday_str)

    customer_rows = list(
        payments_col.aggregate(
            [
                {"$match": {"date": today_str, "payment_type": {"$ne": "WITHDRAWAL"}}},
                {"$group": {"_id": "$customer_id"}},
                {"$count": "count"},
            ]
        )
    )
    customers_paid_today = int(customer_rows[0].get("count", 0) if customer_rows else 0)

    if sales_yesterday == 0 and sales_today > 0:
        sales_change_pct = 100.0
        sales_change_dir = "up"
    elif sales_yesterday == 0 and sales_today == 0:
        sales_change_pct = 0.0
        sales_change_dir = "no_change"
    else:
        sales_change_pct = round(((sales_today - sales_yesterday) / sales_yesterday) * 100, 1)
        sales_change_dir = "up" if sales_change_pct >= 0 else "down"

    avg_payment_today = round((sales_today / payments_count_today), 2) if payments_count_today else 0.0

    expense_today_rows = list(
        manager_expenses_col.aggregate(
            [
                {"$match": {"created_at": {"$gte": start_today, "$lt": end_today}, "status": "Approved"}},
                {"$group": {"_id": None, "total": {"$sum": {"$toDouble": {"$ifNull": ["$amount", 0]}}}}},
            ]
        )
    )
    expense_today_approved = round(_safe_amount(expense_today_rows[0].get("total")) if expense_today_rows else 0.0, 2)

    expense_week_rows = list(
        manager_expenses_col.aggregate(
            [
                {"$match": {"created_at": {"$gte": start_week, "$lt": end_week}, "status": "Approved"}},
                {"$group": {"_id": "$category", "total": {"$sum": {"$toDouble": {"$ifNull": ["$amount", 0]}}}}},
                {"$sort": {"total": -1}},
                {"$limit": 10},
            ]
        )
    )
    expense_week_categories = []
    expense_week_approved_total = 0.0
    for row in expense_week_rows:
        total_val = _safe_amount(row.get("total"))
        expense_week_categories.append({"category": row.get("_id") or "Uncategorized", "total": round(total_val, 2)})
        expense_week_approved_total += total_val

    sales_mtd_rows = list(
        payments_col.aggregate(
            [
                {"$match": {"date": {"$gte": start_month.strftime("%Y-%m-%d"), "$lte": today_str}, "payment_type": {"$ne": "WITHDRAWAL"}}},
                {"$group": {"_id": None, "total": {"$sum": "$amount"}}},
            ]
        )
    )
    sales_mtd = _safe_amount(sales_mtd_rows[0].get("total")) if sales_mtd_rows else 0.0
    expense_mtd_rows = list(
        manager_expenses_col.aggregate(
            [
                {"$match": {"created_at": {"$gte": start_month, "$lt": end_month}, "status": "Approved"}},
                {"$group": {"_id": None, "total": {"$sum": {"$toDouble": {"$ifNull": ["$amount", 0]}}}}},
            ]
        )
    )
    expense_mtd = _safe_amount(expense_mtd_rows[0].get("total")) if expense_mtd_rows else 0.0
    expense_vs_sales_pct_mtd = round((expense_mtd / sales_mtd) * 100, 1) if sales_mtd else 0.0

    pay_dt_expr = {
        "$ifNull": [
            "$created_at",
            {
                "$dateFromString": {
                    "dateString": {
                        "$concat": [
                            {"$ifNull": ["$date", ""]},
                            " ",
                            {"$ifNull": ["$time", "00:00:00"]},
                        ]
                    },
                    "format": "%Y-%m-%d %H:%M:%S",
                    "onError": None,
                    "onNull": None,
                }
            },
        ]
    }

    def _hourly_series(start_dt, end_dt):
        rows = list(
            payments_col.aggregate(
                [
                    {"$match": {"payment_type": {"$ne": "WITHDRAWAL"}}},
                    {"$addFields": {"pay_dt": pay_dt_expr}},
                    {"$match": {"pay_dt": {"$ne": None, "$gte": start_dt, "$lt": end_dt}}},
                    {"$group": {"_id": {"$hour": "$pay_dt"}, "total": {"$sum": "$amount"}}},
                ]
            )
        )
        hourly = {int(row["_id"]): round(_safe_amount(row.get("total")), 2) for row in rows if row.get("_id") is not None}
        return [hourly.get(h, 0) for h in range(24)]

    hourly_today = _hourly_series(start_today, end_today)
    hourly_yesterday = _hourly_series(start_yesterday, end_yesterday)
    hourly_labels = [f"{h:02d}:00" for h in range(24)]

    month_days = []
    cur = start_month.date()
    while cur <= today:
        month_days.append(cur.strftime("%Y-%m-%d"))
        cur = cur + timedelta(days=1)

    sales_rows = list(
        payments_col.aggregate(
            [
                {"$match": {"date": {"$gte": start_month.strftime("%Y-%m-%d"), "$lte": today_str}, "payment_type": {"$ne": "WITHDRAWAL"}}},
                {"$group": {"_id": "$date", "total": {"$sum": "$amount"}}},
            ]
        )
    )
    sales_map = {row.get("_id"): round(_safe_amount(row.get("total")), 2) for row in sales_rows}
    sales_month = [sales_map.get(day, 0) for day in month_days]

    expense_rows = list(
        manager_expenses_col.aggregate(
            [
                {"$match": {"created_at": {"$gte": start_month, "$lt": end_month}, "status": "Approved"}},
                {"$group": {"_id": {"$dateToString": {"format": "%Y-%m-%d", "date": "$created_at"}}, "total": {"$sum": {"$toDouble": {"$ifNull": ["$amount", 0]}}}}},
            ]
        )
    )
    expense_map = {row.get("_id"): round(_safe_amount(row.get("total")), 2) for row in expense_rows}
    expense_month = [expense_map.get(day, 0) for day in month_days]

    manager_rows = list(
        payments_col.aggregate(
            [
                {"$match": {"date": {"$gte": range_start, "$lte": range_end}, "payment_type": {"$ne": "WITHDRAWAL"}}},
                {"$group": {"_id": "$manager_id", "amount": {"$sum": "$amount"}, "count": {"$sum": 1}}},
                {"$sort": {"amount": -1}},
                {"$limit": 10},
            ]
        )
    )
    manager_ids = [row.get("_id") for row in manager_rows if row.get("_id") is not None]
    manager_name_map = _lookup_name_map("manager", manager_ids)
    managers = []
    for row in manager_rows:
        mid = row.get("_id")
        managers.append(
            {
                "manager_id": str(mid) if mid is not None else "",
                "manager_name": manager_name_map.get(str(mid), "Unknown"),
                "amount": round(_safe_amount(row.get("amount")), 2),
                "count": int(row.get("count", 0) or 0),
            }
        )

    agent_rows = list(
        payments_col.aggregate(
            [
                {"$match": {"date": {"$gte": range_start, "$lte": range_end}, "payment_type": {"$ne": "WITHDRAWAL"}}},
                {"$group": {"_id": "$agent_id", "amount": {"$sum": "$amount"}, "count": {"$sum": 1}, "customers": {"$addToSet": "$customer_id"}}},
                {"$sort": {"amount": -1}},
                {"$limit": 10},
            ]
        )
    )
    agent_ids = [row.get("_id") for row in agent_rows if row.get("_id") is not None]
    agent_name_map = _lookup_name_map("agent", agent_ids)
    agents = []
    for row in agent_rows:
        aid = row.get("_id")
        customers = row.get("customers") or []
        agents.append(
            {
                "agent_id": str(aid) if aid is not None else "",
                "agent_name": agent_name_map.get(str(aid), "Unknown"),
                "amount": round(_safe_amount(row.get("amount")), 2),
                "count": int(row.get("count", 0) or 0),
                "customers": int(len(customers)),
            }
        )

    return jsonify(
        ok=True,
        kpis={
            "sales_today": sales_today,
            "sales_yesterday": sales_yesterday,
            "sales_change_pct": round(sales_change_pct, 1),
            "sales_change_dir": sales_change_dir,
            "customers_paid_today": customers_paid_today,
            "avg_payment_today": avg_payment_today,
            "payments_count_today": payments_count_today,
            "expense_today_approved": expense_today_approved,
            "expense_week_approved_total": round(expense_week_approved_total, 2),
            "expense_vs_sales_pct_mtd": round(expense_vs_sales_pct_mtd, 1),
        },
        charts={
            "hourly": {"labels": hourly_labels, "today": hourly_today, "yesterday": hourly_yesterday},
            "sales_month": {"labels": month_days, "values": sales_month},
            "expense_month": {"labels": month_days, "values": expense_month},
            "expense_week_by_cat": {
                "labels": [row["category"] for row in expense_week_categories],
                "values": [row["total"] for row in expense_week_categories],
            },
        },
        leaderboards={"managers": managers, "agents": agents},
        expense_week_categories=expense_week_categories,
    )


@executive_bp.route("/executive/dashboard/kpis-today")
def dashboard_kpis_today():
    today = datetime.utcnow().date()
    yesterday = today - timedelta(days=1)
    today_str = today.strftime("%Y-%m-%d")
    yesterday_str = yesterday.strftime("%Y-%m-%d")

    def _sum_for_date(date_str):
        rows = list(
            payments_col.aggregate(
                [
                    {"$match": {"date": date_str, "payment_type": {"$ne": "WITHDRAWAL"}}},
                    {"$group": {"_id": None, "total": {"$sum": "$amount"}}},
                ]
            )
        )
        return round(_safe_amount(rows[0].get("total")) if rows else 0.0, 2)

    today_sales = _sum_for_date(today_str)
    yesterday_sales = _sum_for_date(yesterday_str)

    if yesterday_sales == 0 and today_sales > 0:
        pct_change = 100.0
        change_label = "New"
    elif yesterday_sales == 0 and today_sales == 0:
        pct_change = 0.0
        change_label = "No change"
    else:
        pct_change = round(((today_sales - yesterday_sales) / yesterday_sales) * 100, 1)
        change_label = "Up" if pct_change >= 0 else "Down"

    top_manager = None
    top_rows = list(
        payments_col.aggregate(
            [
                {"$match": {"date": today_str, "payment_type": {"$ne": "WITHDRAWAL"}}},
                {"$group": {"_id": "$manager_id", "total": {"$sum": "$amount"}, "count": {"$sum": 1}}},
                {"$sort": {"total": -1}},
                {"$limit": 1},
            ]
        )
    )
    if top_rows:
        row = top_rows[0]
        manager_id = row.get("_id")
        top_manager = {
            "manager_id": str(manager_id) if manager_id is not None else "",
            "manager_name": _lookup_manager_name(manager_id),
            "total_sales": round(_safe_amount(row.get("total")), 2),
            "payment_count": int(row.get("count", 0) or 0),
        }

    return jsonify(
        {
            "ok": True,
            "today_sales": today_sales,
            "yesterday_sales": yesterday_sales,
            "pct_change": pct_change,
            "change_label": change_label,
            "top_manager": top_manager,
        }
    )


@executive_bp.route("/executive/dashboard/week-expenses-by-category")
def dashboard_week_expenses_by_category():
    today = datetime.utcnow().date()
    start_of_week = today - timedelta(days=today.weekday())
    start_dt = datetime.combine(start_of_week, datetime.min.time())
    end_dt = datetime.utcnow()

    pipeline = [
        {"$match": {"created_at": {"$gte": start_dt, "$lte": end_dt}, "status": "Approved"}},
        {"$group": {"_id": "$category", "total": {"$sum": {"$toDouble": {"$ifNull": ["$amount", 0]}}}}},
        {"$sort": {"total": -1}},
        {"$limit": 5},
    ]
    rows = list(manager_expenses_col.aggregate(pipeline))

    top_categories = []
    total_week = 0.0
    for row in rows:
        total = _safe_amount(row.get("total"))
        top_categories.append({"category": row.get("_id") or "Uncategorized", "total": round(total, 2)})
        total_week += total

    return jsonify(ok=True, total_week=round(total_week, 2), top_categories=top_categories)


@executive_bp.route("/executive/dashboard/trends")
def dashboard_trends():
    today = datetime.utcnow().date()
    start_of_month = datetime(today.year, today.month, 1)
    if today.month == 12:
        next_month = datetime(today.year + 1, 1, 1)
    else:
        next_month = datetime(today.year, today.month + 1, 1)

    end_of_month = (next_month - timedelta(days=1)).date()
    start_str = start_of_month.strftime("%Y-%m-%d")
    end_str = end_of_month.strftime("%Y-%m-%d")

    days = []
    cur = start_of_month.date()
    while cur <= end_of_month:
        days.append(cur.strftime("%Y-%m-%d"))
        cur = cur + timedelta(days=1)

    sales_rows = list(
        payments_col.aggregate(
            [
                {"$match": {"date": {"$gte": start_str, "$lte": end_str}, "payment_type": {"$ne": "WITHDRAWAL"}}},
                {"$group": {"_id": "$date", "total": {"$sum": "$amount"}}},
            ]
        )
    )
    sales_map = {row.get("_id"): round(_safe_amount(row.get("total")), 2) for row in sales_rows}

    expense_rows = list(
        manager_expenses_col.aggregate(
            [
                {"$match": {"created_at": {"$gte": start_of_month, "$lt": next_month}, "status": "Approved"}},
                {
                    "$group": {
                        "_id": {"$dateToString": {"format": "%Y-%m-%d", "date": "$created_at"}},
                        "total": {"$sum": {"$toDouble": {"$ifNull": ["$amount", 0]}}},
                    }
                },
            ]
        )
    )
    expense_map = {row.get("_id"): round(_safe_amount(row.get("total")), 2) for row in expense_rows}

    sales_daily = [sales_map.get(day, 0) for day in days]
    expense_daily = [expense_map.get(day, 0) for day in days]

    return jsonify(ok=True, days=days, sales_daily=sales_daily, expense_daily=expense_daily)

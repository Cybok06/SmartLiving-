# routes/meeting_report.py

from flask import Blueprint, render_template, request, redirect, url_for, session, jsonify
from bson import ObjectId
from datetime import datetime, timedelta
import calendar

from db import db

meeting_report_bp = Blueprint("meeting_report", __name__, url_prefix="/meeting-report")

# Collections
users_col       = db["users"]
customers_col   = db["customers"]
payments_col    = db["payments"]
login_logs_col  = db["login_logs"]
leads_col       = db["leads"]          # assuming this exists
sales_close_col = db["sales_close"]    # not strictly required here, but available


# ---------- Helpers ----------

def _get_current_role():
    """
    Detect logged-in role from session keys set in login.py.
    Returns ('admin'|'manager'|'executive'|None, user_id_str|None)
    """
    if "admin_id" in session:
        return "admin", session["admin_id"]
    if "executive_id" in session:
        return "executive", session["executive_id"]
    if "manager_id" in session:
        return "manager", session["manager_id"]
    return None, None


def _parse_date(s, default_dt):
    if not s:
        return default_dt
    try:
        return datetime.strptime(s, "%Y-%m-%d")
    except Exception:
        return default_dt


def _date_to_str(dt):
    return dt.strftime("%Y-%m-%d")


def _this_month_range():
    """
    Returns (start_dt, end_dt) for THIS MONTH (1st–last day, full days).
    """
    today = datetime.utcnow()
    start = today.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

    if today.month == 12:
        next_month = today.replace(
            year=today.year + 1,
            month=1,
            day=1,
            hour=0,
            minute=0,
            second=0,
            microsecond=0,
        )
    else:
        next_month = today.replace(
            month=today.month + 1,
            day=1,
            hour=0,
            minute=0,
            second=0,
            microsecond=0,
        )

    end = next_month - timedelta(microseconds=1)
    return start, end


def _month_range(year, month):
    """
    Returns (start_dt, end_dt) for a GIVEN MONTH (1st–last day, full days).
    """
    start = datetime(year, month, 1, 0, 0, 0, 0)
    if month == 12:
        next_month = datetime(year + 1, 1, 1, 0, 0, 0, 0)
    else:
        next_month = datetime(year, month + 1, 1, 0, 0, 0, 0)
    end = next_month - timedelta(microseconds=1)
    return start, end


def _year_month_str(year, month, day):
    return f"{year:04d}-{month:02d}-{day:02d}"


# ---------- MAIN PAGE (filters only) ----------

@meeting_report_bp.route("/", methods=["GET"])
def overview():
    role, user_id = _get_current_role()
    if role is None:
        return redirect(url_for("login.login"))

    # ----- Load managers (for filters) -----
    mgr_query = {"role": "manager"}
    managers_raw = list(
        users_col.find(
            mgr_query,
            {"name": 1, "branch": 1, "phone": 1, "image_url": 1}
        ).sort("name", 1)
    )

    # Branch list
    branches = sorted({m.get("branch", "") for m in managers_raw if m.get("branch")})

    # Manager filter options
    manager_options = [
        {
            "id": str(m["_id"]),
            "name": m.get("name", "Unknown"),
            "branch": m.get("branch", "")
        }
        for m in managers_raw
    ]

    # Agents list (for client-side dropdown / search)
    # IMPORTANT: cover both manager_id as ObjectId and as string.
    agents_data = []
    for m in managers_raw:
        mgr_id_str = str(m["_id"])
        agent_match = {
            "role": "agent",
            "$or": [
                {"manager_id": m["_id"]},     # manager_id stored as ObjectId
                {"manager_id": mgr_id_str},   # manager_id stored as string
            ],
        }
        agents = list(
            users_col.find(
                agent_match,
                {"name": 1, "branch": 1, "phone": 1, "image_url": 1}
            ).sort("name", 1)
        )
        for ag in agents:
            agents_data.append({
                "id": str(ag["_id"]),
                "name": ag.get("name", "Unknown"),
                "branch": ag.get("branch", ""),
                "phone": ag.get("phone", ""),
                "image_url": ag.get("image_url", ""),
                "manager_id": mgr_id_str,
                "manager_name": m.get("name", "Unknown"),
            })

    # Default date range = THIS MONTH
    default_start, default_end = _this_month_range()

    # Current year/month (for top month filter)
    today = datetime.utcnow()
    current_year = today.year
    current_month = today.month

    return render_template(
        "meeting_report_overview.html",
        role=role,
        branches=branches,
        managers=manager_options,
        agents=agents_data,
        default_start=_date_to_str(default_start),
        default_end=_date_to_str(default_end),
        current_year=current_year,
        current_month=current_month,
    )


# ---------- PAGE: full agent performance (separate page) ----------

@meeting_report_bp.route("/agent-performance", methods=["GET"])
def agent_performance_page():
    """
    Separate page for a deeper agent performance / leaderboard view.
    Uses /meeting-report/team-metrics JSON API on the front-end.
    """
    role, user_id = _get_current_role()
    if role is None:
        return redirect(url_for("login.login"))

    # Load managers once (for filters)
    mgr_query = {"role": "manager"}
    managers_raw = list(
        users_col.find(
            mgr_query,
            {"name": 1, "branch": 1}
        ).sort("name", 1)
    )

    branches = sorted({m.get("branch", "") for m in managers_raw if m.get("branch")})

    manager_options = [
        {
            "id": str(m["_id"]),
            "name": m.get("name", "Unknown"),
            "branch": m.get("branch", "")
        }
        for m in managers_raw
    ]

    today = datetime.utcnow()
    current_year = today.year
    current_month = today.month

    return render_template(
        "agent_performance.html",
        role=role,
        branches=branches,
        managers=manager_options,
        current_year=current_year,
        current_month=current_month,
    )


# ---------- JSON API: metrics for a SINGLE AGENT ----------

@meeting_report_bp.route("/agent-metrics", methods=["GET"])
def agent_metrics():
    role, user_id = _get_current_role()
    if role is None:
        return jsonify(ok=False, message="Unauthorized"), 401

    agent_id = (request.args.get("agent_id") or "").strip()
    if not agent_id:
        return jsonify(ok=False, message="agent_id is required"), 400

    # ----- Determine main range from month/year OR explicit start/end -----
    default_start, default_end = _this_month_range()

    month_param = request.args.get("month")
    year_param = request.args.get("year")

    start_dt = default_start
    end_dt = default_end

    if year_param and month_param:
        # If valid year + month are provided, they take priority
        try:
            y = int(year_param)
            m = int(month_param)
            if 1 <= m <= 12:
                start_dt, end_dt = _month_range(y, m)
        except Exception:
            start_dt, end_dt = default_start, default_end
    else:
        # Fallback to the existing start/end behaviour
        start_str_in = request.args.get("start") or _date_to_str(default_start)
        end_str_in   = request.args.get("end")   or _date_to_str(default_end)

        start_dt = _parse_date(start_str_in, default_start)
        end_dt   = _parse_date(end_str_in, default_end)
        if end_dt < start_dt:
            end_dt = start_dt

    start_str = _date_to_str(start_dt)
    end_str   = _date_to_str(end_dt)

    # ----- Year for MONTHLY trend -----
    try:
        trend_year = int(year_param) if year_param else start_dt.year
    except Exception:
        trend_year = start_dt.year

    # Optional comparison range (still supported)
    compare_start_str = request.args.get("compare_start") or ""
    compare_end_str   = request.args.get("compare_end") or ""
    compare = None
    if compare_start_str and compare_end_str:
        cs_dt = _parse_date(compare_start_str, start_dt)
        ce_dt = _parse_date(compare_end_str, cs_dt)
        if ce_dt < cs_dt:
            ce_dt = cs_dt
        compare = {
            "start_dt": cs_dt,
            "end_dt": ce_dt,
            "start_str": _date_to_str(cs_dt),
            "end_str": _date_to_str(ce_dt),
        }

    # ----- Agent + Manager info -----
    try:
        ag_oid = ObjectId(agent_id)
    except Exception:
        return jsonify(ok=False, message="Invalid agent_id"), 400

    agent = users_col.find_one({"_id": ag_oid, "role": "agent"})
    if not agent:
        return jsonify(ok=False, message="Agent not found"), 404

    mgr_name = ""
    mgr_branch = ""
    manager_id = agent.get("manager_id")
    if manager_id:
        try:
            mgr_doc = users_col.find_one({"_id": manager_id}, {"name": 1, "branch": 1})
        except Exception:
            mgr_doc = None
        if mgr_doc:
            mgr_name = mgr_doc.get("name", "")
            mgr_branch = mgr_doc.get("branch", "")

    # ---------- MAIN RANGE METRICS (DATE RANGE) ----------

    # Payments (sales) in main range – optimized projection
    pay_q = {
        "agent_id": agent_id,
        "payment_type": {"$ne": "WITHDRAWAL"},
        "date": {"$gte": start_str, "$lte": end_str}
    }
    payments = list(
        payments_col.find(
            pay_q,
            {"amount": 1, "date": 1, "customer_id": 1}
        )
    )

    total_sales = 0.0
    payments_count = len(payments)
    payments_by_date = {}
    totals_by_customer = {}

    for p in payments:
        try:
            amt = float(p.get("amount", 0.0))
        except Exception:
            amt = 0.0
        total_sales += amt

        d = p.get("date") or start_str  # 'YYYY-MM-DD'
        payments_by_date.setdefault(d, {"date": d, "amount": 0.0, "count": 0})
        payments_by_date[d]["amount"] += amt
        payments_by_date[d]["count"] += 1

        cust_id = p.get("customer_id")
        if cust_id:
            key = str(cust_id)
            totals_by_customer.setdefault(key, 0.0)
            totals_by_customer[key] += amt

    payments_by_date_list = sorted(
        payments_by_date.values(), key=lambda x: x["date"]
    )

    # Customers under this agent (ALL TIME)
    cust_q = {"agent_id": agent_id}
    total_customers = customers_col.count_documents(cust_q)

    # Active customers IN RANGE (based on payments)
    active_customer_ids = payments_col.distinct("customer_id", pay_q)
    active_customers = len(active_customer_ids) if active_customer_ids else 0
    attendance_rate = (active_customers / total_customers * 100) if total_customers > 0 else 0.0
    inactive_customers_count = max(total_customers - active_customers, 0)

    # Top active customers (by amount in main range)
    top_active = []
    if totals_by_customer:
        sorted_items = sorted(
            totals_by_customer.items(),
            key=lambda kv: kv[1],
            reverse=True
        )[:5]
        for cid_str, tot_amt in sorted_items:
            try:
                coid = ObjectId(cid_str)
                cdoc = customers_col.find_one({"_id": coid}, {"name": 1, "phone_number": 1})
            except Exception:
                cdoc = None
            if not cdoc:
                continue
            top_active.append({
                "name": cdoc.get("name", "Unknown"),
                "phone": cdoc.get("phone_number", "N/A"),
                "amount_paid": round(tot_amt, 2),
            })

    # Inactive customers (no payment IN RANGE) – for drill-down (limited)
    inactive = []
    if total_customers > 0:
        all_customers = list(customers_col.find(
            {"agent_id": agent_id},
            {"name": 1, "phone_number": 1}
        ))
        active_id_set = {str(cid) for cid in active_customer_ids}
        for c in all_customers:
            cid_str = str(c["_id"])
            if cid_str in active_id_set:
                continue
            last_pay = payments_col.find_one(
                {"customer_id": c["_id"], "payment_type": {"$ne": "WITHDRAWAL"}},
                sort=[("date", -1)],
                projection={"date": 1},
            )
            last_date = last_pay.get("date") if last_pay else None
            inactive.append({
                "name": c.get("name", "Unknown"),
                "phone": c.get("phone_number", "N/A"),
                "last_payment_date": last_date
            })
        inactive = inactive[:5]

    # Agent attendance (login logs) in date range
    attend_q = {
        "agent_id": agent_id,
        "timestamp": {"$gte": start_dt, "$lte": end_dt}
    }
    login_docs = list(login_logs_col.find(attend_q, {"timestamp": 1}))
    present_dates = set()
    for log in login_docs:
        ts = log.get("timestamp")
        if isinstance(ts, datetime):
            present_dates.add(ts.date())
    present_days = len(present_dates)
    working_days = (end_dt.date() - start_dt.date()).days + 1
    working_days = working_days if working_days > 0 else 0

    # Leads in main range
    leads_total = 0
    leads_converted = 0
    leads_recent = []
    try:
        leads_cursor = leads_col.find(
            {
                "agent_id": agent_id,
                "created_at": {"$gte": start_dt, "$lte": end_dt}
            },
            {"name": 1, "source": 1, "status": 1}
        )
        for ld in leads_cursor:
            leads_total += 1
            status = (ld.get("status") or "").lower()
            if status in ("converted", "closed", "closed won"):
                leads_converted += 1
            if len(leads_recent) < 5:
                leads_recent.append({
                    "name": ld.get("name", "Unknown"),
                    "source": ld.get("source", "N/A"),
                    "status": ld.get("status", "N/A"),
                })
    except Exception:
        leads_total = 0
        leads_converted = 0
        leads_recent = []

    conv_rate = (leads_converted / leads_total * 100) if leads_total > 0 else 0.0

    # ---------- COMPARISON RANGE (optional) ----------

    compare_summary = None
    if compare:
        c_pay_q = {
            "agent_id": agent_id,
            "payment_type": {"$ne": "WITHDRAWAL"},
            "date": {"$gte": compare["start_str"], "$lte": compare["end_str"]}
        }
        c_payments = list(
            payments_col.find(
                c_pay_q,
                {"amount": 1}
            )
        )
        c_total_sales = 0.0
        for p in c_payments:
            try:
                c_total_sales += float(p.get("amount", 0.0))
            except Exception:
                pass

        c_payments_count = len(c_payments)
        c_active_ids = payments_col.distinct("customer_id", c_pay_q)
        c_active_customers = len(c_active_ids) if c_active_ids else 0

        def _pct_change(new, old):
            if old == 0:
                return None
            return (new - old) / old * 100.0

        compare_summary = {
            "start": compare["start_str"],
            "end": compare["end_str"],
            "total_sales": round(c_total_sales, 2),
            "payments_count": c_payments_count,
            "active_customers": c_active_customers,
            "delta_sales_pct": _pct_change(total_sales, c_total_sales),
            "delta_payments_pct": _pct_change(payments_count, c_payments_count),
            "delta_active_customers_pct": _pct_change(active_customers, c_active_customers),
        }

    # ---------- YEARLY MONTH-BY-MONTH TREND (payments + work days) ----------

    months_info = []
    for m in range(1, 13):
        total_days_in_month = calendar.monthrange(trend_year, m)[1]
        months_info.append({
            "month": m,
            "label": calendar.month_abbr[m],
            "total_amount": 0.0,
            "payments_count": 0,
            "worked_days": 0,
            "total_days": total_days_in_month,
            "_day_counts": {}  # internal, will be removed before return
        })

    year_start_str = _year_month_str(trend_year, 1, 1)
    year_end_str   = _year_month_str(trend_year, 12, 31)

    year_pay_q = {
        "agent_id": agent_id,
        "payment_type": {"$ne": "WITHDRAWAL"},
        "date": {"$gte": year_start_str, "$lte": year_end_str}
    }
    year_payments = payments_col.find(
        year_pay_q,
        {"amount": 1, "date": 1}
    )

    for p in year_payments:
        d_str = p.get("date")
        if not d_str:
            continue
        try:
            y, m, d = [int(x) for x in d_str.split("-")]
        except Exception:
            continue
        if y != trend_year or m < 1 or m > 12:
            continue

        idx = m - 1
        try:
            amt = float(p.get("amount", 0.0))
        except Exception:
            amt = 0.0

        months_info[idx]["total_amount"] += amt
        months_info[idx]["payments_count"] += 1

        day_counts = months_info[idx]["_day_counts"]
        day_counts[d_str] = day_counts.get(d_str, 0) + 1

    for m_info in months_info:
        day_counts = m_info.get("_day_counts", {})
        # >10 payments = "worked"
        worked_days = sum(1 for _, cnt in day_counts.items() if cnt > 10)
        m_info["worked_days"] = worked_days
        m_info["total_amount"] = round(m_info["total_amount"], 2)
        if "_day_counts" in m_info:
            del m_info["_day_counts"]

    yearly_trend = {
        "year": trend_year,
        "months": months_info
    }

    # ---------- Build response ----------

    data = {
        "ok": True,
        "agent": {
            "id": agent_id,
            "name": agent.get("name", "Unknown"),
            "branch": agent.get("branch", ""),
            "phone": agent.get("phone", ""),
            "image_url": agent.get("image_url", ""),
            "manager_name": mgr_name,
            "manager_branch": mgr_branch,
        },
        "range": {
            "start": start_str,
            "end": end_str,
        },
        "summary": {
            "total_sales": round(total_sales, 2),
            "payments_count": payments_count,
            "total_customers": total_customers,
            "active_customers": active_customers,
            "inactive_customers": inactive_customers_count,
            "attendance_rate": round(attendance_rate, 1),
            "present_days": present_days,
            "working_days": working_days,
            "leads_total": leads_total,
            "leads_converted": leads_converted,
            "conversion_rate": round(conv_rate, 1),
        },
        "payments_by_date": [
            {
                "date": row["date"],
                "amount": round(row["amount"], 2),
                "count": row["count"],
            }
            for row in payments_by_date_list
        ],
        "customers": {
            "top_active": top_active,
            "inactive": inactive,
        },
        "leads": {
            "recent": leads_recent,
        },
        "attendance": {
            "present_days": present_days,
            "working_days": working_days,
        },
        "compare": compare_summary,
        "yearly_trend": yearly_trend,
    }

    return jsonify(data)


# ---------- JSON API: TEAM / LEADERBOARD METRICS (ALL AGENTS) ----------

@meeting_report_bp.route("/team-metrics", methods=["GET"])
def team_metrics():
    """
    Aggregated metrics for ALL agents under a selected manager or branch.
    Used for the agent performance page leaderboard and charts.

    Query params:
      - manager_id (optional)
      - branch (optional)
      - month, year (optional; if present, override start/end)
      - start, end (date range; defaults = this month)
    """
    role, user_id = _get_current_role()
    if role is None:
        return jsonify(ok=False, message="Unauthorized"), 401

    manager_id_str = (request.args.get("manager_id") or "").strip()
    branch = (request.args.get("branch") or "").strip()

    if not manager_id_str and not branch:
        return jsonify(ok=False, message="manager_id or branch is required"), 400

    # ----- Date range (month/year first, then start/end) -----
    default_start, default_end = _this_month_range()

    month_param = request.args.get("month")
    year_param = request.args.get("year")

    start_dt = default_start
    end_dt = default_end

    if year_param and month_param:
        try:
            y = int(year_param)
            m = int(month_param)
            if 1 <= m <= 12:
                start_dt, end_dt = _month_range(y, m)
        except Exception:
            start_dt, end_dt = default_start, default_end
    else:
        start_str_in = request.args.get("start") or _date_to_str(default_start)
        end_str_in   = request.args.get("end")   or _date_to_str(default_end)

        start_dt = _parse_date(start_str_in, default_start)
        end_dt   = _parse_date(end_str_in, default_end)
        if end_dt < start_dt:
            end_dt = start_dt

    start_str = _date_to_str(start_dt)
    end_str   = _date_to_str(end_dt)

    # ----- Pick agents under manager / branch -----
    agent_query = {"role": "agent"}

    if manager_id_str:
        # Support manager_id stored as ObjectId OR as string in users collection
        try:
            mgr_oid = ObjectId(manager_id_str)
            agent_query["$or"] = [
                {"manager_id": mgr_oid},
                {"manager_id": manager_id_str},
            ]
        except Exception:
            # If for some reason it's not a valid ObjectId, fall back to string match
            agent_query["manager_id"] = manager_id_str
    elif branch:
        agent_query["branch"] = branch

    agents = list(
        users_col.find(
            agent_query,
            {"name": 1, "branch": 1, "phone": 1, "image_url": 1}
        )
    )
    if not agents:
        return jsonify(ok=True, agents=[], range={"start": start_str, "end": end_str})

    agent_ids_str = [str(a["_id"]) for a in agents]

    # ----- Payments aggregation (one query for all agents) -----
    pay_match = {
        "agent_id": {"$in": agent_ids_str},
        "payment_type": {"$ne": "WITHDRAWAL"},
        "date": {"$gte": start_str, "$lte": end_str}
    }

    pay_pipeline = [
        {"$match": pay_match},
        {
            "$group": {
                "_id": {
                    "agent_id": "$agent_id",
                    "date": "$date"
                },
                "total_amount_day": {"$sum": "$amount"},
                "payments_count_day": {"$sum": 1},
                "customers_in_day": {"$addToSet": "$customer_id"},
            }
        },
        {
            "$group": {
                "_id": "$_id.agent_id",
                "total_amount": {"$sum": "$total_amount_day"},
                "payments_count": {"$sum": "$payments_count_day"},
                "customer_ids": {"$addToSet": "$customers_in_day"},
                "work_days": {
                    "$sum": {
                        "$cond": [
                            {"$gt": ["$payments_count_day", 10]},
                            1,
                            0
                        ]
                    }
                }
            }
        }
    ]

    pay_stats = list(payments_col.aggregate(pay_pipeline, allowDiskUse=True))
    payments_by_agent = {}
    for doc in pay_stats:
        agent_key = doc["_id"]
        # customer_ids is a list of sets, flatten
        raw_sets = doc.get("customer_ids", [])
        flat_customers = set()
        for s in raw_sets:
            for cid in s:
                flat_customers.add(cid)

        payments_by_agent[agent_key] = {
            "total_amount": float(doc.get("total_amount", 0.0)),
            "payments_count": int(doc.get("payments_count", 0)),
            "active_customers": len(flat_customers),
            "work_days": int(doc.get("work_days", 0)),
        }

    # ----- Total customers per agent (all time) -----
    cust_pipeline = [
        {"$match": {"agent_id": {"$in": agent_ids_str}}},
        {
            "$group": {
                "_id": "$agent_id",
                "total_customers": {"$sum": 1}
            }
        }
    ]
    cust_stats = list(customers_col.aggregate(cust_pipeline, allowDiskUse=True))
    customers_by_agent = {
        doc["_id"]: int(doc.get("total_customers", 0))
        for doc in cust_stats
    }

    # ----- Leads per agent in date range -----
    leads_match = {
        "agent_id": {"$in": agent_ids_str},
        "created_at": {"$gte": start_dt, "$lte": end_dt}
    }
    leads_pipeline = [
        {"$match": leads_match},
        {
            "$group": {
                "_id": "$agent_id",
                "total_leads": {"$sum": 1},
                "converted_leads": {
                    "$sum": {
                        "$cond": [
                            {
                                "$in": [
                                    {"$toLower": {"$ifNull": ["$status", ""]}},
                                    ["converted", "closed", "closed won"]
                                ]
                            },
                            1,
                            0
                        ]
                    }
                }
            }
        }
    ]
    leads_stats = list(leads_col.aggregate(leads_pipeline, allowDiskUse=True))
    leads_by_agent = {
        doc["_id"]: {
            "total_leads": int(doc.get("total_leads", 0)),
            "converted_leads": int(doc.get("converted_leads", 0)),
        }
        for doc in leads_stats
    }

    # ----- Build per-agent summaries -----
    total_days_range = (end_dt.date() - start_dt.date()).days + 1
    if total_days_range < 0:
        total_days_range = 0

    agent_rows = []
    for a in agents:
        aid_str = str(a["_id"])

        pay_info = payments_by_agent.get(aid_str, {})
        cust_total = customers_by_agent.get(aid_str, 0)
        lead_info = leads_by_agent.get(aid_str, {})

        total_amount = float(pay_info.get("total_amount", 0.0))
        payments_count = int(pay_info.get("payments_count", 0))
        active_customers = int(pay_info.get("active_customers", 0))
        work_days = int(pay_info.get("work_days", 0))

        total_leads = int(lead_info.get("total_leads", 0))
        converted_leads = int(lead_info.get("converted_leads", 0))
        conv_rate = (converted_leads / total_leads * 100.0) if total_leads > 0 else 0.0

        agent_rows.append({
            "id": aid_str,
            "name": a.get("name", "Unknown"),
            "branch": a.get("branch", ""),
            "phone": a.get("phone", ""),
            "image_url": a.get("image_url", ""),
            "total_sales": round(total_amount, 2),
            "payments_count": payments_count,
            "total_customers": cust_total,
            "active_customers": active_customers,
            "inactive_customers": max(cust_total - active_customers, 0),
            "work_days": work_days,
            "calendar_days": total_days_range,
            "leads_total": total_leads,
            "leads_converted": converted_leads,
            "conversion_rate": round(conv_rate, 1),
        })

    return jsonify(
        ok=True,
        range={"start": start_str, "end": end_str},
        agents=agent_rows
    )

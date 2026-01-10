# routes/manager_meeting_report.py

from flask import Blueprint, render_template, request, redirect, url_for, session, jsonify
from bson import ObjectId
from datetime import datetime, timedelta
import calendar

from db import db

manager_meeting_report_bp = Blueprint(
    "manager_meeting_report",
    __name__,
    url_prefix="/manager/meeting-report"
)

# Collections
users_col       = db["users"]
customers_col   = db["customers"]
payments_col    = db["payments"]
login_logs_col  = db["login_logs"]
leads_col       = db["leads"]          # assuming this exists
sales_close_col = db["sales_close"]    # not strictly required here, but available


"""
PERFORMANCE NOTES (DB INDEXES – IMPORTANT TO ADD IN MONGO SHELL):

For best performance, ensure these indexes exist:

payments_col:
  - createIndex({ agent_id: 1, date: 1, payment_type: 1 })
  - (optional) createIndex({ customer_id: 1, date: 1 })

customers_col:
  - createIndex({ agent_id: 1 })

login_logs_col:
  - createIndex({ agent_id: 1, timestamp: 1 })

leads_col:
  - createIndex({ agent_id: 1, created_at: 1 })
"""


# ---------- Helpers ----------

def _get_manager_id():
    """
    Returns the current logged-in manager's ID (string) from session,
    or None if not a manager / not logged in.
    """
    manager_id = session.get("manager_id")
    if not manager_id:
        return None
    return str(manager_id)


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


# ---------- MAIN PAGE (manager-scoped overview) ----------

@manager_meeting_report_bp.route("/", methods=["GET"])
def overview():
    """
    Manager dashboard for performance + agent performance.
    Uses a manager-only template (separate from admin).
    """
    manager_id_str = _get_manager_id()
    if not manager_id_str:
        # Only managers can access this page
        return redirect(url_for("login.login"))

    # Load this manager's document (ObjectId or string)
    mgr_oid = None
    try:
        mgr_oid = ObjectId(manager_id_str)
    except Exception:
        pass

    manager_doc = None
    if mgr_oid:
        manager_doc = users_col.find_one(
            {"_id": mgr_oid, "role": "manager"},
            {"name": 1, "branch": 1, "phone": 1, "image_url": 1}
        )
    if not manager_doc:
        # Fallback if manager_id is stored as plain string
        manager_doc = users_col.find_one(
            {"_id": manager_id_str, "role": "manager"},
            {"name": 1, "branch": 1, "phone": 1, "image_url": 1}
        )

    if not manager_doc:
        # Session says manager but user not found → force logout
        return redirect(url_for("login.logout"))

    # Single-branch context for this manager (template still expects a list)
    branches = sorted({manager_doc.get("branch", "")} if manager_doc.get("branch") else [])

    # Manager filter options (single manager, but keep array for template compatibility)
    manager_options = [
        {
            "id": str(manager_doc["_id"]),
            "name": manager_doc.get("name", "Unknown"),
            "branch": manager_doc.get("branch", "")
        }
    ]

    # Agents list (for this manager only)
    agents_data = []
    mgr_id_str = str(manager_doc["_id"])

    agent_match = {
        "role": "agent",
        "$or": [
            {"manager_id": manager_doc["_id"]},  # manager_id stored as ObjectId
            {"manager_id": mgr_id_str},          # manager_id stored as string
        ],
    }
    agents_cursor = users_col.find(
        agent_match,
        {"name": 1, "branch": 1, "phone": 1, "image_url": 1}
    ).sort("name", 1)

    for ag in agents_cursor:
        agents_data.append({
            "id": str(ag["_id"]),
            "name": ag.get("name", "Unknown"),
            "branch": ag.get("branch", ""),
            "phone": ag.get("phone", ""),
            "image_url": ag.get("image_url", ""),
            "manager_id": mgr_id_str,
            "manager_name": manager_doc.get("name", "Unknown"),
        })

    # Default date range = THIS MONTH
    default_start, default_end = _this_month_range()

    # Current year/month (for top month filter)
    today = datetime.utcnow()
    current_year = today.year
    current_month = today.month

    # Manager-specific template
    return render_template(
        "manager_meeting_report_overview.html",
        role="manager",
        branches=branches,
        managers=manager_options,
        agents=agents_data,
        default_start=_date_to_str(default_start),
        default_end=_date_to_str(default_end),
        current_year=current_year,
        current_month=current_month,
    )


# ---------- PAGE: full agent performance (manager view) ----------

@manager_meeting_report_bp.route("/agent-performance", methods=["GET"])
def agent_performance_page():
    """
    Separate page for deeper agent performance / leaderboard view
    for THIS manager only.
    Uses /manager/meeting-report/team-metrics JSON API on the front-end.
    """
    manager_id_str = _get_manager_id()
    if not manager_id_str:
        return redirect(url_for("login.login"))

    # Load this manager once (filters are trivial here)
    mgr_oid = None
    try:
        mgr_oid = ObjectId(manager_id_str)
    except Exception:
        pass

    manager_doc = None
    if mgr_oid:
        manager_doc = users_col.find_one(
            {"_id": mgr_oid, "role": "manager"},
            {"name": 1, "branch": 1}
        )
    if not manager_doc:
        manager_doc = users_col.find_one(
            {"_id": manager_id_str, "role": "manager"},
            {"name": 1, "branch": 1}
        )

    if not manager_doc:
        return redirect(url_for("login.logout"))

    branches = sorted({manager_doc.get("branch", "")} if manager_doc.get("branch") else [])
    manager_options = [
        {
            "id": str(manager_doc["_id"]),
            "name": manager_doc.get("name", "Unknown"),
            "branch": manager_doc.get("branch", "")
        }
    ]

    today = datetime.utcnow()
    current_year = today.year
    current_month = today.month

    # Reuse agent_performance.html, scoped to this manager
    return render_template(
        "agent_performance.html",
        role="manager",
        branches=branches,
        managers=manager_options,
        current_year=current_year,
        current_month=current_month,
    )


# ---------- JSON API: metrics for a SINGLE AGENT OR MANAGER SCOPE ----------

@manager_meeting_report_bp.route("/agent-metrics", methods=["GET"])
def agent_metrics():
    """
    Manager-scoped metrics.

    Supports:
      scope = "agent"   → single agent under this manager (requires agent_id)
      scope = "manager" → aggregated performance for all agents under this manager
                          (no agent_id needed, uses manager from session)
    """
    manager_id_str = _get_manager_id()
    if not manager_id_str:
        return jsonify(ok=False, message="Unauthorized"), 401

    # Scope (default = agent)
    scope = (request.args.get("scope") or "agent").strip().lower()
    if scope not in ("agent", "manager"):
        scope = "agent"

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

    # ----- Year for MONTHLY trend -----
    try:
        trend_year = int(year_param) if year_param else start_dt.year
    except Exception:
        trend_year = start_dt.year

    # Optional comparison range
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

    # ----- Load manager doc (for security + labels) -----
    mgr_oid = None
    try:
        mgr_oid = ObjectId(manager_id_str)
    except Exception:
        pass

    if mgr_oid:
        mgr_doc = users_col.find_one(
            {"_id": mgr_oid, "role": "manager"},
            {"name": 1, "branch": 1, "phone": 1, "image_url": 1}
        )
    else:
        mgr_doc = users_col.find_one(
            {"_id": manager_id_str, "role": "manager"},
            {"name": 1, "branch": 1, "phone": 1, "image_url": 1}
        )

    if not mgr_doc:
        return jsonify(ok=False, message="Manager not found"), 404

    mgr_name   = mgr_doc.get("name", "")
    mgr_branch = mgr_doc.get("branch", "")

    # ----- Determine which IDs we are aggregating over -----
    agent_ids_str = []
    subject_payload = {}

    if scope == "agent":
        # Single-agent view (must belong to this manager)
        agent_id = (request.args.get("agent_id") or "").strip()
        if not agent_id:
            return jsonify(ok=False, message="agent_id is required for agent scope"), 400

        try:
            ag_oid = ObjectId(agent_id)
        except Exception:
            return jsonify(ok=False, message="Invalid agent_id"), 400

        agent = users_col.find_one({"_id": ag_oid, "role": "agent"})
        if not agent:
            return jsonify(ok=False, message="Agent not found"), 404

        # Ensure this agent belongs to the logged-in manager
        agent_mgr_id = agent.get("manager_id")
        if isinstance(agent_mgr_id, ObjectId):
            agent_mgr_str = str(agent_mgr_id)
        else:
            agent_mgr_str = str(agent_mgr_id) if agent_mgr_id is not None else None

        if agent_mgr_str != manager_id_str:
            return jsonify(ok=False, message="Forbidden: agent not under this manager"), 403

        agent_ids_str = [agent_id]

        subject_payload = {
            "id": agent_id,
            "name": agent.get("name", "Unknown"),
            "branch": agent.get("branch", ""),
            "phone": agent.get("phone", ""),
            "image_url": agent.get("image_url", ""),
            "manager_name": mgr_name,
            "manager_branch": mgr_branch,
            "scope": "agent",  # for front-end labelling
        }

    else:
        # scope == "manager" → aggregate all agents under this manager
        agent_query = {"role": "agent"}
        try:
            mgr_oid = ObjectId(manager_id_str)
            agent_query["$or"] = [
                {"manager_id": mgr_oid},
                {"manager_id": manager_id_str},
            ]
        except Exception:
            agent_query["manager_id"] = manager_id_str

        agents = list(users_col.find(agent_query, {"_id": 1}))
        agent_ids_str = [str(a["_id"]) for a in agents]

        subject_payload = {
            "id": manager_id_str,
            "name": mgr_name or "Manager",
            "branch": mgr_branch,
            "phone": mgr_doc.get("phone", ""),
            "image_url": mgr_doc.get("image_url", ""),
            "manager_name": mgr_name,
            "manager_branch": mgr_branch,
            "scope": "manager",  # for front-end labelling
        }

    # If no agents found in manager scope, return a clean zeroed structure
    working_days = (end_dt.date() - start_dt.date()).days + 1
    if working_days < 0:
        working_days = 0

    if not agent_ids_str:
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
            })

        yearly_trend = {"year": trend_year, "months": months_info}

        return jsonify({
            "ok": True,
            "agent": subject_payload,
            "range": {"start": start_str, "end": end_str},
            "summary": {
                "total_sales": 0.0,
                "payments_count": 0,
                "total_customers": 0,
                "active_customers": 0,
                "inactive_customers": 0,
                "attendance_rate": 0.0,
                "present_days": 0,
                "working_days": working_days,
                "leads_total": 0,
                "leads_converted": 0,
                "conversion_rate": 0.0,
            },
            "payments_by_date": [],
            "customers": {
                "top_active": [],
                "inactive": [],
            },
            "leads": {
                "recent": [],
            },
            "attendance": {
                "present_days": 0,
                "working_days": working_days,
            },
            "compare": None,
            "yearly_trend": yearly_trend,
        })

    # Common filter for all queries in this scope
    agent_filter = {"$in": agent_ids_str}

    # ---------- MAIN RANGE METRICS (DATE RANGE) ----------

    pay_q = {
        "agent_id": agent_filter,
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
    active_customer_ids_set = set()  # PERF: build active customers from already-fetched payments

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
            totals_by_customer[cust_id] = totals_by_customer.get(cust_id, 0.0) + amt
            active_customer_ids_set.add(cust_id)

    payments_by_date_list = sorted(
        payments_by_date.values(), key=lambda x: x["date"]
    )

    # Customers under these agents (ALL TIME)
    cust_q = {"agent_id": agent_filter}
    total_customers = customers_col.count_documents(cust_q)

    # Active customers IN RANGE (based on payments)
    active_customers = len(active_customer_ids_set)
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
        for cid, tot_amt in sorted_items:
            # cid may already be an ObjectId; if not, try to convert
            coid = cid
            if not isinstance(coid, ObjectId):
                try:
                    coid = ObjectId(cid)
                except Exception:
                    coid = None
            if not coid:
                continue

            cdoc = customers_col.find_one(
                {"_id": coid},
                {"name": 1, "phone_number": 1}
            )
            if not cdoc:
                continue

            top_active.append({
                "name": cdoc.get("name", "Unknown"),
                "phone": cdoc.get("phone_number", "N/A"),
                "amount_paid": round(tot_amt, 2),
            })

    # Build a mapping of last payment date per customer (all time, for these agents)
    # PERF: single aggregate instead of N x find_one
    last_payments_by_customer = {}
    last_pay_pipeline = [
        {
            "$match": {
                "agent_id": agent_filter,
                "payment_type": {"$ne": "WITHDRAWAL"},
                "customer_id": {"$ne": None},
            }
        },
        {"$sort": {"date": -1}},
        {
            "$group": {
                "_id": "$customer_id",
                "last_date": {"$first": "$date"},
            }
        },
    ]
    for doc in payments_col.aggregate(last_pay_pipeline, allowDiskUse=True):
        last_payments_by_customer[str(doc["_id"])] = doc.get("last_date")

    # Inactive customers (no payment IN RANGE) – for drill-down (limited to 5)
    inactive = []
    if total_customers > 0:
        all_customers_cursor = customers_col.find(
            {"agent_id": agent_filter},
            {"name": 1, "phone_number": 1}
        )
        active_id_set_str = {str(cid) for cid in active_customer_ids_set}

        for c in all_customers_cursor:
            cid_str = str(c["_id"])
            if cid_str in active_id_set_str:
                continue

            last_date = last_payments_by_customer.get(cid_str)
            inactive.append({
                "name": c.get("name", "Unknown"),
                "phone": c.get("phone_number", "N/A"),
                "last_payment_date": last_date
            })

            if len(inactive) >= 5:
                break

    # Attendance (login logs) in date range – aggregated across all agents
    attend_q = {
        "agent_id": agent_filter,
        "timestamp": {"$gte": start_dt, "$lte": end_dt}
    }
    login_docs = list(login_logs_col.find(attend_q, {"timestamp": 1}))
    present_dates = set()
    for log in login_docs:
        ts = log.get("timestamp")
        if isinstance(ts, datetime):
            present_dates.add(ts.date())
    present_days = len(present_dates)

    # Leads in main range (for these agents)
    leads_total = 0
    leads_converted = 0
    leads_recent = []
    try:
        leads_cursor = leads_col.find(
            {
                "agent_id": agent_filter,
                "created_at": {"$gte": start_dt, "$lte": end_dt}
            },
            {"name": 1, "source": 1, "status": 1}
        ).sort("created_at", -1)  # newest first
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
            "agent_id": agent_filter,
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
        # PERF: reuse structure style – just distinct via aggregation data
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
        "agent_id": agent_filter,
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
        worked_days_month = sum(1 for _, cnt in day_counts.items() if cnt > 10)
        m_info["worked_days"] = worked_days_month
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
        "agent": subject_payload,
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


# ---------- JSON API: TEAM / LEADERBOARD METRICS (ALL AGENTS UNDER THIS MANAGER) ----------

@manager_meeting_report_bp.route("/team-metrics", methods=["GET"])
def team_metrics():
    """
    Aggregated metrics for ALL agents under the logged-in manager.
    Used for the manager's agent performance page leaderboard and charts.

    Query params:
      - month, year (optional; if present, override start/end)
      - start, end (date range; defaults = this month)
    """
    manager_id_str = _get_manager_id()
    if not manager_id_str:
        return jsonify(ok=False, message="Unauthorized"), 401

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

    # ----- Pick agents under THIS manager -----
    agent_query = {"role": "agent"}

    try:
        mgr_oid = ObjectId(manager_id_str)
        agent_query["$or"] = [
            {"manager_id": mgr_oid},
            {"manager_id": manager_id_str},
        ]
    except Exception:
        agent_query["manager_id"] = manager_id_str

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

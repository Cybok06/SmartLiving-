# routes/admin_dashboard.py
from __future__ import annotations
from flask import Blueprint, render_template, redirect, url_for, session
from datetime import datetime, timedelta
from bson import ObjectId
from db import db

admin_dashboard_bp = Blueprint("admin_dashboard", __name__, url_prefix="/admin")

users_col       = db["users"]
complaints_col  = db["complaints"]

def _today_range_utc():
    now = datetime.utcnow()
    start = datetime(now.year, now.month, now.day)
    end = start + timedelta(days=1)
    return start, end

@admin_dashboard_bp.get("/dashboard")
def dashboard():
    # Require admin login
    admin_id = session.get("admin_id")
    if not admin_id:
        return redirect(url_for("login.login"))

    admin_doc = users_col.find_one({"_id": ObjectId(admin_id)}) or {}

    # --- Complaints summary ---
    now = datetime.utcnow()
    start_today, end_today = _today_range_utc()

    q_unresolved = { "status": {"$nin": ["Resolved", "Closed"]} }
    q_breaching  = { "status": {"$nin": ["Resolved", "Closed"]}, "sla_due": {"$lte": now} }
    q_resolved_30 = {
        "status": {"$in": ["Resolved", "Closed"]},
        "date_closed": {"$gte": now - timedelta(days=30)}
    }

    open_count   = complaints_col.count_documents(q_unresolved)
    breaching    = complaints_col.count_documents(q_breaching)
    resolved_30  = complaints_col.count_documents(q_resolved_30)

    # Today’s activity (optional quick stats)
    opened_today = complaints_col.count_documents({ "created_at": {"$gte": start_today, "$lt": end_today} })
    closed_today = complaints_col.count_documents({
        "status": {"$in": ["Resolved", "Closed"]},
        "date_closed": {"$gte": start_today, "$lt": end_today}
    })

    # Top issue types (simple aggregation)
    pipeline = [
        {"$group": {"_id": "$issue_type", "n": {"$sum": 1}}},
        {"$sort": {"n": -1}},
        {"$limit": 6}
    ]
    issue_tops = list(complaints_col.aggregate(pipeline))
    issue_tops = [{"issue": (x.get("_id") or "Uncategorized"), "count": x.get("n", 0)} for x in issue_tops]

    # Recent complaints (latest 8)
    recent = list(complaints_col.find({}).sort([("created_at", -1)]).limit(8))
    for r in recent:
        r["_id"] = str(r["_id"])
        for k in ("date_reported", "date_closed", "sla_due", "created_at", "updated_at"):
            if isinstance(r.get(k), datetime):
                r[k] = r[k].strftime("%Y-%m-%d")

    stats = {
        "open": open_count,
        "breaching": breaching,
        "resolved_30": resolved_30,
        "opened_today": opened_today,
        "closed_today": closed_today
    }

    return render_template(
        "admin_dashboard.html",
        admin=admin_doc,
        stats=stats,
        issue_tops=issue_tops,
        recent=recent
    )

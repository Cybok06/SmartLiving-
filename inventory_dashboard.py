from __future__ import annotations
from flask import Blueprint, render_template, redirect, url_for, session, jsonify, request
from bson import ObjectId
from datetime import datetime, timedelta, date
from collections import Counter
from typing import Any, Dict, Optional

from db import db

# 🔒 DO NOT TOUCH (per your request)
inventory_dashboard = Blueprint("inventory_dashboard", __name__, template_folder="templates")

@inventory_dashboard.route("/inventory/dashboard")
def inventory_dashboard_view():
    if "inventory_id" not in session:
        return redirect(url_for("login.login"))
    username = session.get("inventory_name") or session.get("username", "Inventory User")
    return render_template("inventory_dashboard.html", username=username)

# ---------- Mongo collections (only what we use) ----------
orders_col               = db["orders"]
order_events_col         = db["order_events"]
users_col                = db["users"]
packages_col             = db["packages"]
transfers_col            = db["product_transfers"]    # transfer KPI
stopped_customers_col    = db["stopped_customers"]    # closures KPI

# ---------- helpers ----------
def _oid(v):
    try:
        return ObjectId(str(v))
    except Exception:
        return None

def _iso_day(dt: Optional[datetime]) -> Optional[str]:
    if not dt or not isinstance(dt, datetime):
        return None
    return dt.date().isoformat()

def _last_n_days(n: int):
    today = date.today()
    return [(today - timedelta(days=i)).isoformat() for i in range(n-1, -1, -1)]

def _require_inventory_like():
    uid = session.get("inventory_id") or session.get("admin_id") or session.get("executive_id")
    if not uid:
        return None
    return users_col.find_one({"_id": _oid(uid)})

# ---------- data endpoint consumed by the template ----------
@inventory_dashboard.route("/inventory/dashboard/data")
def inventory_dashboard_data():
    user = _require_inventory_like()
    if not user:
        return jsonify(ok=False, message="Unauthorized"), 401

    branch     = (request.args.get("branch") or "").strip() or None
    manager_id = (request.args.get("manager_id") or "").strip() or None
    date_from  = (request.args.get("date_from") or "").strip() or None
    date_to    = (request.args.get("date_to") or "").strip() or None

    # ---- Orders filter (for KPIs + charts) ----
    q_orders: Dict[str, Any] = {}
    if branch:
        q_orders["branch"] = branch
    if manager_id:
        mid = _oid(manager_id)
        if mid:
            q_orders["manager_id"] = mid
    if date_from or date_to:
        dr = {}
        if date_from:
            try: dr["$gte"] = datetime.strptime(date_from, "%Y-%m-%d")
            except: pass
        if date_to:
            try: dr["$lt"]  = datetime.strptime(date_to, "%Y-%m-%d") + timedelta(days=1)
            except: pass
        if dr:
            q_orders["updated_at"] = dr

    orders = list(orders_col.find(q_orders, {
        "branch":1,"manager_id":1,"status":1,"items":1,"updated_at":1
    }).limit(5000))

    managers_index = {str(u["_id"]): u.get("name") for u in users_col.find({"role":"manager"},{"_id":1,"name":1})}

    # ---- KPIs & tallies from orders ----
    status_counts = Counter()
    outstanding_lines = 0
    postponed_lines   = 0
    out_by_branch     = Counter()
    out_by_manager    = Counter()

    for o in orders:
        status_counts[o.get("status","unknown")] += 1
        br = o.get("branch") or "-"
        mid = str(o.get("manager_id")) if o.get("manager_id") else None
        for it in (o.get("items") or []):
            qty  = int(it.get("qty",0) or 0)
            delv = int(it.get("delivered_qty",0) or 0)
            if qty - delv > 0 and (it.get("status") or "") != "delivered":
                outstanding_lines += 1
                out_by_branch[br] += 1
                if mid: out_by_manager[mid] += 1
            if (it.get("status") or "") == "postponed":
                postponed_lines += 1

    # ---- Packages KPIs ----
    pkg_filter: Dict[str, Any] = {}
    if branch:
        ags = list(users_col.find({"role":"agent","branch":branch},{"_id":1}))
        pkg_filter["agent_id"] = {"$in": [str(a["_id"]) for a in ags]}
    packages_pending = packages_col.count_documents({**pkg_filter, "status": {"$ne": "delivered"}})
    seven_days_ago = datetime.utcnow() - timedelta(days=7)
    packages_delivered_7d = packages_col.count_documents({**pkg_filter, "status":"delivered", "delivered_at": {"$gte": seven_days_ago}})

    # ---- Deliveries last 30d (events) ----
    ev_filter: Dict[str, Any] = {"type":"deliver_line", "at": {"$gte": datetime.utcnow() - timedelta(days=30)}}
    if date_from or date_to:
        dr = {}
        if date_from:
            try: dr["$gte"] = datetime.strptime(date_from, "%Y-%m-%d")
            except: pass
        if date_to:
            try: dr["$lt"]  = datetime.strptime(date_to, "%Y-%m-%d") + timedelta(days=1)
            except: pass
        if dr:
            ev_filter["at"] = dr

    events = list(order_events_col.find(ev_filter, {"order_id":1,"at":1,"payload":1}).sort([("at",-1)]).limit(5000))

    # Branch post-filter (events don’t store branch)
    if branch:
        order_ids = list({e["order_id"] for e in events if e.get("order_id")})
        branch_map = {}
        if order_ids:
            for od in orders_col.find({"_id":{"$in": order_ids}}, {"_id":1,"branch":1}):
                branch_map[od["_id"]] = od.get("branch")
        events = [e for e in events if branch_map.get(e.get("order_id")) == branch]

    deliveries_by_day = Counter()
    for e in events:
        d = _iso_day(e.get("at"))
        if d: deliveries_by_day[d] += 1
    last30 = _last_n_days(30)
    deliveries_series = [deliveries_by_day.get(day, 0) for day in last30]

    # ---- Top managers & outstanding by branch ----
    top_mgr = out_by_manager.most_common(10)
    top_mgr_labels = [managers_index.get(mid, mid[-6:] if mid else "-") for mid,_ in top_mgr]
    top_mgr_values = [cnt for _,cnt in top_mgr]

    ob_pairs  = out_by_branch.most_common()
    ob_labels = [b for b,_ in ob_pairs]
    ob_values = [v for _,v in ob_pairs]

    # ---- Recent deliveries table (latest 50 from filtered events) ----
    recent = []
    for e in events[:50]:
        o = orders_col.find_one({"_id": e["order_id"]}, {"manager_id":1,"branch":1})
        mid = str(o["manager_id"]) if o else None
        recent.append({
            "when": e["at"].strftime("%Y-%m-%d %H:%M") if isinstance(e.get("at"), datetime) else "",
            "branch": (o.get("branch") if o else "-") or "-",
            "manager": managers_index.get(mid) or "—",
            "item": (e.get("payload") or {}).get("item_name") or "-",
            "qty": (e.get("payload") or {}).get("qty", 0),
            "order_id": str(e.get("order_id"))
        })

    # ---- Transfers / Closures (last 14 days) ----
    cutoff14 = datetime.utcnow() - timedelta(days=14)
    try:
        transfers_14 = transfers_col.count_documents({"at": {"$gte": cutoff14}})
    except Exception:
        transfers_14 = 0
    try:
        closures_14 = stopped_customers_col.count_documents({"closed_at": {"$gte": cutoff14}})
    except Exception:
        closures_14 = 0

    # ---- Respond ----
    return jsonify(
        ok=True,
        kpis={
            "orders_open": int(status_counts.get("open",0)),
            "orders_partial": int(status_counts.get("partially_delivered",0)),
            "orders_closed": int(status_counts.get("closed",0)),
            "outstanding_lines": int(outstanding_lines),
            "postponed_lines": int(postponed_lines),
            "packages_pending": int(packages_pending),
            "packages_delivered_7d": int(packages_delivered_7d),
            "transfers_14d": int(transfers_14),
            "closures_14d": int(closures_14),
        },
        charts={
            "status_donut": {
                "labels": ["Open","Partially Delivered","Closed"],
                "values": [
                    int(status_counts.get("open",0)),
                    int(status_counts.get("partially_delivered",0)),
                    int(status_counts.get("closed",0))
                ]
            },
            "outstanding_by_branch": { "labels": ob_labels, "values": ob_values },
            "deliveries_last30":     { "labels": last30,    "values": deliveries_series },
            "top_managers_outstanding": { "labels": top_mgr_labels, "values": top_mgr_values }
        },
        recent_deliveries=recent
    )

# executive_expense.py
from __future__ import annotations

from flask import Blueprint, render_template, request, jsonify, session, redirect, url_for
from bson.objectid import ObjectId
from datetime import datetime, timedelta
from typing import Tuple, Optional, List, Dict, Any

from db import db

executive_expense_bp = Blueprint("executive_expense", __name__, url_prefix="/executive-expense")

users_col    = db["users"]
expenses_col = db["manager_expenses"]


# ---------- helpers ----------
def _today_str() -> str:
    return datetime.utcnow().strftime("%Y-%m-%d")


def _current_exec_session() -> Tuple[Optional[str], Optional[str]]:
    if session.get("executive_id"):
        return "executive_id", session["executive_id"]
    if session.get("admin_id"):  # allow admin to act as exec if needed
        return "admin_id", session["admin_id"]
    return None, None


def _ensure_exec_or_redirect():
    _, uid = _current_exec_session()
    if not uid:
        return redirect(url_for("login.login"))
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


def _range_dates(key: str) -> Tuple[datetime, datetime]:
    """
    Quick ranges in UTC.
    """
    now = datetime.utcnow()
    key = (key or "").lower()
    if key == "today":
        start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        return start, start + timedelta(days=1)
    if key == "week":
        start = (now - timedelta(days=now.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
        return start, now
    if key == "month":
        start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        if start.month == 12:
            end = start.replace(year=start.year + 1, month=1)
        else:
            end = start.replace(month=start.month + 1)
        return start, end
    if key == "last7":
        return now - timedelta(days=7), now
    if key == "last30":
        return now - timedelta(days=30), now
    # default: last 30 days
    return now - timedelta(days=30), now


def _parse_date(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d")
    except Exception:
        return None


def _manager_ids_for_branch(branch: str) -> List[str]:
    """
    Return list of manager_id strings for a given branch name.
    """
    branch = (branch or "").strip()
    if not branch:
        return []
    cursor = users_col.find(
        {"role": "manager", "branch": branch},
        {"_id": 1}
    )
    return [str(doc["_id"]) for doc in cursor]


def _build_date_range_from_request(args) -> Tuple[datetime, datetime]:
    """
    Use custom start/end if provided; otherwise fall back to quick range.
    """
    start_str = args.get("start") or ""
    end_str   = args.get("end") or ""
    range_key = args.get("range", "last30")

    start = _parse_date(start_str)
    end   = _parse_date(end_str)

    if start or end:
        # Custom mode
        now = datetime.utcnow()
        if not start:
            # fallback: 30 days before end or now
            base = end or now
            start = base - timedelta(days=30)
        if not end:
            end = now
        # make end exclusive by adding a day
        end = end + timedelta(days=1)
        return start, end

    # Quick range
    return _range_dates(range_key)


def _build_match_filter(args) -> Dict[str, Any]:
    """
    Shared filter builder for /stats and /list.
    """
    status  = (args.get("status") or "Unapproved").title()
    manager = (args.get("manager_id") or "").strip()
    branch  = (args.get("branch") or "").strip()

    if status not in ("All", "Approved", "Unapproved"):
        status = "Unapproved"

    start, end = _build_date_range_from_request(args)

    match: Dict[str, Any] = {
        "created_at": {"$gte": start, "$lte": end}
    }

    if status != "All":
        match["status"] = status

    if manager:
        match["manager_id"] = manager
    elif branch:
        manager_ids = _manager_ids_for_branch(branch)
        if manager_ids:
            match["manager_id"] = {"$in": manager_ids}
        else:
            # No managers in this branch → no results
            match["manager_id"] = "__none__"

    return match


# ---------- page ----------
@executive_expense_bp.route("/", methods=["GET"])
def exec_expense_page():
    """
    Default view:
      - Unapproved expenses only
      - Last 30 days
      - Branch-based table (branch column and filters)
    """
    scope = _ensure_exec_or_redirect()
    if not isinstance(scope, tuple):
        return scope
    exec_id, exec_doc = scope

    # managers for filter dropdown
    managers_cur = users_col.find(
        {"role": "manager"},
        {"_id": 1, "name": 1, "branch": 1}
    ).sort("name", 1)

    managers = [
        {
            "_id": str(m["_id"]),
            "name": m.get("name", "Manager"),
            "branch": m.get("branch", "")
        }
        for m in managers_cur
    ]

    # branch options from managers
    branch_set = sorted({m["branch"] for m in managers if m.get("branch")})
    branches = [{"name": b} for b in branch_set]

    # Initial table: Unapproved, last 30 days, all branches/managers
    match = _build_match_filter(
        {
            "status": "Unapproved",
            "range": "last30"
        }
    )
    docs = list(
        expenses_col.find(
            match,
            {
                "_id": 1,
                "manager_id": 1,
                "date": 1,
                "time": 1,
                "category": 1,
                "description": 1,
                "amount": 1,
                "status": 1,
            }
        ).sort([("created_at", -1)]).limit(300)
    )

    # map managers
    mids = {doc.get("manager_id") for doc in docs if doc.get("manager_id")}
    m_map: Dict[str, Dict[str, str]] = {}
    if mids:
        obj_ids = []
        for mid in mids:
            try:
                obj_ids.append(ObjectId(mid))
            except Exception:
                # If any legacy string ids, you could handle them here
                pass
        if obj_ids:
            for u in users_col.find(
                {"_id": {"$in": obj_ids}},
                {"_id": 1, "name": 1, "branch": 1}
            ):
                m_map[str(u["_id"])] = {
                    "name": u.get("name", "Manager"),
                    "branch": u.get("branch", "")
                }

    rows = []
    for d in docs:
        amt = float(d.get("amount", 0) or 0)
        mid = d.get("manager_id", "")
        info = m_map.get(mid, {"name": "Manager", "branch": ""})
        rows.append({
            "_id": str(d["_id"]),
            "manager_name": info["name"],
            "branch": info["branch"],
            "date": d.get("date", ""),
            "time": d.get("time", ""),
            "category": d.get("category", ""),
            "description": d.get("description", ""),
            "status": d.get("status", "Unapproved"),
            "amount": f"{amt:,.2f}"
        })

    return render_template(
        "executive_expense.html",
        executive_name=exec_doc.get("name", "Executive"),
        managers=managers,
        branches=branches,
        today=_today_str(),
        rows=rows
    )


# ---------- stats ----------
@executive_expense_bp.route("/stats", methods=["GET"])
def exec_stats():
    """
    Query:
      range=today|week|month|last7|last30|custom
      status=All|Approved|Unapproved
      manager_id=<id or empty for all>
      branch=<branch name or empty>
      start=YYYY-MM-DD (optional, for custom)
      end=YYYY-MM-DD   (optional, for custom)
      group=category|branch  (default: category)

    Response:
      {
        ok: true,
        total: float,
        items: [{name, total}]
      }
    """
    scope = _ensure_exec_or_redirect()
    if not isinstance(scope, tuple):
        return jsonify(ok=False, message="Please log in."), 401

    args = request.args
    group_by = (args.get("group") or "category").lower()
    if group_by not in ("category", "branch"):
        group_by = "category"

    match = _build_match_filter(args)

    pipeline = [{"$match": match}]

    if group_by == "branch":
        # Join with users to fetch branch from manager_id
        pipeline += [
            {
                "$lookup": {
                    "from": "users",
                    "let": {"mid": "$manager_id"},
                    "pipeline": [
                        {
                            "$match": {
                                "$expr": {
                                    "$eq": ["$_id", {"$toObjectId": "$$mid"}]
                                }
                            }
                        },
                        {"$project": {"branch": 1}}
                    ],
                    "as": "mgr"
                }
            },
            {
                "$unwind": {
                    "path": "$mgr",
                    "preserveNullAndEmptyArrays": True
                }
            },
            {
                "$group": {
                    "_id": {"$ifNull": ["$mgr.branch", "Unknown"]},
                    "sum_amount": {
                        "$sum": {"$ifNull": ["$amount", 0]}
                    }
                }
            },
            {"$sort": {"sum_amount": -1}}
        ]
    else:
        pipeline += [
            {
                "$group": {
                    "_id": "$category",
                    "sum_amount": {
                        "$sum": {"$ifNull": ["$amount", 0]}
                    }
                }
            },
            {"$sort": {"sum_amount": -1}}
        ]

    agg = list(expenses_col.aggregate(pipeline))
    items = [
        {"name": a["_id"], "total": float(a["sum_amount"] or 0)}
        for a in agg
    ]
    total = sum(x["total"] for x in items)

    return jsonify(ok=True, total=round(total, 2), items=items[:50])


# ---------- list (for table refresh with filters) ----------
@executive_expense_bp.route("/list", methods=["GET"])
def exec_list():
    scope = _ensure_exec_or_redirect()
    if not isinstance(scope, tuple):
        return jsonify(ok=False, message="Please log in."), 401

    match = _build_match_filter(request.args)

    docs = list(
        expenses_col.find(
            match,
            {
                "_id": 1,
                "manager_id": 1,
                "date": 1,
                "time": 1,
                "category": 1,
                "description": 1,
                "amount": 1,
                "status": 1,
            }
        ).sort([("created_at", -1)]).limit(500)
    )

    mids = {doc.get("manager_id") for doc in docs if doc.get("manager_id")}
    m_map: Dict[str, Dict[str, str]] = {}
    if mids:
        obj_ids = []
        for mid in mids:
            try:
                obj_ids.append(ObjectId(mid))
            except Exception:
                pass
        if obj_ids:
            for u in users_col.find(
                {"_id": {"$in": obj_ids}},
                {"_id": 1, "name": 1, "branch": 1}
            ):
                m_map[str(u["_id"])] = {
                    "name": u.get("name", "Manager"),
                    "branch": u.get("branch", "")
                }

    rows = []
    for d in docs:
        info = m_map.get(d.get("manager_id", ""), {"name": "Manager", "branch": ""})
        rows.append({
            "_id": str(d["_id"]),
            "manager_name": info["name"],
            "branch": info["branch"],
            "date": d.get("date", ""),
            "time": d.get("time", ""),
            "category": d.get("category", ""),
            "description": d.get("description", ""),
            "status": d.get("status", "Unapproved"),
            "amount": f"{float(d.get('amount', 0) or 0):,.2f}"
        })
    return jsonify(ok=True, rows=rows)


# ---------- approve / delete ----------
@executive_expense_bp.route("/approve", methods=["POST"])
def approve_expense():
    scope = _ensure_exec_or_redirect()
    if not isinstance(scope, tuple):
        return jsonify(ok=False, message="Please log in."), 401
    exec_id, _ = scope

    eid = request.form.get("id") or (request.json.get("id") if request.is_json else "")
    if not eid:
        return jsonify(ok=False, message="Missing expense id."), 400

    try:
        res = expenses_col.update_one(
            {"_id": ObjectId(eid), "status": {"$ne": "Approved"}},
            {
                "$set": {
                    "status": "Approved",
                    "approved_by": exec_id,
                    "approved_at": datetime.utcnow(),
                    "updated_at": datetime.utcnow()
                }
            }
        )
        if res.modified_count == 1:
            return jsonify(ok=True, message="Expense approved.")
        return jsonify(ok=False, message="Already approved or not found.")
    except Exception:
        return jsonify(ok=False, message="Invalid expense id."), 400


@executive_expense_bp.route("/delete", methods=["POST"])
def delete_expense():
    scope = _ensure_exec_or_redirect()
    if not isinstance(scope, tuple):
        return jsonify(ok=False, message="Please log in."), 401

    eid = request.form.get("id") or (request.json.get("id") if request.is_json else "")
    if not eid:
        return jsonify(ok=False, message="Missing expense id."), 400
    try:
        res = expenses_col.delete_one({"_id": ObjectId(eid)})
        if res.deleted_count == 1:
            return jsonify(ok=True, message="Expense deleted.")
        return jsonify(ok=False, message="Expense not found.")
    except Exception:
        return jsonify(ok=False, message="Invalid expense id."), 400

# routes/executive_deposits.py
from flask import Blueprint, render_template, request, redirect, url_for, flash, session, jsonify
from bson import ObjectId
from datetime import datetime, timezone
from pymongo import ReturnDocument

from db import db
from services.deposit_analytics import compute_deposit_analytics

executive_deposits_bp = Blueprint(
    "executive_deposits",
    __name__,
    url_prefix="/executive/deposits",
)

# Collections
users_col                 = db["users"]
manager_deposits_col      = db["manager_deposits"]
branch_deposit_totals_col = db["branch_deposit_totals"]


def _require_executive_session():
    exec_id = session.get("executive_id")
    if not exec_id or not ObjectId.is_valid(exec_id):
        flash("Please log in as an executive to continue.", "error")
        return None, None

    exec_doc = users_col.find_one({"_id": ObjectId(exec_id), "role": "executive"})
    if not exec_doc:
        session.clear()
        flash("Access denied. Please log in as an executive.", "error")
        return None, None

    status = str(exec_doc.get("status", "")).lower()
    if status in ("not active", "inactive", "disabled"):
        session.clear()
        flash("Your account is not active. Contact an administrator.", "error")
        return None, None

    return exec_id, exec_doc


@executive_deposits_bp.route("/", methods=["GET"])
def list_view():
    exec_id, exec_doc = _require_executive_session()
    if not exec_id:
        return redirect(url_for("login.login"))

    # -------- filters (unchanged) ----------
    status = (request.args.get("status") or "submitted").strip()
    branch = (request.args.get("branch") or "").strip()
    analytics_branch = (request.args.get("analytics_branch") or "").strip()
    custom_start = (request.args.get("start") or "").strip()
    custom_end = (request.args.get("end") or "").strip()
    year_raw = (request.args.get("year") or "").strip()
    year = None
    if year_raw:
        try:
            year = int(year_raw)
        except Exception:
            year = None

    query = {}
    if status in ("submitted", "approved", "rejected"):
        query["status"] = status
    if branch:
        query["branch_name"] = branch
    if year:
        start_dt = datetime(year, 1, 1, tzinfo=timezone.utc)
        end_dt = datetime(year, 12, 31, 23, 59, 59, 999999, tzinfo=timezone.utc)
        query["created_at"] = {"$gte": start_dt, "$lte": end_dt}

    # -------- table data (unchanged) ----------
    deposits = list(
        manager_deposits_col.find(query).sort("created_at", -1).limit(200)
    )
    for d in deposits:
        try:
            d["id_str"] = str(d["_id"])
        except Exception:
            d["id_str"] = ""

    branches = manager_deposits_col.distinct("branch_name")
    unconfirmed_count = manager_deposits_col.count_documents({"status": "submitted"})
    allowed_branches = sorted([b for b in branches if b])

    if analytics_branch and analytics_branch not in allowed_branches:
        analytics_branch = ""

    unapproved_query = {"status": "submitted"}
    if branch:
        unapproved_query["branch_name"] = branch
    if year:
        start_dt = datetime(year, 1, 1, tzinfo=timezone.utc)
        end_dt = datetime(year, 12, 31, 23, 59, 59, 999999, tzinfo=timezone.utc)
        unapproved_query["created_at"] = {"$gte": start_dt, "$lte": end_dt}
    unapproved_count = manager_deposits_col.count_documents(unapproved_query)
    unapproved_deposits = list(
        manager_deposits_col.find(unapproved_query).sort("created_at", -1).limit(50)
    )
    for d in unapproved_deposits:
        try:
            d["id_str"] = str(d["_id"])
        except Exception:
            d["id_str"] = ""

    # -------- NEW: manager totals for confirmed deposits ----------
    match_confirmed = {"status": "approved"}
    if branch:
        match_confirmed["branch_name"] = branch
    if year:
        start_dt = datetime(year, 1, 1, tzinfo=timezone.utc)
        end_dt = datetime(year, 12, 31, 23, 59, 59, 999999, tzinfo=timezone.utc)
        match_confirmed["created_at"] = {"$gte": start_dt, "$lte": end_dt}

    pipeline = [
        {"$match": match_confirmed},
        {"$group": {
            "_id": {
                "manager_id": "$manager_id",
                "manager_name": "$manager_name",
                "branch_name": "$branch_name"
            },
            "total_amount": {"$sum": "$amount"},
            "count": {"$sum": 1}
        }},
        {"$sort": {"total_amount": -1}}
    ]
    manager_totals_raw = list(manager_deposits_col.aggregate(pipeline))

    manager_totals = []
    grand_total_approved = 0.0
    total_approved_count = 0

    for r in manager_totals_raw:
        mid   = (r["_id"] or {}).get("manager_id", "")
        mname = (r["_id"] or {}).get("manager_name", "Unknown")
        bname = (r["_id"] or {}).get("branch_name", "Unassigned")
        amt   = float(r.get("total_amount", 0))
        cnt   = int(r.get("count", 0))
        grand_total_approved += amt
        total_approved_count += cnt
        manager_totals.append({
            "manager_id": mid,
            "manager_name": mname,
            "branch_name": bname,
            "total_amount": amt,
            "count": cnt,
        })

    year_options = []
    try:
        year_options = [
            int(r["_id"])
            for r in manager_deposits_col.aggregate([
                {"$match": {"created_at": {"$type": "date"}}},
                {"$group": {"_id": {"$year": "$created_at"}}},
                {"$sort": {"_id": -1}},
            ])
            if r.get("_id")
        ]
    except Exception:
        year_options = []

    analytics = compute_deposit_analytics(
        branch_name=analytics_branch or None,
        custom_start=custom_start,
        custom_end=custom_end,
    )

    return render_template(
        "executive/deposits.html",
        executive_name=exec_doc.get("name") or exec_doc.get("username") or "Executive",
        deposits=deposits,
        status=status,
        branch=branch,
        analytics_branch=analytics_branch,
        custom_start=custom_start,
        custom_end=custom_end,
        year=year,
        year_options=year_options,
        branches=allowed_branches,
        unconfirmed_count=unconfirmed_count,
        # NEW context:
        manager_totals=manager_totals,
        grand_total_approved=grand_total_approved,
        total_approved_count=total_approved_count,
        unapproved_deposits=unapproved_deposits,
        unapproved_count=unapproved_count,
        analytics=analytics,
        analytics_json=analytics,
    )


@executive_deposits_bp.route("/<deposit_id>/approve", methods=["POST"])
def approve(deposit_id):
    exec_id, exec_doc = _require_executive_session()
    if not exec_id:
        return redirect(url_for("login.login"))

    if not ObjectId.is_valid(deposit_id):
        flash("Invalid deposit id.", "danger")
        return redirect(url_for("executive_deposits.list_view"))

    updated = manager_deposits_col.find_one_and_update(
        {"_id": ObjectId(deposit_id), "status": "submitted"},
        {"$set": {
            "status": "approved",
            "approved_at": datetime.now(timezone.utc),
            "approved_by_id": exec_id,
            "approved_by_name": exec_doc.get("name") or exec_doc.get("username"),
        }},
        return_document=ReturnDocument.AFTER
    )

    if not updated:
        flash("Deposit could not be approved (maybe already processed).", "warning")
        return redirect(url_for("executive_deposits.list_view", status="submitted"))

    branch_name = updated.get("branch_name") or "Unassigned"
    amount = float(updated.get("amount") or 0)

    branch_deposit_totals_col.update_one(
        {"branch_name": branch_name},
        {"$inc": {"total_amount": amount},
         "$set": {"updated_at": datetime.now(timezone.utc)}},
        upsert=True
    )

    flash(f"Approved deposit of GHS {amount:.2f} for branch {branch_name}.", "success")
    return redirect(url_for("executive_deposits.list_view", status="submitted"))


@executive_deposits_bp.route("/<deposit_id>/reject", methods=["POST"])
def reject(deposit_id):
    exec_id, exec_doc = _require_executive_session()
    if not exec_id:
        return redirect(url_for("login.login"))

    if not ObjectId.is_valid(deposit_id):
        flash("Invalid deposit id.", "danger")
        return redirect(url_for("executive_deposits.list_view"))

    reason = (request.form.get("reason") or "").strip()

    updated = manager_deposits_col.find_one_and_update(
        {"_id": ObjectId(deposit_id), "status": "submitted"},
        {"$set": {
            "status": "rejected",
            "rejected_at": datetime.now(timezone.utc),
            "rejected_by_id": exec_id,
            "rejected_by_name": exec_doc.get("name") or exec_doc.get("username"),
            "reject_reason": reason,
        }},
        return_document=ReturnDocument.AFTER
    )

    if not updated:
        flash("Deposit could not be rejected (maybe already processed).", "warning")
        return redirect(url_for("executive_deposits.list_view", status="submitted"))

    flash("Deposit rejected.", "info")
    return redirect(url_for("executive_deposits.list_view", status="submitted"))


@executive_deposits_bp.route("/unconfirmed-count", methods=["GET"])
def unconfirmed_count():
    exec_id, _ = _require_executive_session()
    if not exec_id:
        return jsonify({"count": 0}), 401
    count = manager_deposits_col.count_documents({"status": "submitted"})
    return jsonify({"count": int(count)})

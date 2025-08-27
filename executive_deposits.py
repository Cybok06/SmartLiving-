# routes/executive_deposits.py
from flask import Blueprint, render_template, request, redirect, url_for, flash, session, jsonify
from bson import ObjectId
from datetime import datetime, timezone
from pymongo import ReturnDocument

from db import db

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

    query = {}
    if status in ("submitted", "approved", "rejected"):
        query["status"] = status
    if branch:
        query["branch_name"] = branch

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

    # -------- NEW: manager totals for confirmed deposits ----------
    match_confirmed = {"status": "approved"}
    if branch:
        match_confirmed["branch_name"] = branch

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

    return render_template(
        "executive/deposits.html",
        executive_name=exec_doc.get("name") or exec_doc.get("username") or "Executive",
        deposits=deposits,
        status=status,
        branch=branch,
        branches=sorted([b for b in branches if b]),
        unconfirmed_count=unconfirmed_count,
        # NEW context:
        manager_totals=manager_totals,
        grand_total_approved=grand_total_approved,
        total_approved_count=total_approved_count,
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

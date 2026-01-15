# routes/agent_complaints.py
from __future__ import annotations

from datetime import datetime
from typing import Optional, Dict, Any

from flask import (
    Blueprint, render_template, request, redirect,
    url_for, session, jsonify
)
from bson import ObjectId

from db import db


agent_complaints_bp = Blueprint(
    "agent_complaints",
    __name__,
    url_prefix="/agent/complaints"
)

# Collections
complaints_col = db["complaints"]
users_col = db["users"]


# ---------- Helpers ----------

def _get_current_agent_id() -> Optional[str]:
    """
    Returns the currently logged-in agent id as a string.

    Adjust this to match your login/session logic:
    - if you store 'agent_id' in session, it will use that
    - else it falls back to 'user_id'
    """
    candidate_ids = [
        session.get("agent_id"),
        session.get("user_id"),
        session.get("agent_user_id"),
        session.get("agent_oid"),
    ]
    for raw in candidate_ids:
        if not raw:
            continue
        raw_str = str(raw)
        try:
            oid = ObjectId(raw_str)
        except Exception:
            oid = None
        if oid:
            doc = users_col.find_one({"_id": oid, "role": "agent"}, {"_id": 1})
            if doc:
                agent_id = str(doc["_id"])
                print(f"[agent_complaints] agent_id resolved via oid: {agent_id}")
                return agent_id
        doc = users_col.find_one({"_id": raw_str, "role": "agent"}, {"_id": 1})
        if doc:
            agent_id = str(doc["_id"])
            print(f"[agent_complaints] agent_id resolved via string id: {agent_id}")
            return agent_id

    username = (session.get("agent_username") or session.get("username") or "").strip()
    if username:
        doc = users_col.find_one(
            {"role": "agent", "$or": [{"username": username}, {"name": username}]},
            {"_id": 1},
        )
        if doc:
            agent_id = str(doc["_id"])
            print(f"[agent_complaints] agent_id resolved via username: {agent_id}")
            return agent_id

    print("[agent_complaints] agent_id not resolved from session")
    return None


def _assigned_filter_for_agent(agent_id: str):
    """
    Return a value suitable for 'assigned_to_id' query that
    matches both ObjectId and legacy string values.
    """
    try:
        agent_oid = ObjectId(agent_id)
    except Exception:
        agent_oid = None

    if agent_oid:
        # Match either ObjectId or string (for old data)
        return {"$in": [agent_oid, str(agent_oid), agent_id]}
    # Fallback: just string
    return agent_id


def _build_query(agent_id: str, bucket: str, status: str, q: str) -> Dict[str, Any]:
    """
    Build Mongo query to fetch only complaints assigned to the current agent.
    """
    assigned_filter = _assigned_filter_for_agent(agent_id)

    query: Dict[str, Any] = {
        "assigned_to_id": assigned_filter
    }

    # Bucket logic
    bucket = (bucket or "").lower()
    if bucket == "resolved":
        query["status"] = {"$in": ["Resolved", "Closed"]}
    elif bucket == "all":
        # no extra filter; but if explicit status is supplied we override below
        pass
    else:
        # default "open" bucket
        query["status"] = {"$nin": ["Resolved", "Closed"]}

    # Explicit status filter (overrides bucket status)
    if status:
        query["status"] = status

    # Text search
    if q:
        rx = {"$regex": q, "$options": "i"}
        query["$or"] = [
            {"ticket_no": rx},
            {"customer_name": rx},
            {"customer_phone": rx},
            {"issue_type": rx},
            {"channel": rx},
        ]

    return query


# ---------- Views ----------

@agent_complaints_bp.route("/", methods=["GET"], endpoint="agent_complaints_home")
def agent_complaints_home():
    """
    Agent-facing complaints view:
    - shows only complaints assigned_to_id == current agent
    """
    agent_id = _get_current_agent_id()
    if not agent_id:
        # Adjust to your auth blueprint / route name
        return redirect(url_for("login.login"))

    bucket = (request.args.get("bucket") or "open").lower()
    status = (request.args.get("status") or "").strip()
    q = (request.args.get("q") or "").strip()

    query = _build_query(agent_id, bucket, status, q)
    assigned_filter = _assigned_filter_for_agent(agent_id)

    items = list(
        complaints_col
        .find(query)
        .sort("date_reported", -1)
        .limit(200)
    )

    # Stats for top chips
    open_query = {
        "assigned_to_id": assigned_filter,
        "status": {"$nin": ["Resolved", "Closed"]},
    }
    resolved_query = {
        "assigned_to_id": assigned_filter,
        "status": {"$in": ["Resolved", "Closed"]},
    }
    open_count = complaints_col.count_documents(open_query)
    resolved_count = complaints_col.count_documents(resolved_query)

    stats = {
        "open": open_count,
        "resolved": resolved_count,
        "total": open_count + resolved_count,
    }

    # Limit what agents can choose for status
    statuses = ["Assigned", "In Progress", "Waiting for Customer", "Resolved"]

    filters = {
        "bucket": bucket,
        "status": status,
        "q": q,
    }

    return render_template(
        "agent_complaint.html",
        items=items,
        stats=stats,
        filters=filters,
        statuses=statuses,
    )


@agent_complaints_bp.route("/<complaint_id>/status", methods=["POST"])
def agent_update_complaint_status(complaint_id):
    """
    Allow agent to update status for *their own* complaints only.
    Uses a simplified allowed status list.
    """
    agent_id = _get_current_agent_id()
    if not agent_id:
        return jsonify(ok=False, message="Not authenticated"), 401

    new_status = (request.form.get("status") or "").strip()
    allowed_statuses = ["Assigned", "In Progress", "Waiting for Customer", "Resolved"]

    if new_status not in allowed_statuses:
        return jsonify(ok=False, message="Status not allowed"), 400

    try:
        cid = ObjectId(complaint_id)
    except Exception:
        return jsonify(ok=False, message="Invalid complaint id"), 400

    # Ensure this ticket belongs to the current agent
    assigned_filter = _assigned_filter_for_agent(agent_id)
    complaint = complaints_col.find_one({
        "_id": cid,
        "assigned_to_id": assigned_filter
    })
    if not complaint:
        return jsonify(ok=False, message="Complaint not found or not assigned to you"), 404

    now = datetime.utcnow()
    now_str = now.strftime("%Y-%m-%d %H:%M:%S")

    update_fields = {
        "status": new_status,
        "updated_at": now,
        "updated_by_role": "agent",
        "updated_by_id": agent_id,
    }

    # Optionally stamp a closed date when agent resolves
    if new_status == "Resolved" and not complaint.get("date_closed"):
        update_fields["date_closed"] = now

    complaints_col.update_one(
        {"_id": cid},
        {"$set": update_fields}
    )

    return jsonify(ok=True, status=new_status, updated_at=now_str)

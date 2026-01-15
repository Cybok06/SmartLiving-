from __future__ import annotations

from datetime import datetime
from typing import Optional, Set, List, Dict, Any

from flask import (
    Blueprint,
    render_template,
    request,
    jsonify,
    redirect,
    url_for,
    session,
)
from bson import ObjectId

from db import db
from services.activity_audit import audit_action

transfer_customer_bp = Blueprint(
    "transfer_customer", __name__, url_prefix="/transfer-customers"
)

# Collections
users_col = db["users"]
customers_col = db["customers"]
transfer_logs_col = db["customer_transfer_logs"]  # small audit collection


# ---------- Helpers ----------

def _get_current_role() -> tuple[Optional[str], Optional[str]]:
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


def _safe_oid(val: Any) -> Optional[ObjectId]:
    """
    Try to convert to ObjectId. If fails, return None.
    """
    if not val:
        return None
    try:
        return ObjectId(str(val))
    except Exception:
        return None


def _get_manager_allowed_agent_ids(manager_user_id: Optional[str]) -> Set[str]:
    """
    For a manager, return a set of agent _id strings they are allowed to act on.
    Matching is done where users.role == 'agent' and users.manager_id == manager
    (string or ObjectId).
    """
    allowed: Set[str] = set()
    if not manager_user_id:
        return allowed

    mgr_oid = _safe_oid(manager_user_id)
    query: Dict[str, Any] = {"role": "agent"}

    if mgr_oid:
        query["$or"] = [
            {"manager_id": mgr_oid},
            {"manager_id": manager_user_id},
        ]
    else:
        query["manager_id"] = manager_user_id

    for ag in users_col.find(query, {"_id": 1}):
        allowed.add(str(ag["_id"]))

    return allowed


def _build_agent_projection() -> Dict[str, int]:
    return {
        "name": 1,
        "branch": 1,
        "phone": 1,
        "status": 1,
        "manager_id": 1,
        "role": 1,
    }


# ---------- PAGE: TRANSFER UI ----------

@transfer_customer_bp.route("/", methods=["GET"])
def transfer_page():
    role, user_id = _get_current_role()
    if role is None:
        return redirect(url_for("login.login"))

    # Load managers basic info (used to show manager_name on agents)
    managers_raw = list(
        users_col.find(
            {"role": "manager"},
            {"name": 1, "branch": 1},
        )
    )
    mgr_map: Dict[str, Dict[str, Any]] = {str(m["_id"]): m for m in managers_raw}

    # Agents list; if manager is logged in, restrict to their own agents only
    agent_query: Dict[str, Any] = {"role": "agent"}

    if role == "manager" and user_id:
        # Reuse the same criteria as permission checks
        mgr_oid = _safe_oid(user_id)
        if mgr_oid:
            agent_query["$or"] = [
                {"manager_id": mgr_oid},
                {"manager_id": user_id},
            ]
        else:
            agent_query["manager_id"] = user_id

    agents_raw = list(
        users_col.find(agent_query, _build_agent_projection()).sort("name", 1)
    )

    agents_data: List[Dict[str, Any]] = []
    for ag in agents_raw:
        ag_id_str = str(ag["_id"])
        branch = ag.get("branch", "") or ""

        mgr_id = ag.get("manager_id")
        mgr_doc = None
        if isinstance(mgr_id, ObjectId):
            mgr_doc = mgr_map.get(str(mgr_id))
        elif isinstance(mgr_id, str):
            mgr_doc = mgr_map.get(mgr_id)

        mgr_name = mgr_doc.get("name") if mgr_doc else ""
        mgr_branch = mgr_doc.get("branch") if mgr_doc else ""

        agents_data.append(
            {
                "id": ag_id_str,
                "name": ag.get("name", "Unknown"),
                "branch": branch,
                "phone": ag.get("phone", ""),
                "status": ag.get("status", "Active"),
                "manager_id": str(mgr_id) if mgr_id else "",
                "manager_name": mgr_name,
                "manager_branch": mgr_branch,
            }
        )

    # branches no longer needed, but harmless to pass
    return render_template(
        "transfer_customer.html",
        role=role,
        agents=agents_data,
        branches=[],
    )


# ---------- PAGE: TRANSFER LOGS (MANAGER / EXEC / ADMIN) ----------

@transfer_customer_bp.route("/logs", methods=["GET"])
def transfer_logs_page():
    """
    Simple UI page to view transfer logs.

    - Admin / Executive: see all logs (latest 100)
    - Manager: see only logs where from_agent or to_agent
      is under that manager.
    """
    role, user_id = _get_current_role()
    if role is None:
        return redirect(url_for("login.login"))

    query: Dict[str, Any] = {}
    if role == "manager" and user_id:
        allowed_agents = _get_manager_allowed_agent_ids(user_id)
        if allowed_agents:
            query = {
                "$or": [
                    {"from_agent_id": {"$in": list(allowed_agents)}},
                    {"to_agent_id": {"$in": list(allowed_agents)}},
                ]
            }
        else:
            query = {"_id": {"$exists": False}}  # returns no docs

    logs_cursor = (
        transfer_logs_col.find(query)
        .sort("performed_at", -1)
        .limit(100)
    )

    logs: List[Dict[str, Any]] = []
    for log in logs_cursor:
        performed_at = log.get("performed_at")
        if isinstance(performed_at, datetime):
            performed_at_str = performed_at.strftime("%Y-%m-%d %H:%M")
        else:
            performed_at_str = ""

        customers = log.get("customers", []) or []

        logs.append(
            {
                "id": str(log.get("_id")),
                "from_agent_id": log.get("from_agent_id", ""),
                "from_agent_name": log.get("from_agent_name", ""),
                "to_agent_id": log.get("to_agent_id", ""),
                "to_agent_name": log.get("to_agent_name", ""),
                "total_customers": log.get("total_customers", 0),
                "selection_count": log.get("selection_count", 0),
                "updated_customers": log.get("updated_customers", 0),
                "performed_by_role": log.get("performed_by_role", ""),
                "performed_by_id": log.get("performed_by_id", ""),
                "performed_at_str": performed_at_str,
                "customers": customers,
            }
        )

    return render_template(
        "manager_transfer_logs.html",
        role=role,
        logs=logs,
    )


# ---------- API: LIST CUSTOMERS FOR AGENT ----------

@transfer_customer_bp.route("/customers", methods=["GET"])
def list_customers_for_agent():
    """
    GET /transfer-customers/customers?agent_id=...
    Returns customers for a given agent, respecting manager-level restrictions.

    Response:
    {
      "ok": true,
      "customers": [
        {
          "_id": "...",
          "name": "...",
          "phone_number": "...",
          "location": "...",
          "occupation": "...",
          "comment": "...",
          "status": "completed | pending | ...",
        },
        ...
      ]
    }
    """
    role, user_id = _get_current_role()
    if role is None:
        return jsonify(ok=False, message="Unauthorized"), 401

    agent_id = (request.args.get("agent_id") or "").strip()
    if not agent_id:
        return jsonify(ok=False, message="agent_id is required"), 400

    # Load the agent to ensure it exists and is role='agent'
    agent_oid = _safe_oid(agent_id)
    agent = users_col.find_one(
        {"_id": agent_oid},
        _build_agent_projection(),
    )
    if not agent or agent.get("role") != "agent":
        return jsonify(ok=False, message="Agent not found or invalid"), 404

    # Manager restriction: manager can only fetch customers for agents under them
    if role == "manager" and user_id:
        allowed = _get_manager_allowed_agent_ids(user_id)
        if agent_id not in allowed:
            return jsonify(
                ok=False,
                message="You can only view customers for agents under your supervision",
            ), 403

    # Build filter; agent_id may be stored as string or ObjectId in customers
    match_filter: Dict[str, Any] = {"$or": [{"agent_id": agent_id}]}
    agent_oid = _safe_oid(agent_id)
    if agent_oid:
        match_filter["$or"].append({"agent_id": agent_oid})

    customers_cursor = customers_col.find(
        match_filter,
        {
            "name": 1,
            "phone_number": 1,
            "location": 1,
            "occupation": 1,
            "comment": 1,
            "status": 1,
        },
    ).sort("name", 1)

    customers: List[Dict[str, Any]] = []
    for c in customers_cursor:
        customers.append(
            {
                "_id": str(c["_id"]),
                "name": c.get("name", ""),
                "phone_number": c.get("phone_number", ""),
                "location": c.get("location", ""),
                "occupation": c.get("occupation", ""),
                "comment": c.get("comment", ""),
                "status": c.get("status", ""),
            }
        )

    return jsonify(ok=True, customers=customers)


# ---------- API: RUN TRANSFER ----------

@transfer_customer_bp.route("/run", methods=["POST"])
@audit_action("customer.transferred", "Transferred Customer", entity_type="customer")
def run_transfer():
    """
    POST JSON:
      {
        "from_agent_id": "...",
        "to_agent_id": "...",
        "customer_ids": ["...", "..."],  # optional; if empty -> all customers of from_agent
        "dry_run": false                # optional
      }

    Behaviour:
      - If customer_ids is provided and non-empty:
          update only those customers (and still require they belong to from_agent).
      - Else:
          finds all customers where agent_id == from_agent_id (string or ObjectId).
      - Updates agent_id to to_agent_id.
      - Also aligns manager_id to the destination agent's manager_id.
      - Respects manager restrictions: manager can only move between agents under them.
      - Returns counts for UI (for progress + confirmation).
      - Logs each transfer with customer names + phone numbers.
    """
    role, user_id = _get_current_role()
    if role is None:
        return jsonify(ok=False, message="Unauthorized"), 401

    payload = request.get_json(silent=True) or {}
    from_agent_id = (payload.get("from_agent_id") or "").strip()
    to_agent_id = (payload.get("to_agent_id") or "").strip()
    dry_run = bool(payload.get("dry_run", False))

    raw_customer_ids = payload.get("customer_ids") or []
    if not isinstance(raw_customer_ids, list):
        raw_customer_ids = []

    if not from_agent_id or not to_agent_id:
        return jsonify(ok=False, message="Both from_agent_id and to_agent_id are required"), 400

    if from_agent_id == to_agent_id:
        return jsonify(ok=False, message="Source and destination agents cannot be the same"), 400

    # Load agents
    from_agent = users_col.find_one(
        {"_id": _safe_oid(from_agent_id)},
        _build_agent_projection(),
    )
    to_agent = users_col.find_one(
        {"_id": _safe_oid(to_agent_id)},
        _build_agent_projection(),
    )

    if not from_agent or from_agent.get("role") != "agent":
        return jsonify(ok=False, message="Source agent not found or invalid"), 404
    if not to_agent or to_agent.get("role") != "agent":
        return jsonify(ok=False, message="Destination agent not found or invalid"), 404

    # Manager-level restriction: manager can only move customers between agents under them
    if role == "manager" and user_id:
        allowed_agents = _get_manager_allowed_agent_ids(user_id)
        if from_agent_id not in allowed_agents or to_agent_id not in allowed_agents:
            return jsonify(
                ok=False,
                message="You can only transfer between agents under your supervision",
            ), 403

    # Build customer_ids (ObjectIds) if provided
    customer_oids: List[ObjectId] = []
    for cid in raw_customer_ids:
        oid = _safe_oid(cid)
        if oid:
            customer_oids.append(oid)

    # Filter for customers to be moved (agent_id may be stored as string or ObjectId)
    agent_match_or = [{"agent_id": from_agent_id}]
    fa_oid = _safe_oid(from_agent_id)
    if fa_oid:
        agent_match_or.append({"agent_id": fa_oid})

    if customer_oids:
        match_filter: Dict[str, Any] = {
            "$and": [
                {"_id": {"$in": customer_oids}},
                {"$or": agent_match_or},
            ]
        }
    else:
        # all customers of the source agent
        match_filter = {"$or": agent_match_or}

    total_customers = customers_col.count_documents(match_filter)

    # Destination manager_id to align
    dest_mgr_id: Any = to_agent.get("manager_id")
    # If manager_id is string that looks like ObjectId, try to convert
    if isinstance(dest_mgr_id, str):
        maybe_oid = _safe_oid(dest_mgr_id)
        if maybe_oid:
            dest_mgr_id = maybe_oid

    selection_count = len(customer_oids) if customer_oids else total_customers

    if dry_run:
        # Just report how many would be affected
        return jsonify(
            ok=True,
            dry_run=True,
            total_customers=total_customers,
            selection_count=selection_count,
            updated_customers=0,
            from_agent={
                "id": from_agent_id,
                "name": from_agent.get("name", ""),
                "branch": from_agent.get("branch", ""),
            },
            to_agent={
                "id": to_agent_id,
                "name": to_agent.get("name", ""),
                "branch": to_agent.get("branch", ""),
            },
        )

    if total_customers == 0:
        return jsonify(
            ok=True,
            dry_run=False,
            total_customers=0,
            selection_count=selection_count,
            updated_customers=0,
            from_agent={
                "id": from_agent_id,
                "name": from_agent.get("name", ""),
                "branch": from_agent.get("branch", ""),
            },
            to_agent={
                "id": to_agent_id,
                "name": to_agent.get("name", ""),
                "branch": to_agent.get("branch", ""),
            },
            message="No customers found for the selected criteria (agent / selection).",
        )

    # Fetch customers BEFORE update so logs capture correct ids/names/phones
    customers_for_log_cursor = customers_col.find(
        match_filter,
        {
            "name": 1,
            "phone_number": 1,
            "location": 1,
            "occupation": 1,
        },
    )
    customers_for_log: List[Dict[str, Any]] = []
    for c in customers_for_log_cursor:
        customers_for_log.append(
            {
                "id": str(c["_id"]),
                "name": c.get("name", ""),
                "phone_number": c.get("phone_number", ""),
                "location": c.get("location", ""),
                "occupation": c.get("occupation", ""),
            }
        )

    update_doc: Dict[str, Any] = {
        "$set": {
            "agent_id": to_agent_id,
        }
    }
    # Also align manager_id with destination agent's manager (if any)
    if dest_mgr_id:
        update_doc["$set"]["manager_id"] = dest_mgr_id

    result = customers_col.update_many(match_filter, update_doc)
    updated_count = result.modified_count

    # Small audit log (non-blocking) – with full customer info
    try:
        transfer_logs_col.insert_one(
            {
                "from_agent_id": from_agent_id,
                "from_agent_name": from_agent.get("name", ""),
                "to_agent_id": to_agent_id,
                "to_agent_name": to_agent.get("name", ""),
                "customer_ids": raw_customer_ids,  # raw list passed in
                "customers": customers_for_log,    # full list actually affected (name + phone)
                "total_customers": total_customers,
                "selection_count": selection_count,
                "updated_customers": updated_count,
                "performed_by_role": role,
                "performed_by_id": user_id,
                "performed_at": datetime.utcnow(),
            }
        )
    except Exception:
        # Logging failure should not break the main operation
        pass

    return jsonify(
        ok=True,
        dry_run=False,
        total_customers=total_customers,
        selection_count=selection_count,
        updated_customers=updated_count,
        from_agent={
            "id": from_agent_id,
            "name": from_agent.get("name", ""),
            "branch": from_agent.get("branch", ""),
        },
        to_agent={
            "id": to_agent_id,
            "name": to_agent.get("name", ""),
            "branch": to_agent.get("branch", ""),
        },
    )

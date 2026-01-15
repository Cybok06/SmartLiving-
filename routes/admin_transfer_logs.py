# routes/admin_transfer_logs.py
from __future__ import annotations

from datetime import datetime
from typing import Optional, Dict, Any, List, Set

from flask import (
    Blueprint,
    render_template,
    request,
    redirect,
    url_for,
    session,
)
from bson import ObjectId

from db import db

admin_transfer_logs_bp = Blueprint(
    "admin_transfer_logs", __name__, url_prefix="/admin-transfer-logs"
)

# Collections
users_col = db["users"]
transfer_logs_col = db["customer_transfer_logs"]


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
    if not val:
        return None
    try:
        return ObjectId(str(val))
    except Exception:
        return None


def _get_manager_agent_ids(manager_id: str) -> Set[str]:
    """
    For a given manager _id (string), return all agent _id strings under that manager.
    """
    if not manager_id:
        return set()

    mgr_oid = _safe_oid(manager_id)
    query: Dict[str, Any] = {"role": "agent"}

    if mgr_oid:
        query["$or"] = [
            {"manager_id": mgr_oid},
            {"manager_id": manager_id},
        ]
    else:
        query["manager_id"] = manager_id

    ids: Set[str] = set()
    for ag in users_col.find(query, {"_id": 1}):
        ids.add(str(ag["_id"]))
    return ids


def _build_user_projection() -> Dict[str, int]:
    return {
        "name": 1,
        "branch": 1,
        "phone": 1,
        "status": 1,
        "role": 1,
        "manager_id": 1,
    }


# ---------- PAGE (ADMIN LOGS) ----------

@admin_transfer_logs_bp.route("/", methods=["GET"])
def admin_transfer_logs_page():
    """
    Admin view of all customer transfer logs with filters:
    - manager_id (by which manager the agents belong to)
    - from_agent_id / to_agent_id
    - performed_by_role
    - date_from / date_to
    """
    role, _ = _get_current_role()
    if role != "admin":
        return redirect(url_for("login.login"))

    # --- Filters from query string ---
    manager_id = (request.args.get("manager_id") or "").strip()
    from_agent_id = (request.args.get("from_agent_id") or "").strip()
    to_agent_id = (request.args.get("to_agent_id") or "").strip()
    performed_by_role = (request.args.get("performed_by_role") or "").strip()
    date_from_str = (request.args.get("date_from") or "").strip()
    date_to_str = (request.args.get("date_to") or "").strip()

    query: Dict[str, Any] = {}

    # Filter by manager (agents under that manager)
    if manager_id:
        agent_ids = _get_manager_agent_ids(manager_id)
        if agent_ids:
            query["$or"] = [
                {"from_agent_id": {"$in": list(agent_ids)}},
                {"to_agent_id": {"$in": list(agent_ids)}},
            ]
        else:
            # No agents for that manager => no logs will match; we can short-circuit later
            query["from_agent_id"] = "__no_such_agent__"

    if from_agent_id:
        query["from_agent_id"] = from_agent_id

    if to_agent_id:
        query["to_agent_id"] = to_agent_id

    if performed_by_role:
        query["performed_by_role"] = performed_by_role

    # Date range filter (performed_at is UTC datetime)
    date_filter: Dict[str, Any] = {}
    if date_from_str:
        try:
            dt_from = datetime.strptime(date_from_str, "%Y-%m-%d")
            date_filter["$gte"] = dt_from
        except ValueError:
            pass
    if date_to_str:
        try:
            # include full day => add 1 day and use < next day
            dt_to = datetime.strptime(date_to_str, "%Y-%m-%d")
            dt_to_end = dt_to.replace(hour=23, minute=59, second=59, microsecond=999999)
            date_filter["$lte"] = dt_to_end
        except ValueError:
            pass
    if date_filter:
        query["performed_at"] = date_filter

    # --- Fetch logs ---
    logs_cursor = (
        transfer_logs_col.find(query)
        .sort("performed_at", -1)
        .limit(200)  # keep it light
    )

    logs_raw: List[Dict[str, Any]] = list(logs_cursor)

    # Collect all user ids we need to resolve to names
    user_ids: set[str] = set()
    agent_ids: set[str] = set()

    for log in logs_raw:
        fa = log.get("from_agent_id")
        ta = log.get("to_agent_id")
        pb = log.get("performed_by_id")

        if fa:
            agent_ids.add(str(fa))
            user_ids.add(str(fa))
        if ta:
            agent_ids.add(str(ta))
            user_ids.add(str(ta))
        if pb:
            user_ids.add(str(pb))

    # Load user data (agents + performed_by)
    users_map: Dict[str, Dict[str, Any]] = {}
    if user_ids:
        u_cursor = users_col.find({"_id": {"$in": [ _safe_oid(u) for u in user_ids if _safe_oid(u) ]}}, _build_user_projection())
        for u in u_cursor:
            users_map[str(u["_id"])] = u

    # For filter dropdowns: load all managers, agents
    managers_raw = list(
        users_col.find(
            {"role": "manager"},
            {"name": 1, "branch": 1},
        ).sort("name", 1)
    )
    managers = [
        {
            "id": str(m["_id"]),
            "name": m.get("name", "Unknown"),
            "branch": m.get("branch", "") or "",
        }
        for m in managers_raw
    ]

    agents_raw = list(
        users_col.find({"role": "agent"}, {"name": 1, "branch": 1}).sort("name", 1)
    )
    agents = [
        {
            "id": str(a["_id"]),
            "name": a.get("name", "Unknown"),
            "branch": a.get("branch", "") or "",
        }
        for a in agents_raw
    ]

    # Build logs for template
    logs: List[Dict[str, Any]] = []
    for log in logs_raw:
        fa_id = str(log.get("from_agent_id", ""))
        ta_id = str(log.get("to_agent_id", ""))
        pb_id = str(log.get("performed_by_id", ""))

        from_agent = users_map.get(fa_id, {})
        to_agent = users_map.get(ta_id, {})
        performer = users_map.get(pb_id, {})

        performed_at = log.get("performed_at")
        if isinstance(performed_at, datetime):
            performed_at_str = performed_at.strftime("%Y-%m-%d %H:%M")
        else:
            performed_at_str = ""

        customer_details = log.get("customer_details", []) or []
        customer_count = len(customer_details)

        # sample customers for quick preview
        sample_items: List[str] = []
        for c in customer_details[:3]:
            nm = (c.get("name") or "").strip()
            ph = (c.get("phone_number") or "").strip()
            if nm and ph:
                sample_items.append(f"{nm} ({ph})")
            elif nm:
                sample_items.append(nm)
            elif ph:
                sample_items.append(ph)
        sample_customers = ", ".join(sample_items)

        logs.append(
            {
                "id": str(log.get("_id")),
                "performed_at_str": performed_at_str,
                "from_agent_name": from_agent.get("name", ""),
                "from_agent_branch": from_agent.get("branch", ""),
                "to_agent_name": to_agent.get("name", ""),
                "to_agent_branch": to_agent.get("branch", ""),
                "performed_by_name": performer.get("name", ""),
                "performed_by_role": log.get("performed_by_role", ""),
                "total_customers": log.get("total_customers", 0),
                "updated_customers": log.get("updated_customers", 0),
                "selection_count": log.get("selection_count"),
                "customer_count": customer_count,
                "sample_customers": sample_customers,
            }
        )

    return render_template(
        "admin_transfer_logs.html",
        role=role,
        logs=logs,
        managers=managers,
        agents=agents,
        # keep current filters so the form stays populated
        filter_manager_id=manager_id,
        filter_from_agent_id=from_agent_id,
        filter_to_agent_id=to_agent_id,
        filter_performed_by_role=performed_by_role,
        filter_date_from=date_from_str,
        filter_date_to=date_to_str,
    )

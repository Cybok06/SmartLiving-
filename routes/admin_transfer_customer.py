# admin_transfer_customer.py
from __future__ import annotations

from typing import Optional, Dict, Any, List

from flask import (
    Blueprint,
    render_template,
    redirect,
    url_for,
    session,
)

from bson import ObjectId

from db import db

admin_transfer_customer_bp = Blueprint(
    "admin_transfer_customer", __name__, url_prefix="/admin-transfer-customers"
)

# Collections
users_col = db["users"]


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


def _build_user_projection() -> Dict[str, int]:
    return {
        "name": 1,
        "branch": 1,
        "phone": 1,
        "status": 1,
        "manager_id": 1,
        "role": 1,
        "image_url": 1,
    }


# ---------- PAGE (ADMIN ONLY) ----------

@admin_transfer_customer_bp.route("/", methods=["GET"])
def admin_transfer_page():
    """
    Admin-facing page:
    - Admin can select any manager (or 'All managers')
    - Then select source agent and destination agent
    - Uses shared /transfer-customers APIs for the actual transfer.
    """
    role, user_id = _get_current_role()
    if role != "admin":
        # Only admin should see this page. Others go to login.
        return redirect(url_for("login.login"))

    # Load all managers
    managers_raw = list(
        users_col.find(
            {"role": "manager"},
            {
                "name": 1,
                "branch": 1,
                "phone": 1,
                "status": 1,
                "image_url": 1,
            },
        ).sort("name", 1)
    )

    managers: List[Dict[str, Any]] = []
    manager_map: Dict[str, Dict[str, Any]] = {}

    for m in managers_raw:
        mid = str(m["_id"])
        manager_map[mid] = m
        managers.append(
            {
                "id": mid,
                "name": m.get("name", "Unknown"),
                "branch": m.get("branch", "") or "",
                "phone": m.get("phone", "") or "",
                "status": m.get("status", "Active"),
                "image_url": m.get("image_url", ""),
            }
        )

    # Load all agents, with manager linkage
    agents_raw = list(
        users_col.find(
            {"role": "agent"},
            _build_user_projection(),
        ).sort("name", 1)
    )

    agents: List[Dict[str, Any]] = []
    for ag in agents_raw:
        ag_id = str(ag["_id"])
        branch = ag.get("branch", "") or ""
        mgr_id = ag.get("manager_id")

        mgr_doc = None
        if isinstance(mgr_id, ObjectId):
            mgr_doc = manager_map.get(str(mgr_id))
        elif isinstance(mgr_id, str):
            mgr_doc = manager_map.get(mgr_id)

        mgr_name = mgr_doc.get("name") if mgr_doc else ""
        mgr_branch = mgr_doc.get("branch") if mgr_doc else ""
        mgr_id_str = str(mgr_id) if mgr_id else ""

        agents.append(
            {
                "id": ag_id,
                "name": ag.get("name", "Unknown"),
                "branch": branch,
                "phone": ag.get("phone", "") or "",
                "status": ag.get("status", "Active"),
                "manager_id": mgr_id_str,
                "manager_name": mgr_name,
                "manager_branch": mgr_branch,
                "image_url": ag.get("image_url", ""),
            }
        )

    return render_template(
        "admin_transfer_customer.html",
        role=role,
        managers=managers,
        agents=agents,
    )

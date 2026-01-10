# routes/manager_sidebar.py
from __future__ import annotations

from typing import Optional, Set
from flask import Blueprint, session
from bson import ObjectId

from db import db

manager_sidebar_bp = Blueprint("manager_sidebar", __name__)

complaints_col = db["complaints"]
users_col = db["users"]


def _get_current_manager_id() -> Optional[str]:
    manager_id = session.get("manager_id")
    if not manager_id:
        return None
    return str(manager_id)


def _manager_agent_ids(manager_id: str) -> Set:
    if not manager_id:
        return set()
    try:
        mgr_oid = ObjectId(manager_id)
        query = {"role": "agent", "$or": [{"manager_id": mgr_oid}, {"manager_id": manager_id}]}
    except Exception:
        mgr_oid = None
        query = {"role": "agent", "manager_id": manager_id}

    agents = list(users_col.find(query, {"_id": 1}))
    ids = set()
    for a in agents:
        a_id = a.get("_id")
        if a_id is None:
            continue
        ids.add(a_id)
        ids.add(str(a_id))
    return ids


@manager_sidebar_bp.app_context_processor
def inject_manager_sidebar_data():
    """
    Inject `manager_unsolved_complaints` into all templates for the current manager.
    """
    manager_id = _get_current_manager_id()
    if not manager_id:
        return {"manager_unsolved_complaints": 0}

    agent_ids = _manager_agent_ids(manager_id)
    if not agent_ids:
        return {"manager_unsolved_complaints": 0}

    count = complaints_col.count_documents({
        "assigned_to_id": {"$in": list(agent_ids)},
        "status": {"$nin": ["Resolved", "Closed"]},
    })

    return {"manager_unsolved_complaints": count}

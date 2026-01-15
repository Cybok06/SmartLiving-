# routes/agent_sidebar.py
from __future__ import annotations

from typing import Optional
from flask import Blueprint, session
from bson import ObjectId

from db import db

agent_sidebar_bp = Blueprint("agent_sidebar", __name__)

complaints_col = db["complaints"]


def _get_current_agent_id() -> Optional[str]:
    agent_id = session.get("agent_id") or session.get("user_id")
    if not agent_id:
        return None
    return str(agent_id)


def _assigned_filter_for_agent(agent_id: str):
    try:
        agent_oid = ObjectId(agent_id)
    except Exception:
        agent_oid = None

    if agent_oid:
        return {"$in": [agent_oid, agent_id]}
    return agent_id


@agent_sidebar_bp.app_context_processor
def inject_agent_sidebar_data():
    """
    Inject `agent_unsolved_complaints` into all templates for the current agent.
    """
    agent_id = _get_current_agent_id()
    if not agent_id:
        return {"agent_unsolved_complaints": 0}

    assigned_filter = _assigned_filter_for_agent(agent_id)

    count = complaints_col.count_documents({
        "assigned_to_id": assigned_filter,
        "status": {"$nin": ["Resolved", "Closed"]},
    })

    return {"agent_unsolved_complaints": count}

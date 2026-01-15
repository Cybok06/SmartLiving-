from __future__ import annotations

from datetime import datetime
from typing import Dict, Any, List
import re

from flask import request, jsonify, session

from db import db
from hr_backend.hr_dashboard import hr_bp, _hr_access_guard

roles_col = db["hr_roles"]

DEFAULT_ROLES: List[Dict[str, str]] = [
    {"name": "Agent", "key": "agent"},
    {"name": "Manager", "key": "manager"},
    {"name": "Admin", "key": "admin"},
    {"name": "Executive", "key": "executive"},
    {"name": "Inventory", "key": "inventory"},
]
DEFAULT_ROLE_KEYS = {r["key"] for r in DEFAULT_ROLES}


def normalize_role_key(name: str) -> str:
    s = (name or "").strip().lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s


def _ensure_role_indexes() -> None:
    try:
        roles_col.create_index("key", unique=True)
        roles_col.create_index("name", unique=True)
    except Exception:
        # Index creation should not break runtime flows.
        pass


_ensure_role_indexes()


def get_role_options() -> List[Dict[str, str]]:
    roles: List[Dict[str, str]] = []
    seen = set()

    for r in DEFAULT_ROLES:
        roles.append(r)
        seen.add(r["key"])

    for doc in roles_col.find({}, {"name": 1, "key": 1}).sort("name", 1):
        key = (doc.get("key") or "").strip()
        name = (doc.get("name") or "").strip()
        if not key or key in seen:
            continue
        roles.append({"name": name or key.replace("_", " ").title(), "key": key})
        seen.add(key)

    return roles


def _actor_user_id() -> str | None:
    return (
        session.get("admin_id")
        or session.get("executive_id")
        or session.get("manager_id")
    )


@hr_bp.route("/roles/add", methods=["POST"])
def add_role():
    if not _hr_access_guard():
        return jsonify({"success": False, "message": "Not authorized."}), 401

    payload = request.get_json(silent=True) or {}
    name = (payload.get("name") or "").strip()
    if not name:
        return jsonify({"success": False, "message": "Role name is required."}), 400

    key = normalize_role_key(name)
    if not key:
        return jsonify({"success": False, "message": "Invalid role name."}), 400

    existing = roles_col.find_one({"$or": [{"key": key}, {"name": name}]})
    if existing:
        return jsonify(
            {
                "success": True,
                "role": {
                    "_id": str(existing.get("_id")),
                    "name": existing.get("name") or name,
                    "key": existing.get("key") or key,
                },
            }
        )

    doc: Dict[str, Any] = {
        "name": name,
        "key": key,
        "created_at": datetime.utcnow(),
    }
    actor_id = _actor_user_id()
    if actor_id:
        doc["created_by"] = actor_id

    try:
        res = roles_col.insert_one(doc)
    except Exception:
        existing = roles_col.find_one({"key": key})
        if existing:
            return jsonify(
                {
                    "success": True,
                    "role": {
                        "_id": str(existing.get("_id")),
                        "name": existing.get("name") or name,
                        "key": existing.get("key") or key,
                    },
                }
            )
        return jsonify({"success": False, "message": "Role already exists."}), 409

    return jsonify(
        {
            "success": True,
            "role": {
                "_id": str(res.inserted_id),
                "name": name,
                "key": key,
            },
        }
    )

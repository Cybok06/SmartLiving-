from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Dict, List

from db import db

login_logs_col = db["login_logs"]
users_col = db["users"]


def ensure_login_log_indexes() -> None:
    try:
        login_logs_col.create_index([("user_id", 1), ("timestamp", -1)])
        login_logs_col.create_index([("username", 1), ("timestamp", -1)])
        login_logs_col.create_index([("role", 1), ("timestamp", -1)])
        login_logs_col.create_index([("ip", 1), ("timestamp", -1)])
    except Exception:
        pass


def ensure_user_indexes() -> None:
    try:
        users_col.create_index([("username", 1)], unique=True)
        users_col.create_index([("role", 1)])
        users_col.create_index([("branch", 1)])
        users_col.create_index([("status", 1)])
    except Exception:
        pass


def get_login_logs_for_user(user_id: str, limit: int = 20) -> List[Dict[str, Any]]:
    if not user_id:
        return []
    rows = list(
        login_logs_col.find({"user_id": str(user_id)})
        .sort("timestamp", -1)
        .limit(limit)
    )
    for r in rows:
        try:
            r["_id"] = str(r.get("_id"))
        except Exception:
            r["_id"] = ""
    return rows


def get_login_stats_for_user(user_id: str, days: int = 30) -> Dict[str, Any]:
    if not user_id:
        return {
            "last_login": None,
            "total_logins": 0,
            "unique_ips": 0,
            "unique_devices": 0,
        }

    now = datetime.utcnow()
    since = now - timedelta(days=days)
    base_q = {"user_id": str(user_id)}

    last = login_logs_col.find_one(base_q, sort=[("timestamp", -1)])
    total_logins = login_logs_col.count_documents({**base_q, "timestamp": {"$gte": since}})
    unique_ips = len(login_logs_col.distinct("ip", {**base_q, "timestamp": {"$gte": since}}))
    unique_devices = len(login_logs_col.distinct("device.raw", {**base_q, "timestamp": {"$gte": since}}))

    return {
        "last_login": last.get("timestamp") if last else None,
        "total_logins": int(total_logins or 0),
        "unique_ips": int(unique_ips or 0),
        "unique_devices": int(unique_devices or 0),
    }


def annotate_login_logs(logs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    recent_locations = []
    for idx, log in enumerate(logs):
        geo = log.get("geo") or {}
        ip_loc = log.get("ip_location") or {}
        loc_key = (
            (ip_loc.get("country") or "").strip(),
            (ip_loc.get("region") or "").strip(),
            (ip_loc.get("city") or "").strip(),
        )
        if idx < 5 and any(loc_key):
            recent_locations.append(loc_key)

        log["is_new_location"] = False
        if idx >= 5 and any(loc_key) and loc_key not in recent_locations:
            log["is_new_location"] = True

        accuracy = geo.get("accuracy_m")
        try:
            log["is_low_accuracy"] = float(accuracy) > 5000
        except Exception:
            log["is_low_accuracy"] = False

    return logs

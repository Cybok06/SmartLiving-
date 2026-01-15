from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Dict, List
from urllib.parse import urlparse, urlencode
import csv
import io

from bson import ObjectId
import bcrypt as bcrypt_lib
from flask import (
    Blueprint,
    Response,
    flash,
    jsonify,
    redirect,
    render_template,
    request,
    session,
    url_for,
)

from db import db
from config_constants import DEFAULT_PROFILE_IMAGE_URL
from services.login_audit import ensure_login_log_indexes, ensure_user_indexes
from services.activity_audit import log_activity
from login import get_current_identity

exec_users_bp = Blueprint("executive_users", __name__)

users_col = db["users"]
login_logs_col = db["login_logs"]
activity_logs_col = db["activity_logs"]
deleted_users_col = db["deleted"]


@exec_users_bp.record_once
def _on_load(state) -> None:
    ensure_login_log_indexes()
    ensure_user_indexes()
    try:
        deleted_users_col.create_index([("deleted_at", -1)])
        deleted_users_col.create_index([("type", 1), ("original.username", 1)])
    except Exception:
        pass


def _exec_users_guard() -> bool:
    role = (session.get("role") or "").lower().strip()
    if session.get("executive_id") or role == "executive":
        return True
    if session.get("admin_id") or role == "admin":
        return True
    return False


def _exec_users_write_guard() -> bool:
    role = (session.get("role") or "").lower().strip()
    if session.get("executive_id") or role == "executive":
        return True
    return False


def _safe_origin() -> bool:
    origin = request.headers.get("Origin") or ""
    referer = request.headers.get("Referer") or ""
    host = request.host

    def _match(netloc: str) -> bool:
        if not netloc:
            return True
        if netloc == host:
            return True
        # Allow localhost/127.0.0.1 interchange for dev
        def _split(nl: str):
            parts = nl.split(":")
            return parts[0], parts[1] if len(parts) > 1 else ""
        h_host, h_port = _split(host)
        n_host, n_port = _split(netloc)
        if h_port != n_port:
            return False
        if {h_host, n_host} <= {"localhost", "127.0.0.1"}:
            return True
        return False

    if origin:
        parsed = urlparse(origin)
        if parsed.netloc and not _match(parsed.netloc):
            return False
    if referer:
        parsed = urlparse(referer)
        if parsed.netloc and not _match(parsed.netloc):
            return False
    return True


def _is_ajax_request() -> bool:
    return (request.headers.get("X-Requested-With") or "").lower() == "xmlhttprequest"


def _safe_next_url(next_url: str | None) -> str | None:
    if not next_url:
        return None
    parsed = urlparse(next_url)
    if parsed.scheme or parsed.netloc:
        return None
    if not parsed.path.startswith("/"):
        return None
    return next_url


def _coerce_object_id(value: str):
    if ObjectId.is_valid(value):
        return ObjectId(value)
    return value


def _normalize_status(value: Any) -> str:
    raw = str(value or "").strip().lower()
    if raw in ("not active", "disabled", "inactive"):
        return "Not Active"
    if raw in ("active", ""):
        return "Active"
    return "Active"


def _effective_active(user_doc: Dict[str, Any]) -> bool:
    status = _normalize_status(user_doc.get("status") or user_doc.get("employment_status"))
    if status != "Active":
        return False
    if user_doc.get("account_locked") is True:
        return False
    if user_doc.get("is_active") is False:
        return False
    return True


def _status_query(status: str) -> Dict[str, Any] | None:
    if not status:
        return None
    status_lc = status.strip().lower()
    if status_lc == "not active":
        return {
            "$or": [
                {"status": {"$in": ["Not Active", "not active", "Disabled", "disabled", "Inactive", "inactive"]}},
                {"employment_status": {"$in": ["Not Active", "not active", "Disabled", "disabled", "Inactive", "inactive"]}},
            ]
        }
    if status_lc == "active":
        return {
            "$or": [
                {"status": {"$in": ["Active", "active", "", None]}},
                {"status": {"$exists": False}},
                {"employment_status": {"$in": ["Active", "active", "", None]}},
                {"employment_status": {"$exists": False}},
            ]
        }
    return {"status": status}


def _range_days_from_key(range_key: str) -> int:
    key = (range_key or "").strip().lower()
    if key == "7d":
        return 7
    if key == "90d":
        return 90
    return 30


def _day_labels(days: int) -> List[str]:
    today = datetime.utcnow().date()
    return [
        (today - timedelta(days=days - 1 - i)).strftime("%Y-%m-%d")
        for i in range(days)
    ]


def _parse_day_str(date_str: str | None) -> datetime | None:
    if not date_str:
        return None
    try:
        return datetime.strptime(date_str, "%Y-%m-%d")
    except Exception:
        return None


def _range_from_key(range_key: str) -> tuple[datetime, datetime, List[str]]:
    today = datetime.utcnow().date()
    if range_key == "today":
        start = datetime(today.year, today.month, today.day)
        end = start + timedelta(days=1)
    elif range_key == "yesterday":
        start = datetime(today.year, today.month, today.day) - timedelta(days=1)
        end = start + timedelta(days=1)
    elif range_key == "last_7":
        end = datetime(today.year, today.month, today.day) + timedelta(days=1)
        start = end - timedelta(days=7)
    elif range_key == "this_month":
        start = datetime(today.year, today.month, 1)
        if today.month == 12:
            end = datetime(today.year + 1, 1, 1)
        else:
            end = datetime(today.year, today.month + 1, 1)
    else:
        end = datetime(today.year, today.month, today.day) + timedelta(days=1)
        start = end - timedelta(days=30)

    labels = []
    cur = start.date()
    while cur < end.date():
        labels.append(cur.strftime("%Y-%m-%d"))
        cur = cur + timedelta(days=1)
    return start, end, labels


def _list_users_with_metrics(
    *,
    role: str,
    branch: str,
    status: str,
    search: str,
    page: int,
    page_size: int,
    range_days: int,
) -> Dict[str, Any]:
    filters: List[Dict[str, Any]] = []
    if role:
        filters.append({"role": role})
    if branch:
        filters.append({"branch": branch})
    status_q = _status_query(status)
    if status_q:
        filters.append(status_q)
    if search:
        filters.append(
            {
                "$or": [
                    {"name": {"$regex": search, "$options": "i"}},
                    {"username": {"$regex": search, "$options": "i"}},
                    {"phone": {"$regex": search, "$options": "i"}},
                    {"email": {"$regex": search, "$options": "i"}},
                ]
            }
        )

    if not filters:
        query: Dict[str, Any] = {}
    elif len(filters) == 1:
        query = filters[0]
    else:
        query = {"$and": filters}

    total = users_col.count_documents(query)
    cursor = (
        users_col.find(
            query,
            {
                "name": 1,
                "username": 1,
                "role": 1,
                "branch": 1,
                "status": 1,
                "employment_status": 1,
                "account_locked": 1,
                "is_active": 1,
                "image_url": 1,
            },
        )
        .sort([("name", 1), ("username", 1)])
        .skip((page - 1) * page_size)
        .limit(page_size)
    )

    rows = list(cursor)
    user_ids = [str(u.get("_id")) for u in rows]

    last_login_map: Dict[str, datetime] = {}
    if user_ids:
        for row in login_logs_col.aggregate(
            [
                {"$match": {"user_id": {"$in": user_ids}}},
                {"$group": {"_id": "$user_id", "last_login": {"$max": "$timestamp"}}},
            ]
        ):
            last_login_map[str(row.get("_id"))] = row.get("last_login")

    since = datetime.utcnow() - timedelta(days=range_days)
    logins_range_map: Dict[str, int] = {}
    if user_ids:
        for row in login_logs_col.aggregate(
            [
                {"$match": {"user_id": {"$in": user_ids}, "timestamp": {"$gte": since}}},
                {"$group": {"_id": "$user_id", "count": {"$sum": 1}}},
            ]
        ):
            logins_range_map[str(row.get("_id"))] = int(row.get("count") or 0)

    activity_tracking = False
    activities_range_map: Dict[str, int] = {}
    try:
        activity_tracking = activity_logs_col.estimated_document_count() > 0
    except Exception:
        activity_tracking = False

    if activity_tracking and user_ids:
        for row in activity_logs_col.aggregate(
            [
                {"$match": {"user_id": {"$in": user_ids}, "timestamp": {"$gte": since}}},
                {"$group": {"_id": "$user_id", "count": {"$sum": 1}}},
            ]
        ):
            activities_range_map[str(row.get("_id"))] = int(row.get("count") or 0)

    data_rows = []
    for u in rows:
        uid = str(u.get("_id"))
        effective_active = _effective_active(u)
        last_login = last_login_map.get(uid)
        data_rows.append(
            {
                "id": uid,
                "name": u.get("name") or "Unknown",
                "username": u.get("username") or "",
                "role": (u.get("role") or "unknown").lower(),
                "branch": u.get("branch") or "Unassigned",
                "status": "Active" if effective_active else "Not Active",
                "image_url": u.get("image_url") or "",
                "last_login": last_login.isoformat() if isinstance(last_login, datetime) else "",
                "logins_range": int(logins_range_map.get(uid) or 0),
                "activities_range": int(activities_range_map.get(uid) or 0) if activity_tracking else None,
            }
        )

    return {
        "rows": data_rows,
        "total": int(total or 0),
        "activity_tracking": activity_tracking,
    }


def _reports_context(range_key: str, branch: str, role: str, status: str) -> Dict[str, Any]:
    range_days = _range_days_from_key(range_key)
    start_dt = datetime.utcnow() - timedelta(days=range_days)

    base_user_q: Dict[str, Any] = {}
    if branch:
        base_user_q["branch"] = branch
    if role:
        base_user_q["role"] = role
    status_q = _status_query(status)
    if status_q:
        base_user_q.update(status_q)

    users = list(users_col.find(base_user_q, {"name": 1, "username": 1, "role": 1, "branch": 1, "status": 1}))
    user_ids = [str(u.get("_id")) for u in users]

    login_counts = {}
    last_login = {}
    if user_ids:
        for row in login_logs_col.aggregate(
            [
                {"$match": {"user_id": {"$in": user_ids}}},
                {"$group": {"_id": "$user_id", "last_login": {"$max": "$timestamp"}, "total": {"$sum": 1}}},
            ]
        ):
            uid = str(row.get("_id"))
            last_login[uid] = row.get("last_login")
            login_counts[uid] = int(row.get("total") or 0)

    login_counts_range = {}
    if user_ids:
        for row in login_logs_col.aggregate(
            [
                {"$match": {"user_id": {"$in": user_ids}, "timestamp": {"$gte": start_dt}}},
                {"$group": {"_id": "$user_id", "count": {"$sum": 1}}},
            ]
        ):
            login_counts_range[str(row.get("_id"))] = int(row.get("count") or 0)

    activity_counts_range = {}
    last_activity = {}
    activity_tracking = False
    try:
        activity_tracking = activity_logs_col.estimated_document_count() > 0
    except Exception:
        activity_tracking = False
    if activity_tracking and user_ids:
        for row in activity_logs_col.aggregate(
            [
                {"$match": {"user_id": {"$in": user_ids}}},
                {"$group": {"_id": "$user_id", "last_activity": {"$max": "$timestamp"}}},
            ]
        ):
            last_activity[str(row.get("_id"))] = row.get("last_activity")
        for row in activity_logs_col.aggregate(
            [
                {"$match": {"user_id": {"$in": user_ids}, "timestamp": {"$gte": start_dt}}},
                {"$group": {"_id": "$user_id", "count": {"$sum": 1}}},
            ]
        ):
            activity_counts_range[str(row.get("_id"))] = int(row.get("count") or 0)

    risk_rows = []
    if user_ids:
        ip_rows = list(
            login_logs_col.aggregate(
                [
                    {"$match": {"user_id": {"$in": user_ids}, "timestamp": {"$gte": start_dt}}},
                    {"$group": {"_id": "$user_id", "ips": {"$addToSet": "$ip"}}},
                ]
            )
        )
        ip_map = {str(row.get("_id")): len(row.get("ips") or []) for row in ip_rows}
        risk_rows = sorted(
            [
                {
                    "id": str(u.get("_id")),
                    "name": u.get("name") or "Unknown",
                    "username": u.get("username") or "",
                    "role": u.get("role") or "",
                    "branch": u.get("branch") or "",
                    "ip_count": int(ip_map.get(str(u.get("_id")), 0)),
                }
                for u in users
            ],
            key=lambda x: x["ip_count"],
            reverse=True,
        )[:10]

    def _score(uid: str) -> int:
        return int(login_counts_range.get(uid, 0)) + int(activity_counts_range.get(uid, 0))

    active_rank = sorted(
        [
            {
                "id": str(u.get("_id")),
                "name": u.get("name") or "Unknown",
                "username": u.get("username") or "",
                "role": u.get("role") or "",
                "branch": u.get("branch") or "",
                "score": _score(str(u.get("_id"))),
                "logins": int(login_counts_range.get(str(u.get("_id")), 0)),
                "activities": int(activity_counts_range.get(str(u.get("_id")), 0)) if activity_tracking else 0,
            }
            for u in users
        ],
        key=lambda x: x["score"],
        reverse=True,
    )[:10]

    dormant_rank = sorted(
        [
            {
                "id": str(u.get("_id")),
                "name": u.get("name") or "Unknown",
                "username": u.get("username") or "",
                "role": u.get("role") or "",
                "branch": u.get("branch") or "",
                "last_login": last_login.get(str(u.get("_id"))),
                "last_activity": last_activity.get(str(u.get("_id"))),
            }
            for u in users
        ],
        key=lambda x: (x["last_login"] or datetime.min, x["last_activity"] or datetime.min),
    )[:10]

    role_counts = {}
    status_counts = {}
    for u in users:
        role_counts[u.get("role") or "unknown"] = role_counts.get(u.get("role") or "unknown", 0) + 1
        status_counts[_normalize_status(u.get("status"))] = status_counts.get(_normalize_status(u.get("status")), 0) + 1

    new_accounts = [
        {
            "id": str(u.get("_id")),
            "name": u.get("name") or "Unknown",
            "username": u.get("username") or "",
            "role": u.get("role") or "",
            "branch": u.get("branch") or "",
            "created_at": u.get("date_registered") or u.get("created_at"),
        }
        for u in users
        if isinstance(u.get("date_registered") or u.get("created_at"), datetime)
        and (u.get("date_registered") or u.get("created_at")) >= start_dt
    ]

    inactive_active_status = [
        {
            "id": str(u.get("_id")),
            "name": u.get("name") or "Unknown",
            "username": u.get("username") or "",
            "role": u.get("role") or "",
            "branch": u.get("branch") or "",
            "last_login": last_login.get(str(u.get("_id"))),
        }
        for u in users
        if _normalize_status(u.get("status")) == "Active"
        and last_login.get(str(u.get("_id"))) is not None
        and last_login.get(str(u.get("_id"))) < start_dt
    ][:10]

    return {
        "range_key": range_key,
        "range_days": range_days,
        "activity_tracking": activity_tracking,
        "most_active": active_rank,
        "most_dormant": dormant_rank,
        "risk_rows": risk_rows,
        "role_counts": role_counts,
        "status_counts": status_counts,
        "new_accounts": new_accounts,
        "inactive_active": inactive_active_status,
    }


@exec_users_bp.get("/executive/users")
def executive_users_list():
    if not _exec_users_guard():
        return redirect(url_for("login.login"))

    role = (request.args.get("role") or "").strip().lower()
    branch = (request.args.get("branch") or "").strip()
    status = (request.args.get("status") or "").strip()
    search = (request.args.get("search") or "").strip()
    range_key = (request.args.get("range") or "30d").strip().lower()
    page = max(int(request.args.get("page") or 1), 1)
    page_size = min(max(int(request.args.get("per_page") or 12), 6), 48)
    range_days = _range_days_from_key(range_key)

    role_counts = {r: 0 for r in ["admin", "manager", "agent", "executive", "inventory", "hr", "accounting", "unknown"]}
    total_users = users_col.estimated_document_count()
    try:
        for row in users_col.aggregate(
            [
                {"$group": {"_id": {"$toLower": {"$ifNull": ["$role", "unknown"]}}, "count": {"$sum": 1}}},
            ]
        ):
            key = row.get("_id") or "unknown"
            role_counts[key] = int(row.get("count") or 0)
    except Exception:
        pass

    branches = users_col.distinct("branch", {"branch": {"$ne": ""}})
    branches = sorted([b for b in branches if b])

    if _is_ajax_request():
        data = _list_users_with_metrics(
            role=role,
            branch=branch,
            status=status,
            search=search,
            page=page,
            page_size=page_size,
            range_days=range_days,
        )
        query_params = {
            "role": role,
            "branch": branch,
            "status": status,
            "search": search,
            "range": range_key,
            "per_page": page_size,
        }
        query_params = {k: v for k, v in query_params.items() if v}
        return render_template(
            "executive_users/partials/users_list_inner.html",
            rows=data["rows"],
            pagination={
                "page": page,
                "per_page": page_size,
                "total": data["total"],
            },
            filters={
                "role": role,
                "branch": branch,
                "status": status,
                "search": search,
                "range": range_key,
            },
            query_string=urlencode(query_params),
            role_counts=role_counts,
            total_users=int(total_users or 0),
            branches=branches,
            roles=["admin", "manager", "agent", "executive", "inventory", "hr", "accounting"],
            statuses=["Active", "Not Active"],
            activity_tracking=data["activity_tracking"],
            can_manage=_exec_users_write_guard(),
            next_url=request.full_path.rstrip("?"),
        )

    return render_template(
        "executive/users_list.html",
        role_counts=role_counts,
        total_users=int(total_users or 0),
        branches=branches,
        roles=["admin", "manager", "agent", "executive", "inventory", "hr", "accounting"],
        statuses=["Active", "Not Active"],
        can_manage=_exec_users_write_guard(),
    )


@exec_users_bp.get("/executive/users/data")
def executive_users_data():
    if not _exec_users_guard():
        return jsonify({"ok": False, "message": "Not authorized"}), 401

    role = (request.args.get("role") or "").strip().lower()
    branch = (request.args.get("branch") or "").strip()
    status = (request.args.get("status") or "").strip()
    search = (request.args.get("search") or "").strip()
    page = max(int(request.args.get("page") or 1), 1)
    page_size = min(max(int(request.args.get("page_size") or 20), 5), 100)

    filters: List[Dict[str, Any]] = []
    if role:
        filters.append({"role": role})
    if branch:
        filters.append({"branch": branch})
    status_q = _status_query(status)
    if status_q:
        filters.append(status_q)
    if search:
        filters.append(
            {
                "$or": [
                    {"name": {"$regex": search, "$options": "i"}},
                    {"username": {"$regex": search, "$options": "i"}},
                    {"phone": {"$regex": search, "$options": "i"}},
                    {"email": {"$regex": search, "$options": "i"}},
                ]
            }
        )

    if not filters:
        query: Dict[str, Any] = {}
    elif len(filters) == 1:
        query = filters[0]
    else:
        query = {"$and": filters}

    total = users_col.count_documents(query)
    cursor = (
        users_col.find(
            query,
            {
                "name": 1,
                "username": 1,
                "role": 1,
                "branch": 1,
                "status": 1,
                "employment_status": 1,
                "account_locked": 1,
                "is_active": 1,
                "image_url": 1,
            },
        )
        .sort([("name", 1), ("username", 1)])
        .skip((page - 1) * page_size)
        .limit(page_size)
    )

    rows = list(cursor)
    user_ids = [str(u.get("_id")) for u in rows]

    last_login_map: Dict[str, datetime] = {}
    if user_ids:
        for row in login_logs_col.aggregate(
            [
                {"$match": {"user_id": {"$in": user_ids}}},
                {"$group": {"_id": "$user_id", "last_login": {"$max": "$timestamp"}}},
            ]
        ):
            last_login_map[str(row.get("_id"))] = row.get("last_login")

    since_30 = datetime.utcnow() - timedelta(days=30)
    logins_30_map: Dict[str, int] = {}
    if user_ids:
        for row in login_logs_col.aggregate(
            [
                {"$match": {"user_id": {"$in": user_ids}, "timestamp": {"$gte": since_30}}},
                {"$group": {"_id": "$user_id", "count": {"$sum": 1}}},
            ]
        ):
            logins_30_map[str(row.get("_id"))] = int(row.get("count") or 0)

    data_rows = []
    for u in rows:
        uid = str(u.get("_id"))
        status_raw = u.get("status") or u.get("employment_status") or ""
        effective_active = _effective_active(u)
        last_login = last_login_map.get(uid)
        data_rows.append(
            {
                "id": uid,
                "name": u.get("name") or "Unknown",
                "username": u.get("username") or "",
                "role": (u.get("role") or "unknown").lower(),
                "branch": u.get("branch") or "Unassigned",
                "status": "Active" if effective_active else "Not Active",
                "image_url": u.get("image_url") or DEFAULT_PROFILE_IMAGE_URL,
                "last_login": last_login.isoformat() if isinstance(last_login, datetime) else "",
                "logins_30d": int(logins_30_map.get(uid) or 0),
            }
        )

    return jsonify(
        {
            "ok": True,
            "rows": data_rows,
            "pagination": {
                "page": page,
                "page_size": page_size,
                "total": int(total or 0),
            },
        }
    )


@exec_users_bp.get("/executive/users/<user_id>")
def executive_user_profile(user_id: str):
    if not _exec_users_guard():
        return redirect(url_for("login.login"))

    if _is_ajax_request():
        lookup_id = _coerce_object_id(user_id)
        user_doc = users_col.find_one({"_id": lookup_id})
        if not user_doc and ObjectId.is_valid(user_id):
            user_doc = users_col.find_one({"_id": str(user_id)})
        if not user_doc:
            return render_template("executive_users/partials/user_profile_inner.html", user=None)

        user_doc["_id"] = str(user_doc.get("_id"))
        user_doc["status_display"] = _normalize_status(user_doc.get("status") or user_doc.get("employment_status"))
        if isinstance(user_doc.get("date_registered"), datetime):
            user_doc["date_registered"] = user_doc["date_registered"].strftime("%Y-%m-%d")
        if isinstance(user_doc.get("created_at"), datetime):
            user_doc["created_at"] = user_doc["created_at"].strftime("%Y-%m-%d")

        return render_template(
            "executive_users/partials/user_profile_inner.html",
            user=user_doc,
            can_manage=_exec_users_write_guard(),
        )

    lookup_id = _coerce_object_id(user_id)
    user_doc = users_col.find_one({"_id": lookup_id})
    if not user_doc and ObjectId.is_valid(user_id):
        user_doc = users_col.find_one({"_id": str(user_id)})
    if not user_doc:
        return render_template(
            "executive/user_profile.html",
            user=None,
            can_manage=_exec_users_write_guard(),
        )

    user_doc["_id"] = str(user_doc.get("_id"))
    user_doc["status_display"] = _normalize_status(user_doc.get("status") or user_doc.get("employment_status"))
    if isinstance(user_doc.get("date_registered"), datetime):
        user_doc["date_registered"] = user_doc["date_registered"].strftime("%Y-%m-%d")
    if isinstance(user_doc.get("created_at"), datetime):
        user_doc["created_at"] = user_doc["created_at"].strftime("%Y-%m-%d")

    return render_template(
        "executive/user_profile.html",
        user=user_doc,
        can_manage=_exec_users_write_guard(),
    )


@exec_users_bp.get("/executive/users/reports")
def executive_users_reports():
    if not _exec_users_guard():
        return redirect(url_for("login.login"))

    range_key = (request.args.get("range") or "30d").strip().lower()
    role = (request.args.get("role") or "").strip().lower()
    branch = (request.args.get("branch") or "").strip()
    status = (request.args.get("status") or "").strip()

    if _is_ajax_request():
        ctx = _reports_context(range_key, branch, role, status)
        branches = users_col.distinct("branch", {"branch": {"$ne": ""}})
        branches = sorted([b for b in branches if b])
        return render_template(
            "executive_users/partials/users_reports_inner.html",
            reports=ctx,
            filters={"range": range_key, "role": role, "branch": branch, "status": status},
            roles=["admin", "manager", "agent", "executive", "inventory", "hr", "accounting"],
            statuses=["Active", "Not Active"],
            branches=branches,
        )

    return render_template(
        "executive_users/users_shell.html",
        initial_url=request.full_path.rstrip("?"),
    )


@exec_users_bp.get("/executive/users/<user_id>/logs")
def executive_user_logs(user_id: str):
    if not _exec_users_guard():
        return jsonify({"ok": False, "message": "Not authorized"}), 401

    lookup_id = _coerce_object_id(user_id)
    user_doc = users_col.find_one({"_id": lookup_id})
    if not user_doc and ObjectId.is_valid(user_id):
        user_doc = users_col.find_one({"_id": str(user_id)})
    if not user_doc:
        return jsonify({"ok": False, "message": "User not found"}), 404

    uid = str(user_doc.get("_id"))
    now = datetime.utcnow()
    since_7 = now - timedelta(days=7)
    since_30 = now - timedelta(days=30)

    total_logins = login_logs_col.count_documents({"user_id": uid})
    logins_7 = login_logs_col.count_documents({"user_id": uid, "timestamp": {"$gte": since_7}})
    logins_30 = login_logs_col.count_documents({"user_id": uid, "timestamp": {"$gte": since_30}})
    last_login_doc = login_logs_col.find_one({"user_id": uid}, sort=[("timestamp", -1)])
    unique_ips_30 = len(login_logs_col.distinct("ip", {"user_id": uid, "timestamp": {"$gte": since_30}}))
    unique_devices_30 = len(login_logs_col.distinct("device.raw", {"user_id": uid, "timestamp": {"$gte": since_30}}))

    recent_logs = list(
        login_logs_col.find({"user_id": uid}).sort("timestamp", -1).limit(20)
    )
    recent_rows = []
    for log in recent_logs:
        ip_loc = log.get("ip_location") or {}
        device = log.get("device") or {}
        geo = log.get("geo") or {}
        recent_rows.append(
            {
                "timestamp": log.get("timestamp").isoformat() if isinstance(log.get("timestamp"), datetime) else "",
                "ip": log.get("ip") or "",
                "city": ip_loc.get("city") or "",
                "country": ip_loc.get("country") or "",
                "browser": device.get("browser") or "",
                "os": device.get("os") or "",
                "location_available": bool(log.get("location_available")),
                "geo": {
                    "lat": geo.get("lat"),
                    "lng": geo.get("lng"),
                    "accuracy_m": geo.get("accuracy_m"),
                },
            }
        )

    days = int(request.args.get("days") or 30)
    if days not in (14, 30):
        days = 30
    day_labels = _day_labels(days)
    start_dt = datetime.strptime(day_labels[0], "%Y-%m-%d")
    trend_rows = list(
        login_logs_col.aggregate(
            [
                {"$match": {"user_id": uid, "timestamp": {"$gte": start_dt}}},
                {
                    "$group": {
                        "_id": {"$dateToString": {"format": "%Y-%m-%d", "date": "$timestamp"}},
                        "count": {"$sum": 1},
                    }
                },
            ]
        )
    )
    trend_map = {row.get("_id"): int(row.get("count") or 0) for row in trend_rows}
    trend_values = [trend_map.get(label, 0) for label in day_labels]

    hourly_rows = list(
        login_logs_col.aggregate(
            [
                {"$match": {"user_id": uid, "timestamp": {"$gte": since_7}}},
                {"$group": {"_id": {"$hour": "$timestamp"}, "count": {"$sum": 1}}},
            ]
        )
    )
    hourly_map = {int(row.get("_id")): int(row.get("count") or 0) for row in hourly_rows if row.get("_id") is not None}
    hourly_values = [hourly_map.get(h, 0) for h in range(24)]

    return jsonify(
        {
            "ok": True,
            "kpis": {
                "total_logins": int(total_logins or 0),
                "logins_7d": int(logins_7 or 0),
                "logins_30d": int(logins_30 or 0),
                "last_login": last_login_doc.get("timestamp").isoformat()
                if last_login_doc and isinstance(last_login_doc.get("timestamp"), datetime)
                else "",
                "unique_ips_30d": int(unique_ips_30 or 0),
                "unique_devices_30d": int(unique_devices_30 or 0),
            },
            "recent_logs": recent_rows,
            "trend": {"labels": day_labels, "values": trend_values},
            "hourly": {"labels": [f"{h:02d}:00" for h in range(24)], "values": hourly_values},
        }
    )


@exec_users_bp.get("/executive/users/<user_id>/logins/by_day")
def executive_user_logins_by_day(user_id: str):
    if not _exec_users_guard():
        return jsonify({"ok": False, "message": "Not authorized"}), 401

    lookup_id = _coerce_object_id(user_id)
    user_doc = users_col.find_one({"_id": lookup_id})
    if not user_doc and ObjectId.is_valid(user_id):
        user_doc = users_col.find_one({"_id": str(user_id)})
    if not user_doc:
        return jsonify({"ok": False, "message": "User not found"}), 404

    uid = str(user_doc.get("_id"))
    latest = login_logs_col.find_one({"user_id": uid}, sort=[("timestamp", -1)])
    date_param = (request.args.get("date") or "").strip()
    selected = _parse_day_str(date_param)
    if not selected:
        if latest and isinstance(latest.get("timestamp"), datetime):
            latest_ts = latest.get("timestamp")
            selected = datetime(latest_ts.year, latest_ts.month, latest_ts.day)
        else:
            today = datetime.utcnow()
            selected = datetime(today.year, today.month, today.day)

    start_dt = datetime(selected.year, selected.month, selected.day)
    end_dt = start_dt + timedelta(days=1)

    rows = list(
        login_logs_col.aggregate(
            [
                {"$match": {"user_id": uid, "timestamp": {"$gte": start_dt, "$lt": end_dt}}},
                {"$group": {"_id": {"$hour": "$timestamp"}, "count": {"$sum": 1}}},
            ]
        )
    )
    hourly_map = {int(r.get("_id")): int(r.get("count") or 0) for r in rows if r.get("_id") is not None}
    values = [hourly_map.get(h, 0) for h in range(24)]

    total_logins = sum(values)
    first_doc = login_logs_col.find_one(
        {"user_id": uid, "timestamp": {"$gte": start_dt, "$lt": end_dt}},
        sort=[("timestamp", 1)],
    )
    last_doc = login_logs_col.find_one(
        {"user_id": uid, "timestamp": {"$gte": start_dt, "$lt": end_dt}},
        sort=[("timestamp", -1)],
    )
    unique_ips = len(login_logs_col.distinct("ip", {"user_id": uid, "timestamp": {"$gte": start_dt, "$lt": end_dt}}))

    return jsonify(
        {
            "ok": True,
            "date": start_dt.strftime("%Y-%m-%d"),
            "labels": [f"{h:02d}:00" for h in range(24)],
            "values": values,
            "kpis": {
                "total_logins": int(total_logins),
                "first_login": first_doc.get("timestamp").isoformat() if first_doc and isinstance(first_doc.get("timestamp"), datetime) else "",
                "last_login": last_doc.get("timestamp").isoformat() if last_doc and isinstance(last_doc.get("timestamp"), datetime) else "",
                "unique_ips": int(unique_ips or 0),
            },
        }
    )


@exec_users_bp.post("/executive/users/<user_id>/toggle_status")
def executive_user_toggle_status(user_id: str):
    if not _exec_users_write_guard():
        return redirect(url_for("login.login"))
    if not _safe_origin():
        return "Forbidden", 403

    lookup_id = _coerce_object_id(user_id)
    user_doc = users_col.find_one({"_id": lookup_id})
    if not user_doc and ObjectId.is_valid(user_id):
        user_doc = users_col.find_one({"_id": str(user_id)})
    if not user_doc:
        flash("User not found.", "error")
        return redirect(url_for("executive_users.executive_users_list"))

    current = _normalize_status(user_doc.get("status") or user_doc.get("employment_status"))
    new_status = "Not Active" if current == "Active" else "Active"
    users_col.update_one({"_id": user_doc.get("_id")}, {"$set": {"status": new_status}})
    log_activity(
        action="user.status_toggled",
        action_label="Toggled User Status",
        entity_type="user",
        entity_id=str(user_doc.get("_id")),
        meta={"status": new_status},
        req=request,
    )
    flash(f"User status updated to {new_status}.", "success")
    next_url = _safe_next_url(request.form.get("next") or request.args.get("next"))
    if next_url:
        return redirect(next_url)
    return redirect(url_for("executive_users.executive_users_list"))


@exec_users_bp.route("/executive/users/new", methods=["GET", "POST"])
def executive_user_create():
    if not _exec_users_write_guard():
        return redirect(url_for("login.login"))

    allowed_roles = ["admin", "executive", "manager", "inventory", "hr", "accounting"]

    if request.method == "POST":
        if not _safe_origin():
            return "Forbidden", 403
        username = (request.form.get("username") or "").strip()
        password = request.form.get("password") or ""
        role = (request.form.get("role") or "").strip().lower()

        if not username:
            flash("Username is required.", "error")
            return redirect(url_for("executive_users.executive_user_create"))
        if not password:
            flash("Password is required.", "error")
            return redirect(url_for("executive_users.executive_user_create"))
        if role not in allowed_roles:
            flash("Invalid role selected.", "error")
            return redirect(url_for("executive_users.executive_user_create"))
        if users_col.find_one({"username": username}):
            flash("Username already exists.", "error")
            return redirect(url_for("executive_users.executive_user_create"))

        name = (request.form.get("name") or "").strip()
        phone = (request.form.get("phone") or "").strip()
        email = (request.form.get("email") or "").strip()
        gender = (request.form.get("gender") or "").strip()
        branch = (request.form.get("branch") or "").strip()
        position = (request.form.get("position") or "").strip()
        location = (request.form.get("location") or "").strip()
        start_date = (request.form.get("start_date") or "").strip()
        status = (request.form.get("status") or "Active").strip() or "Active"
        image_url = (request.form.get("image_url") or "").strip()
        cf_image_id = (request.form.get("image_id") or "").strip()

        try:
            new_hash = bcrypt_lib.hashpw(password.encode("utf-8"), bcrypt_lib.gensalt(rounds=12)).decode("utf-8")
        except Exception:
            flash("Failed to hash password.", "error")
            return redirect(url_for("executive_users.executive_user_create"))

        now = datetime.utcnow()
        user_doc = {
            "username": username,
            "password": new_hash,
            "role": role,
            "name": name,
            "phone": phone,
            "email": email,
            "gender": gender,
            "branch": branch,
            "position": position,
            "location": location,
            "start_date": start_date,
            "image_url": image_url or None,
            "cf_image_id": cf_image_id or None,
            "status": status or "Active",
            "date_registered": now,
            "updated_at": now,
        }
        res = users_col.insert_one(user_doc)

        log_activity(
            action="user.created",
            action_label="Created User",
            entity_type="user",
            entity_id=str(res.inserted_id),
            meta={"username": username, "role": role},
            req=request,
        )

        flash("User created successfully.", "success")
        return redirect(url_for("executive_users.executive_users_list"))

    return render_template(
        "executive/users_create.html",
        roles=allowed_roles,
        default_profile_image=DEFAULT_PROFILE_IMAGE_URL,
    )


@exec_users_bp.post("/executive/users/<user_id>/update")
def executive_user_update(user_id: str):
    if not _exec_users_write_guard():
        return redirect(url_for("login.login"))
    if not _safe_origin():
        return "Forbidden", 403

    lookup_id = _coerce_object_id(user_id)
    user_doc = users_col.find_one({"_id": lookup_id})
    if not user_doc and ObjectId.is_valid(user_id):
        user_doc = users_col.find_one({"_id": str(user_id)})
    if not user_doc:
        flash("User not found.", "error")
        return redirect(url_for("executive_users.executive_users_list"))

    allowed_roles = {"admin", "executive", "manager", "inventory", "hr", "accounting"}
    role = (request.form.get("role") or user_doc.get("role") or "").strip().lower()
    if role not in allowed_roles:
        flash("Invalid role selected.", "error")
        return redirect(url_for("executive_users.executive_user_profile", user_id=user_id))

    update = {
        "name": (request.form.get("name") or "").strip(),
        "phone": (request.form.get("phone") or "").strip(),
        "email": (request.form.get("email") or "").strip(),
        "gender": (request.form.get("gender") or "").strip(),
        "branch": (request.form.get("branch") or "").strip(),
        "position": (request.form.get("position") or "").strip(),
        "location": (request.form.get("location") or "").strip(),
        "start_date": (request.form.get("start_date") or "").strip(),
        "status": (request.form.get("status") or user_doc.get("status") or "Active").strip(),
        "role": role,
        "image_url": (request.form.get("image_url") or user_doc.get("image_url") or "").strip() or None,
        "cf_image_id": (request.form.get("image_id") or user_doc.get("cf_image_id") or "").strip() or None,
        "updated_at": datetime.utcnow(),
    }

    users_col.update_one({"_id": user_doc.get("_id")}, {"$set": update})
    log_activity(
        action="user.updated",
        action_label="Updated User",
        entity_type="user",
        entity_id=str(user_doc.get("_id")),
        meta={"username": user_doc.get("username"), "role": role},
        req=request,
    )
    flash("User updated successfully.", "success")
    return redirect(url_for("executive_users.executive_user_profile", user_id=user_id))


@exec_users_bp.post("/executive/users/<user_id>/change_password")
def executive_user_change_password(user_id: str):
    if not _exec_users_write_guard():
        return redirect(url_for("login.login"))
    if not _safe_origin():
        return "Forbidden", 403

    lookup_id = _coerce_object_id(user_id)
    user_doc = users_col.find_one({"_id": lookup_id})
    if not user_doc and ObjectId.is_valid(user_id):
        user_doc = users_col.find_one({"_id": str(user_id)})
    if not user_doc:
        flash("User not found.", "error")
        return redirect(url_for("executive_users.executive_users_list"))

    new_password = request.form.get("new_password") or ""
    confirm_password = request.form.get("confirm_password") or ""
    if not new_password or new_password != confirm_password:
        flash("Password confirmation does not match.", "error")
        return redirect(url_for("executive_users.executive_user_profile", user_id=user_id))

    new_hash = bcrypt_lib.hashpw(new_password.encode("utf-8"), bcrypt_lib.gensalt(rounds=12)).decode("utf-8")
    users_col.update_one(
        {"_id": user_doc.get("_id")},
        {"$set": {"password": new_hash, "updated_at": datetime.utcnow()}},
    )
    log_activity(
        action="user.password_changed",
        action_label="Changed User Password",
        entity_type="user",
        entity_id=str(user_doc.get("_id")),
        meta={"username": user_doc.get("username")},
        req=request,
    )
    flash("Password updated successfully.", "success")
    return redirect(url_for("executive_users.executive_user_profile", user_id=user_id))


@exec_users_bp.post("/executive/users/<user_id>/delete")
def executive_user_delete(user_id: str):
    if not _exec_users_write_guard():
        return redirect(url_for("login.login"))
    if not _safe_origin():
        return "Forbidden", 403

    lookup_id = _coerce_object_id(user_id)
    user_doc = users_col.find_one({"_id": lookup_id})
    if not user_doc and ObjectId.is_valid(user_id):
        user_doc = users_col.find_one({"_id": str(user_id)})
    if not user_doc:
        flash("User not found.", "error")
        return redirect(url_for("executive_users.executive_users_list"))

    ident = get_current_identity()
    deleted_users_col.insert_one(
        {
            "type": "user",
            "deleted_at": datetime.utcnow(),
            "deleted_by": {"user_id": ident.get("user_id"), "username": ident.get("name"), "role": ident.get("role")},
            "original": user_doc,
            "original_user_id": str(user_doc.get("_id")),
        }
    )
    users_col.delete_one({"_id": user_doc.get("_id")})

    log_activity(
        action="user.deleted",
        action_label="Deleted User",
        entity_type="user",
        entity_id=str(user_doc.get("_id")),
        meta={"username": user_doc.get("username")},
        req=request,
    )
    flash("User deleted.", "success")
    return redirect(url_for("executive_users.executive_users_list"))


@exec_users_bp.get("/executive/users/deleted")
def executive_users_deleted():
    if not _exec_users_write_guard():
        return redirect(url_for("login.login"))

    rows = list(deleted_users_col.find({"type": "user"}).sort("deleted_at", -1).limit(300))
    out = []
    for d in rows:
        original = d.get("original") or {}
        deleted_at = d.get("deleted_at")
        out.append(
            {
                "id": str(d.get("_id")),
                "name": original.get("name") or "Unknown",
                "username": original.get("username") or "",
                "role": original.get("role") or "",
                "branch": original.get("branch") or "",
                "deleted_at": deleted_at.isoformat() if isinstance(deleted_at, datetime) else "",
                "deleted_by": (d.get("deleted_by") or {}).get("username") or "",
            }
        )
    return render_template("executive/users_deleted.html", rows=out)


@exec_users_bp.post("/executive/users/deleted/<deleted_id>/restore")
def executive_users_restore(deleted_id: str):
    if not _exec_users_write_guard():
        return redirect(url_for("login.login"))
    if not _safe_origin():
        return "Forbidden", 403

    del_doc = deleted_users_col.find_one({"_id": _coerce_object_id(deleted_id), "type": "user"})
    if not del_doc and ObjectId.is_valid(deleted_id):
        del_doc = deleted_users_col.find_one({"_id": ObjectId(deleted_id), "type": "user"})
    if not del_doc:
        flash("Deleted user not found.", "error")
        return redirect(url_for("executive_users.executive_users_deleted"))

    original = del_doc.get("original") or {}
    username = original.get("username") or ""
    if username and users_col.find_one({"username": username}):
        flash("Cannot restore: username already exists.", "error")
        return redirect(url_for("executive_users.executive_users_deleted"))

    original.pop("_id", None)
    original["updated_at"] = datetime.utcnow()
    res = users_col.insert_one(original)
    deleted_users_col.delete_one({"_id": del_doc.get("_id")})

    log_activity(
        action="user.restored",
        action_label="Restored User",
        entity_type="user",
        entity_id=str(res.inserted_id),
        meta={"username": username},
        req=request,
    )
    flash("User restored.", "success")
    return redirect(url_for("executive_users.executive_users_deleted"))


@exec_users_bp.post("/executive/users/deleted/<deleted_id>/purge")
def executive_users_purge(deleted_id: str):
    if not _exec_users_write_guard():
        return redirect(url_for("login.login"))
    if not _safe_origin():
        return "Forbidden", 403

    deleted_users_col.delete_one({"_id": _coerce_object_id(deleted_id), "type": "user"})
    flash("Deleted record purged.", "success")
    return redirect(url_for("executive_users.executive_users_deleted"))


@exec_users_bp.get("/executive/users/<user_id>/activities/summary")
def executive_user_activities_summary(user_id: str):
    if not _exec_users_guard():
        return jsonify({"ok": False, "message": "Not authorized"}), 401

    lookup_id = _coerce_object_id(user_id)
    user_doc = users_col.find_one({"_id": lookup_id})
    if not user_doc and ObjectId.is_valid(user_id):
        user_doc = users_col.find_one({"_id": str(user_id)})
    if not user_doc:
        return jsonify({"ok": False, "message": "User not found"}), 404

    uid = str(user_doc.get("_id"))
    range_key = (request.args.get("range") or "last_30").strip().lower()
    start_dt, end_dt, labels = _range_from_key(range_key)

    trend_rows = list(
        activity_logs_col.aggregate(
            [
                {"$match": {"user_id": uid, "timestamp": {"$gte": start_dt, "$lt": end_dt}}},
                {"$group": {"_id": "$day", "count": {"$sum": 1}}},
            ]
        )
    )
    trend_map = {row.get("_id"): int(row.get("count") or 0) for row in trend_rows}
    trend_values = [trend_map.get(label, 0) for label in labels]

    top_actions = list(
        activity_logs_col.aggregate(
            [
                {"$match": {"user_id": uid, "timestamp": {"$gte": start_dt, "$lt": end_dt}}},
                {"$group": {"_id": {"action": "$action", "label": "$action_label"}, "count": {"$sum": 1}}},
                {"$sort": {"count": -1}},
                {"$limit": 8},
            ]
        )
    )
    top_actions_list = [
        {
            "action": (row.get("_id") or {}).get("action"),
            "label": (row.get("_id") or {}).get("label") or (row.get("_id") or {}).get("action"),
            "count": int(row.get("count") or 0),
        }
        for row in top_actions
    ]

    totals = {
        "total": int(sum(trend_values)),
        "days": len(labels),
        "avg_per_day": round((sum(trend_values) / len(labels)), 2) if labels else 0,
    }

    return jsonify(
        {
            "ok": True,
            "trend": {"labels": labels, "values": trend_values},
            "top_actions": top_actions_list,
            "totals": totals,
        }
    )


@exec_users_bp.get("/executive/users/<user_id>/activities/by_day")
def executive_user_activities_by_day(user_id: str):
    if not _exec_users_guard():
        return jsonify({"ok": False, "message": "Not authorized"}), 401

    lookup_id = _coerce_object_id(user_id)
    user_doc = users_col.find_one({"_id": lookup_id})
    if not user_doc and ObjectId.is_valid(user_id):
        user_doc = users_col.find_one({"_id": str(user_id)})
    if not user_doc:
        return jsonify({"ok": False, "message": "User not found"}), 404

    uid = str(user_doc.get("_id"))
    date_param = (request.args.get("date") or "").strip()
    selected = _parse_day_str(date_param)
    if not selected:
        today = datetime.utcnow()
        selected = datetime(today.year, today.month, today.day)
    start_dt = datetime(selected.year, selected.month, selected.day)
    end_dt = start_dt + timedelta(days=1)

    rows = list(
        activity_logs_col.aggregate(
            [
                {"$match": {"user_id": uid, "timestamp": {"$gte": start_dt, "$lt": end_dt}}},
                {"$group": {"_id": {"$hour": "$timestamp"}, "count": {"$sum": 1}}},
            ]
        )
    )
    hourly_map = {int(r.get("_id")): int(r.get("count") or 0) for r in rows if r.get("_id") is not None}
    values = [hourly_map.get(h, 0) for h in range(24)]

    total_actions = sum(values)
    first_doc = activity_logs_col.find_one(
        {"user_id": uid, "timestamp": {"$gte": start_dt, "$lt": end_dt}},
        sort=[("timestamp", 1)],
    )
    last_doc = activity_logs_col.find_one(
        {"user_id": uid, "timestamp": {"$gte": start_dt, "$lt": end_dt}},
        sort=[("timestamp", -1)],
    )
    unique_ips = len(activity_logs_col.distinct("ip", {"user_id": uid, "timestamp": {"$gte": start_dt, "$lt": end_dt}}))

    return jsonify(
        {
            "ok": True,
            "date": start_dt.strftime("%Y-%m-%d"),
            "labels": [f"{h:02d}:00" for h in range(24)],
            "values": values,
            "kpis": {
                "total_actions": int(total_actions),
                "first_action": first_doc.get("timestamp").isoformat() if first_doc and isinstance(first_doc.get("timestamp"), datetime) else "",
                "last_action": last_doc.get("timestamp").isoformat() if last_doc and isinstance(last_doc.get("timestamp"), datetime) else "",
                "unique_ips": int(unique_ips or 0),
            },
        }
    )


@exec_users_bp.get("/executive/users/<user_id>/activities/list")
def executive_user_activities_list(user_id: str):
    if not _exec_users_guard():
        return jsonify({"ok": False, "message": "Not authorized"}), 401

    lookup_id = _coerce_object_id(user_id)
    user_doc = users_col.find_one({"_id": lookup_id})
    if not user_doc and ObjectId.is_valid(user_id):
        user_doc = users_col.find_one({"_id": str(user_id)})
    if not user_doc:
        return jsonify({"ok": False, "message": "User not found"}), 404

    uid = str(user_doc.get("_id"))
    page = max(int(request.args.get("page") or 1), 1)
    page_size = min(max(int(request.args.get("page_size") or 25), 10), 100)
    range_key = (request.args.get("range") or "").strip().lower()
    date_param = (request.args.get("date") or "").strip()
    action = (request.args.get("action") or "").strip()
    entity_type = (request.args.get("entity_type") or "").strip()
    role = (request.args.get("role") or "").strip().lower()

    q: Dict[str, Any] = {"user_id": uid}
    if action:
        q["action"] = action
    if entity_type:
        q["entity_type"] = entity_type
    if role:
        q["role"] = role

    if date_param:
        selected = _parse_day_str(date_param)
        if selected:
            start_dt = datetime(selected.year, selected.month, selected.day)
            end_dt = start_dt + timedelta(days=1)
            q["timestamp"] = {"$gte": start_dt, "$lt": end_dt}
    elif range_key:
        start_dt, end_dt, _ = _range_from_key(range_key)
        q["timestamp"] = {"$gte": start_dt, "$lt": end_dt}

    total = activity_logs_col.count_documents(q)
    rows = list(
        activity_logs_col.find(q)
        .sort("timestamp", -1)
        .skip((page - 1) * page_size)
        .limit(page_size)
    )

    out = []
    for r in rows:
        ts = r.get("timestamp")
        ip_loc = (r.get("meta") or {}).get("ip_location") or {}
        out.append(
            {
                "id": str(r.get("_id")),
                "timestamp": ts.isoformat() if isinstance(ts, datetime) else "",
                "action": r.get("action") or "",
                "action_label": r.get("action_label") or "",
                "entity_type": r.get("entity_type") or "",
                "entity_id": r.get("entity_id") or "",
                "meta": r.get("meta") or {},
                "ip": r.get("ip") or "",
                "city": ip_loc.get("city") or "",
                "country": ip_loc.get("country") or "",
            }
        )

    return jsonify(
        {
            "ok": True,
            "rows": out,
            "pagination": {"page": page, "page_size": page_size, "total": int(total or 0)},
        }
    )


@exec_users_bp.get("/executive/users/<user_id>/activities/export.csv")
def executive_user_activities_export(user_id: str):
    if not _exec_users_guard():
        return redirect(url_for("login.login"))

    lookup_id = _coerce_object_id(user_id)
    user_doc = users_col.find_one({"_id": lookup_id})
    if not user_doc and ObjectId.is_valid(user_id):
        user_doc = users_col.find_one({"_id": str(user_id)})
    if not user_doc:
        flash("User not found.", "error")
        return redirect(url_for("executive_users.executive_users_list"))

    uid = str(user_doc.get("_id"))
    range_key = (request.args.get("range") or "").strip().lower()
    date_param = (request.args.get("date") or "").strip()
    q: Dict[str, Any] = {"user_id": uid}

    if date_param:
        selected = _parse_day_str(date_param)
        if selected:
            start_dt = datetime(selected.year, selected.month, selected.day)
            end_dt = start_dt + timedelta(days=1)
            q["timestamp"] = {"$gte": start_dt, "$lt": end_dt}
    elif range_key:
        start_dt, end_dt, _ = _range_from_key(range_key)
        q["timestamp"] = {"$gte": start_dt, "$lt": end_dt}

    rows = list(activity_logs_col.find(q).sort("timestamp", -1))

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(
        [
            "timestamp",
            "action",
            "action_label",
            "entity_type",
            "entity_id",
            "role",
            "ip",
            "country",
            "city",
        ]
    )
    for r in rows:
        meta = r.get("meta") or {}
        ip_loc = meta.get("ip_location") or {}
        ts = r.get("timestamp")
        writer.writerow(
            [
                ts.isoformat() if isinstance(ts, datetime) else "",
                r.get("action") or "",
                r.get("action_label") or "",
                r.get("entity_type") or "",
                r.get("entity_id") or "",
                r.get("role") or "",
                r.get("ip") or "",
                ip_loc.get("country") or "",
                ip_loc.get("city") or "",
            ]
        )

    csv_data = output.getvalue()
    output.close()
    filename = f"user_{uid}_activities.csv"
    return Response(
        csv_data,
        mimetype="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


@exec_users_bp.get("/executive/users/<user_id>/export_logs.csv")
def executive_user_export_logs(user_id: str):
    if not _exec_users_guard():
        return redirect(url_for("login.login"))

    lookup_id = _coerce_object_id(user_id)
    user_doc = users_col.find_one({"_id": lookup_id})
    if not user_doc and ObjectId.is_valid(user_id):
        user_doc = users_col.find_one({"_id": str(user_id)})
    if not user_doc:
        flash("User not found.", "error")
        return redirect(url_for("executive_users.executive_users_list"))

    uid = str(user_doc.get("_id"))
    rows = list(login_logs_col.find({"user_id": uid}).sort("timestamp", -1))

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(
        [
            "timestamp",
            "ip",
            "username",
            "role",
            "browser",
            "os",
            "is_mobile",
            "country",
            "region",
            "city",
            "isp",
            "location_available",
            "geo_lat",
            "geo_lng",
            "geo_accuracy_m",
        ]
    )
    for log in rows:
        device = log.get("device") or {}
        ip_loc = log.get("ip_location") or {}
        geo = log.get("geo") or {}
        writer.writerow(
            [
                log.get("timestamp").isoformat() if isinstance(log.get("timestamp"), datetime) else "",
                log.get("ip") or "",
                log.get("username") or "",
                log.get("role") or "",
                device.get("browser") or "",
                device.get("os") or "",
                device.get("is_mobile") or False,
                ip_loc.get("country") or "",
                ip_loc.get("region") or "",
                ip_loc.get("city") or "",
                ip_loc.get("isp") or "",
                bool(log.get("location_available")),
                geo.get("lat"),
                geo.get("lng"),
                geo.get("accuracy_m"),
            ]
        )

    csv_data = output.getvalue()
    output.close()
    filename = f"user_{uid}_login_logs.csv"
    return Response(
        csv_data,
        mimetype="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )

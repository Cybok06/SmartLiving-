# hr_backend/hr_reminders.py
from datetime import datetime
from typing import Any, Dict, List, Optional

from flask import jsonify, request, render_template
from bson import ObjectId

from db import db
from hr_backend.hr_dashboard import hr_bp, _hr_access_guard

users_col = db["users"]
reminders_col = db["hr_reminders"]


# -------------------------------
# Helpers
# -------------------------------
def _now() -> datetime:
    return datetime.utcnow()


def _safe_oid(s: str) -> Optional[ObjectId]:
    try:
        return ObjectId(str(s))
    except Exception:
        return None


def _dt_to_str(dt: Any) -> str:
    if isinstance(dt, datetime):
        return dt.isoformat()
    return str(dt) if dt else ""


def _parse_due(s: str) -> Optional[datetime]:
    """
    Accepts:
    - 'YYYY-MM-DD'
    - 'YYYY-MM-DDTHH:MM' (from <input type="datetime-local">)
    """
    if not s:
        return None
    s = str(s).strip()
    try:
        if "T" in s:
            return datetime.strptime(s[:16], "%Y-%m-%dT%H:%M")
        return datetime.strptime(s[:10], "%Y-%m-%d")
    except Exception:
        return None


def _audit(action: str, by: str = "HR", note: str = "") -> Dict[str, Any]:
    return {"action": action, "by": by, "at": _now(), "note": note}


def _serialize_reminder(doc: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "id": str(doc.get("_id")),
        "title": doc.get("title") or "",
        "message": doc.get("message") or "",
        "status": doc.get("status") or "Open",               # Open | Done | Snoozed | Cancelled
        "priority": doc.get("priority") or "Normal",         # Low | Normal | High | Critical
        "channel": doc.get("channel") or "In-App",           # In-App (future: WhatsApp/Email/SMS)
        "due_at": _dt_to_str(doc.get("due_at")),
        "snooze_until": _dt_to_str(doc.get("snooze_until")),
        "created_at": _dt_to_str(doc.get("created_at")),
        "updated_at": _dt_to_str(doc.get("updated_at")),
        "created_by": doc.get("created_by") or "HR",

        "assignees": doc.get("assignees") or [],             # [{user_id,name,branch,role}]
        "mentions": doc.get("mentions") or [],               # [{user_id,name}]
        "tags": doc.get("tags") or [],                       # ["Payroll","Training"]
        "audit": doc.get("audit") or [],                     # [{action,by,at,note}]
    }


def _is_overdue(doc: Dict[str, Any], now: datetime) -> bool:
    """
    Overdue if:
      - status is Open
      - due_at exists and is in the past
      - and not currently snoozed into the future
    """
    status = doc.get("status") or "Open"
    if status != "Open":
        return False

    due_at = doc.get("due_at")
    if not isinstance(due_at, datetime):
        return False

    # If snoozed until in future, do not treat as overdue
    snooze_until = doc.get("snooze_until")
    if isinstance(snooze_until, datetime) and snooze_until > now:
        return False

    return due_at < now


# -------------------------------
# Page
# -------------------------------
@hr_bp.route("/reminders", methods=["GET"], endpoint="reminders_page")
def reminders_page():
    if not _hr_access_guard():
        return render_template("unauthorized.html"), 401
    return render_template("hr_pages/hr_reminders_page.html", active_page="reminders")


# -------------------------------
# Employees list (for mentions + assignees)
# -------------------------------
@hr_bp.route("/reminders/employees_list", methods=["GET"], endpoint="reminders_employees_list")
def reminders_employees_list():
    if not _hr_access_guard():
        return jsonify(ok=False, message="Unauthorized"), 401

    cursor = users_col.find(
        {"is_deleted": {"$ne": True}},
        {
            "name": 1, "role": 1, "position": 1,
            "branch": 1, "store_name": 1, "store": 1,
            "employee_code": 1, "staff_id": 1
        },
    ).sort("name", 1)

    employees: List[Dict[str, Any]] = []
    branches = set()
    roles = set()

    for u in cursor:
        br = u.get("branch") or u.get("store_name") or u.get("store") or ""
        rl = u.get("role") or u.get("position") or ""
        if br:
            branches.add(br)
        if rl:
            roles.add(rl)

        employees.append({
            "id": str(u["_id"]),
            "name": u.get("name") or "",
            "branch": br,
            "role": rl,
            "employee_code": u.get("employee_code") or u.get("staff_id") or "",
        })

    return jsonify(
        ok=True,
        employees=employees,
        branches=sorted(list(branches)),
        roles=sorted(list(roles)),
    )


# -------------------------------
# Create reminder
# -------------------------------
@hr_bp.route("/reminders/create", methods=["POST"], endpoint="reminders_create")
def reminders_create():
    if not _hr_access_guard():
        return jsonify(ok=False, message="Unauthorized"), 401

    payload = request.get_json(silent=True) or {}

    title = (payload.get("title") or "").strip()
    message = (payload.get("message") or "").strip()
    priority = (payload.get("priority") or "Normal").strip()
    channel = (payload.get("channel") or "In-App").strip()
    due_raw = (payload.get("due_at") or "").strip()

    tags = payload.get("tags") or []
    assignee_ids = payload.get("assignee_ids") or []
    mention_ids = payload.get("mention_ids") or []

    if not title:
        return jsonify(ok=False, message="Title is required."), 400
    if not message:
        return jsonify(ok=False, message="Message is required."), 400

    allowed_priority = ("Low", "Normal", "High", "Critical")
    if priority not in allowed_priority:
        priority = "Normal"

    due_at = _parse_due(due_raw)
    if due_raw and not due_at:
        return jsonify(ok=False, message="Invalid due date/time format."), 400

    if not isinstance(tags, list):
        tags = []
    tags = [str(t).strip() for t in tags if str(t).strip()][:10]

    if not isinstance(assignee_ids, list):
        assignee_ids = []
    if not isinstance(mention_ids, list):
        mention_ids = []

    # resolve assignees + mentions
    assignee_oids = [oid for oid in (_safe_oid(x) for x in assignee_ids) if oid]
    mention_oids = [oid for oid in (_safe_oid(x) for x in mention_ids) if oid]

    needed = list(set(assignee_oids + mention_oids))
    users = list(users_col.find(
        {"_id": {"$in": needed}},
        {"name": 1, "branch": 1, "store_name": 1, "store": 1, "role": 1, "position": 1}
    ))
    u_map = {str(u["_id"]): u for u in users}

    assignees = []
    for sid in assignee_ids:
        u = u_map.get(str(sid))
        if not u:
            continue
        assignees.append({
            "user_id": str(sid),
            "name": u.get("name") or "",
            "branch": u.get("branch") or u.get("store_name") or u.get("store") or "",
            "role": u.get("role") or u.get("position") or "",
        })

    mentions = []
    for mid in mention_ids:
        u = u_map.get(str(mid))
        if not u:
            continue
        mentions.append({"user_id": str(mid), "name": u.get("name") or ""})

    now = _now()
    doc: Dict[str, Any] = {
        "title": title,
        "message": message,
        "priority": priority,
        "channel": channel or "In-App",
        "status": "Open",

        "due_at": due_at,
        "snooze_until": None,

        "assignees": assignees,
        "mentions": mentions,
        "tags": tags,

        "created_by": "HR",
        "created_at": now,
        "updated_at": now,
        "is_deleted": False,

        "audit": [_audit("Reminder created", "HR")],
    }

    ins = reminders_col.insert_one(doc)
    return jsonify(ok=True, message="Reminder created.", reminder_id=str(ins.inserted_id))


# -------------------------------
# List reminders with filters
# -------------------------------
@hr_bp.route("/reminders/list", methods=["GET"], endpoint="reminders_list")
def reminders_list():
    if not _hr_access_guard():
        return jsonify(ok=False, message="Unauthorized"), 401

    q = (request.args.get("q") or "").strip().lower()
    status = (request.args.get("status") or "").strip()
    priority = (request.args.get("priority") or "").strip()
    branch = (request.args.get("branch") or "").strip()

    due_from = _parse_due(request.args.get("from") or "")
    due_to = _parse_due(request.args.get("to") or "")

    try:
        limit_i = max(10, min(1000, int(request.args.get("limit") or "200")))
    except Exception:
        limit_i = 200

    base = {"is_deleted": {"$ne": True}}
    cursor = reminders_col.find(base).sort("created_at", -1).limit(limit_i)

    rows: List[Dict[str, Any]] = []
    kpis = {"total": 0, "open": 0, "done": 0, "overdue": 0}

    now = _now()

    for doc in cursor:
        row = _serialize_reminder(doc)

        # branch filter (any assignee in branch)
        if branch:
            ok_branch = any((a.get("branch") or "") == branch for a in (row.get("assignees") or []))
            if not ok_branch:
                continue

        if status and row.get("status") != status:
            continue
        if priority and row.get("priority") != priority:
            continue

        # due range filter
        if due_from or due_to:
            d = doc.get("due_at")
            if isinstance(d, datetime):
                if due_from and d < due_from:
                    continue
                if due_to and d > due_to:
                    continue
            else:
                # if filtering by due and this has no due date -> hide it
                continue

        # search
        if q:
            hay = " ".join([
                row.get("title", ""),
                row.get("message", ""),
                row.get("priority", ""),
                row.get("status", ""),
                " ".join([a.get("name", "") for a in (row.get("assignees") or [])]),
                " ".join([m.get("name", "") for m in (row.get("mentions") or [])]),
                " ".join(row.get("tags") or []),
            ]).lower()
            if q not in hay:
                continue

        # KPIs
        kpis["total"] += 1
        if row.get("status") == "Open":
            kpis["open"] += 1
        if row.get("status") == "Done":
            kpis["done"] += 1
        if _is_overdue(doc, now):
            kpis["overdue"] += 1

        rows.append(row)

    meta = {
        "statuses": ["Open", "Done", "Snoozed", "Cancelled"],
        "priorities": ["Low", "Normal", "High", "Critical"],
    }
    return jsonify(ok=True, reminders=rows, kpis=kpis, meta=meta)


# -------------------------------
# Update status (and optional note)
# -------------------------------
@hr_bp.route("/reminders/<reminder_id>/status", methods=["POST"], endpoint="reminders_update_status")
def reminders_update_status(reminder_id):
    if not _hr_access_guard():
        return jsonify(ok=False, message="Unauthorized"), 401

    oid = _safe_oid(reminder_id)
    if not oid:
        return jsonify(ok=False, message="Invalid reminder ID."), 400

    payload = request.get_json(silent=True) or {}
    status = (payload.get("status") or "").strip()
    note = (payload.get("note") or "").strip()
    snooze_until_raw = (payload.get("snooze_until") or "").strip()

    allowed = ("Open", "Done", "Snoozed", "Cancelled")
    if status not in allowed:
        return jsonify(ok=False, message="Invalid status."), 400

    snooze_until = _parse_due(snooze_until_raw) if snooze_until_raw else None
    if snooze_until_raw and not snooze_until:
        return jsonify(ok=False, message="Invalid snooze date/time."), 400

    now = _now()
    upd = {
        "$set": {
            "status": status,
            "snooze_until": snooze_until if status == "Snoozed" else None,
            "updated_at": now,
        },
        "$push": {
            "audit": {
                "$each": [_audit(f"Status → {status}", "HR", note)],
                "$position": 0,
            }
        }
    }

    res = reminders_col.update_one({"_id": oid}, upd)
    if not res.matched_count:
        return jsonify(ok=False, message="Reminder not found."), 404

    return jsonify(ok=True, message="Status updated.")


# -------------------------------
# Soft delete
# -------------------------------
@hr_bp.route("/reminders/<reminder_id>/delete", methods=["POST"], endpoint="reminders_delete")
def reminders_delete(reminder_id):
    if not _hr_access_guard():
        return jsonify(ok=False, message="Unauthorized"), 401

    oid = _safe_oid(reminder_id)
    if not oid:
        return jsonify(ok=False, message="Invalid reminder ID."), 400

    now = _now()
    res = reminders_col.update_one(
        {"_id": oid},
        {
            "$set": {"is_deleted": True, "updated_at": now},
            "$push": {"audit": {"$each": [_audit("Reminder deleted", "HR")], "$position": 0}},
        },
    )
    if not res.matched_count:
        return jsonify(ok=False, message="Reminder not found."), 404

    return jsonify(ok=True, message="Reminder deleted.")

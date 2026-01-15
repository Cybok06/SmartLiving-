from datetime import datetime
from functools import wraps
import json
import traceback
from typing import Any, Dict, List

import requests
from bson import ObjectId
from flask import Blueprint, abort, jsonify, redirect, render_template, request, session, url_for
from flask_login import current_user
from werkzeug.utils import secure_filename

from db import db
from services.activity_audit import audit_action

issues_bp = Blueprint("issues", __name__, url_prefix="/issues")

issues_col = db["issues"]
users_col = db["users"]
images_col = db["images"]

# -------------------------------
# Cloudflare Images
# -------------------------------
CF_ACCOUNT_ID = "63e6f91eec9591f77699c4b434ab44c6"
CF_IMAGES_TOKEN = "Brz0BEfl_GqEUjEghS2UEmLZhK39EUmMbZgu_hIo"
CF_HASH = "h9fmMoa1o2c2P55TcWJGOg"
DEFAULT_VARIANT = "public"
ALLOWED_EXTENSIONS = {"png", "jpg", "jpeg", "gif"}


def _now() -> datetime:
    return datetime.utcnow()


def _oid(val: str | None) -> ObjectId | None:
    if not val:
        return None
    try:
        return ObjectId(val)
    except Exception:
        return None


def _user_doc_by_id(user_id: str | None) -> Dict[str, Any] | None:
    if not user_id:
        return None
    oid = _oid(user_id)
    if oid:
        doc = users_col.find_one({"_id": oid})
        if doc:
            return doc
    return users_col.find_one({"_id": user_id})


def _allowed_file(filename: str) -> bool:
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS


def _wants_json() -> bool:
    if request.headers.get("X-Requested-With") == "XMLHttpRequest":
        return True
    accept = request.headers.get("Accept") or ""
    return "application/json" in accept or request.is_json


def _sanitize_attachments(items: Any) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    if not isinstance(items, list):
        return out
    for it in items:
        if not isinstance(it, dict):
            continue
        url = (it.get("url") or "").strip()
        if not url:
            continue
        out.append(
            {
                "url": url,
                "image_id": it.get("image_id") or "",
                "variant": it.get("variant") or "",
                "name": it.get("name") or "",
                "mimetype": it.get("mimetype") or "",
            }
        )
    return out


def _get_current_user() -> Dict[str, Any]:
    if getattr(current_user, "is_authenticated", False):
        user_id = str(getattr(current_user, "id", "") or "")
        role = (getattr(current_user, "role", "") or "").lower()
    else:
        role = ""
        user_id = ""
        if session.get("executive_id"):
            role = "executive"
            user_id = str(session.get("executive_id"))
        elif session.get("manager_id"):
            role = "manager"
            user_id = str(session.get("manager_id"))
        elif session.get("agent_id"):
            role = "agent"
            user_id = str(session.get("agent_id"))

    if not role or not user_id:
        return {}

    user = _user_doc_by_id(user_id) or {}
    manager_id = user.get("manager_id")
    manager_name = ""
    if manager_id:
        mgr = users_col.find_one({"_id": manager_id})
        if mgr:
            manager_name = mgr.get("name") or ""

    return {
        "id": user_id,
        "role": role,
        "name": user.get("name") or user.get("username") or "User",
        "branch_id": str(user.get("branch_id") or ""),
        "branch_name": user.get("branch") or "",
        "manager_id": str(manager_id) if manager_id else "",
        "manager_name": manager_name,
    }


def _require_user(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        user = _get_current_user()
        if not user:
            return redirect(url_for("login.login"))
        return fn(*args, **kwargs)

    return wrapper


def _dashboard_url(role: str) -> str:
    if role == "executive":
        return url_for("executive_dashboard.executive_dashboard")
    if role == "manager":
        return url_for("manager_dashboard.manager_dashboard_view")
    if role == "agent":
        return url_for("dashboard_agent.agent_dashboard")
    return url_for("login.login")


def _add_participant(participants: List[Dict[str, str]], user_id: str, role: str, name: str):
    for p in participants:
        if p.get("user_id") == user_id and p.get("role") == role:
            return
    participants.append({"user_id": user_id, "role": role, "name": name})


def _normalize_issue(doc: Dict[str, Any]) -> Dict[str, Any]:
    if not doc:
        return {}
    doc = dict(doc)
    doc["_id"] = str(doc.get("_id"))
    doc.setdefault("title", "Issue")
    doc.setdefault("priority", "MEDIUM")
    doc.setdefault("category", "Other")
    doc.setdefault("messages", [])
    doc.setdefault("participants", [])
    if not doc.get("assigned_to_role") and doc.get("to_role"):
        doc["assigned_to_role"] = doc.get("to_role")
    if not doc.get("assigned_to_id") and doc.get("to_manager_id"):
        doc["assigned_to_id"] = doc.get("to_manager_id")
    if not doc.get("creator_id") and doc.get("created_by"):
        doc["creator_id"] = doc.get("created_by")
    if not doc.get("creator_name") and doc.get("created_by_name"):
        doc["creator_name"] = doc.get("created_by_name")

    status_map = {
        "Under Review": "IN_PROGRESS",
        "Resolved": "RESOLVED",
    }
    status = (doc.get("status") or "OPEN")
    status = status_map.get(status, status)
    if isinstance(status, str):
        status = status.upper()
    doc["status"] = status
    messages = doc.get("messages") or []
    for m in messages:
        if "attachments" not in m:
            m["attachments"] = []
    doc["messages"] = messages
    return doc


def _can_view_issue(issue: Dict[str, Any], user: Dict[str, Any]) -> bool:
    if not issue or not user:
        return False

    user_id = user["id"]
    role = user["role"]

    if issue.get("creator_id") == user_id:
        return True

    for p in issue.get("participants", []):
        if p.get("user_id") == user_id and p.get("role") == role:
            return True

    if role == "executive" and issue.get("assigned_to_role") == "executive":
        return True

    if role == "manager":
        if issue.get("assigned_to_role") == "manager" and issue.get("assigned_to_id") == user_id:
            return True
        if issue.get("creator_role") == "agent" and issue.get("creator_manager_id") == user_id:
            return True

    return False


def _can_reply(issue: Dict[str, Any], user: Dict[str, Any]) -> bool:
    return _can_view_issue(issue, user)


def _can_update_status(issue: Dict[str, Any], user: Dict[str, Any], new_status: str) -> bool:
    if not issue or not user:
        return False
    role = user["role"]
    user_id = user["id"]

    if new_status == "CLOSED":
        return role == "executive" and issue.get("assigned_to_role") == "executive"

    if role == "executive" and issue.get("assigned_to_role") == "executive":
        return True

    if role == "manager" and issue.get("assigned_to_role") == "manager" and issue.get("assigned_to_id") == user_id:
        return True

    return False


def _issue_row_json(doc: Dict[str, Any]) -> Dict[str, Any]:
    last_dt = doc.get("last_message_at")
    return {
        "id": str(doc.get("_id")),
        "title": doc.get("title") or "Issue",
        "priority": doc.get("priority") or "MEDIUM",
        "status": doc.get("status") or "OPEN",
        "creator_name": doc.get("creator_name") or "",
        "assigned_to_name": doc.get("assigned_to_name") or (doc.get("assigned_to_role") or "").capitalize(),
        "branch_name": doc.get("branch_name") or "",
        "last_message_preview": doc.get("last_message_preview") or "",
        "last_message_at": last_dt.isoformat() if isinstance(last_dt, datetime) else "",
        "last_message_ago": time_ago(last_dt),
        "last_message_fmt": format_dt(last_dt),
    }


def _list_executives() -> List[Dict[str, str]]:
    rows = list(users_col.find({"role": "executive"}))
    out = []
    for r in rows:
        out.append({"id": str(r["_id"]), "name": r.get("name") or r.get("username") or "Executive"})
    return out


@issues_bp.app_template_filter("format_dt")
def format_dt(value):
    if isinstance(value, datetime):
        return value.strftime("%d %b %Y, %H:%M")
    return ""


@issues_bp.app_template_filter("time_ago")
def time_ago(value):
    if not isinstance(value, datetime):
        return ""
    delta = _now() - value
    seconds = int(delta.total_seconds())
    if seconds < 60:
        return "just now"
    minutes = seconds // 60
    if minutes < 60:
        return f"{minutes}m ago"
    hours = minutes // 60
    if hours < 24:
        return f"{hours}h ago"
    days = hours // 24
    return f"{days}d ago"


@issues_bp.route("/", methods=["GET"])
@_require_user
def issues_home():
    return redirect(url_for("issues.inbox"))


@issues_bp.route("/inbox", methods=["GET"])
@_require_user
def inbox():
    user = _get_current_user()
    role = user["role"]
    user_id = user["id"]

    tab = (request.args.get("tab") or "assigned").lower()
    status = (request.args.get("status") or "").strip()
    priority = (request.args.get("priority") or "").strip()
    category = (request.args.get("category") or "").strip()
    q = (request.args.get("q") or "").strip()
    start = (request.args.get("start") or "").strip()
    end = (request.args.get("end") or "").strip()

    page = int(request.args.get("page") or 1)
    page = page if page > 0 else 1
    per_page = 20
    skip = (page - 1) * per_page

    query: Dict[str, Any] = {}

    if tab == "created":
        if role == "manager":
            query["$or"] = [
                {"creator_id": user_id},
                {"creator_manager_id": user_id},
            ]
        else:
            query["creator_id"] = user_id
    else:
        if role == "executive":
            query["assigned_to_role"] = "executive"
        elif role == "manager":
            query["assigned_to_role"] = "manager"
            query["assigned_to_id"] = user_id
        else:
            query["creator_id"] = user_id

    if status and status != "ALL":
        query["status"] = status
    if priority and priority != "ALL":
        query["priority"] = priority
    if category and category != "ALL":
        query["category"] = category
    if q:
        query["$or"] = query.get("$or", []) + [
            {"title": {"$regex": q, "$options": "i"}},
            {"last_message_preview": {"$regex": q, "$options": "i"}},
            {"creator_name": {"$regex": q, "$options": "i"}},
            {"assigned_to_name": {"$regex": q, "$options": "i"}},
            {"branch_name": {"$regex": q, "$options": "i"}},
        ]
    if start or end:
        rng: Dict[str, Any] = {}
        if start:
            try:
                rng["$gte"] = datetime.strptime(start, "%Y-%m-%d")
            except Exception:
                pass
        if end:
            try:
                rng["$lte"] = datetime.strptime(end, "%Y-%m-%d")
            except Exception:
                pass
        if rng:
            query["created_at"] = rng

    total = issues_col.count_documents(query)
    rows = list(issues_col.find(query).sort("last_message_at", -1).skip(skip).limit(per_page))
    rows = [_normalize_issue(r) for r in rows]

    total_pages = max(1, (total + per_page - 1) // per_page)

    if _wants_json():
        return jsonify(
            ok=True,
            rows=[_issue_row_json(r) for r in rows],
            page=page,
            total_pages=total_pages,
            total=total,
        )

    return render_template(
        "issues_inbox.html",
        issues=rows,
        tab=tab,
        role=role,
        page=page,
        total_pages=total_pages,
        dashboard_url=_dashboard_url(role),
    )


@issues_bp.route("/new", methods=["GET", "POST"])
@_require_user
@audit_action("issue.created", "Created Issue", entity_type="issue")
def new_issue():
    user = _get_current_user()
    role = user["role"]
    if role not in ("agent", "manager"):
        return abort(403)

    executives = _list_executives()

    if request.method == "POST":
        title = (request.form.get("title") or "").strip()
        message = (request.form.get("message") or "").strip()
        category = (request.form.get("category") or "Other").strip()
        priority = (request.form.get("priority") or "MEDIUM").strip()
        recipient = (request.form.get("recipient") or "").strip().lower()
        executive_id = (request.form.get("executive_id") or "").strip()
        attachment_raw = (request.form.get("attachment_urls") or "").strip()
        attachments: List[Dict[str, Any]] = []
        if attachment_raw:
            try:
                attachments = _sanitize_attachments(json.loads(attachment_raw))
            except Exception:
                attachments = []

        if not title or (not message and not attachments):
            msg = "Title and message are required."
            if _wants_json():
                return jsonify(ok=False, message=msg), 400
            return render_template(
                "issues_new.html",
                error=msg,
                role=role,
                executives=executives,
                dashboard_url=_dashboard_url(role),
                branch_name=user.get("branch_name") or "",
            )

        if role == "manager":
            recipient = "executive"
        elif role == "agent" and recipient not in ("manager", "executive"):
            recipient = "manager"

        assigned_to_role = recipient
        assigned_to_id = ""
        assigned_to_name = ""

        if recipient == "manager":
            if not user.get("manager_id"):
                msg = "No manager assigned to your account."
                if _wants_json():
                    return jsonify(ok=False, message=msg), 400
                return render_template(
                    "issues_new.html",
                    error=msg,
                    role=role,
                    executives=executives,
                    dashboard_url=_dashboard_url(role),
                    branch_name=user.get("branch_name") or "",
                )
            assigned_to_id = user["manager_id"]
            mgr = _user_doc_by_id(assigned_to_id) or {}
            assigned_to_name = mgr.get("name") or "Manager"
        else:
            if executive_id:
                assigned_to_id = executive_id
            elif executives:
                assigned_to_id = executives[0]["id"]
            else:
                msg = "No executive accounts found."
                if _wants_json():
                    return jsonify(ok=False, message=msg), 400
                return render_template(
                    "issues_new.html",
                    error=msg,
                    role=role,
                    executives=executives,
                    dashboard_url=_dashboard_url(role),
                    branch_name=user.get("branch_name") or "",
                )
            ex = _user_doc_by_id(assigned_to_id) or {}
            assigned_to_name = ex.get("name") or "Executive"

        now = _now()
        participants: List[Dict[str, str]] = []
        _add_participant(participants, user["id"], user["role"], user["name"])
        _add_participant(participants, assigned_to_id, assigned_to_role, assigned_to_name)

        preview = message[:160] if message else "Attachment"
        doc = {
            "title": title,
            "priority": priority,
            "status": "OPEN",
            "category": category,
            "branch_id": user.get("branch_id") or "",
            "branch_name": user.get("branch_name") or "",
            "creator_id": user["id"],
            "creator_role": user["role"],
            "creator_name": user["name"],
            "creator_manager_id": user.get("manager_id") or "",
            "creator_manager_name": user.get("manager_name") or "",
            "assigned_to_role": assigned_to_role,
            "assigned_to_id": assigned_to_id,
            "assigned_to_name": assigned_to_name,
            "participants": participants,
            "last_message_at": now,
            "last_message_preview": preview,
            "created_at": now,
            "updated_at": now,
            "escalation": {"escalated": False},
            "messages": [
                {
                    "message_id": str(ObjectId()),
                    "sender_id": user["id"],
                    "sender_role": user["role"],
                    "sender_name": user["name"],
                    "text": message,
                    "attachments": attachments or [],
                    "created_at": now,
                }
            ],
        }
        new_id = issues_col.insert_one(doc).inserted_id
        if _wants_json():
            return jsonify(ok=True, issue_id=str(new_id), redirect_url=url_for("issues.issue_thread", issue_id=str(new_id)))
        return redirect(url_for("issues.issue_thread", issue_id=str(new_id)))

    return render_template(
        "issues_new.html",
        role=role,
        executives=executives,
        dashboard_url=_dashboard_url(role),
        branch_name=user.get("branch_name") or "",
    )


@issues_bp.route("/report", methods=["GET"])
@_require_user
def report_redirect():
    return redirect(url_for("issues.new_issue"))


@issues_bp.route("/<issue_id>", methods=["GET"])
@_require_user
def issue_thread(issue_id):
    user = _get_current_user()
    issue = issues_col.find_one({"_id": _oid(issue_id)})
    if not issue:
        return "Issue not found", 404

    issue = _normalize_issue(issue)
    if not _can_view_issue(issue, user):
        return "Unauthorized", 403

    can_update = _can_update_status(issue, user, "IN_PROGRESS")
    can_close = user["role"] == "executive" and issue.get("assigned_to_role") == "executive"
    can_escalate = user["role"] == "manager" and issue.get("assigned_to_role") == "manager" and issue.get("assigned_to_id") == user["id"]

    executives = _list_executives() if can_escalate else []

    return render_template(
        "issues_thread.html",
        issue=issue,
        role=user["role"],
        current_user_id=user["id"],
        can_update=can_update,
        can_close=can_close,
        can_escalate=can_escalate,
        executives=executives,
        dashboard_url=_dashboard_url(user["role"]),
    )


@issues_bp.route("/<issue_id>/reply", methods=["POST"])
@_require_user
@audit_action("issue.replied", "Replied to Issue", entity_type="issue", entity_id_from="issue_id")
def issue_reply(issue_id):
    user = _get_current_user()
    issue = issues_col.find_one({"_id": _oid(issue_id)})
    if not issue:
        return jsonify(ok=False, message="Issue not found."), 404

    issue = _normalize_issue(issue)
    if not _can_reply(issue, user):
        return jsonify(ok=False, message="Unauthorized."), 403

    text = (request.form.get("message") or "").strip()
    attachment_raw = (request.form.get("attachment_urls") or "").strip()
    attachments: List[Dict[str, Any]] = []
    if attachment_raw:
        try:
            attachments = _sanitize_attachments(json.loads(attachment_raw))
        except Exception:
            attachments = []

    if not text and not attachments:
        return jsonify(ok=False, message="Message is required."), 400

    msg = {
        "message_id": str(ObjectId()),
        "sender_id": user["id"],
        "sender_role": user["role"],
        "sender_name": user["name"],
        "text": text,
        "attachments": attachments or [],
        "created_at": _now(),
    }
    preview = text[:160] if text else "Attachment"
    issues_col.update_one(
        {"_id": _oid(issue_id)},
        {
            "$push": {"messages": msg},
            "$set": {
                "last_message_at": _now(),
                "last_message_preview": preview,
                "updated_at": _now(),
            },
        },
    )
    if _wants_json():
        msg_out = dict(msg)
        msg_out["created_at"] = format_dt(msg["created_at"])
        return jsonify(ok=True, message=msg_out)
    return redirect(url_for("issues.issue_thread", issue_id=issue_id))


@issues_bp.route("/<issue_id>/status", methods=["POST"])
@_require_user
@audit_action("issue.status_updated", "Updated Issue Status", entity_type="issue", entity_id_from="issue_id")
def issue_status(issue_id):
    user = _get_current_user()
    issue = issues_col.find_one({"_id": _oid(issue_id)})
    if not issue:
        return jsonify(ok=False, message="Issue not found."), 404

    issue = _normalize_issue(issue)
    status = (request.form.get("status") or "").strip().upper()
    if not status and request.is_json:
        status = (request.json.get("status") or "").strip().upper()
    allowed = {"OPEN", "IN_PROGRESS", "ESCALATED", "RESOLVED", "CLOSED"}
    if status not in allowed:
        return jsonify(ok=False, message="Invalid status."), 400

    if status == "CLOSED" and issue.get("status") != "RESOLVED":
        return jsonify(ok=False, message="Resolve before closing."), 400

    if not _can_update_status(issue, user, status):
        return jsonify(ok=False, message="Unauthorized."), 403

    issues_col.update_one(
        {"_id": _oid(issue_id)},
        {"$set": {"status": status, "updated_at": _now()}},
    )
    if _wants_json():
        return jsonify(ok=True, status=status)
    return redirect(url_for("issues.issue_thread", issue_id=issue_id))


@issues_bp.route("/<issue_id>/escalate", methods=["POST"])
@_require_user
@audit_action("issue.escalated", "Escalated Issue", entity_type="issue", entity_id_from="issue_id")
def issue_escalate(issue_id):
    user = _get_current_user()
    issue = issues_col.find_one({"_id": _oid(issue_id)})
    if not issue:
        return jsonify(ok=False, message="Issue not found."), 404

    issue = _normalize_issue(issue)
    if not (user["role"] == "manager" and issue.get("assigned_to_role") == "manager" and issue.get("assigned_to_id") == user["id"]):
        return jsonify(ok=False, message="Unauthorized."), 403

    reason = (request.form.get("reason") or "").strip()
    executive_id = (request.form.get("executive_id") or "").strip()
    if not reason:
        return jsonify(ok=False, message="Escalation reason required."), 400

    exec_doc = _user_doc_by_id(executive_id) if executive_id else None
    if not exec_doc:
        execs = _list_executives()
        if not execs:
            return jsonify(ok=False, message="No executive available."), 400
        executive_id = execs[0]["id"]
        exec_doc = _user_doc_by_id(executive_id)

    assigned_to_name = exec_doc.get("name") or "Executive"

    participants = issue.get("participants") or []
    _add_participant(participants, executive_id, "executive", assigned_to_name)
    _add_participant(participants, user["id"], user["role"], user["name"])

    issues_col.update_one(
        {"_id": _oid(issue_id)},
        {
            "$set": {
                "assigned_to_role": "executive",
                "assigned_to_id": executive_id,
                "assigned_to_name": assigned_to_name,
                "status": "ESCALATED",
                "participants": participants,
                "updated_at": _now(),
                "escalation": {
                    "escalated": True,
                    "escalated_by_id": user["id"],
                    "escalated_by_name": user["name"],
                    "escalated_at": _now(),
                    "escalated_reason": reason,
                },
            }
        },
    )
    if _wants_json():
        return jsonify(ok=True, status="ESCALATED", assigned_to_name=assigned_to_name)
    return redirect(url_for("issues.issue_thread", issue_id=issue_id))


@issues_bp.route("/upload_image", methods=["POST"])
@_require_user
def upload_issue_image():
    try:
        if "image" not in request.files:
            return jsonify(success=False, error="No file provided."), 400

        image = request.files["image"]
        if not image or not image.filename:
            return jsonify(success=False, error="No selected file."), 400

        if not _allowed_file(image.filename):
            return jsonify(success=False, error="File type not allowed."), 400

        direct_url = f"https://api.cloudflare.com/client/v4/accounts/{CF_ACCOUNT_ID}/images/v2/direct_upload"
        headers = {"Authorization": f"Bearer {CF_IMAGES_TOKEN}"}

        res = requests.post(direct_url, headers=headers, data={}, timeout=20)
        try:
            j = res.json()
        except Exception:
            return jsonify(success=False, error="Cloudflare direct_upload returned non-JSON."), 502

        if not j.get("success"):
            return jsonify(success=False, error="Cloudflare direct_upload failed.", details=j), 400

        upload_url = j["result"]["uploadURL"]
        image_id = j["result"]["id"]

        up = requests.post(
            upload_url,
            files={
                "file": (
                    secure_filename(image.filename),
                    image.stream,
                    image.mimetype or "application/octet-stream",
                )
            },
            timeout=60,
        )
        try:
            uj = up.json()
        except Exception:
            return jsonify(success=False, error="Cloudflare upload returned non-JSON."), 502

        if not uj.get("success"):
            return jsonify(success=False, error="Cloudflare upload failed.", details=uj), 400

        variant = request.args.get("variant", DEFAULT_VARIANT)
        image_url = f"https://imagedelivery.net/{CF_HASH}/{image_id}/{variant}"

        images_col.insert_one(
            {
                "provider": "cloudflare_images",
                "context": "issue_attachment",
                "issue_id": request.form.get("issue_id") or None,
                "message_temp_id": request.form.get("message_temp_id") or None,
                "image_id": image_id,
                "variant": variant,
                "url": image_url,
                "original_filename": secure_filename(image.filename),
                "mimetype": image.mimetype,
                "created_at": _now(),
            }
        )

        return jsonify(
            success=True,
            image_url=image_url,
            image_id=image_id,
            variant=variant,
            name=secure_filename(image.filename),
            mimetype=image.mimetype,
        )
    except Exception:
        traceback.print_exc()
        return jsonify(success=False, error="Upload failed."), 500


@issues_bp.route("/count", methods=["GET"])
@_require_user
def issue_count():
    user = _get_current_user()
    role = user["role"]
    user_id = user["id"]
    active = ["OPEN", "IN_PROGRESS", "ESCALATED"]

    if role == "executive":
        q = {"assigned_to_role": "executive", "status": {"$in": active}}
    elif role == "manager":
        q = {"assigned_to_role": "manager", "assigned_to_id": user_id, "status": {"$in": active}}
    else:
        q = {"creator_id": user_id, "status": {"$in": active}}

    count = issues_col.count_documents(q)
    return jsonify(ok=True, count=int(count))


# Indexes (run once in shell or migration):
# issues_col.create_index([("assigned_to_role", 1), ("assigned_to_id", 1), ("status", 1)])
# issues_col.create_index([("creator_id", 1), ("created_at", -1)])
# issues_col.create_index([("last_message_at", -1)])
# issues_col.create_index([("branch_id", 1)])

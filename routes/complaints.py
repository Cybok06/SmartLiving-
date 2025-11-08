# routes/complaints.py
from __future__ import annotations

from datetime import datetime, timedelta
from typing import Optional

from flask import (
    Blueprint, render_template, request, jsonify,
    redirect, url_for, flash, session
)
from bson import ObjectId
import requests
from urllib.parse import quote

from db import db

complaints_bp = Blueprint("complaints", __name__, url_prefix="/complaints")

# === Config (replace with env vars in production) ===
ARKESEL_API_KEY = "b3dheEVqUWNyeVBuUGxDVWFxZ0E"
SMS_SENDER      = "SMARTLIVING"
SMS_FALLBACK    = "Call 0556064611 / 0509639836 / 0598948132"

# === Collections ===
customers_col   = db["customers"]
users_col       = db["users"]
complaints_col  = db["complaints"]

# === Helpers ===
ISSUE_SLA_DEFAULTS = {
    "Payment": 1,
    "Security/Compliance": 1,
    "General Enquiry": 1,
    "Delivery": 2,
    "Product Fault": 5,
    "Collection/Agent": 2,
}
CHANNELS = ["Walk-In", "Call", "WhatsApp", "Email", "Facebook", "Instagram", "Other"]
ISSUE_TYPES = list(ISSUE_SLA_DEFAULTS.keys())
STATUSES = ["Assigned", "In Progress", "Waiting for Customer", "Resolved", "Closed"]


def _admin_required() -> bool:
    """Return True if admin logged in; on AJAX return 401 externally."""
    if "admin_id" not in session:
        return False
    return True


def _parse_date(d: str | None) -> Optional[datetime]:
    if not d:
        return None
    try:
        return datetime.strptime(d, "%Y-%m-%d")
    except Exception:
        return None


def _sla_due(date_reported: datetime, sla_days: int) -> datetime:
    return date_reported + timedelta(days=max(int(sla_days), 0))


def _breached(sla_due: datetime, date_closed: Optional[datetime]) -> bool:
    if not sla_due:
        return False
    check_dt = date_closed or datetime.utcnow()
    return check_dt.date() > sla_due.date()


def _normalize_phone(raw: str | None) -> Optional[str]:
    """Return MSISDN like 23354XXXXXXX or None if invalid."""
    if not raw:
        return None
    p = raw.strip().replace(" ", "").replace("-", "").replace("+", "")
    if p.startswith("0") and len(p) == 10:
        p = "233" + p[1:]
    if p.startswith("233") and len(p) == 12:
        return p
    return None


def _send_sms(msisdn: str, message: str) -> tuple[bool, str]:
    """Send via Arkesel; return (ok, status_text)."""
    try:
        url = (
            "https://sms.arkesel.com/sms/api?action=send-sms"
            f"&api_key={ARKESEL_API_KEY}"
            f"&to={msisdn}"
            f"&from={SMS_SENDER}"
            f"&sms={quote(message)}"
        )
        resp = requests.get(url, timeout=12)
        ok = (resp.status_code == 200 and '"code":"ok"' in resp.text)
        return ok, ("sent" if ok else f"failed[{resp.status_code}]")
    except Exception as e:
        return False, f"error:{e!s}"


def _default_ack_text(full_name: str, ticket_no: str, issue: str, date_str: str) -> str:
    first = (full_name or "Customer").split()[0]
    return (
        f"Dear {first}, your complaint {ticket_no} about '{issue}' "
        f"was received on {date_str}. We are working on it. {SMS_FALLBACK}"
    )


# ===== AJAX: lookup by phone =====
@complaints_bp.get("/lookup_phone")
def lookup_phone():
    if not _admin_required():
        return jsonify(ok=False, message="Unauthorized"), 401

    phone = (request.args.get("phone") or "").strip()
    if not phone:
        return jsonify(ok=False, message="Phone required"), 400

    # exact, else last-9
    c = customers_col.find_one({"phone_number": phone})
    if not c and len(phone) >= 9:
        last9 = phone[-9:]
        c = customers_col.find_one({"phone_number": {"$regex": last9 + "$"}})

    if not c:
        return jsonify(ok=True, found=False)

    # agent (assigned_to)
    agent_doc = None
    agent_id = c.get("agent_id")
    try:
        if agent_id:
            agent_doc = users_col.find_one({"_id": ObjectId(agent_id)})
    except Exception:
        agent_doc = None

    # manager fallback for branch
    manager_doc = None
    try:
        if c.get("manager_id"):
            manager_doc = users_col.find_one({"_id": ObjectId(c["manager_id"])})
    except Exception:
        pass

    payload = {
        "found": True,
        "customer_name": c.get("name", ""),
        "customer_phone": c.get("phone_number", phone),
        "branch": (agent_doc and agent_doc.get("branch")) or (manager_doc and manager_doc.get("branch")) or "",
        "assigned_to_name": (agent_doc and agent_doc.get("name")) or "",
        "assigned_to_id": str(agent_doc["_id"]) if agent_doc else None,
        "customer_id": str(c["_id"]),
    }
    return jsonify(ok=True, **payload)


# ===== List page (admin) =====
@complaints_bp.get("/")
def list_complaints():
    if "admin_id" not in session:
        return redirect(url_for("login.login"))

    q = {}
    status = request.args.get("status") or ""
    if status:
        q["status"] = status

    term = (request.args.get("q") or "").strip()
    if term:
        q["$or"] = [
            {"ticket_no": {"$regex": term, "$options": "i"}},
            {"customer_name": {"$regex": term, "$options": "i"}},
            {"customer_phone": {"$regex": term, "$options": "i"}},
            {"issue_type": {"$regex": term, "$options": "i"}},
            {"assigned_to_name": {"$regex": term, "$options": "i"}},
        ]

    date_from = _parse_date(request.args.get("from"))
    date_to   = _parse_date(request.args.get("to"))
    if date_from or date_to:
        q["date_reported"] = {}
        if date_from: q["date_reported"]["$gte"] = date_from
        if date_to:   q["date_reported"]["$lte"] = date_to

    items = list(complaints_col.find(q).sort([("date_reported", -1)]).limit(500))

    # stats
    open_count = complaints_col.count_documents({"status": {"$in": ["Assigned", "In Progress", "Waiting for Customer"]}})
    breaching  = complaints_col.count_documents({"sla_due": {"$lt": datetime.utcnow()}, "status": {"$nin": ["Resolved", "Closed"]}})
    resolved   = complaints_col.count_documents({"status": {"$in": ["Resolved", "Closed"]}})

    # stringify for template
    for it in items:
        it["_id"] = str(it["_id"])
        for k in ("date_reported", "date_closed", "sla_due", "created_at", "updated_at"):
            if isinstance(it.get(k), datetime):
                it[k] = it[k].strftime("%Y-%m-%d")

    return render_template(
        "complaints.html",
        items=items,
        statuses=STATUSES,
        issue_types=ISSUE_TYPES,
        channels=CHANNELS,
        sla_defaults=ISSUE_SLA_DEFAULTS,
        filters={"status": status, "q": term, "from": request.args.get("from",""), "to": request.args.get("to","")},
        stats={"open": open_count, "breaching": breaching, "resolved": resolved},
        today=datetime.utcnow().strftime("%Y-%m-%d"),
    )


# ===== Create (AJAX from modal) =====
@complaints_bp.post("/create")
def create_complaint():
    if not _admin_required():
        return jsonify(ok=False, message="Unauthorized"), 401

    # Basic fields
    date_str = request.form.get("date_reported") or datetime.utcnow().strftime("%Y-%m-%d")
    date_reported = _parse_date(date_str) or datetime.utcnow()

    branch       = (request.form.get("branch") or "").strip()
    channel      = (request.form.get("channel") or "").strip()
    customer_name  = (request.form.get("customer_name") or "").strip()
    customer_phone = (request.form.get("customer_phone") or "").strip()
    issue_type   = (request.form.get("issue_type") or "").strip()
    description  = (request.form.get("description") or "").strip()

    assigned_to_id   = (request.form.get("assigned_to_id") or "").strip() or None
    assigned_to_name = (request.form.get("assigned_to_name") or "").strip()
    due_str = request.form.get("due_date") or ""
    sla_days = int(request.form.get("sla_days") or ISSUE_SLA_DEFAULTS.get(issue_type, 1))

    send_sms_flag = (request.form.get("send_sms") or "").lower() in ("true", "1", "yes", "on")

    status = "Assigned"
    resolution_notes = ""
    customer_feedback = ""
    date_closed = None

    sla_due = _parse_date(due_str) if due_str else _sla_due(date_reported, sla_days)
    breached = _breached(sla_due, None)

    # Ticket number: INC-YYYYMMDD-###
    today_str = date_reported.strftime("%Y%m%d")
    last = complaints_col.find_one(
        {"ticket_no": {"$regex": f"^INC-{today_str}-\\d{{3}}$"}},
        sort=[("ticket_no", -1)]
    )
    seq = 1
    if last:
        try:
            seq = int(last["ticket_no"].split("-")[-1]) + 1
        except Exception:
            seq = 1
    ticket_no = f"INC-{today_str}-{seq:03d}"

    doc = {
        "ticket_no": ticket_no,
        "date_reported": date_reported,
        "branch": branch,
        "channel": channel if channel in CHANNELS else channel or "Call",
        "customer_name": customer_name,
        "customer_phone": customer_phone,
        "issue_type": issue_type if issue_type in ISSUE_TYPES else issue_type,
        "description": description,

        "assigned_to_id": ObjectId(assigned_to_id) if assigned_to_id else None,
        "assigned_to_name": assigned_to_name,

        "status": status,
        "resolution_notes": resolution_notes,
        "customer_feedback": customer_feedback,

        "date_closed": date_closed,
        "sla_days": sla_days,
        "sla_due": sla_due,
        "sla_breached": breached,

        "created_by": session.get("admin_id"),
        "created_at": datetime.utcnow(),
        "updated_at": datetime.utcnow(),

        # optional SMS audit trail
        "sms_events": [],
    }
    ins = complaints_col.insert_one(doc)

    # Optional SMS on create
    sms_result = None
    if send_sms_flag:
        msisdn = _normalize_phone(customer_phone)
        if msisdn:
            text = _default_ack_text(customer_name, ticket_no, issue_type, date_reported.strftime("%Y-%m-%d"))
            ok, status_txt = _send_sms(msisdn, text)
            sms_result = {"when": datetime.utcnow(), "to": msisdn, "ok": ok, "status": status_txt, "kind": "create_ack"}
            complaints_col.update_one({"_id": ins.inserted_id}, {"$push": {"sms_events": sms_result}})
        else:
            sms_result = {"when": datetime.utcnow(), "to": customer_phone, "ok": False, "status": "invalid_phone", "kind": "create_ack"}
            complaints_col.update_one({"_id": ins.inserted_id}, {"$push": {"sms_events": sms_result}})

    return jsonify(
        ok=True,
        id=str(ins.inserted_id),
        ticket_no=ticket_no,
        date_reported=date_reported.strftime("%Y-%m-%d"),
        customer_name=customer_name,
        customer_phone=customer_phone,
        issue_type=issue_type,
        assigned_to_name=assigned_to_name,
        status=status,
        sla_due=sla_due.strftime("%Y-%m-%d"),
        sla_breached=breached,
        description=description,
        sms_result=sms_result,
    )


# ===== Detail (for “See details” modal) =====
@complaints_bp.get("/<cid>/detail")
def complaint_detail(cid):
    if not _admin_required():
        return jsonify(ok=False, message="Unauthorized"), 401

    try:
        oid = ObjectId(cid)
    except Exception:
        return jsonify(ok=False, message="Invalid id"), 400

    doc = complaints_col.find_one({"_id": oid})
    if not doc:
        return jsonify(ok=False, message="Not found"), 404

    # Prepare JSON-safe payload
    def _fmt_dt(v):
        if isinstance(v, datetime):
            return v.strftime("%Y-%m-%d")
        return v

    out = {
        "id": str(doc["_id"]),
        "ticket_no": doc.get("ticket_no", ""),
        "date_reported": _fmt_dt(doc.get("date_reported")),
        "branch": doc.get("branch", ""),
        "channel": doc.get("channel", ""),
        "customer_name": doc.get("customer_name", ""),
        "customer_phone": doc.get("customer_phone", ""),
        "issue_type": doc.get("issue_type", ""),
        "description": doc.get("description", ""),
        "assigned_to_name": doc.get("assigned_to_name", ""),
        "assigned_to_id": str(doc["assigned_to_id"]) if doc.get("assigned_to_id") else None,
        "status": doc.get("status", ""),
        "resolution_notes": doc.get("resolution_notes", ""),
        "customer_feedback": doc.get("customer_feedback", ""),
        "sla_days": doc.get("sla_days", ""),
        "sla_due": _fmt_dt(doc.get("sla_due")),
        "sla_breached": bool(doc.get("sla_breached", False)),
        "date_closed": _fmt_dt(doc.get("date_closed")),
        "created_at": _fmt_dt(doc.get("created_at")),
        "updated_at": _fmt_dt(doc.get("updated_at")),
        "sms_events": [
            {
                "when": e.get("when").strftime("%Y-%m-%d %H:%M:%S") if isinstance(e.get("when"), datetime) else str(e.get("when")),
                "to": e.get("to"),
                "ok": e.get("ok"),
                "status": e.get("status"),
                "kind": e.get("kind", "manual"),
            } for e in (doc.get("sms_events") or [])
        ],
    }
    return jsonify(ok=True, complaint=out)


# ===== Update description / notes (AJAX) =====
@complaints_bp.post("/<cid>/update_fields")
def update_fields(cid):
    if not _admin_required():
        return jsonify(ok=False, message="Unauthorized"), 401

    try:
        oid = ObjectId(cid)
    except Exception:
        return jsonify(ok=False, message="Invalid id"), 400

    fields = {}
    # Allow simple updates; extend if you like
    if "description" in request.form:
        fields["description"] = (request.form.get("description") or "").strip()
    if "resolution_notes" in request.form:
        fields["resolution_notes"] = (request.form.get("resolution_notes") or "").strip()
    if "customer_feedback" in request.form:
        fields["customer_feedback"] = (request.form.get("customer_feedback") or "").strip()

    if not fields:
        return jsonify(ok=False, message="No updatable fields provided"), 400

    fields["updated_at"] = datetime.utcnow()
    complaints_col.update_one({"_id": oid}, {"$set": fields})
    return jsonify(ok=True)


# ===== Quick status update (AJAX) =====
@complaints_bp.post("/<cid>/status")
def update_status(cid):
    if not _admin_required():
        return jsonify(ok=False, message="Unauthorized"), 401

    try:
        oid = ObjectId(cid)
    except Exception:
        return jsonify(ok=False, message="Invalid id"), 400

    status = request.form.get("status") or ""
    notes  = request.form.get("resolution_notes") or ""
    feedback = request.form.get("customer_feedback") or ""

    if status not in STATUSES:
        return jsonify(ok=False, message="Invalid status"), 400

    now = datetime.utcnow()
    update = {"$set": {
        "status": status,
        "resolution_notes": notes,
        "customer_feedback": feedback,
        "updated_at": now,
    }}

    doc = complaints_col.find_one({"_id": oid})
    if not doc:
        return jsonify(ok=False, message="Not found"), 404

    sla_due = doc.get("sla_due")
    if isinstance(sla_due, str):
        sla_due = _parse_date(sla_due)

    if status in ("Resolved", "Closed"):
        update["$set"]["date_closed"] = now
        update["$set"]["sla_breached"] = _breached(sla_due, now)
    else:
        update["$set"]["date_closed"] = None
        update["$set"]["sla_breached"] = _breached(sla_due, None)

    complaints_col.update_one({"_id": oid}, update)
    return jsonify(ok=True)


# ===== Manual SMS notify (AJAX button in modal) =====
@complaints_bp.post("/<cid>/notify")
def notify_customer(cid):
    if not _admin_required():
        return jsonify(ok=False, message="Unauthorized"), 401

    try:
        oid = ObjectId(cid)
    except Exception:
        return jsonify(ok=False, message="Invalid id"), 400

    doc = complaints_col.find_one({"_id": oid})
    if not doc:
        return jsonify(ok=False, message="Not found"), 404

    # Custom message allowed; else default
    custom_msg = (request.form.get("message") or "").strip()
    msisdn = _normalize_phone(doc.get("customer_phone"))
    if not msisdn:
        return jsonify(ok=False, message="Invalid customer phone for SMS"), 400

    if not custom_msg:
        custom_msg = _default_ack_text(
            doc.get("customer_name", ""),
            doc.get("ticket_no", ""),
            doc.get("issue_type", ""),
            (doc.get("date_reported").strftime("%Y-%m-%d")
             if isinstance(doc.get("date_reported"), datetime)
             else str(doc.get("date_reported")))
        )

    ok, status_txt = _send_sms(msisdn, custom_msg)
    event = {
        "when": datetime.utcnow(),
        "to": msisdn,
        "ok": ok,
        "status": status_txt,
        "kind": "manual_notify",
        "preview": custom_msg[:200]
    }
    complaints_col.update_one({"_id": oid}, {"$push": {"sms_events": event}})

    return jsonify(ok=ok, status=status_txt)
# ===== Executive view (read-only) =====
@complaints_bp.get("/executive")
def executive_view():
    # Allow admin or executive (adjust this gate as needed)
    if "admin_id" not in session and "executive_id" not in session:
        return redirect(url_for("login.login"))

    q = {}
    status = (request.args.get("status") or "").strip()
    if status:
        q["status"] = status

    issue_type = (request.args.get("issue_type") or "").strip()
    if issue_type:
        q["issue_type"] = issue_type

    channel = (request.args.get("channel") or "").strip()
    if channel:
        q["channel"] = channel

    term = (request.args.get("q") or "").strip()
    if term:
        q["$or"] = [
            {"ticket_no": {"$regex": term, "$options": "i"}},
            {"customer_name": {"$regex": term, "$options": "i"}},
            {"customer_phone": {"$regex": term, "$options": "i"}},
            {"issue_type": {"$regex": term, "$options": "i"}},
            {"assigned_to_name": {"$regex": term, "$options": "i"}},
            {"branch": {"$regex": term, "$options": "i"}},
        ]

    def _pdate(d):
        try:
            return datetime.strptime(d, "%Y-%m-%d") if d else None
        except Exception:
            return None

    dfrom = _pdate(request.args.get("from"))
    dto   = _pdate(request.args.get("to"))
    if dfrom or dto:
        q["date_reported"] = {}
        if dfrom: q["date_reported"]["$gte"] = dfrom
        if dto:   q["date_reported"]["$lte"] = dto

    items = list(complaints_col.find(q).sort([("date_reported", -1)]).limit(800))

    # Stats for header
    open_statuses = ["Assigned", "In Progress", "Waiting for Customer"]
    open_count = complaints_col.count_documents({"status": {"$in": open_statuses}})
    breaching  = complaints_col.count_documents({"sla_due": {"$lt": datetime.utcnow()}, "status": {"$nin": ["Resolved", "Closed"]}})
    resolved   = complaints_col.count_documents({"status": {"$in": ["Resolved", "Closed"]}})

    # stringify for template
    for it in items:
        it["_id"] = str(it["_id"])
        for k in ("date_reported", "date_closed", "sla_due", "created_at", "updated_at"):
            if isinstance(it.get(k), datetime):
                it[k] = it[k].strftime("%Y-%m-%d")

    return render_template(
        "complaints_exec.html",
        items=items,
        statuses=STATUSES,
        issue_types=ISSUE_TYPES,
        channels=CHANNELS,
        filters={
            "status": status,
            "issue_type": issue_type,
            "channel": channel,
            "q": term,
            "from": request.args.get("from",""),
            "to": request.args.get("to","")
        },
        stats={"open": open_count, "breaching": breaching, "resolved": resolved},
        today=datetime.utcnow().strftime("%Y-%m-%d"),
    )


# ===== Lightweight badge feed for sidebar =====
@complaints_bp.get("/executive_unresolved_count")
def executive_unresolved_count():
    if "admin_id" not in session and "executive_id" not in session:
        return jsonify(ok=False, message="Unauthorized"), 401

    open_statuses = ["Assigned", "In Progress", "Waiting for Customer"]
    open_count = complaints_col.count_documents({"status": {"$in": open_statuses}})
    breaching   = complaints_col.count_documents({"sla_due": {"$lt": datetime.utcnow()}, "status": {"$nin": ["Resolved", "Closed"]}})

    return jsonify(ok=True, open=open_count, breaching=breaching)


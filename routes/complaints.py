from __future__ import annotations

from datetime import datetime, timedelta, date
from typing import Optional, Dict, Any, List

from flask import (
    Blueprint, render_template, request, jsonify,
    redirect, url_for, flash, session
)
from bson import ObjectId
import requests
from urllib.parse import quote

from db import db
from services.activity_audit import audit_action

complaints_bp = Blueprint("complaints", __name__, url_prefix="/complaints")

# === Config (replace with env vars in production) ===
ARKESEL_API_KEY = "b3dheEVqUWNyeVBuUGxDVWFxZ0E"
SMS_SENDER      = "SMARTLIVING"
SMS_FALLBACK    = "Call 0556064611 / 0509639836 / 0598948132"

# === Collections ===
customers_col           = db["customers"]
users_col               = db["users"]
complaints_col          = db["complaints"]
payments_col            = db["payments"]           # For customer activity
packages_col            = db["packages"]           # For customer activity (packaging)
customer_activities_col = db["customer_activities"]  # Manual / admin-logged activities

# === Helpers / Config ===
ISSUE_SLA_DEFAULTS: Dict[str, int] = {
    "Payment": 1,
    "Security/Compliance": 1,
    "General Enquiry": 1,
    "Delivery": 2,
    "Product Fault": 5,
    "Collection/Agent": 2,
}
CHANNELS = ["Walk-In", "Call", "WhatsApp", "Email", "Facebook", "Instagram", "Other"]
ISSUE_TYPES = list(ISSUE_SLA_DEFAULTS.keys())
STATUSES = ["Unassigned", "Assigned", "In Progress", "Waiting for Customer", "Resolved", "Closed"]

# Logical “buckets” for filtering / stats
OPEN_STATUSES = ["Unassigned", "Assigned", "In Progress", "Waiting for Customer"]
RESOLVED_STATUSES = ["Resolved", "Closed"]


def _admin_required() -> bool:
    """Return True if admin or manager logged in; on AJAX caller can act on 401."""
    return "admin_id" in session or "manager_id" in session


def _current_role() -> Optional[str]:
    if "admin_id" in session:
        return "admin"
    if "manager_id" in session:
        return "manager"
    return None


def _current_user_id() -> Optional[str]:
    return session.get("admin_id") or session.get("manager_id")


def _safe_oid(raw: Optional[str]) -> Optional[ObjectId]:
    if not raw:
        return None
    try:
        return ObjectId(raw)
    except Exception:
        return None


def _get_manager_agents(manager_id: str) -> tuple[list[dict], set]:
    """
    Return (agents_for_form, agent_id_values_set) for a manager.
    agent_id_values_set includes both ObjectId and string values to match legacy data.
    """
    if not manager_id:
        return [], set()

    mgr_oid = _safe_oid(manager_id)
    query: Dict[str, Any] = {"role": "agent"}
    if mgr_oid:
        query["$or"] = [{"manager_id": mgr_oid}, {"manager_id": manager_id}]
    else:
        query["manager_id"] = manager_id

    agents = list(users_col.find(query, {"name": 1}).sort("name", 1))
    agent_id_values = set()
    agents_for_form = []
    for a in agents:
        a_id = a.get("_id")
        if a_id is None:
            continue
        a_id_str = str(a_id)
        agent_id_values.add(a_id)
        agent_id_values.add(a_id_str)
        agents_for_form.append({"id": a_id_str, "name": a.get("name", "")})

    return agents_for_form, agent_id_values


def _manager_scope_filter(manager_id: str) -> Optional[Dict[str, Any]]:
    if not manager_id:
        return None
    _, agent_id_values = _get_manager_agents(manager_id)
    if not agent_id_values:
        return {"assigned_to_id": {"$in": []}}
    return {"$or": [{"assigned_to_id": {"$in": list(agent_id_values)}}, {"created_by": manager_id}]}


def _scoped_complaint_query(oid: ObjectId) -> Dict[str, Any]:
    role = _current_role()
    if role != "manager":
        return {"_id": oid}
    scope = _manager_scope_filter(str(session.get("manager_id")))
    if scope:
        return {"$and": [{"_id": oid}, scope]}
    return {"_id": oid}


def _parse_date(d: Optional[str]) -> Optional[datetime]:
    if not d:
        return None
    try:
        return datetime.strptime(d, "%Y-%m-%d")
    except Exception:
        return None


def _sla_due(date_reported: datetime, sla_days: int) -> datetime:
    return date_reported + timedelta(days=max(int(sla_days), 0))


def _breached(sla_due: Optional[datetime], date_closed: Optional[datetime]) -> bool:
    if not sla_due:
        return False
    check_dt = date_closed or datetime.utcnow()
    return check_dt.date() > sla_due.date()


def _normalize_phone(raw: Optional[str]) -> Optional[str]:
    """
    Normalise to MSISDN 23354XXXXXXX.
    Returns None if invalid.
    """
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


def _quick_range(period: str) -> tuple[Optional[datetime], Optional[datetime], str]:
    """
    Convert a quick filter (today|week|month|custom) into (from, to, label).
    - today:    today only
    - week:     last 7 days (including today)
    - month:    first day of current month to today
    - custom:   None, None (use explicit from/to)
    """
    period = (period or "").lower()
    today_d: date = datetime.utcnow().date()

    if period == "today":
        start = datetime.combine(today_d, datetime.min.time())
        end   = datetime.combine(today_d, datetime.max.time())
        return start, end, "today"
    elif period == "week":
        start = datetime.combine(today_d - timedelta(days=6), datetime.min.time())
        end   = datetime.combine(today_d, datetime.max.time())
        return start, end, "week"
    elif period == "month":
        first_day = today_d.replace(day=1)
        start = datetime.combine(first_day, datetime.min.time())
        end   = datetime.combine(today_d, datetime.max.time())
        return start, end, "month"
    else:
        return None, None, "custom"


# ===== AJAX: lookup by phone =====
@complaints_bp.get("/lookup_phone")
def lookup_phone():
    if not _admin_required():
        return jsonify(ok=False, message="Unauthorized"), 401

    phone = (request.args.get("phone") or "").strip()
    if not phone:
        return jsonify(ok=False, message="Phone required"), 400

    # exact, else last-9 fallback
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

    # If manager is logged in, limit agent auto-fill to their team
    if _current_role() == "manager" and agent_doc:
        manager_id = session.get("manager_id")
        mgr_oid = _safe_oid(str(manager_id))
        agent_mgr = agent_doc.get("manager_id")
        if mgr_oid:
            if agent_mgr not in (mgr_oid, str(mgr_oid), str(manager_id)):
                agent_doc = None
        else:
            if agent_mgr != str(manager_id):
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
        "customer_id": str(c["_id"]),   # allows us to link complaint/activity to customer
    }
    return jsonify(ok=True, **payload)


# ===== AJAX: agent list (refresh) =====
@complaints_bp.get("/agents")
def complaints_agents():
    if not _admin_required():
        return jsonify(ok=False, message="Unauthorized"), 401

    role = _current_role()
    if role == "manager":
        manager_id = session.get("manager_id")
        agents_for_form, _ = _get_manager_agents(str(manager_id))
        return jsonify(ok=True, agents=agents_for_form)

    agents = list(users_col.find({"role": "agent"}, {"name": 1}).sort("name", 1))
    agents_for_form = [{"id": str(a["_id"]), "name": a.get("name", "")} for a in agents if a.get("_id")]
    return jsonify(ok=True, agents=agents_for_form)


# ===== List page (admin) =====
@complaints_bp.get("/")
def list_complaints():
    role = _current_role()
    if not role:
        return redirect(url_for("login.login"))

    q: Dict[str, Any] = {}

    # Logical bucket: all | open | closed | breached
    bucket = (request.args.get("bucket") or "").strip().lower()

    status = request.args.get("status") or ""
    if status and not bucket:
        # When bucket is set, we ignore explicit status to avoid conflict
        q["status"] = status

    # Agent scope + form list
    agents_for_form: List[Dict[str, Any]] = []
    manager_agent_ids: set = set()
    if role == "manager":
        manager_id = session.get("manager_id")
        agents_for_form, manager_agent_ids = _get_manager_agents(str(manager_id))
    else:
        agents = list(users_col.find({"role": "agent"}, {"name": 1}).sort("name", 1))
        agents_for_form = [{"id": str(a["_id"]), "name": a.get("name", "")} for a in agents if a.get("_id")]

    # Filter by assigned agent (id)
    assigned_to_id = (request.args.get("assigned_to_id") or "").strip()
    if assigned_to_id:
        manager_agent_id_strs = {a["id"] for a in agents_for_form}
        if role == "manager" and assigned_to_id not in manager_agent_id_strs:
            assigned_to_id = ""
        else:
            agent_oid = _safe_oid(assigned_to_id)
            q["assigned_to_id"] = {"$in": [agent_oid, assigned_to_id]} if agent_oid else assigned_to_id

    # Filter by customer phone (partial)
    customer_phone = (request.args.get("customer_phone") or "").strip()
    if customer_phone:
        q["customer_phone"] = {"$regex": customer_phone, "$options": "i"}

    # Smart free-text search
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

    # Quick range filter (today/week/month/custom)
    period = request.args.get("range") or "custom"
    quick_from, quick_to, active_range = _quick_range(period)

    date_from = _parse_date(request.args.get("from")) or quick_from
    date_to   = _parse_date(request.args.get("to")) or quick_to

    if date_from or date_to:
        q["date_reported"] = {}
        if date_from: q["date_reported"]["$gte"] = date_from
        if date_to:   q["date_reported"]["$lte"] = date_to

    # Apply bucket filters last so they “win”
    if bucket == "open":
        q["status"] = {"$in": OPEN_STATUSES}
    elif bucket == "closed":
        q["status"] = {"$in": RESOLVED_STATUSES}
    elif bucket == "breached":
        q["status"] = {"$nin": RESOLVED_STATUSES}
        q["sla_breached"] = True

    # Apply manager scope filter at the end
    if role == "manager":
        scope = _manager_scope_filter(str(session.get("manager_id")))
        if scope:
            q = {"$and": [q, scope]} if q else scope

    items = list(complaints_col.find(q).sort([("date_reported", -1)]).limit(500))

    # Global stats (all-time)
    stats_scope = {}
    if role == "manager":
        scope = _manager_scope_filter(str(session.get("manager_id")))
        if scope:
            stats_scope = scope

    open_count = complaints_col.count_documents({"$and": [stats_scope, {"status": {"$in": OPEN_STATUSES}}]} if stats_scope else {"status": {"$in": OPEN_STATUSES}})
    breaching  = complaints_col.count_documents({
        "sla_due": {"$lt": datetime.utcnow()},
        "status": {"$nin": RESOLVED_STATUSES},
        **(stats_scope or {})
    })
    resolved_all = complaints_col.count_documents({"$and": [stats_scope, {"status": {"$in": RESOLVED_STATUSES}}]} if stats_scope else {"status": {"$in": RESOLVED_STATUSES}})

    # Stats in the selected period: resolved issues
    resolved_in_range = None
    if date_from or date_to:
        closed_query: Dict[str, Any] = {"status": {"$in": RESOLVED_STATUSES}}
        if date_from or date_to:
            closed_query["date_closed"] = {}
            if date_from: closed_query["date_closed"]["$gte"] = date_from
            if date_to:   closed_query["date_closed"]["$lte"] = date_to
        if stats_scope:
            closed_query = {"$and": [closed_query, stats_scope]}
        resolved_in_range = complaints_col.count_documents(closed_query)

    # === Resolved recent section (last 10, filterable by phone) ===
    resolved_phone = (request.args.get("resolved_phone") or "").strip()
    resolved_q: Dict[str, Any] = {"status": {"$in": RESOLVED_STATUSES}}
    if resolved_phone:
        resolved_q["customer_phone"] = {"$regex": resolved_phone, "$options": "i"}

    if stats_scope:
        resolved_q = {"$and": [resolved_q, stats_scope]}

    resolved_recent = list(
        complaints_col.find(resolved_q).sort([("date_closed", -1), ("date_reported", -1)]).limit(10)
    )

    # stringify for template
    def _stringify_dates(doc: Dict[str, Any]) -> None:
        for k in ("date_reported", "date_closed", "sla_due", "created_at", "updated_at"):
            if isinstance(doc.get(k), datetime):
                doc[k] = doc[k].strftime("%Y-%m-%d")

    for it in items:
        it["_id"] = str(it["_id"])
        _stringify_dates(it)

    for it in resolved_recent:
        it["_id"] = str(it["_id"])
        _stringify_dates(it)

    return render_template(
        "complaints.html",
        items=items,
        resolved_recent=resolved_recent,
        statuses=STATUSES,
        issue_types=ISSUE_TYPES,
        channels=CHANNELS,
        sla_defaults=ISSUE_SLA_DEFAULTS,
        filters={
            "status": status,
            "q": term,
            "from": request.args.get("from", ""),
            "to": request.args.get("to", ""),
            "range": active_range,
            "bucket": bucket,
            "assigned_to_id": assigned_to_id,
            "customer_phone": customer_phone,
        },
        resolved_filters={
            "customer_phone": resolved_phone,
        },
        stats={
            "open": open_count,
            "breaching": breaching,
            "resolved": resolved_all,
            "resolved_in_range": resolved_in_range,
        },
        agents=agents_for_form,
        today=datetime.utcnow().strftime("%Y-%m-%d"),
    )


# ===== Create (AJAX from modal) =====
@complaints_bp.post("/create")
@audit_action("complaint.created", "Created Complaint", entity_type="complaint")
def create_complaint():
    if not _admin_required():
        return jsonify(ok=False, message="Unauthorized"), 401
    role = _current_role()

    # Basic fields
    date_str = request.form.get("date_reported") or datetime.utcnow().strftime("%Y-%m-%d")
    date_reported = _parse_date(date_str) or datetime.utcnow()

    branch          = (request.form.get("branch") or "").strip()
    channel         = (request.form.get("channel") or "").strip()
    customer_name   = (request.form.get("customer_name") or "").strip()
    customer_phone  = (request.form.get("customer_phone") or "").strip()
    issue_type      = (request.form.get("issue_type") or "").strip()
    description     = (request.form.get("description") or "").strip()

    customer_id_str = (request.form.get("customer_id") or "").strip()
    customer_id     = None
    if customer_id_str:
        try:
            customer_id = ObjectId(customer_id_str)
        except Exception:
            customer_id = None

    assigned_to_id_raw = (request.form.get("assigned_to_id") or "").strip() or None
    due_str          = request.form.get("due_date") or ""
    sla_days = int(request.form.get("sla_days") or ISSUE_SLA_DEFAULTS.get(issue_type, 1))

    send_sms_flag = (request.form.get("send_sms") or "").lower() in ("true", "1", "yes", "on")

    status = "Unassigned"
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

    assigned_to_oid = None
    assigned_to_name = ""
    if assigned_to_id_raw:
        assigned_to_oid = _safe_oid(assigned_to_id_raw)
        if not assigned_to_oid:
            return jsonify(ok=False, message="Invalid agent selection"), 400
        a_doc = users_col.find_one({"_id": assigned_to_oid, "role": "agent"}, {"name": 1})
        if not a_doc:
            return jsonify(ok=False, message="Assigned agent not found"), 404

        if role == "manager":
            _, manager_agent_ids = _get_manager_agents(str(session.get("manager_id")))
            if assigned_to_oid not in manager_agent_ids and str(assigned_to_oid) not in manager_agent_ids:
                return jsonify(ok=False, message="Invalid agent selection"), 403

        assigned_to_name = a_doc.get("name", "")
        status = "Assigned"

    doc: Dict[str, Any] = {
        "ticket_no": ticket_no,
        "date_reported": date_reported,
        "branch": branch,
        "channel": channel if channel in CHANNELS else channel or "Call",
        "customer_name": customer_name,
        "customer_phone": customer_phone,
        "issue_type": issue_type if issue_type in ISSUE_TYPES else issue_type,
        "description": description,

        "customer_id": customer_id,  # link complaint to customer when known

        "assigned_to_id": assigned_to_oid,
        "assigned_to_name": assigned_to_name,

        "status": status,
        "resolution_notes": resolution_notes,
        "customer_feedback": customer_feedback,

        "date_closed": date_closed,
        "sla_days": sla_days,
        "sla_due": sla_due,
        "sla_breached": breached,

        "created_by": _current_user_id(),
        "created_by_role": role,
        "created_at": datetime.utcnow(),
        "updated_at": datetime.utcnow(),

        # SMS audit trail
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
        assigned_to_id=str(assigned_to_oid) if assigned_to_oid else None,
        customer_id=str(customer_id) if customer_id else None,
        status=status,
        sla_due=sla_due.strftime("%Y-%m-%d"),
        sla_breached=breached,
        description=description,
        sms_result=sms_result,
    )


@complaints_bp.get("/debug_last_assigned")
def complaints_debug_last_assigned():
    if not _admin_required():
        return jsonify(ok=False, message="Unauthorized"), 401

    agent_id = (request.args.get("agent_id") or "").strip()
    if not agent_id:
        return jsonify(ok=False, message="agent_id required"), 400

    agent_oid = _safe_oid(agent_id)
    match_values = [agent_id]
    if agent_oid:
        match_values.append(agent_oid)
        match_values.append(str(agent_oid))

    rows = list(
        complaints_col.find({"assigned_to_id": {"$in": match_values}})
        .sort([("date_reported", -1)])
        .limit(10)
    )
    out = []
    for row in rows:
        dr = row.get("date_reported")
        date_reported = dr.strftime("%Y-%m-%d") if isinstance(dr, datetime) else dr
        out.append({
            "id": str(row.get("_id")),
            "ticket_no": row.get("ticket_no"),
            "status": row.get("status"),
            "assigned_to_id": str(row.get("assigned_to_id")) if row.get("assigned_to_id") else None,
            "assigned_to_name": row.get("assigned_to_name", ""),
            "date_reported": date_reported,
        })

    return jsonify(ok=True, complaints=out)


# ===== Detail (for “See details” modal) =====
@complaints_bp.get("/<cid>/detail")
def complaint_detail(cid):
    if not _admin_required():
        return jsonify(ok=False, message="Unauthorized"), 401

    try:
        oid = ObjectId(cid)
    except Exception:
        return jsonify(ok=False, message="Invalid id"), 400

    doc = complaints_col.find_one(_scoped_complaint_query(oid))
    if not doc:
        return jsonify(ok=False, message="Not found"), 404

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
        "customer_id": str(doc.get("customer_id")) if doc.get("customer_id") else None,
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
@audit_action("complaint.updated", "Updated Complaint", entity_type="complaint", entity_id_from="cid")
def update_fields(cid):
    if not _admin_required():
        return jsonify(ok=False, message="Unauthorized"), 401

    try:
        oid = ObjectId(cid)
    except Exception:
        return jsonify(ok=False, message="Invalid id"), 400

    fields: Dict[str, Any] = {}
    if "description" in request.form:
        fields["description"] = (request.form.get("description") or "").strip()
    if "resolution_notes" in request.form:
        fields["resolution_notes"] = (request.form.get("resolution_notes") or "").strip()
    if "customer_feedback" in request.form:
        fields["customer_feedback"] = (request.form.get("customer_feedback") or "").strip()

    if not fields:
        return jsonify(ok=False, message="No updatable fields provided"), 400

    doc = complaints_col.find_one(_scoped_complaint_query(oid))
    if not doc:
        return jsonify(ok=False, message="Not found"), 404

    fields["updated_at"] = datetime.utcnow()
    complaints_col.update_one(_scoped_complaint_query(oid), {"$set": fields})
    return jsonify(ok=True)


# ===== Quick status update (AJAX) =====
@complaints_bp.post("/<cid>/status")
@audit_action("complaint.status_updated", "Updated Complaint Status", entity_type="complaint", entity_id_from="cid")
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
    update: Dict[str, Any] = {"$set": {
        "status": status,
        "resolution_notes": notes,
        "customer_feedback": feedback,
        "updated_at": now,
    }}

    doc = complaints_col.find_one(_scoped_complaint_query(oid))
    if not doc:
        return jsonify(ok=False, message="Not found"), 404

    sla_due = doc.get("sla_due")
    if isinstance(sla_due, str):
        sla_due = _parse_date(sla_due)

    if status in RESOLVED_STATUSES:
        update["$set"]["date_closed"] = now
        update["$set"]["sla_breached"] = _breached(sla_due, now)
    else:
        update["$set"]["date_closed"] = None
        update["$set"]["sla_breached"] = _breached(sla_due, None)

    complaints_col.update_one(_scoped_complaint_query(oid), update)
    return jsonify(ok=True)


# ===== Manual SMS notify (AJAX button in modal) =====
@complaints_bp.post("/<cid>/notify")
@audit_action("complaint.notified", "Notified Complaint", entity_type="complaint", entity_id_from="cid")
def notify_customer(cid):
    if not _admin_required():
        return jsonify(ok=False, message="Unauthorized"), 401

    try:
        oid = ObjectId(cid)
    except Exception:
        return jsonify(ok=False, message="Invalid id"), 400

    doc = complaints_col.find_one(_scoped_complaint_query(oid))
    if not doc:
        return jsonify(ok=False, message="Not found"), 404

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
    complaints_col.update_one(_scoped_complaint_query(oid), {"$push": {"sms_events": event}})

    return jsonify(ok=ok, status=status_txt)


# ===== Executive view (read-only) =====
@complaints_bp.get("/executive")
def executive_view():
    # Allow admin or executive (adjust gate as needed)
    if "admin_id" not in session and "executive_id" not in session:
        return redirect(url_for("login.login"))

    q: Dict[str, Any] = {}
    status = (request.args.get("status") or "").strip()
    if status:
        q["status"] = status

    issue_type = (request.args.get("issue_type") or "").strip()
    if issue_type:
        q["issue_type"] = issue_type

    channel = (request.args.get("channel") or "").strip()
    if channel:
        q["channel"] = channel

    assigned_to_id = (request.args.get("assigned_to_id") or "").strip()
    if assigned_to_id:
        try:
            q["assigned_to_id"] = ObjectId(assigned_to_id)
        except Exception:
            pass

    customer_phone = (request.args.get("customer_phone") or "").strip()
    if customer_phone:
        q["customer_phone"] = {"$regex": customer_phone, "$options": "i"}

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

    def _pdate(d: Optional[str]) -> Optional[datetime]:
        try:
            return datetime.strptime(d, "%Y-%m-%d") if d else None
        except Exception:
            return None

    period = request.args.get("range") or "custom"
    quick_from, quick_to, active_range = _quick_range(period)

    dfrom = _pdate(request.args.get("from")) or quick_from
    dto   = _pdate(request.args.get("to")) or quick_to
    if dfrom or dto:
        q["date_reported"] = {}
        if dfrom: q["date_reported"]["$gte"] = dfrom
        if dto:   q["date_reported"]["$lte"] = dto

    items = list(complaints_col.find(q).sort([("date_reported", -1)]).limit(800))

    open_count = complaints_col.count_documents({"status": {"$in": OPEN_STATUSES}})
    breaching  = complaints_col.count_documents({
        "sla_due": {"$lt": datetime.utcnow()},
        "status": {"$nin": RESOLVED_STATUSES}
    })
    resolved_all = complaints_col.count_documents({"status": {"$in": RESOLVED_STATUSES}})

    resolved_in_range = None
    if dfrom or dto:
        closed_query: Dict[str, Any] = {"status": {"$in": RESOLVED_STATUSES}}
        if dfrom or dto:
            closed_query["date_closed"] = {}
            if dfrom: closed_query["date_closed"]["$gte"] = dfrom
            if dto:   closed_query["date_closed"]["$lte"] = dto
        resolved_in_range = complaints_col.count_documents(closed_query)

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
            "from": request.args.get("from", ""),
            "to": request.args.get("to", ""),
            "range": active_range,
            "assigned_to_id": assigned_to_id,
            "customer_phone": customer_phone,
        },
        stats={
            "open": open_count,
            "breaching": breaching,
            "resolved": resolved_all,
            "resolved_in_range": resolved_in_range,
        },
        today=datetime.utcnow().strftime("%Y-%m-%d"),
    )


# ===== Lightweight badge feed for sidebar =====
@complaints_bp.get("/executive_unresolved_count")
def executive_unresolved_count():
    if "admin_id" not in session and "executive_id" not in session:
        return jsonify(ok=False, message="Unauthorized"), 401

    open_count = complaints_col.count_documents({"status": {"$in": OPEN_STATUSES}})
    breaching   = complaints_col.count_documents({
        "sla_due": {"$lt": datetime.utcnow()},
        "status": {"$nin": RESOLVED_STATUSES}
    })

    return jsonify(ok=True, open=open_count, breaching=breaching)


# ===== History view: resolved / closed issues only =====
@complaints_bp.get("/history")
def complaints_history():
    """
    Show only Resolved / Closed issues.
    Supports same filters: q, assigned_to_id, customer_phone, from/to, quick range.
    Ideal for a 'History / Resolved' tab.
    """
    if not _admin_required():
        return redirect(url_for("login.login"))

    q: Dict[str, Any] = {"status": {"$in": RESOLVED_STATUSES}}

    assigned_to_id = (request.args.get("assigned_to_id") or "").strip()
    if assigned_to_id:
        try:
            q["assigned_to_id"] = ObjectId(assigned_to_id)
        except Exception:
            pass

    customer_phone = (request.args.get("customer_phone") or "").strip()
    if customer_phone:
        q["customer_phone"] = {"$regex": customer_phone, "$options": "i"}

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

    period = request.args.get("range") or "custom"
    quick_from, quick_to, active_range = _quick_range(period)

    dfrom = _parse_date(request.args.get("from")) or quick_from
    dto   = _parse_date(request.args.get("to")) or quick_to
    if dfrom or dto:
        q["date_closed"] = {}
        if dfrom: q["date_closed"]["$gte"] = dfrom
        if dto:   q["date_closed"]["$lte"] = dto

    items = list(complaints_col.find(q).sort([("date_closed", -1)]).limit(500))

    for it in items:
        it["_id"] = str(it["_id"])
        for k in ("date_reported", "date_closed", "sla_due", "created_at", "updated_at"):
            if isinstance(it.get(k), datetime):
                it[k] = it[k].strftime("%Y-%m-%d")

    return render_template(
        "complaints_history.html",
        items=items,
        filters={
            "q": term,
            "from": request.args.get("from", ""),
            "to": request.args.get("to", ""),
            "range": active_range,
            "assigned_to_id": assigned_to_id,
            "customer_phone": customer_phone,
        },
        today=datetime.utcnow().strftime("%Y-%m-%d"),
    )


# ===== Customer Activity Timeline (payments + packages + complaints + manual) =====
@complaints_bp.get("/customer_activity")
def customer_activity():
    """
    Return a merged activity timeline for a customer:
      - Payments (payments_col)
      - Packaging submissions (packages_col)
      - Complaints (complaints_col)
      - Manual activities (customer_activities_col)
    Search by customer_id OR phone.

    Works even if:
      - There is no customer doc, but activities exist for that phone.
      - Customer has no complaints (only payments/packages/manual).

    Response: JSON => { ok, customer, activities: [...] }
    """
    if not _admin_required():
        return jsonify(ok=False, message="Unauthorized"), 401

    customer_id_str = (request.args.get("customer_id") or "").strip()
    phone = (request.args.get("phone") or "").strip()

    customer = None
    cust_oid: Optional[ObjectId] = None

    # 1) Try by customer_id
    if customer_id_str:
        try:
            cust_oid = ObjectId(customer_id_str)
            customer = customers_col.find_one({"_id": cust_oid})
        except Exception:
            customer = None

    # 2) Fallback: try by phone in customers collection
    if not customer and phone:
        # match whole, or ending digits
        customer = customers_col.find_one({
            "phone_number": {"$regex": phone + "$"}
        })
        if customer:
            cust_oid = customer["_id"]

    # We'll still attempt activities by phone if no customer exists
    # phone_number used for regex; prefer canonical from customer if present
    phone_number = (customer.get("phone_number") if customer else None) or phone or None

    activities: List[Dict[str, Any]] = []

    # --- Payments ---
    pay_query: Dict[str, Any] = {}
    if cust_oid:
        pay_query["customer_id"] = cust_oid
    elif phone_number:
        # Optional fallback: in case payments store phone
        pay_query["customer_phone"] = {"$regex": phone_number + "$"}

    if pay_query:
        pay_cursor = payments_col.find(pay_query).sort([("date", 1), ("time", 1)])
        for p in pay_cursor:
            date_str = p.get("date") or ""
            time_str = p.get("time") or "00:00:00"
            try:
                ts = datetime.strptime(f"{date_str} {time_str}", "%Y-%m-%d %H:%M:%S")
            except Exception:
                ts = _parse_date(date_str) or datetime.utcnow()

            activities.append({
                "type": "payment",
                "timestamp": ts.isoformat(),
                "date": date_str,
                "time": time_str,
                "amount": float(p.get("amount", 0) or 0),
                "method": p.get("method", ""),
                "payment_type": p.get("payment_type", ""),
                "product_index": p.get("product_index"),
                "product_name": p.get("product_name"),
                "note": p.get("note") if isinstance(p.get("note"), str) else "",
            })

    # --- Packages ---
    pkg_query: Dict[str, Any] = {}
    if cust_oid:
        pkg_query["customer_id"] = cust_oid
    elif phone_number:
        # Optional: if packages store phone
        pkg_query["customer_phone"] = {"$regex": phone_number + "$"}

    if pkg_query:
        pkg_cursor = packages_col.find(pkg_query).sort([("submitted_at", 1)])
        for pkg in pkg_cursor:
            ts = pkg.get("submitted_at") or datetime.utcnow()
            if isinstance(ts, str):
                try:
                    ts = datetime.fromisoformat(ts)
                except Exception:
                    ts = datetime.utcnow()

            product_info = pkg.get("product", {}) or {}
            meta = pkg.get("purchase_meta", {}) or {}

            activities.append({
                "type": "package",
                "timestamp": ts.isoformat(),
                "submitted_at": ts.isoformat(),
                "status": pkg.get("status", "submitted"),
                "source": pkg.get("source", "customer_profile"),
                "product_name": product_info.get("name"),
                "product_total": float(product_info.get("total", 0) or 0),
                "purchase_type": meta.get("purchase_type"),
                "purchase_date": meta.get("purchase_date"),
                "end_date": meta.get("end_date"),
            })

    # --- Complaints (by customer_id AND/OR phone) ---
    complaint_query: Dict[str, Any] = {}
    ors: List[Dict[str, Any]] = []

    if cust_oid:
        ors.append({"customer_id": cust_oid})
    if phone_number:
        ors.append({"customer_phone": {"$regex": phone_number + "$"}})

    if ors:
        complaint_query["$or"] = ors
        cmp_cursor = complaints_col.find(complaint_query).sort([("date_reported", 1)])
        for c in cmp_cursor:
            dt = c.get("date_reported") or datetime.utcnow()
            if isinstance(dt, str):
                dt = _parse_date(dt) or datetime.utcnow()

            sla_due_val = c.get("sla_due")
            if isinstance(sla_due_val, datetime):
                sla_due_str = sla_due_val.strftime("%Y-%m-%d")
            elif isinstance(sla_due_val, str):
                sla_due_str = sla_due_val
            else:
                sla_due_str = None

            activities.append({
                "type": "complaint",
                "timestamp": dt.isoformat(),
                "ticket_no": c.get("ticket_no"),
                "date_reported": dt.strftime("%Y-%m-%d"),
                "issue_type": c.get("issue_type"),
                "status": c.get("status"),
                "channel": c.get("channel"),
                "description": c.get("description"),
                "assigned_to_name": c.get("assigned_to_name"),
                "sla_due": sla_due_str,
                "sla_breached": bool(c.get("sla_breached", False)),
            })

    # --- Manual / admin-logged activities ---
    manual_query: Dict[str, Any] = {}
    manual_ors: List[Dict[str, Any]] = []
    if cust_oid:
        manual_ors.append({"customer_id": cust_oid})
    if phone_number:
        manual_ors.append({"customer_phone": {"$regex": phone_number + "$"}})
    if manual_ors:
        manual_query["$or"] = manual_ors
        manual_cursor = customer_activities_col.find(manual_query).sort([("created_at", 1)])
        for a in manual_cursor:
            ts = a.get("created_at") or datetime.utcnow()
            if isinstance(ts, str):
                try:
                    ts = datetime.fromisoformat(ts)
                except Exception:
                    ts = datetime.utcnow()

            activities.append({
                "type": a.get("activity_type", "manual"),
                "timestamp": ts.isoformat(),
                "title": a.get("title", "Manual activity"),
                "note": a.get("note", ""),
                "created_at": ts.strftime("%Y-%m-%d %H:%M:%S"),
                "created_by": a.get("created_by"),
                "created_by_name": a.get("created_by_name", ""),
            })

    # If we still have no customer doc but activities exist,
    # build a lightweight virtual customer from first activity.
    if not customer and activities:
        first = activities[0]
        # Try to infer a name from complaints if any
        name = ""
        if complaint_query.get("$or"):
            first_cmp = complaints_col.find_one(complaint_query)
            if first_cmp:
                name = first_cmp.get("customer_name", "") or ""

        customer = {
            "_id": None,
            "name": name,
            "phone_number": phone_number,
            "branch": None,
        }
        cust_oid = None  # no real ID in DB

    # If no customer and no activities → nothing to show
    if not customer and not activities:
        return jsonify(ok=False, message="No data found for this customer/phone"), 404

    # Final sort (oldest → newest)
    activities.sort(key=lambda a: a.get("timestamp", ""))

    cust_payload = {
        "id": str(cust_oid) if cust_oid else None,
        "name": customer.get("name", "") if customer else "",
        "phone_number": customer.get("phone_number") if customer else phone_number,
        "branch": customer.get("branch") if customer else None,
    }

    return jsonify(ok=True, customer=cust_payload, activities=activities)


# ===== Create Manual Activity (AJAX) =====
@complaints_bp.post("/customer_activity/manual")
@audit_action("customer.activity_logged", "Logged Customer Activity", entity_type="customer")
def customer_activity_manual():
    """
    Create a manual activity for a customer.
    Accepts:
      - customer_id (optional)
      - phone (optional, but required if no customer_id)
      - title (short label)
      - note (details / comment)
      - activity_type (optional, defaults to 'manual')
    """
    if not _admin_required():
        return jsonify(ok=False, message="Unauthorized"), 401

    customer_id_str = (request.form.get("customer_id") or "").strip()
    phone = (request.form.get("phone") or "").strip()
    title = (request.form.get("title") or "").strip() or "Manual activity"
    note = (request.form.get("note") or "").strip()
    activity_type = (request.form.get("activity_type") or "").strip() or "manual"

    cust_oid: Optional[ObjectId] = None
    if customer_id_str:
        try:
            cust_oid = ObjectId(customer_id_str)
        except Exception:
            return jsonify(ok=False, message="Invalid customer_id"), 400

    if not cust_oid and not phone:
        return jsonify(ok=False, message="Provide at least customer_id or phone"), 400

    created_at = datetime.utcnow()
    created_by = _current_user_id()
    created_by_role = _current_role()

    # Optional: resolve user name if stored in users_col
    created_by_name = ""
    try:
        if created_by:
            user_oid = ObjectId(created_by)
            user_doc = users_col.find_one({"_id": user_oid})
            if user_doc:
                created_by_name = user_doc.get("name", "") or ""
    except Exception:
        created_by_name = ""

    doc: Dict[str, Any] = {
        "customer_id": cust_oid,
        "customer_phone": phone or None,
        "activity_type": activity_type,
        "title": title,
        "note": note,
        "created_at": created_at,
        "created_by": created_by,
        "created_by_role": created_by_role,
        "created_by_name": created_by_name,
    }

    ins = customer_activities_col.insert_one(doc)

    payload = {
        "id": str(ins.inserted_id),
        "type": activity_type,
        "timestamp": created_at.isoformat(),
        "title": title,
        "note": note,
        "created_at": created_at.strftime("%Y-%m-%d %H:%M:%S"),
        "created_by": created_by,
        "created_by_name": created_by_name,
    }
    return jsonify(ok=True, activity=payload)

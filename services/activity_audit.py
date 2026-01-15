from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Optional, Tuple

from flask import g, request as flask_request

from db import db
from login import get_current_identity, _parse_device

activity_logs_col = db["activity_logs"]


def ensure_activity_log_indexes() -> None:
    try:
        activity_logs_col.create_index([("user_id", 1), ("timestamp", -1)])
        activity_logs_col.create_index([("day", 1), ("user_id", 1)])
        activity_logs_col.create_index([("month", 1), ("user_id", 1)])
        activity_logs_col.create_index([("action", 1), ("timestamp", -1)])
    except Exception:
        pass


def _now_utc() -> datetime:
    return datetime.utcnow()


def _date_key(ts: datetime) -> Tuple[str, str]:
    day = ts.strftime("%Y-%m-%d")
    month = ts.strftime("%Y-%m")
    return day, month


def _request_identity(req) -> Tuple[str, str, str]:
    ip = req.headers.get("X-Forwarded-For", "").split(",")[0].strip() or req.remote_addr
    user_agent = req.headers.get("User-Agent")
    return ip, user_agent, (user_agent or "")


def _safe_meta_from_request(req) -> Dict[str, Any]:
    allowlist = {
        "amount",
        "branch",
        "customer_name",
        "customer",
        "payment_type",
        "status",
        "reference",
        "title",
        "name",
        "note",
    }
    meta: Dict[str, Any] = {}
    try:
        data = {}
        if req.is_json:
            data = req.get_json(silent=True) or {}
        elif req.form:
            data = req.form.to_dict()
        for key in allowlist:
            if key in data and data[key] not in (None, ""):
                meta[key] = data[key]
    except Exception:
        return {}
    return meta


def log_activity(
    action: str,
    action_label: str,
    entity_type: Optional[str] = None,
    entity_id: Optional[str] = None,
    meta: Optional[Dict[str, Any]] = None,
    req=None,
) -> Optional[str]:
    """
    Writes an activity log. Avoid sensitive payloads in meta.
    """
    ident = get_current_identity()
    if not ident.get("is_authenticated"):
        return None

    req_obj = req or flask_request
    timestamp = _now_utc()
    day, month = _date_key(timestamp)

    ip = None
    user_agent = None
    device = None
    if req_obj:
        ip, user_agent, ua_raw = _request_identity(req_obj)
        device = _parse_device(ua_raw)

    doc = {
        "user_id": str(ident.get("user_id") or ""),
        "username": ident.get("name") or "",
        "role": ident.get("role") or "",
        "action": action,
        "action_label": action_label,
        "entity_type": entity_type,
        "entity_id": entity_id,
        "meta": meta or {},
        "ip": ip,
        "user_agent": user_agent,
        "device": device,
        "timestamp": timestamp,
        "day": day,
        "month": month,
    }

    try:
        res = activity_logs_col.insert_one(doc)
        g.activity_logged = True
        return str(res.inserted_id)
    except Exception:
        return None


def resolve_action_for_request(req) -> Tuple[str, str, Optional[str]]:
    """
    Best-effort action resolver for auto audit logging.
    """
    path = (req.path or "").lower()
    endpoint = (req.endpoint or "").lower()
    corpus = f"{path} {endpoint}"

    entity_map = [
        ("customer", "Customer", ["customer", "customers"]),
        ("payment", "Payment", ["payment", "payments", "pay"]),
        ("invoice", "Invoice", ["invoice", "invoices"]),
        ("bill", "Bill", ["bill", "bills"]),
        ("expense", "Expense", ["expense", "expenses"]),
        ("journal", "Journal", ["journal", "journals"]),
        ("bank", "Bank", ["bank", "bank-accounts", "recon"]),
        ("loan", "Loan", ["loan", "loans"]),
        ("payroll", "Payroll", ["payroll"]),
        ("inventory", "Inventory", ["inventory", "stock"]),
        ("issue", "Issue", ["issue", "issues"]),
        ("complaint", "Complaint", ["complaint", "complaints"]),
        ("hr_case", "HR Case", ["case", "cases"]),
        ("hr_exit", "HR Exit", ["exit", "exits"]),
        ("reminder", "Reminder", ["reminder", "reminders"]),
        ("role", "Role", ["role", "roles"]),
        ("employee", "Employee", ["employee", "employees"]),
        ("order", "Order", ["order", "orders"]),
        ("product", "Product", ["product", "products"]),
        ("account", "Account", ["account", "accounts", "accounting"]),
        ("budget", "Budget", ["budget"]),
        ("asset", "Asset", ["asset", "assets"]),
        ("susu", "Susu", ["susu"]),
        ("transfer", "Transfer", ["transfer"]),
    ]

    entity_type = None
    entity_label = "Record"
    for ent, label, keys in entity_map:
        if any(k in corpus for k in keys):
            entity_type = ent
            entity_label = label
            break

    verb_map = [
        ("approve", "approved"),
        ("reject", "rejected"),
        ("close", "closed"),
        ("cancel", "cancelled"),
        ("delete", "deleted"),
        ("remove", "deleted"),
        ("toggle", "toggled"),
        ("transfer", "transferred"),
        ("withdraw", "withdrawn"),
        ("post", "posted"),
        ("import", "imported"),
        ("export", "exported"),
        ("upload", "uploaded"),
        ("create", "created"),
        ("add", "created"),
        ("new", "created"),
        ("quick", "created"),
        ("update", "updated"),
        ("edit", "updated"),
        ("set", "updated"),
        ("status", "updated"),
        ("mark", "updated"),
    ]

    verb = "updated"
    for key, out in verb_map:
        if key in corpus:
            verb = out
            break

    if entity_type:
        action = f"{entity_type}.{verb}"
        label = f"{verb.capitalize()} {entity_label}"
        return action, label, entity_type

    if endpoint:
        action = f"mutation.{endpoint.replace('.', '_')}"
        label = f"Updated via {endpoint}"
    else:
        action = "mutation.request"
        label = "Updated Record"
    return action, label, None


def should_log_request(req, response) -> bool:
    if getattr(g, "activity_logged", False):
        return False
    if req.method in ("GET", "HEAD", "OPTIONS"):
        return False
    if response and response.status_code >= 400:
        return False
    if not req.endpoint:
        return False
    if req.endpoint.startswith("static"):
        return False
    if req.endpoint in ("login.login", "login.logout"):
        return False
    return True


def audit_request(req, response) -> None:
    if not should_log_request(req, response):
        return
    action, label, entity_type = resolve_action_for_request(req)
    entity_id = None
    for key, val in (req.view_args or {}).items():
        if key.endswith("_id") or key in ("id", "code"):
            entity_id = str(val)
            break
    meta = _safe_meta_from_request(req)
    log_activity(
        action=action,
        action_label=label,
        entity_type=entity_type,
        entity_id=entity_id,
        meta={**meta, "path": req.path, "method": req.method},
        req=req,
    )


def audit_action(action: str, label: str, entity_type: Optional[str] = None, entity_id_from: Optional[str] = None):
    """
    Decorator for explicit audit logging on high-value actions.
    """
    def decorator(fn):
        def wrapper(*args, **kwargs):
            resp = fn(*args, **kwargs)
            if flask_request.method in ("GET", "HEAD", "OPTIONS"):
                return resp
            if getattr(g, "activity_logged", False):
                return resp
            entity_id = None
            if entity_id_from:
                entity_id = kwargs.get(entity_id_from)
                if entity_id is None:
                    entity_id = flask_request.form.get(entity_id_from) or flask_request.args.get(entity_id_from)
            meta = _safe_meta_from_request(flask_request)
            log_activity(
                action=action,
                action_label=label,
                entity_type=entity_type,
                entity_id=str(entity_id) if entity_id else None,
                meta={**meta, "path": flask_request.path, "method": flask_request.method},
                req=flask_request,
            )
            return resp
        wrapper.__name__ = fn.__name__
        return wrapper
    return decorator

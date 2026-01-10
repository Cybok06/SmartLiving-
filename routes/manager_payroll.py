from __future__ import annotations

from flask import Blueprint, render_template, session, redirect, url_for, flash, jsonify, request
from bson import ObjectId
from datetime import datetime
from db import db

manager_payroll_bp = Blueprint("manager_payroll", __name__, url_prefix="/manager/payroll")

users_col    = db["users"]
payrolls_col = db["payrolls"]
payroll_deductions_col = db["payroll_deductions"]

# =========================
# Indexes
# =========================
def _ensure_indexes():
    try:
        payrolls_col.create_index([("manager_id", 1), ("payroll_month", 1)], unique=True)
        payrolls_col.create_index([("manager_id", 1), ("created_at", -1)])
        payrolls_col.create_index([("status", 1), ("updated_at", -1)])
        users_col.create_index([("manager_id", 1), ("role", 1)])
    except Exception:
        pass

_ensure_indexes()

ALLOWED_EDIT_STATUSES = {"Draft", "Rejected"}

# =========================
# Helpers
# =========================
def _now():
    return datetime.utcnow()

def _require_manager():
    if "manager_id" not in session:
        flash("Access denied. Please log in as a manager.", "danger")
        return None
    try:
        mid = ObjectId(session["manager_id"])
    except Exception:
        flash("Access denied. Invalid session.", "danger")
        return None

    manager = users_col.find_one({"_id": mid})
    if not manager or manager.get("role") != "manager":
        flash("Access denied. Manager not found.", "danger")
        return None
    return manager

def _month_label(yyyy_mm: str) -> str:
    try:
        y, m = yyyy_mm.split("-")
        m = int(m)
        months = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
        return f"{months[m-1]} {y}"
    except Exception:
        return yyyy_mm

def _normalize_month(month: str) -> str:
    if not month or len(month) != 7 or month[4] != "-":
        raise ValueError("Invalid month format. Use YYYY-MM.")
    y = int(month[:4])
    m = int(month[5:7])
    if m < 1 or m > 12:
        raise ValueError("Invalid month.")
    if y < 2000 or y > 2100:
        raise ValueError("Invalid year.")
    return month

def _agents_under_manager(manager_oid: ObjectId):
    return list(users_col.find(
        {"role": "agent", "manager_id": manager_oid, "status": "Active"},
        {"_id": 1, "name": 1, "image_url": 1, "branch": 1, "status": 1}
    ).sort("name", 1))

def _to_float(x, default=0.0):
    try:
        if x is None or x == "":
            return default
        return float(x)
    except Exception:
        return default

def _safe_oid(s: str):
    try:
        return ObjectId(s)
    except Exception:
        return None

def _load_deductions(employee_id: str, month: str):
    cur = payroll_deductions_col.find(
        {"employee_id": employee_id, "month": month},
        {"amount": 1, "reason": 1, "date": 1},
    ).sort("created_at", 1)
    out = []
    for d in cur:
        out.append({
            "deduction_id": str(d.get("_id")),
            "amount": float(d.get("amount") or 0),
            "reason": d.get("reason") or "",
            "date": d.get("date") or "",
        })
    return out

# =========================
# Payroll Calculations
# =========================
def _gross_from_item(it: dict) -> float:
    """Gross used for HR-final value: if hr_amount exists use it, else manager_amount."""
    ma = _to_float(it.get("manager_amount"), 0.0)
    hr_amt = it.get("hr_amount", None)
    if hr_amt is None or hr_amt == "":
        return max(0.0, ma)
    return max(0.0, _to_float(hr_amt, 0.0))

def _sum_item_deds(it: dict) -> float:
    total = 0.0
    for d in (it.get("deductions") or []):
        amt = _to_float(d.get("amount"), 0.0)
        if amt < 0:
            amt = 0.0
        total += amt
    return total

def _sum_global_deds(gross: float, global_deds: list[dict]) -> float:
    total = 0.0
    for d in (global_deds or []):
        dtype = (d.get("type") or "fixed").lower().strip()
        val = _to_float(d.get("value"), 0.0)
        if val < 0:
            val = 0.0
        if dtype == "percent":
            total += (gross * (val / 100.0))
        else:
            total += val
    return total

def _compute_totals(items: list[dict], global_deds=None):
    items = items or []
    global_deds = global_deds or []

    submitted_total = 0.0
    final_total = 0.0
    net_final_total = 0.0
    edited_count = 0

    for it in items:
        ma = _to_float(it.get("manager_amount"), 0.0)
        submitted_total += max(0.0, ma)

        gross = _gross_from_item(it)
        final_total += gross

        item_ded = _sum_item_deds(it)
        global_ded = _sum_global_deds(gross, global_deds)
        net = max(0.0, gross - item_ded - global_ded)
        net_final_total += net

        if it.get("changed_by_hr"):
            edited_count += 1

    return {
        "submitted_total": round(submitted_total, 2),
        "final_total": round(final_total, 2),
        "net_final_total": round(net_final_total, 2),
        "edited_count": int(edited_count),
        "agent_count": int(len(items))
    }

def _decorate_rows_for_manager(rows: list[dict], global_deds: list[dict]) -> list[dict]:
    """
    Add: gross_final, item_deductions_total, global_deductions_total, net_pay
    so manager can see HR impact clearly.
    """
    out = []
    for r in (rows or []):
        gross = _gross_from_item(r)
        item_ded = _sum_item_deds(r)
        global_ded = _sum_global_deds(gross, global_deds)
        net = max(0.0, gross - item_ded - global_ded)

        rr = dict(r)
        rr["gross_final"] = round(gross, 2)
        rr["item_deductions_total"] = round(item_ded, 2)
        rr["global_deductions_total"] = round(global_ded, 2)
        rr["net_pay"] = round(net, 2)
        out.append(rr)
    return out

def _inject_deductions_for_month(items: list[dict], month: str) -> list[dict]:
    out = []
    for it in (items or []):
        aid = str(it.get("agent_id") or "")
        it2 = dict(it)
        if aid and month:
            it2["deductions"] = _load_deductions(aid, month)
        else:
            it2["deductions"] = it2.get("deductions") or []
        out.append(it2)
    return out

# =========================
# Views
# =========================
@manager_payroll_bp.route("", methods=["GET"])
def payroll_home():
    manager = _require_manager()
    if not manager:
        return redirect(url_for("login.login"))

    return render_template(
        "manager_payroll.html",
        manager_name=manager.get("name", "Manager"),
        manager_id=str(manager["_id"]),
        today_iso=_now().date().isoformat()
    )

# =========================
# API: Month payload
# =========================
@manager_payroll_bp.route("/month/<month>", methods=["GET"])
def payroll_month_payload(month):
    manager = _require_manager()
    if not manager:
        return jsonify(ok=False, message="Not authorized."), 401

    try:
        month = _normalize_month(month)
    except Exception as e:
        return jsonify(ok=False, message=str(e)), 400

    manager_id_str = str(manager["_id"])
    manager_oid = manager["_id"]

    payroll = payrolls_col.find_one({"manager_id": manager_id_str, "payroll_month": month})
    agents = _agents_under_manager(manager_oid)

    items_by_agent = {}
    global_deds = []
    if payroll and isinstance(payroll.get("items"), list):
        for it in payroll["items"]:
            items_by_agent[str(it.get("agent_id"))] = it
        global_deds = payroll.get("global_deductions") or []

    rows = []
    for a in agents:
        aid = str(a["_id"])
        existing = items_by_agent.get(aid, {})

        rows.append({
            "agent_id": aid,
            "agent_name": existing.get("agent_name") or a.get("name", "Agent"),
            "agent_image_url": existing.get("agent_image_url") or a.get("image_url") or "https://via.placeholder.com/64",
            "agent_branch": existing.get("agent_branch") or a.get("branch") or "",
            "agent_status": a.get("status", "Active"),

            "manager_amount": float(existing.get("manager_amount") or 0),
            "manager_note": existing.get("manager_note") or "",

            # HR visibility (manager can SEE)
            "hr_amount": existing.get("hr_amount", None),
            "hr_note": existing.get("hr_note") or "",
            "changed_by_hr": bool(existing.get("changed_by_hr", False)),
            "hr_last_edited_at": existing.get("hr_last_edited_at") if existing else None,

            # Deductions visibility (manager can SEE)
            "deductions": _load_deductions(aid, month),
        })

    status = payroll.get("status") if payroll else "Draft"
    editable = (not payroll) or (status in ALLOWED_EDIT_STATUSES)

    # Decorate with net pay fields for manager dashboard
    rows = _inject_deductions_for_month(rows, month)
    decorated_rows = _decorate_rows_for_manager(rows, global_deds)

    totals = _compute_totals(rows, global_deds)

    payload = {
        "ok": True,
        "month": month,
        "month_label": _month_label(month),
        "payroll": {
            "id": str(payroll["_id"]) if payroll else None,
            "status": status,
            "created_at": payroll.get("created_at").isoformat() if payroll and payroll.get("created_at") else None,
            "updated_at": payroll.get("updated_at").isoformat() if payroll and payroll.get("updated_at") else None,
            "submitted_at": payroll.get("submitted_at").isoformat() if payroll and payroll.get("submitted_at") else None,

            "hr_action_at": payroll.get("hr_action_at").isoformat() if payroll and payroll.get("hr_action_at") else None,
            "hr_action_by": payroll.get("hr_action_by") if payroll else None,
            "hr_comment": payroll.get("hr_comment") if payroll else None,

            # ✅ totals now include net_final_total too
            "totals": totals,

            # ✅ show global deductions set by HR
            "global_deductions": global_deds,

            # ✅ show activity log timeline
            "activity_log": payroll.get("activity_log", []) if payroll else []
        },
        "editable": editable,
        "rows": decorated_rows,
    }

    return jsonify(payload), 200

# =========================
# API: Save draft (preserves HR edits + deductions)
# =========================
@manager_payroll_bp.route("/save", methods=["POST"])
def payroll_save():
    manager = _require_manager()
    if not manager:
        return jsonify(ok=False, message="Not authorized."), 401

    data = request.get_json(silent=True) or {}
    month = data.get("month")
    items = data.get("items", [])

    try:
        month = _normalize_month(month)
    except Exception as e:
        return jsonify(ok=False, message=str(e)), 400

    manager_id_str = str(manager["_id"])
    payroll = payrolls_col.find_one({"manager_id": manager_id_str, "payroll_month": month})

    if payroll and payroll.get("status") not in ALLOWED_EDIT_STATUSES:
        return jsonify(ok=False, message="Payroll is locked. It has already been submitted or processed."), 409

    manager_agents = {str(a["_id"]) for a in _agents_under_manager(manager["_id"])}

    clean_items = []
    for it in items:
        aid = str(it.get("agent_id") or "").strip()
        if not aid or aid not in manager_agents:
            continue

        amt = _to_float(it.get("manager_amount"), 0.0)
        if amt < 0:
            amt = 0.0

        note = (it.get("manager_note") or "").strip()
        agent_name = (it.get("agent_name") or "").strip()

        prev = None
        if payroll:
            for p in payroll.get("items", []):
                if str(p.get("agent_id")) == aid:
                    prev = p
                    break

        clean_items.append({
            "agent_id": aid,
            "agent_name": agent_name or (prev.get("agent_name") if prev else "Agent"),
            "agent_image_url": (it.get("agent_image_url") or (prev.get("agent_image_url") if prev else None) or "https://via.placeholder.com/64"),
            "agent_branch": (it.get("agent_branch") or (prev.get("agent_branch") if prev else "")),

            "manager_amount": round(amt, 2),
            "manager_note": note,

            # preserve HR & deductions (manager cannot overwrite)
            "hr_amount": (prev.get("hr_amount") if prev else None),
            "hr_note": (prev.get("hr_note") if prev else ""),
            "changed_by_hr": bool(prev.get("changed_by_hr", False)) if prev else False,
            "hr_last_edited_at": (prev.get("hr_last_edited_at") if prev else None),
            "deductions": _load_deductions(aid, month),
        })

    now = _now()
    global_deds = (payroll.get("global_deductions") if payroll else []) or []
    clean_items = _inject_deductions_for_month(clean_items, month)
    totals = _compute_totals(clean_items, global_deds)

    if payroll:
        payrolls_col.update_one(
            {"_id": payroll["_id"]},
            {
                "$set": {"items": clean_items, "totals": totals, "updated_at": now},
                "$push": {"activity_log": {"action": "manager_saved", "by": manager_id_str, "at": now, "note": "Manager saved draft"}}
            }
        )
        pid = str(payroll["_id"])
        status = payroll.get("status", "Draft")
    else:
        doc = {
            "manager_id": manager_id_str,
            "payroll_month": month,
            "status": "Draft",
            "items": clean_items,
            "global_deductions": [],
            "totals": totals,
            "created_at": now,
            "updated_at": now,
            "submitted_at": None,
            "hr_action_at": None,
            "hr_action_by": None,
            "hr_comment": None,
            "activity_log": [
                {"action": "created", "by": manager_id_str, "at": now, "note": f"Payroll created for {month}"}
            ]
        }
        ins = payrolls_col.insert_one(doc)
        pid = str(ins.inserted_id)
        status = "Draft"

    return jsonify(ok=True, message="Draft saved.", payroll_id=pid, status=status, totals=totals), 200

# =========================
# API: Submit to HR
# =========================
@manager_payroll_bp.route("/submit", methods=["POST"])
def payroll_submit():
    manager = _require_manager()
    if not manager:
        return jsonify(ok=False, message="Not authorized."), 401

    data = request.get_json(silent=True) or {}
    month = data.get("month")

    try:
        month = _normalize_month(month)
    except Exception as e:
        return jsonify(ok=False, message=str(e)), 400

    manager_id_str = str(manager["_id"])
    payroll = payrolls_col.find_one({"manager_id": manager_id_str, "payroll_month": month})

    if not payroll:
        return jsonify(ok=False, message="No draft found for this month. Save draft first."), 404

    if payroll.get("status") not in ALLOWED_EDIT_STATUSES:
        return jsonify(ok=False, message="Payroll is already submitted or processed."), 409

    items = _inject_deductions_for_month(payroll.get("items", []), month)
    if not items:
        return jsonify(ok=False, message="Payroll has no items. Add agents amounts first."), 400

    if all(float(i.get("manager_amount") or 0) <= 0 for i in items):
        return jsonify(ok=False, message="All amounts are zero. Enter at least one valid amount before submitting."), 400

    now = _now()
    totals = _compute_totals(items, payroll.get("global_deductions") or [])
    payrolls_col.update_one(
        {"_id": payroll["_id"]},
        {
            "$set": {"items": items, "totals": totals, "status": "Submitted", "submitted_at": now, "updated_at": now},
            "$push": {"activity_log": {"action": "submitted", "by": manager_id_str, "at": now, "note": "Submitted to HR"}}
        }
    )

    return jsonify(ok=True, message="Payroll submitted to HR.", status="Submitted"), 200

# =========================
# API: History list (UI calls /manager/payroll/history)
# =========================
@manager_payroll_bp.route("/history", methods=["GET"])
def payroll_history():
    manager = _require_manager()
    if not manager:
        return jsonify(ok=False, message="Not authorized."), 401

    manager_id_str = str(manager["_id"])

    rows = []
    cur = payrolls_col.find(
        {"manager_id": manager_id_str},
        {"payroll_month": 1, "status": 1, "totals": 1, "updated_at": 1, "created_at": 1}
    ).sort("payroll_month", -1).limit(36)

    for p in cur:
        totals = p.get("totals") or {}
        rows.append({
            "id": str(p["_id"]),
            "month": p.get("payroll_month"),
            "month_label": _month_label(p.get("payroll_month", "")),
            "status": p.get("status", "Draft"),
            "updated_at": p.get("updated_at").isoformat() if p.get("updated_at") else None,
            "total_submitted": float(totals.get("submitted_total", 0) or 0),
            "total_final": float(totals.get("final_total", 0) or 0),
            "total_net": float(totals.get("net_final_total", 0) or 0),
        })

    return jsonify(ok=True, rows=rows), 200

# =========================
# API: Details (UI calls /manager/payroll/details/<id>)
# =========================
@manager_payroll_bp.route("/details/<pid>", methods=["GET"])
def payroll_details(pid):
    manager = _require_manager()
    if not manager:
        return jsonify(ok=False, message="Not authorized."), 401

    oid = _safe_oid(pid)
    if not oid:
        return jsonify(ok=False, message="Invalid payroll id."), 400

    manager_id_str = str(manager["_id"])
    payroll = payrolls_col.find_one({"_id": oid, "manager_id": manager_id_str})
    if not payroll:
        return jsonify(ok=False, message="Payroll not found."), 404

    global_deds = payroll.get("global_deductions") or []
    items = _inject_deductions_for_month(payroll.get("items") or [], payroll.get("payroll_month") or "")
    totals = _compute_totals(items, global_deds)

    # decorate items for manager visibility
    for it in items:
        aid = str(it.get("agent_id") or "")
        if aid:
            it["deductions"] = _load_deductions(aid, payroll.get("payroll_month") or "")

    decorated_items = _decorate_rows_for_manager(items, global_deds)

    return jsonify(
        ok=True,
        payroll={
            "id": str(payroll["_id"]),
            "month": payroll.get("payroll_month"),
            "month_label": _month_label(payroll.get("payroll_month", "")),
            "status": payroll.get("status", "Draft"),
            "created_at": payroll.get("created_at").isoformat() if payroll.get("created_at") else None,
            "updated_at": payroll.get("updated_at").isoformat() if payroll.get("updated_at") else None,
            "submitted_at": payroll.get("submitted_at").isoformat() if payroll.get("submitted_at") else None,

            "hr_action_at": payroll.get("hr_action_at").isoformat() if payroll.get("hr_action_at") else None,
            "hr_action_by": payroll.get("hr_action_by"),
            "hr_comment": payroll.get("hr_comment"),

            "global_deductions": global_deds,
            "totals": totals,
            "items": decorated_items,
            "activity_log": payroll.get("activity_log", []),
        }
    ), 200

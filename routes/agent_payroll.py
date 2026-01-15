# routes/agent_payroll.py
from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

from flask import Blueprint, render_template, jsonify, request
from flask_login import login_required, current_user
from bson import ObjectId

from db import db

payrolls_col = db["payrolls"]
users_col = db["users"]
payroll_deductions_col = db["payroll_deductions"]

agent_payroll_bp = Blueprint(
    "agent_payroll",
    __name__,
    template_folder="../templates",
)

# Agent can see HR-reviewed payrolls too
APPROVED_STATUSES = {"Approved", "Paid", "Under Review"}


# ----------------------------
# Helpers (match HR payroll math)
# ----------------------------
def _oid(val: Any) -> Optional[ObjectId]:
    try:
        return ObjectId(str(val))
    except Exception:
        return None


def _to_float(x, default=0.0):
    try:
        if x is None or x == "":
            return default
        return float(x)
    except Exception:
        return default


def _money(n: float) -> float:
    try:
        return round(float(n or 0.0), 2)
    except Exception:
        return 0.0


def _final_gross_agent(item: dict) -> float:
    ma = _to_float(item.get("manager_amount"), 0.0)
    hr_amt = item.get("hr_amount", None)
    if hr_amt is None or hr_amt == "":
        return max(0.0, ma)
    return max(0.0, _to_float(hr_amt, 0.0))


def _apply_global_deductions(gross: float, global_deds) -> float:
    gross = max(0.0, _to_float(gross, 0.0))
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


def _sum_deductions(deds) -> float:
    total = 0.0
    for d in (deds or []):
        amt = _to_float(d.get("amount"), 0.0)
        if amt < 0:
            amt = 0.0
        total += amt
    return total


def _sum_adjustments(adjs):
    add_total = 0.0
    deduct_total = 0.0
    for a in (adjs or []):
        atype = (a.get("type") or "").strip().lower()
        amt = _to_float(a.get("amount"), 0.0)
        if amt < 0:
            amt = 0.0
        if atype == "add":
            add_total += amt
        elif atype == "deduct":
            deduct_total += amt
    return (_money(add_total), _money(deduct_total))


def _net_agent(item: dict, global_deds) -> float:
    gross = _final_gross_agent(item)
    item_deds = _sum_deductions(item.get("deductions"))
    glob_deds = _apply_global_deductions(gross, global_deds)
    add_total, deduct_total = _sum_adjustments(item.get("adjustments"))
    net = gross - item_deds - glob_deds - deduct_total + add_total
    return max(0.0, _money(net))


def _agent_guard():
    if not current_user or not getattr(current_user, "is_authenticated", False):
        return ("Unauthorized", 401)
    if (getattr(current_user, "role", "") or "").lower().strip() != "agent":
        return ("Forbidden", 403)
    return None


def _agent_items_match(agent_id_str: str) -> Dict[str, Any]:
    agent_oid = _oid(agent_id_str)
    ors = [{"items.agent_id": agent_id_str}]
    if agent_oid:
        ors.append({"items.agent_id": agent_oid})
    return {"$or": ors}


def _agent_id_variants(agent_id_str: str):
    vals = [agent_id_str]
    agent_oid = _oid(agent_id_str)
    if agent_oid:
        vals.append(agent_oid)
        vals.append(str(agent_oid))
    return vals


def _load_deductions(employee_id_str: str, month: str):
    q = {"employee_id": {"$in": _agent_id_variants(employee_id_str)}}
    if month:
        q["month"] = month
    cur = payroll_deductions_col.find(
        q,
        {"amount": 1, "reason": 1, "date": 1},
    ).sort("created_at", 1)
    out = []
    for d in cur:
        date_val = d.get("date") or ""
        if isinstance(date_val, datetime):
            date_str = date_val.strftime("%Y-%m-%d")
        else:
            date_str = str(date_val)[:10]
        out.append(
            {
                "deduction_id": str(d.get("_id")),
                "amount": float(d.get("amount") or 0),
                "reason": d.get("reason") or "",
                "date": date_str,
            }
        )
    return out


def _extract_agent_item(payroll_doc: Dict[str, Any], agent_id_str: str) -> Dict[str, Any]:
    items = payroll_doc.get("items") or []
    item = next((x for x in items if str(x.get("agent_id")) == str(agent_id_str)), None) or {}

    item = dict(item)
    item["manager_amount"] = _to_float(item.get("manager_amount"), 0.0)
    item["hr_amount"] = None if item.get("hr_amount") in [None, ""] else _to_float(item.get("hr_amount"), 0.0)
    item["deductions"] = _load_deductions(agent_id_str, payroll_doc.get("payroll_month") or "")
    item["adjustments"] = item.get("adjustments") or []

    global_deds = payroll_doc.get("global_deductions") or []

    gross_final = _final_gross_agent(item)
    net_final = _net_agent(item, global_deds)

    return {
        "agent_id": str(item.get("agent_id") or agent_id_str),
        "agent_name": item.get("agent_name", ""),
        "agent_branch": item.get("agent_branch", ""),
        "agent_image_url": item.get("agent_image_url", ""),
        "manager_amount": _money(item["manager_amount"]),
        "hr_amount": item["hr_amount"],  # can be None
        "gross_final": _money(gross_final),
        "net_final": _money(net_final),
        "item_deductions_total": _money(_sum_deductions(item.get("deductions"))),
        "global_deductions_total": _money(_apply_global_deductions(gross_final, global_deds)),
        "deductions": item.get("deductions") or [],
        "adjustments": item.get("adjustments") or [],
        "manager_note": item.get("manager_note", ""),
        "hr_note": item.get("hr_note", ""),
        "changed_by_hr": bool(item.get("changed_by_hr")),
    }


# ----------------------------
# Wages/Tips (read-only for agent)
# ----------------------------
def _serialize_wage_entry(entry: Dict[str, Any]) -> Dict[str, Any]:
    date_val = entry.get("date")
    if isinstance(date_val, datetime):
        date_str = date_val.strftime("%Y-%m-%d")
    elif isinstance(date_val, str):
        date_str = date_val[:10]
    else:
        date_str = ""
    return {
        "id": str(entry.get("_id")) if entry.get("_id") else None,
        "amount": float(entry.get("amount", 0) or 0),
        "reason": entry.get("reason", ""),
        "date": date_str,
        "created_at": entry.get("created_at").isoformat() if isinstance(entry.get("created_at"), datetime) else "",
    }


@agent_payroll_bp.route("/agent/payroll/wages_tips", methods=["GET"])
@login_required
def agent_get_wages_tips():
    guard = _agent_guard()
    if guard:
        msg, code = guard
        return jsonify(ok=False, message=msg), code

    agent_id_str = str(current_user.id)
    agent_oid = _oid(agent_id_str)

    emp = None
    if agent_oid:
        emp = users_col.find_one({"_id": agent_oid}, {"wages_tips": 1})
    if not emp:
        emp = users_col.find_one({"_id": agent_id_str}, {"wages_tips": 1})

    if not emp:
        return jsonify(ok=False, message="Employee not found."), 404

    raw_list: List[Dict[str, Any]] = emp.get("wages_tips") or []
    items = [_serialize_wage_entry(e) for e in raw_list]
    items.sort(key=lambda x: x.get("date") or "", reverse=True)

    return jsonify(ok=True, items=items)


# -------------------------------
# Page
# -------------------------------
@agent_payroll_bp.route("/agent/payroll", methods=["GET"])
@login_required
def agent_payroll_home():
    guard = _agent_guard()
    if guard:
        msg, code = guard
        return msg, code

    agent_id_str = str(current_user.id)

    emp = None
    agent_oid = _oid(agent_id_str)
    if agent_oid:
        emp = users_col.find_one({"_id": agent_oid}, {"name": 1, "branch": 1, "image_url": 1, "phone": 1})
    if not emp:
        emp = users_col.find_one({"_id": agent_id_str}, {"name": 1, "branch": 1, "image_url": 1, "phone": 1})

    agent_name = (emp or {}).get("name") or "Agent"
    today_iso = datetime.utcnow().date().isoformat()

    return render_template(
        "agent_pages/agent_payroll.html",
        agent_name=agent_name,
        today_iso=today_iso,
    )


# -------------------------------
# JSON: months available for THIS agent (approved only)
# -------------------------------
@agent_payroll_bp.route("/agent/payroll/months", methods=["GET"])
@login_required
def agent_payroll_months():
    guard = _agent_guard()
    if guard:
        msg, code = guard
        return jsonify(ok=False, message=msg), code

    agent_id_str = str(current_user.id)

    months = payrolls_col.distinct(
        "payroll_month",
        {
            "status": {"$in": list(APPROVED_STATUSES)},
            **_agent_items_match(agent_id_str),
        },
    )
    months = sorted([m for m in months if isinstance(m, str)], reverse=True)
    return jsonify(ok=True, months=months)


# -------------------------------
# JSON: approved payroll for a month (agent view)
# -------------------------------
@agent_payroll_bp.route("/agent/payroll/month/<month>", methods=["GET"])
@login_required
def agent_month_payroll(month: str):
    guard = _agent_guard()
    if guard:
        msg, code = guard
        return jsonify(ok=False, message=msg), code

    agent_id_str = str(current_user.id)
    month = (month or "").strip()[:7]  # expects YYYY-MM

    doc = payrolls_col.find_one(
        {
            "payroll_month": month,
            "status": {"$in": list(APPROVED_STATUSES)},
            **_agent_items_match(agent_id_str),
        },
        {
            "payroll_month": 1,
            "status": 1,
            "updated_at": 1,
            "hr_action_at": 1,
            "hr_comment": 1,
            "global_deductions": 1,
            "totals": 1,
            "items": 1,
        },
    )

    if not doc:
        return jsonify(ok=True, found=False, message="No approved payroll for this month yet.")

    item = _extract_agent_item(doc, agent_id_str)

    return jsonify(
        ok=True,
        found=True,
        payroll={
            "id": str(doc.get("_id")),
            "payroll_month": doc.get("payroll_month"),
            "status": doc.get("status"),
            "updated_at": doc.get("updated_at").isoformat() if isinstance(doc.get("updated_at"), datetime) else "",
            "hr_action_at": doc.get("hr_action_at").isoformat() if isinstance(doc.get("hr_action_at"), datetime) else "",
            "hr_comment": doc.get("hr_comment") or "",
            "global_deductions": doc.get("global_deductions") or [],
            "totals": doc.get("totals") or {},
            "item": item,
        },
    )


# -------------------------------
# JSON: payroll history (approved only)
# -------------------------------
@agent_payroll_bp.route("/agent/payroll/history", methods=["GET"])
@login_required
def agent_payroll_history():
    guard = _agent_guard()
    if guard:
        msg, code = guard
        return jsonify(ok=False, message=msg), code

    agent_id_str = str(current_user.id)

    cursor = payrolls_col.find(
        {
            "status": {"$in": list(APPROVED_STATUSES)},
            **_agent_items_match(agent_id_str),
        },
        {
            "payroll_month": 1,
            "status": 1,
            "updated_at": 1,
            "global_deductions": 1,
            "items": 1,
        },
    ).sort("payroll_month", -1).limit(60)

    rows = []
    for doc in cursor:
        item = _extract_agent_item(doc, agent_id_str)
        rows.append(
            {
                "id": str(doc.get("_id")),
                "payroll_month": doc.get("payroll_month"),
                "status": doc.get("status"),
                "gross_final": item.get("gross_final", 0),
                "net_final": item.get("net_final", 0),
                "updated_at": doc.get("updated_at").isoformat() if isinstance(doc.get("updated_at"), datetime) else "",
            }
        )

    return jsonify(ok=True, rows=rows)


# -------------------------------
# JSON: deductions history (agent view)
# -------------------------------
@agent_payroll_bp.route("/agent/payroll/deductions", methods=["GET"])
@login_required
def agent_payroll_deductions():
    guard = _agent_guard()
    if guard:
        msg, code = guard
        return jsonify(ok=False, message=msg), code

    agent_id_str = str(current_user.id)
    month = (request.args.get("month") or "").strip()[:7]

    q = {"employee_id": {"$in": _agent_id_variants(agent_id_str)}}
    if month:
        q["month"] = month

    cur = payroll_deductions_col.find(
        q,
        {"amount": 1, "reason": 1, "date": 1, "month": 1, "created_at": 1},
    ).sort("created_at", -1)

    rows = []
    for d in cur:
        date_val = d.get("date") or ""
        if isinstance(date_val, datetime):
            date_str = date_val.strftime("%Y-%m-%d")
        else:
            date_str = str(date_val)[:10]
        rows.append(
            {
                "id": str(d.get("_id")),
                "month": d.get("month") or "",
                "date": date_str,
                "reason": d.get("reason") or "",
                "amount": float(d.get("amount") or 0),
                "created_at": d.get("created_at").isoformat() if isinstance(d.get("created_at"), datetime) else "",
            }
        )

    return jsonify(ok=True, rows=rows)

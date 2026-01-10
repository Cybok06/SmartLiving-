from __future__ import annotations

from flask import Blueprint, render_template, request, Response, jsonify
from datetime import date, datetime
import re
import io
import csv
import json

from db import db
from bson import ObjectId

acc_payroll_calc = Blueprint(
    "acc_payroll_calc",
    __name__,
    template_folder="../templates",
)

# Mongo collection for monthly payroll runs
payroll_col = db["payroll_runs"]
users_col = db["users"]
payroll_records_col = db["payroll_records"]
payroll_deductions_col = db["payroll_deductions"]
payroll_audit_col = db["payroll_audit_logs"]


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


def _audit(action: str, entity: str, entity_id: str = "", meta: dict | None = None):
    try:
        payroll_audit_col.insert_one({
            "action": action,
            "entity": entity,
            "entity_id": entity_id,
            "meta": meta or {},
            "created_at": datetime.utcnow(),
        })
    except Exception:
        pass


def _load_deductions(employee_id: str, month: str):
    cursor = payroll_deductions_col.find(
        {"employee_id": employee_id, "month": month},
        {"amount": 1, "reason": 1, "date": 1},
    ).sort("created_at", 1)
    rows = []
    for d in cursor:
        rows.append({
            "deduction_id": str(d.get("_id")),
            "amount": _money(_to_float(d.get("amount"), 0.0)),
            "reason": d.get("reason") or "",
            "date": (d.get("date") or ""),
        })
    return rows


def _ensure_indexes():
    try:
        payroll_records_col.create_index([("employee_id", 1), ("month", 1)], unique=True)
        payroll_deductions_col.create_index([("employee_id", 1), ("month", 1), ("created_at", -1)])
    except Exception:
        pass


_ensure_indexes()


# ------------- Pages -------------


@acc_payroll_calc.route("/payroll/calculator", methods=["GET"])
def payroll_calculator():
    """
    Payroll calculator screen.
    All entries are manual; calculations happen in the browser.

    For the default month (current month), we try to load
    an existing saved payroll run and hydrate the UI.
    """
    today = date.today()
    default_period = today.strftime("%Y-%m")  # for <input type="month">

    # Try to load existing payroll for default period
    doc = payroll_col.find_one({"period": default_period})
    payroll_data = None
    if doc:
        doc = dict(doc)
        doc.pop("_id", None)
        payroll_data = doc

    return render_template(
        "accounting/payroll_calculator.html",
        default_period=default_period,
        payroll_data=payroll_data or {},
    )


@acc_payroll_calc.route("/payroll/load", methods=["GET"])
def payroll_load():
    """
    Load payroll data for a given period (used when switching months on the UI).
    Returns JSON:
    { ok: bool, data: {period, staff, totals, signatories}, message?: str }
    """
    period = (request.args.get("period") or "").strip()
    if not period:
        return jsonify(ok=False, message="Missing period (YYYY-MM)."), 400

    doc = payroll_col.find_one({"period": period})
    if not doc:
        # no data yet, front-end will start fresh
        empty = {
            "period": period,
            "staff": [],
            "totals": {},
            "signatories": {},
        }
        return jsonify(ok=True, data=empty)

    doc = dict(doc)
    doc.pop("_id", None)
    return jsonify(ok=True, data=doc)


@acc_payroll_calc.route("/payroll/save", methods=["POST"])
def payroll_save():
    """
    Save (upsert) a monthly payroll run.
    Expects JSON body:
    {
      "period": "YYYY-MM",
      "staff": [...],
      "totals": {...},
      "signatories": {
        "prepared_by": "...",
        "checked_by": "...",
        "approved_by": "..."
      }
    }
    """
    try:
        data = request.get_json(force=True, silent=False)
    except Exception:
        return jsonify(ok=False, message="Invalid JSON body"), 400

    if not isinstance(data, dict):
        return jsonify(ok=False, message="Invalid payload format."), 400

    period = (data.get("period") or "").strip()
    staff = data.get("staff") or []
    totals = data.get("totals") or {}
    signatories = data.get("signatories") or {}

    if not period:
        return jsonify(ok=False, message="Missing payroll period (YYYY-MM)."), 400
    if not staff:
        return jsonify(ok=False, message="No staff rows to save."), 400

    now = datetime.utcnow()

    doc = {
        "period": period,
        "staff": staff,
        "totals": totals,
        "signatories": {
            "prepared_by": signatories.get("prepared_by", ""),
            "checked_by": signatories.get("checked_by", ""),
            "approved_by": signatories.get("approved_by", ""),
        },
        "updated_at": now,
    }

    payroll_col.update_one(
        {"period": period},
        {
            "$set": doc,
            "$setOnInsert": {"created_at": now},
        },
        upsert=True,
    )

    # Also upsert per-employee payroll records when employee matches are found.
    try:
        period = _normalize_month(period)
    except Exception:
        # keep main save intact even if period is invalid format for records
        return jsonify(ok=True, message=f"Payroll saved for {period}."), 200

    for row in staff:
        emp_name = (row.get("employee") or "").strip()
        emp_id = str(row.get("employee_id") or "").strip()
        if not emp_name and not emp_id:
            continue

        emp_doc = None
        if emp_id:
            emp_oid = None
            try:
                emp_oid = ObjectId(emp_id)
            except Exception:
                emp_oid = None
            if emp_oid:
                emp_doc = users_col.find_one({"_id": emp_oid})
            if not emp_doc:
                emp_doc = users_col.find_one({"_id": emp_id})
        if not emp_doc and emp_name:
            emp_doc = users_col.find_one({"username": {"$regex": f"^{re.escape(emp_name)}$", "$options": "i"}}) \
                or users_col.find_one({"name": {"$regex": f"^{re.escape(emp_name)}$", "$options": "i"}})
        if not emp_doc:
            continue

        employee_id = str(emp_doc.get("_id"))
        basic = _money(_to_float(row.get("basic"), 0.0))
        allowances = _money(_to_float(row.get("allowances"), 0.0))
        gross = _money(basic + allowances)
        ssf_employee = _money(basic * 0.055)
        taxable = _money(gross - ssf_employee)
        paye = _money(_to_float(row.get("paye"), 0.0))
        deductions = _load_deductions(employee_id, period)
        deductions_total = _money(sum(_to_float(d.get("amount"), 0.0) for d in deductions))
        net_pay = _money(gross - ssf_employee - paye - deductions_total)
        employer_13 = _money(basic * 0.13)
        total_staff_cost = _money(net_pay + employer_13)
        tier1 = _money(basic * 0.135)
        tier2 = _money(basic * 0.05)

        rec = {
            "employee_id": employee_id,
            "employee_name": emp_doc.get("name") or emp_name,
            "role": emp_doc.get("role") or "",
            "branch": emp_doc.get("branch") or "",
            "month": period,
            "basic_salary": basic,
            "allowances": allowances,
            "gross": gross,
            "ssf_employee": ssf_employee,
            "paye": paye,
            "deductions_total": deductions_total,
            "net_pay": net_pay,
            "employer_13": employer_13,
            "total_staff_cost": total_staff_cost,
            "tier1": tier1,
            "tier2": tier2,
            "deductions": deductions,
            "created_by": "accounting_payroll_calculator",
            "updated_at": now,
        }

        payroll_records_col.update_one(
            {"employee_id": employee_id, "month": period},
            {"$set": rec, "$setOnInsert": {"created_at": now}},
            upsert=True,
        )
        _audit("payroll_created", "payroll_record", entity_id=f"{employee_id}:{period}")

    return jsonify(ok=True, message=f"Payroll saved for {period}."), 200


@acc_payroll_calc.route("/payroll/records", methods=["GET"])
def payroll_records_list():
    month = (request.args.get("month") or "").strip()
    if not month:
        return jsonify(ok=False, message="Missing month (YYYY-MM)."), 400
    try:
        month = _normalize_month(month)
    except Exception as e:
        return jsonify(ok=False, message=str(e)), 400

    rows = list(
        payroll_records_col.find(
            {"month": month},
            {
                "employee_id": 1,
                "employee_name": 1,
                "role": 1,
                "branch": 1,
                "basic_salary": 1,
                "allowances": 1,
                "gross": 1,
                "ssf_employee": 1,
                "paye": 1,
                "deductions_total": 1,
                "net_pay": 1,
                "employer_13": 1,
                "total_staff_cost": 1,
                "tier1": 1,
                "tier2": 1,
                "deductions": 1,
            },
        )
    )

    totals = {
        "basic_salary": 0.0,
        "allowances": 0.0,
        "gross": 0.0,
        "ssf_employee": 0.0,
        "paye": 0.0,
        "deductions_total": 0.0,
        "net_pay": 0.0,
        "employer_13": 0.0,
        "total_staff_cost": 0.0,
        "tier1": 0.0,
        "tier2": 0.0,
    }

    out = []
    for r in rows:
        out.append({**r, "_id": str(r.get("_id"))})
        totals["basic_salary"] += _to_float(r.get("basic_salary"), 0.0)
        totals["allowances"] += _to_float(r.get("allowances"), 0.0)
        totals["gross"] += _to_float(r.get("gross"), 0.0)
        totals["ssf_employee"] += _to_float(r.get("ssf_employee"), 0.0)
        totals["paye"] += _to_float(r.get("paye"), 0.0)
        totals["deductions_total"] += _to_float(r.get("deductions_total"), 0.0)
        totals["net_pay"] += _to_float(r.get("net_pay"), 0.0)
        totals["employer_13"] += _to_float(r.get("employer_13"), 0.0)
        totals["total_staff_cost"] += _to_float(r.get("total_staff_cost"), 0.0)
        totals["tier1"] += _to_float(r.get("tier1"), 0.0)
        totals["tier2"] += _to_float(r.get("tier2"), 0.0)

    totals = {k: _money(v) for k, v in totals.items()}
    return jsonify(ok=True, month=month, rows=out, totals=totals)


@acc_payroll_calc.route("/payroll/export/csv", methods=["POST"])
def payroll_export_csv():
    """
    Export current payroll table as CSV (Excel-compatible).
    Expects a 'payload' field containing JSON:
    {
      "period": "YYYY-MM",
      "staff": [
        {
          "employee": "...",
          "basic": 0,
          "allowances": 0,
          "gross": 0,
          "ssf_employee": 0,
          "taxable": 0,
          "paye": 0,
          "net": 0,
          "employer_13": 0,
          "total_cost": 0,
          "tier1": 0,
          "tier2": 0
        }, ...
      ],
      "totals": { ... },
      "signatories": { ... }
    }

    NOTE: Export is *rows only*:
    - Header row
    - Detail rows
    - Totals row
    No extra text, no sign-off lines.
    """
    payload = request.form.get("payload")
    if not payload:
        return jsonify(ok=False, message="No data to export"), 400

    try:
        data = json.loads(payload)
    except json.JSONDecodeError:
        return jsonify(ok=False, message="Invalid data payload"), 400

    staff = data.get("staff", [])
    totals = data.get("totals", {}) or {}
    period_str = (data.get("period") or "").strip()

    period_for_filename = (
        period_str.replace("-", "_") if period_str else date.today().strftime("%Y_%m")
    )

    out = io.StringIO()
    writer = csv.writer(out)

    # Column header row ONLY
    writer.writerow([
        "Employee",
        "Basic Salary",
        "Total Allowances",
        "Gross",
        "Employee SSF (5.5%)",
        "Taxable Income",
        "PAYE",
        "Net Pay",
        "Employer SSF (13%)",
        "Total Staff Cost",
        "Tier 1 (13.5%)",
        "Tier 2 (5%)",
    ])

    # Detail rows ONLY
    for row in staff:
        writer.writerow([
            row.get("employee", ""),
            f'{row.get("basic", 0):.2f}',
            f'{row.get("allowances", 0):.2f}',
            f'{row.get("gross", 0):.2f}',
            f'{row.get("ssf_employee", 0):.2f}',
            f'{row.get("taxable", 0):.2f}',
            f'{row.get("paye", 0):.2f}',
            f'{row.get("net", 0):.2f}',
            f'{row.get("employer_13", 0):.2f}',
            f'{row.get("total_cost", 0):.2f}',
            f'{row.get("tier1", 0):.2f}',
            f'{row.get("tier2", 0):.2f}',
        ])

    # Totals row (still part of "rows only")
    if staff:
        writer.writerow([
            "TOTALS",
            f'{totals.get("basic", 0):.2f}',
            f'{totals.get("allowances", 0):.2f}',
            f'{totals.get("gross", 0):.2f}',
            f'{totals.get("ssf_employee", 0):.2f}',
            f'{totals.get("taxable", 0):.2f}',
            f'{totals.get("paye", 0):.2f}',
            f'{totals.get("net", 0):.2f}',
            f'{totals.get("employer_13", 0):.2f}',
            f'{totals.get("total_cost", 0):.2f}',
            f'{totals.get("tier1", 0):.2f}',
            f'{totals.get("tier2", 0):.2f}',
        ])

    filename = f"payroll_{period_for_filename}.csv"

    return Response(
        out.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )

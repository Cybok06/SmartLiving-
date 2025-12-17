from __future__ import annotations

from flask import Blueprint, render_template, request, Response, jsonify
from datetime import date, datetime
import io
import csv
import json

from db import db

acc_payroll_calc = Blueprint(
    "acc_payroll_calc",
    __name__,
    template_folder="../templates",
)

# Mongo collection for monthly payroll runs
payroll_col = db["payroll_runs"]


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

    return jsonify(ok=True, message=f"Payroll saved for {period}."), 200


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

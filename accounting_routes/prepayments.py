from __future__ import annotations

from flask import Blueprint, render_template, request, jsonify, Response, session
from datetime import datetime
import io
import csv

from db import db
from accounting_services import post_prepayment, prepayments_outstanding, _parse_month, _month_count

prepayments_bp = Blueprint("prepayments", __name__, template_folder="../templates")

prepayments_col = db["prepayments"]


def _require_accounting_role():
    role = (session.get("role") or "").lower()
    if session.get("admin_id") or session.get("executive_id"):
        return True
    return role == "accounting"


@prepayments_bp.get("/prepayments")
def prepayments_page():
    if not _require_accounting_role():
        return render_template("accounting/prepayments.html", denied=True)

    export = request.args.get("export") == "1"
    rows = list(prepayments_col.find({}).sort("date_dt", -1))

    if export:
        out = io.StringIO()
        w = csv.writer(out)
        w.writerow(["Date", "Category", "Vendor", "Amount", "Start", "End", "Monthly", "Status"])
        for r in rows:
            dt = r.get("date_dt")
            dt_str = dt.strftime("%Y-%m-%d") if isinstance(dt, datetime) else ""
            w.writerow([
                dt_str,
                r.get("category", ""),
                r.get("vendor", ""),
                f"{float(r.get('amount_total', 0) or 0):0.2f}",
                r.get("start_period", ""),
                r.get("end_period", ""),
                f"{float(r.get('monthly_expense_amount', 0) or 0):0.2f}",
                r.get("status", ""),
            ])
        return Response(
            out.getvalue(),
            mimetype="text/csv",
            headers={"Content-Disposition": 'attachment; filename="prepayments.csv"'},
        )

    total_outstanding = prepayments_outstanding(datetime.utcnow())

    # compute remaining per row for display
    for r in rows:
        sp = r.get("start_period") or ""
        ep = r.get("end_period") or ""
        months = _month_count(sp, ep)
        monthly = float(r.get("monthly_expense_amount", 0) or 0)
        r["months_total"] = months
        r["monthly_display"] = f"{monthly:,.2f}"

    return render_template(
        "accounting/prepayments.html",
        rows=rows,
        total_outstanding=total_outstanding,
        denied=False,
    )


@prepayments_bp.post("/prepayments/create")
def create_prepayment():
    if not _require_accounting_role():
        return jsonify(ok=False, message="Unauthorized"), 401

    payload = {
        "date_dt": request.form.get("date"),
        "category": request.form.get("category"),
        "vendor": request.form.get("vendor"),
        "amount_total": request.form.get("amount_total"),
        "start_period": request.form.get("start_period"),
        "end_period": request.form.get("end_period"),
        "created_by": session.get("user_id") or session.get("admin_id") or session.get("executive_id"),
    }
    result = post_prepayment(payload)
    if not result.get("ok"):
        return jsonify(ok=False, message=result.get("message") or "Failed"), 400
    return jsonify(ok=True)

from __future__ import annotations

from flask import Blueprint, render_template, request, jsonify, Response, session
from datetime import datetime
import io
import csv
from bson import ObjectId

from db import db
from accounting_services import post_accrual

accruals_bp = Blueprint("accruals", __name__, template_folder="../templates")

accruals_col = db["accruals"]


def _require_accounting_role():
    role = (session.get("role") or "").lower()
    if session.get("admin_id") or session.get("executive_id"):
        return True
    return role == "accounting"


@accruals_bp.get("/accruals")
def accruals_page():
    if not _require_accounting_role():
        return render_template("accounting/accruals.html", denied=True)

    export = request.args.get("export") == "1"
    rows = list(accruals_col.find({}).sort("date_dt", -1))

    if export:
        out = io.StringIO()
        w = csv.writer(out)
        w.writerow(["Incurred Date", "Category", "Vendor", "Amount", "Due Date", "Status"])
        for r in rows:
            dt = r.get("date_dt")
            dt_str = dt.strftime("%Y-%m-%d") if isinstance(dt, datetime) else ""
            due = r.get("due_date")
            due_str = due.strftime("%Y-%m-%d") if isinstance(due, datetime) else ""
            w.writerow([
                dt_str,
                r.get("category", ""),
                r.get("vendor", ""),
                f"{float(r.get('amount', 0) or 0):0.2f}",
                due_str,
                r.get("status", ""),
            ])
        return Response(
            out.getvalue(),
            mimetype="text/csv",
            headers={"Content-Disposition": 'attachment; filename="accruals.csv"'},
        )

    outstanding_total = sum(
        float(r.get("amount", 0) or 0)
        for r in rows if (r.get("status") or "owing") == "owing"
    )

    return render_template(
        "accounting/accruals.html",
        rows=rows,
        outstanding_total=outstanding_total,
        denied=False,
    )


@accruals_bp.post("/accruals/create")
def create_accrual():
    if not _require_accounting_role():
        return jsonify(ok=False, message="Unauthorized"), 401

    payload = {
        "date_dt": request.form.get("date"),
        "category": request.form.get("category"),
        "vendor": request.form.get("vendor"),
        "amount": request.form.get("amount"),
        "due_date": request.form.get("due_date"),
        "created_by": session.get("user_id") or session.get("admin_id") or session.get("executive_id"),
    }
    result = post_accrual(payload)
    if not result.get("ok"):
        return jsonify(ok=False, message=result.get("message") or "Failed"), 400
    return jsonify(ok=True)


@accruals_bp.post("/accruals/<accrual_id>/mark-paid")
def mark_paid(accrual_id: str):
    if not _require_accounting_role():
        return jsonify(ok=False, message="Unauthorized"), 401
    try:
        oid = ObjectId(accrual_id)
    except Exception:
        return jsonify(ok=False, message="Invalid accrual id."), 400

    linked_payment_id = request.form.get("linked_payment_id") or None
    accruals_col.update_one(
        {"_id": oid},
        {"$set": {
            "status": "paid",
            "linked_payment_id": linked_payment_id,
            "updated_at": datetime.utcnow(),
        }},
    )
    return jsonify(ok=True)

from __future__ import annotations

from flask import Blueprint, render_template, request, url_for, Response, jsonify, session
from datetime import datetime
import io
import csv
from bson import ObjectId

from db import db
from accounting_services import post_withdrawal, post_goods_drawn

private_ledger_bp = Blueprint("private_ledger", __name__, template_folder="../templates")

private_ledger_col = db["private_ledger_entries"]
bank_accounts_col = db["bank_accounts"]
inventory_col = db["inventory"]


def _require_accounting_role():
    role = (session.get("role") or "").lower()
    if session.get("admin_id") or session.get("executive_id"):
        return True
    return role == "accounting"


def _parse_date(val: str | None):
    if not val:
        return None
    try:
        return datetime.strptime(val, "%Y-%m-%d")
    except Exception:
        return None


@private_ledger_bp.get("/private-ledger")
def private_ledger():
    if not _require_accounting_role():
        return render_template("accounting/private_ledger.html", denied=True)

    start_str = (request.args.get("from") or "").strip()
    end_str = (request.args.get("to") or "").strip()
    entry_type = (request.args.get("entry_type") or "").strip()
    source_type = (request.args.get("source_type") or "").strip()
    export = request.args.get("export") == "1"

    start_dt = _parse_date(start_str)
    end_dt = _parse_date(end_str)
    if end_dt:
        end_dt = end_dt.replace(hour=23, minute=59, second=59, microsecond=999999)

    q = {"status": "posted"}
    if start_dt or end_dt:
        q["date_dt"] = {}
        if start_dt:
            q["date_dt"]["$gte"] = start_dt
        if end_dt:
            q["date_dt"]["$lte"] = end_dt
    if entry_type in ("cash_drawing", "goods_drawn", "owner_contribution"):
        q["entry_type"] = entry_type
    if source_type in ("cash", "bank", "momo", "mobile_money"):
        q["source_account_type"] = "mobile_money" if source_type == "momo" else source_type

    rows = list(private_ledger_col.find(q).sort("date_dt", -1))

    total_drawings = sum(
        float(r.get("amount", 0) or 0) for r in rows if r.get("entry_type") == "cash_drawing"
    )
    total_goods_drawn = sum(
        float(r.get("amount", 0) or 0) for r in rows if r.get("entry_type") == "goods_drawn"
    )

    if export:
        out = io.StringIO()
        w = csv.writer(out)
        w.writerow(["Date", "Entry Type", "Source", "Amount", "Memo"])
        for r in rows:
            dt = r.get("date_dt")
            dt_str = dt.strftime("%Y-%m-%d") if isinstance(dt, datetime) else ""
            w.writerow([
                dt_str,
                r.get("entry_type", ""),
                r.get("source_account_type", ""),
                f"{float(r.get('amount', 0) or 0):0.2f}",
                r.get("purpose_text", ""),
            ])
        return Response(
            out.getvalue(),
            mimetype="text/csv",
            headers={"Content-Disposition": 'attachment; filename="private_ledger.csv"'},
        )

    bank_accounts = list(
        bank_accounts_col.find(
            {},
            {"_id": 1, "bank_name": 1, "account_name": 1, "account_type": 1, "account_no": 1},
        ).sort("bank_name", 1)
    )
    accounts_for_select = []
    for a in bank_accounts:
        aid = a.get("_id")
        if not isinstance(aid, ObjectId):
            continue
        label = a.get("bank_name") or a.get("account_name") or "Account"
        if a.get("account_name") and a.get("bank_name"):
            label = f"{a.get('bank_name')} - {a.get('account_name')}"
        accounts_for_select.append({
            "id": str(aid),
            "label": label,
            "account_type": (a.get("account_type") or "bank").lower(),
        })

    inventory_items = list(
        inventory_col.find({}, {"_id": 1, "name": 1, "qty": 1, "cost_price": 1})
        .sort("name", 1)
        .limit(2000)
    )

    return render_template(
        "accounting/private_ledger.html",
        rows=rows,
        total_drawings=total_drawings,
        total_goods_drawn=total_goods_drawn,
        start_str=start_str,
        end_str=end_str,
        entry_type=entry_type,
        source_type=source_type,
        accounts=accounts_for_select,
        inventory_items=inventory_items,
        denied=False,
    )


@private_ledger_bp.post("/private-ledger/cash-drawing")
def create_cash_drawing():
    if not _require_accounting_role():
        return jsonify(ok=False, message="Unauthorized"), 401

    amount = request.form.get("amount")
    account_type = request.form.get("account_type")
    account_id = request.form.get("account_id") or None
    date_str = request.form.get("date") or ""
    memo = request.form.get("memo") or ""

    try:
        date_dt = datetime.fromisoformat(date_str) if date_str else datetime.utcnow()
    except Exception:
        date_dt = datetime.utcnow()

    result = post_withdrawal({
        "amount": amount,
        "account_type": account_type,
        "account_id": account_id,
        "purpose": "drawings",
        "purpose_note": memo,
        "date_dt": date_dt,
        "created_by": session.get("user_id") or session.get("admin_id") or session.get("executive_id"),
    })
    if not result.get("ok"):
        return jsonify(ok=False, message=result.get("message") or "Failed"), 400
    return jsonify(ok=True)


@private_ledger_bp.post("/private-ledger/goods-drawn")
def create_goods_drawn():
    if not _require_accounting_role():
        return jsonify(ok=False, message="Unauthorized"), 401

    product_id = request.form.get("product_id")
    qty = request.form.get("quantity")
    unit_cost = request.form.get("unit_cost")
    date_str = request.form.get("date") or ""
    memo = request.form.get("memo") or ""

    try:
        date_dt = datetime.fromisoformat(date_str) if date_str else datetime.utcnow()
    except Exception:
        date_dt = datetime.utcnow()

    result = post_goods_drawn({
        "product_id": product_id,
        "quantity": qty,
        "unit_cost": unit_cost,
        "date_dt": date_dt,
        "memo": memo,
        "created_by": session.get("user_id") or session.get("admin_id") or session.get("executive_id"),
    })
    if not result.get("ok"):
        return jsonify(ok=False, message=result.get("message") or "Failed"), 400
    return jsonify(ok=True)

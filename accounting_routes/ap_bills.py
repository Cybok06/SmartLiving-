# accounting_routes/ap_bills.py
from __future__ import annotations

from flask import Blueprint, render_template, request, url_for, Response, jsonify
from datetime import datetime
import io, csv, math, re
from typing import Any, Dict, List

from db import db

ap_bills_bp = Blueprint("ap_bills", __name__, template_folder="../templates")

# Mongo collection for AP bills
bills_col = db["ap_bills"]


def _iso(d: str | None):
    """Parse YYYY-MM-DD into datetime or return None."""
    if not d:
        return None
    try:
        return datetime.fromisoformat(d)
    except Exception:
        return None


def _safe_float(v: Any) -> float:
    try:
        return float(v or 0)
    except Exception:
        return 0.0


def _paginate_url(page: int, per: int) -> str:
    args = request.args.to_dict()
    args["page"] = str(page)
    args["per"] = str(per)
    return url_for("ap_bills.bills", **args)


@ap_bills_bp.get("/ap/bills")
def bills():
    """
    Accounts Payable Bills listing.

    - Supports text search (?q=)
    - Optional status filter (?status=)
    - Optional date range (?from=YYYY-MM-DD&to=YYYY-MM-DD)
    - Pagination (?page=&per=)
    - CSV export (?export=1)
    """
    qtxt   = (request.args.get("q") or "").strip()
    status = (request.args.get("status") or "").strip().lower()
    dfrom  = _iso(request.args.get("from"))
    dto    = _iso(request.args.get("to"))
    page   = max(1, int(request.args.get("page", 1)))
    per    = min(100, max(25, int(request.args.get("per", 25))))
    export = request.args.get("export") == "1"

    # ------------------------
    # Build Mongo query
    # ------------------------
    q: Dict[str, Any] = {}

    if qtxt:
        rx = re.compile(re.escape(qtxt), re.IGNORECASE)
        q["$or"] = [
            {"no": rx},
            {"bill_no": rx},
            {"vendor": rx},
            {"vendor_name": rx},
            {"reference": rx},
        ]

    if status:
        # store status lowercase in DB if possible (e.g. "paid","draft","overdue")
        q["status"] = status

    if dfrom or dto:
        q["bill_date_dt"] = {}
        if dfrom:
            q["bill_date_dt"]["$gte"] = datetime(dfrom.year, dfrom.month, dfrom.day)
        if dto:
            q["bill_date_dt"]["$lte"] = datetime(dto.year, dto.month, dto.day, 23, 59, 59, 999999)

    cur = bills_col.find(q).sort([("bill_date_dt", -1), ("_id", -1)])
    docs = list(cur)

    # ------------------------
    # CSV export
    # ------------------------
    if export and docs:
        out = io.StringIO()
        w   = csv.writer(out)
        w.writerow([
            "Bill No",
            "Vendor",
            "Bill Date",
            "Due Date",
            "Currency",
            "Amount",
            "Paid",
            "Balance",
            "Status",
        ])
        for d in docs:
            amt  = _safe_float(d.get("amount"))
            paid = _safe_float(d.get("paid"))
            bal  = _safe_float(d.get("balance", amt - paid))

            w.writerow([
                d.get("no") or d.get("bill_no", ""),
                d.get("vendor_name") or d.get("vendor", ""),
                d.get("bill_date", ""),
                d.get("due_date", ""),
                d.get("currency", "GHS"),
                f"{amt:0.2f}",
                f"{paid:0.2f}",
                f"{bal:0.2f}",
                (d.get("status") or "draft").title(),
            ])

        return Response(
            out.getvalue(),
            mimetype="text/csv",
            headers={"Content-Disposition": 'attachment; filename=\"ap_bills.csv\"'},
        )

    # ------------------------
    # Pagination
    # ------------------------
    total = len(docs)
    pages = max(1, math.ceil(total / per))
    page  = max(1, min(page, pages))
    start = (page - 1) * per
    end   = start + per

    pager = {
        "total": total,
        "page": page,
        "pages": pages,
        "prev_url": _paginate_url(page - 1, per) if page > 1 else None,
        "next_url": _paginate_url(page + 1, per) if page < pages else None,
    }

    export_args = request.args.to_dict(flat=True)
    export_args["export"] = "1"
    export_url = url_for("ap_bills.bills", **export_args)

    # ------------------------
    # Map docs -> rows for template
    # ------------------------
    rows: List[Dict[str, Any]] = []
    for d in docs[start:end]:
        amt  = _safe_float(d.get("amount"))
        paid = _safe_float(d.get("paid"))
        bal  = _safe_float(d.get("balance", amt - paid))

        currency = d.get("currency", "GHS")
        if "symbol" in d:
            currency_symbol = d.get("symbol") or d.get("currency_symbol", "")
        else:
            # very light defaulting
            if currency in ("GHS", "GH₵"):
                currency_symbol = "GH₵"
            elif currency == "USD":
                currency_symbol = "$"
            else:
                currency_symbol = ""

        rows.append({
            "no": d.get("no") or d.get("bill_no", ""),
            "bill_no": d.get("bill_no", ""),
            "vendor": d.get("vendor", ""),
            "vendor_name": d.get("vendor_name", ""),
            "bill_date": d.get("bill_date", ""),
            "due_date": d.get("due_date", ""),
            "currency": currency,
            "currency_symbol": currency_symbol,
            "amount": amt,
            "paid": paid,
            "balance": bal,
            "status": (d.get("status") or "draft").lower(),
        })

    today = datetime.utcnow().date().isoformat()

    return render_template(
        "accounting/ap_bills.html",
        rows=rows,
        pager=pager,
        export_url=export_url,
        today=today,
    )


@ap_bills_bp.post("/ap/bills/quick")
def quick_create():
    """
    Quick-create endpoint for the slide-over form on AP Bills.

    Inserts a basic bill document into ap_bills and returns JSON.
    """
    def _q(x: str | None) -> str:
        return (x or "").strip()

    def _f(x) -> float:
        try:
            return float(str(x).replace(",", ""))
        except Exception:
            return 0.0

    bill_no     = _q(request.form.get("bill_no"))
    reference   = _q(request.form.get("reference"))
    vendor_name = _q(request.form.get("vendor_name"))
    vendor_code = _q(request.form.get("vendor"))
    bill_date_s = _q(request.form.get("bill_date"))
    due_date_s  = _q(request.form.get("due_date"))
    currency    = _q(request.form.get("currency") or "GHS")
    status      = (_q(request.form.get("status")) or "draft").lower()
    amount      = _f(request.form.get("amount"))
    paid        = _f(request.form.get("paid"))
    notes       = _q(request.form.get("notes"))

    if not vendor_name or not bill_date_s or not due_date_s or amount <= 0:
        return jsonify(ok=False, message="Vendor, Bill Date, Due Date and Amount are required."), 400

    # Parse dates
    try:
        bill_date_dt = datetime.fromisoformat(bill_date_s)
    except Exception:
        return jsonify(ok=False, message="Invalid Bill Date."), 400

    try:
        due_date_dt = datetime.fromisoformat(due_date_s)
    except Exception:
        return jsonify(ok=False, message="Invalid Due Date."), 400

    balance = max(amount - paid, 0.0)

    doc = {
        "bill_no": bill_no,
        "no": bill_no,  # you can later change to an auto-number if needed
        "reference": reference,
        "vendor": vendor_code or vendor_name,
        "vendor_name": vendor_name,
        "bill_date": bill_date_s,
        "bill_date_dt": bill_date_dt,
        "due_date": due_date_s,
        "due_date_dt": due_date_dt,
        "currency": currency,
        "amount": amount,
        "paid": paid,
        "balance": balance,
        "status": status,
        "notes": notes,
        "created_at": datetime.utcnow(),
    }

    bills_col.insert_one(doc)
    return jsonify(ok=True, bill_no=bill_no or "")

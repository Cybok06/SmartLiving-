# accounting_routes/ar_payments.py
from __future__ import annotations

from flask import Blueprint, render_template, request, url_for, Response, jsonify
from datetime import datetime
import io
import csv
import math
import re

from db import db
from services.activity_audit import audit_action

ar_payments_bp = Blueprint("ar_payments", __name__, template_folder="../templates")

# AR Receipts collection
rec_col = db["ar_receipts"]

# 👉 AR Clients master list
# These are accounting "clients" stored in `clients` collection.
# Receipts still use fields `customer` and `customer_name` for compatibility.
clients_col = db["clients"]


def _iso(d: str | None):
    try:
        return datetime.fromisoformat(d) if d else None
    except Exception:
        return None


def _paginate_url(page: int, per: int) -> str:
    args = request.args.to_dict()
    args["page"] = str(page)
    args["per"] = str(per)
    return url_for("ar_payments.payments", **args)


def _next_receipt_no() -> str:
    """
    Generate the next receipt number in the form REC-0001, REC-0002, ...
    based on the highest existing REC-#### value.
    """
    last = rec_col.find_one(
        {"no": {"$regex": r"^REC-\d+$"}},
        sort=[("date_dt", -1), ("_id", -1)],
    )
    if not last:
        return "REC-0001"
    m = re.search(r"(\d+)$", last.get("no", ""))
    if not m:
        return "REC-0001"
    num = int(m.group(1)) + 1
    return f"REC-{num:04d}"


@ar_payments_bp.get("/ar/payments")
def payments():
    qtxt = (request.args.get("q") or "").strip()
    # client code, still named `customer` in query string for compatibility
    customer = (request.args.get("customer") or "").strip()
    status = (request.args.get("status") or "").strip().lower()
    dfrom = _iso(request.args.get("from"))
    dto = _iso(request.args.get("to"))
    page = max(1, int(request.args.get("page", 1)))
    per = min(100, max(25, int(request.args.get("per", 25))))
    export = request.args.get("export") == "1"

    q: dict = {}
    if qtxt:
        rx = re.compile(re.escape(qtxt), re.IGNORECASE)
        q["$or"] = [
            {"no": rx},
            {"customer": rx},
            {"customer_name": rx},
            {"reference": rx},
        ]
    if customer:
        q["customer"] = customer
    if status in ("allocated", "partial", "unalloc"):
        q["status"] = status
    if dfrom or dto:
        q["date_dt"] = {}
        if dfrom:
            q["date_dt"]["$gte"] = datetime(dfrom.year, dfrom.month, dfrom.day)
        if dto:
            q["date_dt"]["$lte"] = datetime(dto.year, dto.month, dto.day, 23, 59, 59, 999999)

    cur = rec_col.find(q).sort([("date_dt", -1), ("_id", -1)])
    docs = list(cur)

    # Stats
    cash_impact = sum(float(d.get("amount", 0) or 0) for d in docs)
    unallocated_total = sum(
        max(
            float(d.get("amount", 0) or 0) - float(d.get("allocated", 0) or 0),
            0.0,
        )
        for d in docs
    )
    stats = type("S", (object,), dict(cash_impact=cash_impact, unallocated=unallocated_total))

    # Export
    if export and docs:
        out = io.StringIO()
        w = csv.writer(out)
        w.writerow(
            [
                "Receipt",
                "Date",
                "Client",
                "Method",
                "Amount (GH₵)",
                "Allocated (GH₵)",
                "Unallocated (GH₵)",
                "Reference",
                "Status",
            ]
        )
        for d in docs:
            amt = float(d.get("amount", 0) or 0)
            alloc = float(d.get("allocated", 0) or 0)
            unalloc = max(amt - alloc, 0.0)
            w.writerow(
                [
                    d.get("no", ""),
                    d.get("date", ""),
                    d.get("customer", ""),  # stored field name
                    d.get("method", ""),
                    f"{amt:0.2f}",
                    f"{alloc:0.2f}",
                    f"{unalloc:0.2f}",
                    d.get("reference", ""),
                    d.get("status", ""),
                ]
            )
        return Response(
            out.getvalue(),
            mimetype="text/csv",
            headers={"Content-Disposition": 'attachment; filename="ar_receipts.csv"'},
        )

    total = len(docs)
    pages = max(1, math.ceil(total / per))
    page = max(1, min(page, pages))
    start = (page - 1) * per
    end = start + per
    pager = {
        "total": total,
        "page": page,
        "pages": pages,
        "prev_url": _paginate_url(page - 1, per) if page > 1 else None,
        "next_url": _paginate_url(page + 1, per) if page < pages else None,
    }

    export_args = request.args.to_dict(flat=True)
    export_args["export"] = "1"
    export_url = url_for("ar_payments.payments", **export_args)

    rows = []
    for d in docs[start:end]:
        amt = float(d.get("amount", 0) or 0)
        alloc = float(d.get("allocated", 0) or 0)
        unalloc = max(amt - alloc, 0.0)
        rows.append(
            {
                "no": d.get("no", ""),
                "date": d.get("date", ""),
                "customer": d.get("customer", ""),            # client code
                "customer_name": d.get("customer_name", ""),  # client name
                "method": d.get("method", ""),
                "amount": amt,
                "allocated": alloc,
                "unallocated": unalloc,
                "reference": d.get("reference", ""),
                "status": d.get("status", "unalloc"),
            }
        )

    # 👉 Clients dropdown for the modal (from `clients` collection)
    client_docs = clients_col.find({}, {"code": 1, "name": 1}).sort([("name", 1)])
    customers_list = [
        {"code": c.get("code", ""), "name": c.get("name", "")}
        for c in client_docs
    ]

    next_receipt_no = _next_receipt_no()

    return render_template(
        "accounting/ar_payments.html",
        rows=rows,
        pager=pager,
        stats=stats,
        export_url=export_url,
        customers_list=customers_list,  # template uses this for "Client" dropdown
        next_receipt_no=next_receipt_no,
    )


@ar_payments_bp.post("/ar/payments/quick")
@audit_action("payment.recorded", "Recorded Payment", entity_type="payment")
def quick_create():
    def _q(x: str | None) -> str:
        return (x or "").strip()

    def _f(x) -> float:
        try:
            return float(str(x).replace(",", ""))
        except Exception:
            return 0.0

    no = _q(request.form.get("no"))
    date_str = _q(request.form.get("date"))
    customer = _q(request.form.get("customer"))  # client code (field name stays `customer`)
    method = _q(request.form.get("method"))
    amount = _f(request.form.get("amount"))
    reference = _q(request.form.get("reference"))

    if not date_str or not customer or amount <= 0:
        return jsonify(ok=False, message="Date, Client and Amount are required."), 400

    # Auto-generate receipt number if not provided
    if not no:
        no = _next_receipt_no()
    else:
        if rec_col.find_one({"no": no}):
            return jsonify(ok=False, message="Receipt already exists."), 409

    # Try parse date
    try:
        date_dt = datetime.fromisoformat(date_str)
    except Exception:
        return jsonify(ok=False, message="Invalid date format."), 400

    # Find client record from AR clients master (clients collection)
    client = clients_col.find_one({"code": customer}) or {}

    rec_col.insert_one(
        {
            "no": no,
            "date": date_str,
            "date_dt": date_dt,
            "customer": customer,                      # client code
            "customer_name": client.get("name", ""),   # client name
            "method": method or "Cash",
            "amount": amount,
            "allocated": 0.0,
            "reference": reference,
            "status": "unalloc",
            "created_at": datetime.utcnow(),
        }
    )
    return jsonify(ok=True, no=no)

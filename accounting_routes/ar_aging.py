# accounting_routes/ar_aging.py
from __future__ import annotations

from flask import Blueprint, render_template, request, Response, url_for
from datetime import datetime, date
from typing import Dict, Any, List
import io, csv

from db import db

# Collections
ar_invoices_col = db["ar_invoices"]
rec_col         = db["ar_receipts"]   # AR payments

ar_aging_bp = Blueprint("ar_aging", __name__, template_folder="../templates")


def _parse_as_of(val: str | None) -> date:
    """
    Parse ?as_of=YYYY-MM-DD, or return today's UTC date.
    """
    if not val:
        return datetime.utcnow().date()
    try:
        return datetime.fromisoformat(val).date()
    except Exception:
        return datetime.utcnow().date()


def _to_date(v: Any) -> date | None:
    """
    Try to coerce stored due date into a date object.
    Supports datetime, date, and ISO-like strings.
    """
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, date):
        return v
    if isinstance(v, str):
        try:
            return datetime.fromisoformat(v).date()
        except Exception:
            return None
    return None


def _safe_float(v: Any) -> float:
    try:
        return float(v or 0)
    except Exception:
        return 0.0


@ar_aging_bp.get("/ar/aging")
def aging_report():
    """
    Accounts Receivable Aging report.

    Logic:
    - Pull all invoices (ar_invoices) per client (optional filter).
    - Pull all receipts (ar_receipts) up to the As-Of date.
    - Per client, allocate receipts FIFO against invoices by due date.
    - Any remaining outstanding amount on each invoice is aged by due days.
    """
    # --- Filters / params ---
    as_of_param     = request.args.get("as_of")
    as_of           = _parse_as_of(as_of_param)
    export          = request.args.get("export") == "1"
    # query param still called "customer" for compatibility; semantically this is client code
    customer_filter = (request.args.get("customer") or "").strip()

    # --------------------------
    # 1) LOAD INVOICES PER CLIENT
    # --------------------------
    inv_q: Dict[str, Any] = {}
    if customer_filter:
        inv_q["customer"] = customer_filter

    inv_cur = ar_invoices_col.find(inv_q)

    invoices_by_cust: Dict[str, Dict[str, Any]] = {}

    for inv in inv_cur:
        code = (inv.get("customer") or "").strip() or "UNKNOWN"     # client code
        name = (inv.get("customer_name") or code).strip() or code   # client display name

        amount = _safe_float(inv.get("amount"))
        if amount <= 0:
            # nothing billed => skip
            continue

        # Due date (for aging)
        raw_due  = inv.get("due") or inv.get("due_date")
        due_date = _to_date(raw_due)
        if not due_date:
            # if no due date, treat as due as-of
            due_date = as_of

        cust_block = invoices_by_cust.setdefault(code, {
            "name":     name,
            "invoices": [],
        })
        cust_block["invoices"].append({
            "amount":   amount,
            "due_date": due_date,
        })

    # --------------------------
    # 2) LOAD PAYMENTS PER CLIENT (UP TO AS-OF)
    # --------------------------
    rec_q: Dict[str, Any] = {
        "date_dt": {
            "$lte": datetime(as_of.year, as_of.month, as_of.day, 23, 59, 59, 999999)
        }
    }
    if customer_filter:
        rec_q["customer"] = customer_filter

    rec_cur = rec_col.find(rec_q)

    payments_by_cust: Dict[str, float] = {}

    for r in rec_cur:
        cust = (r.get("customer") or "").strip()
        if not cust:
            continue

        # Use allocated if present, else fall back to full amount
        paid_val = _safe_float(r.get("allocated", r.get("amount")))
        if paid_val <= 0:
            continue

        payments_by_cust[cust] = payments_by_cust.get(cust, 0.0) + paid_val

    # --------------------------
    # 3) BUILD AGED BALANCES PER CLIENT (INVOICE - PAYMENTS FIFO)
    # --------------------------
    per_customer: Dict[str, Dict[str, Any]] = {}

    for code, data in invoices_by_cust.items():
        name = data["name"]
        invs = data["invoices"]
        if not invs:
            continue

        # Sort invoices oldest first for FIFO allocation
        invs.sort(key=lambda x: x["due_date"])

        remaining_pay = payments_by_cust.get(code, 0.0)

        for inv in invs:
            amt = inv["amount"]

            # Apply payments FIFO
            applied       = min(remaining_pay, amt)
            remaining_pay -= applied
            outstanding   = amt - applied

            if outstanding <= 0:
                # Fully paid by receipts up to as_of => skip
                continue

            due_date = inv["due_date"]
            age_days = (as_of - due_date).days
            if age_days < 0:
                age_days = 0  # not yet due, treat as 0–30

            # Decide bucket
            if age_days <= 30:
                bucket_key = "b0_30"
            elif age_days <= 60:
                bucket_key = "b31_60"
            elif age_days <= 90:
                bucket_key = "b61_90"
            else:
                bucket_key = "b90_plus"

            if code not in per_customer:
                per_customer[code] = {
                    "customer_code": code,   # client code
                    "customer_name": name,   # client name
                    "b0_30":   0.0,
                    "b31_60":  0.0,
                    "b61_90":  0.0,
                    "b90_plus":0.0,
                    "total":   0.0,
                }

            per_customer[code][bucket_key] += outstanding
            per_customer[code]["total"]     += outstanding

    # --------------------------
    # 4) BUILD ROWS & STATS
    # --------------------------
    rows: List[Dict[str, Any]] = list(per_customer.values())
    rows.sort(key=lambda r: r["total"], reverse=True)

    total_0_30    = sum(r["b0_30"]   for r in rows)
    total_31_60   = sum(r["b31_60"]  for r in rows)
    total_61_90   = sum(r["b61_90"]  for r in rows)
    total_90_plus = sum(r["b90_plus"]for r in rows)
    total_all     = total_0_30 + total_31_60 + total_61_90 + total_90_plus

    def pct(x: float) -> float:
        return (x / total_all * 100) if total_all > 0 else 0.0

    stats = {
        "b0_30":       round(total_0_30, 2),
        "b31_60":      round(total_31_60, 2),
        "b61_90":      round(total_61_90, 2),
        "b90_plus":    round(total_90_plus, 2),
        "total":       round(total_all, 2),
        "pct_0_30":    pct(total_0_30),
        "pct_31_60":   pct(total_31_60),
        "pct_61_90":   pct(total_61_90),
        "pct_90_plus": pct(total_90_plus),
    }

    # --------------------------
    # 5) CSV EXPORT (CLIENT TERMINOLOGY)
    # --------------------------
    if export:
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow([
            "Client Code", "Client Name",
            "0-30 Days (GH₵)", "31-60 Days (GH₵)",
            "61-90 Days (GH₵)", ">90 Days (GH₵)", "Total Due (GH₵)",
        ])
        for r in rows:
            writer.writerow([
                r["customer_code"],
                r["customer_name"],
                f'{r["b0_30"]:.2f}',
                f'{r["b31_60"]:.2f}',
                f'{r["b61_90"]:.2f}',
                f'{r["b90_plus"]:.2f}',
                f'{r["total"]:.2f}',
            ])
        return Response(
            output.getvalue(),
            mimetype="text/csv",
            headers={"Content-Disposition": 'attachment; filename="ar_aging_report.csv"'},
        )

    # As-of label for UI
    as_of_label = as_of.strftime("%b %d, %Y")
    as_of_input = as_of.isoformat()

    export_args = request.args.to_dict(flat=True)
    export_args["export"] = "1"
    export_url = url_for("ar_aging.aging_report", **export_args)

    return render_template(
        "accounting/ar_aging.html",
        rows=rows,
        stats=stats,
        as_of_label=as_of_label,
        as_of_input=as_of_input,
        export_url=export_url,
    )

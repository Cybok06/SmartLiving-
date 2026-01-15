# accounting_routes/ap_bills.py
from __future__ import annotations

from flask import Blueprint, render_template, request, url_for, Response, jsonify
from datetime import datetime
import io, csv, math, re
from typing import Any, Dict, List, Optional

from bson import ObjectId
from db import db
from services.activity_audit import audit_action

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


def _safe_str(v: Any) -> str:
    return (v or "").strip()


def _paginate_url(page: int, per: int) -> str:
    args = request.args.to_dict()
    args["page"] = str(page)
    args["per"] = str(per)
    return url_for("ap_bills.bills", **args)


def _currency_symbol(currency: str, doc: Dict[str, Any]) -> str:
    if "symbol" in doc:
        return doc.get("symbol") or doc.get("currency_symbol", "") or ""
    currency = (currency or "GHS").strip().upper()
    if currency in ("GHS", "GH₵"):
        return "GH₵"
    if currency == "USD":
        return "$"
    return ""


def _recalc_status(balance: float, existing_status: str | None) -> str:
    s = (existing_status or "draft").lower()
    if balance <= 0:
        return "paid"
    # if already paid becomes unpaid again (shouldn't happen), fallback
    if s == "paid":
        return "approved"
    return s or "approved"


def _append_amount_history_entry(delta_amount: float, note: str, date_dt: datetime) -> Dict[str, Any]:
    return {
        "type": "add_amount",
        "amount": float(delta_amount),
        "note": (note or "").strip(),
        "date": date_dt,
        "created_at": datetime.utcnow(),
    }


# -------------------------------
# LISTING
# -------------------------------
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
    per    = min(60, max(12, int(request.args.get("per", 24))))
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
    # Summary totals
    # ------------------------
    total_amount = 0.0
    total_paid = 0.0
    total_balance = 0.0

    for d in docs:
        amt = _safe_float(d.get("amount"))
        paid = _safe_float(d.get("paid"))
        bal = _safe_float(d.get("balance", amt - paid))
        total_amount += amt
        total_paid += paid
        total_balance += bal

    paid_ratio = (total_paid / total_amount * 100.0) if total_amount > 0 else 0.0
    ap_summary = {
        "total_amount": total_amount,
        "total_paid": total_paid,
        "total_balance": total_balance,
        "paid_pct": int(round(paid_ratio)),
    }

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
            headers={"Content-Disposition": 'attachment; filename="ap_bills.csv"'},
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
    # Map docs -> rows for template (cards)
    # ------------------------
    rows: List[Dict[str, Any]] = []
    for d in docs[start:end]:
        amt  = _safe_float(d.get("amount"))
        paid = _safe_float(d.get("paid"))
        bal  = _safe_float(d.get("balance", amt - paid))
        currency = d.get("currency", "GHS")
        sym = _currency_symbol(currency, d)

        rows.append({
            "_id": str(d.get("_id")),
            "no": d.get("no") or d.get("bill_no", ""),
            "bill_no": d.get("bill_no", ""),
            "reference": d.get("reference", ""),
            "vendor": d.get("vendor", ""),
            "vendor_name": d.get("vendor_name", ""),
            "bill_date": d.get("bill_date", ""),
            "due_date": d.get("due_date", ""),
            "currency": currency,
            "currency_symbol": sym,
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
        ap_summary=ap_summary,
    )


# -------------------------------
# QUICK CREATE (NOW: can add to existing bill)
# -------------------------------
@ap_bills_bp.post("/ap/bills/quick")
@audit_action("bill.created", "Created Bill", entity_type="bill")
def quick_create():
    """
    Quick-create endpoint for the slide-over form on AP Bills.

    NEW BEHAVIOR:
    - If bill_no exists already (and is NOT paid), this will ADD AMOUNT to that bill:
        - increments `amount`
        - recalculates balance
        - pushes to `amount_history` with optional note
    - Otherwise creates a new bill as before.
    """
    def _f(x) -> float:
        try:
            return float(str(x).replace(",", ""))
        except Exception:
            return 0.0

    bill_no     = _safe_str(request.form.get("bill_no"))
    reference   = _safe_str(request.form.get("reference"))
    vendor_name = _safe_str(request.form.get("vendor_name"))
    vendor_code = _safe_str(request.form.get("vendor"))
    bill_date_s = _safe_str(request.form.get("bill_date"))
    due_date_s  = _safe_str(request.form.get("due_date"))
    currency    = (_safe_str(request.form.get("currency")) or "GHS").upper()
    status      = (_safe_str(request.form.get("status")) or "draft").lower()
    amount      = _f(request.form.get("amount"))
    paid        = _f(request.form.get("paid"))
    notes       = _safe_str(request.form.get("notes"))

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

    # ---- If bill_no provided, try to add amount to existing unpaid bill ----
    if bill_no:
        existing = bills_col.find_one({
            "$and": [
                {"$or": [{"bill_no": bill_no}, {"no": bill_no}]},
                {"status": {"$ne": "paid"}}
            ]
        })

        if existing:
            oid = existing["_id"]
            current_amount = _safe_float(existing.get("amount"))
            current_paid   = _safe_float(existing.get("paid"))
            new_amount     = current_amount + amount
            new_balance    = max(new_amount - current_paid, 0.0)
            new_status     = _recalc_status(new_balance, existing.get("status"))

            entry = _append_amount_history_entry(
                delta_amount=amount,
                note=(notes or reference or "Added to existing bill").strip(),
                date_dt=datetime.utcnow(),
            )

            bills_col.update_one(
                {"_id": oid},
                {
                    "$set": {
                        "amount": new_amount,
                        "balance": new_balance,
                        "status": new_status,
                        "updated_at": datetime.utcnow(),
                    },
                    "$push": {"amount_history": entry},
                },
            )

            return jsonify(ok=True, updated=True, bill_id=str(oid), bill_no=bill_no)

    # ---- Create new bill ----
    balance = max(amount - paid, 0.0)

    payment_history: List[Dict[str, Any]] = []
    if paid > 0:
        payment_history.append({
            "amount": paid,
            "method": "Initial",
            "note": "Initial amount at bill creation",
            "date": datetime.utcnow(),
            "created_at": datetime.utcnow(),
        })

    amount_history: List[Dict[str, Any]] = []
    # log initial amount as baseline (useful for audit)
    amount_history.append({
        "type": "initial_amount",
        "amount": amount,
        "note": (notes or reference or "Bill created").strip(),
        "date": datetime.utcnow(),
        "created_at": datetime.utcnow(),
    })

    doc = {
        "bill_no": bill_no,
        "no": bill_no,  # later you can switch to auto-number
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
        "payment_history": payment_history,
        "amount_history": amount_history,
        "created_at": datetime.utcnow(),
    }

    res = bills_col.insert_one(doc)
    return jsonify(ok=True, created=True, bill_id=str(res.inserted_id), bill_no=bill_no or "")


# -------------------------------
# ADD PAYMENT (unchanged logic, kept)
# -------------------------------
@ap_bills_bp.post("/ap/bills/<bill_id>/add-payment")
@audit_action("bill.payment_recorded", "Recorded Bill Payment", entity_type="bill", entity_id_from="bill_id")
def add_payment(bill_id: str):
    """
    Add a payment against a single bill.

    - Increments `paid`
    - Recalculates `balance`
    - Appends to `payment_history`
    """
    try:
        oid = ObjectId(bill_id)
    except Exception:
        return jsonify(ok=False, message="Invalid bill ID."), 400

    bill = bills_col.find_one({"_id": oid})
    if not bill:
        return jsonify(ok=False, message="Bill not found."), 404

    amount = _safe_float(request.form.get("amount"))
    if amount <= 0:
        return jsonify(ok=False, message="Payment amount must be greater than zero."), 400

    payment_date_s = _safe_str(request.form.get("payment_date"))
    method = _safe_str(request.form.get("method"))
    note = _safe_str(request.form.get("note"))

    pay_dt = datetime.utcnow()
    if payment_date_s:
        try:
            pay_dt = datetime.fromisoformat(payment_date_s)
        except Exception:
            pass

    current_paid = _safe_float(bill.get("paid"))
    total_amount = _safe_float(bill.get("amount"))

    new_paid = current_paid + amount
    new_balance = max(total_amount - new_paid, 0.0)

    payment_entry = {
        "amount": amount,
        "method": method,
        "note": note,
        "date": pay_dt,
        "created_at": datetime.utcnow(),
    }

    new_status = bill.get("status", "approved")
    if new_balance <= 0:
        new_status = "paid"
    elif new_balance < total_amount:
        # optional: mark partial
        if (new_status or "").lower() in ("draft", "approved"):
            new_status = "partial"

    bills_col.update_one(
        {"_id": oid},
        {
            "$set": {
                "paid": new_paid,
                "balance": new_balance,
                "status": new_status,
                "updated_at": datetime.utcnow(),
            },
            "$push": {"payment_history": payment_entry},
        }
    )

    return jsonify(ok=True, paid=new_paid, balance=new_balance, status=new_status)


@ap_bills_bp.get("/ap/bills/<bill_id>/payments")
def get_payments(bill_id: str):
    """Return a bill's payment history as JSON."""
    try:
        oid = ObjectId(bill_id)
    except Exception:
        return jsonify(ok=False, message="Invalid bill ID."), 400

    bill = bills_col.find_one({"_id": oid}, {"payment_history": 1, "currency": 1})
    if not bill:
        return jsonify(ok=False, message="Bill not found."), 404

    hist = bill.get("payment_history", []) or []
    results: List[Dict[str, Any]] = []

    for p in hist:
        dt = p.get("date")
        if isinstance(dt, datetime):
            date_str = dt.strftime("%Y-%m-%d")
        else:
            date_str = str(dt or "")
        results.append({
            "amount": _safe_float(p.get("amount")),
            "method": p.get("method") or "",
            "note": p.get("note") or "",
            "date": date_str,
        })

    return jsonify(ok=True, currency=bill.get("currency", "GHS"), payments=results)


# -------------------------------
# NEW: ADD AMOUNT TO EXISTING BILL
# -------------------------------
@ap_bills_bp.post("/ap/bills/<bill_id>/add-amount")
@audit_action("bill.amount_added", "Added Bill Amount", entity_type="bill", entity_id_from="bill_id")
def add_amount(bill_id: str):
    """
    Add more AMOUNT (charges) to an existing bill:
    - increments `amount`
    - recalculates balance = amount - paid
    - pushes to `amount_history` with optional note
    """
    try:
        oid = ObjectId(bill_id)
    except Exception:
        return jsonify(ok=False, message="Invalid bill ID."), 400

    bill = bills_col.find_one({"_id": oid})
    if not bill:
        return jsonify(ok=False, message="Bill not found."), 404

    delta = _safe_float(request.form.get("amount"))
    if delta <= 0:
        return jsonify(ok=False, message="Added amount must be greater than zero."), 400

    note = _safe_str(request.form.get("note"))
    date_s = _safe_str(request.form.get("date"))

    dt = datetime.utcnow()
    if date_s:
        try:
            dt = datetime.fromisoformat(date_s)
        except Exception:
            pass

    current_amount = _safe_float(bill.get("amount"))
    current_paid   = _safe_float(bill.get("paid"))

    new_amount  = current_amount + delta
    new_balance = max(new_amount - current_paid, 0.0)
    new_status  = _recalc_status(new_balance, bill.get("status"))

    entry = _append_amount_history_entry(delta_amount=delta, note=note, date_dt=dt)

    bills_col.update_one(
        {"_id": oid},
        {
            "$set": {
                "amount": new_amount,
                "balance": new_balance,
                "status": new_status,
                "updated_at": datetime.utcnow(),
            },
            "$push": {"amount_history": entry},
        },
    )

    return jsonify(ok=True, amount=new_amount, balance=new_balance, status=new_status)


@ap_bills_bp.get("/ap/bills/<bill_id>/amount-history")
def get_amount_history(bill_id: str):
    """Return a bill's AMOUNT additions history as JSON (amount_history)."""
    try:
        oid = ObjectId(bill_id)
    except Exception:
        return jsonify(ok=False, message="Invalid bill ID."), 400

    bill = bills_col.find_one({"_id": oid}, {"amount_history": 1, "currency": 1})
    if not bill:
        return jsonify(ok=False, message="Bill not found."), 404

    hist = bill.get("amount_history", []) or []
    results: List[Dict[str, Any]] = []

    for p in hist:
        dt = p.get("date")
        if isinstance(dt, datetime):
            date_str = dt.strftime("%Y-%m-%d")
        else:
            date_str = str(dt or "")
        results.append({
            "type": p.get("type") or "",
            "amount": _safe_float(p.get("amount")),
            "note": p.get("note") or "",
            "date": date_str,
        })

    # latest first
    results.sort(key=lambda x: (x.get("date") or ""), reverse=True)

    return jsonify(ok=True, currency=bill.get("currency", "GHS"), items=results)

# accounting_routes/payment_voucher.py
from __future__ import annotations

from flask import (
    Blueprint, render_template, request,
    redirect, url_for, flash, abort
)
from bson import ObjectId
from datetime import datetime
from decimal import Decimal, InvalidOperation

from db import db

payment_vouchers_col = db["payment_vouchers"]

# NOTE:
# - template_folder points to project_root/templates/accounting
# - NO url_prefix here; it will be added in app.py when registering
payment_voucher_bp = Blueprint(
    "payment_voucher",
    __name__,
    template_folder="../templates/accounting",
)


def _to_decimal(val, default: str = "0") -> Decimal:
    """Safely convert any value from the form to Decimal."""
    try:
        return Decimal(str(val or default))
    except (InvalidOperation, TypeError):
        return Decimal(default)


def _next_pv_number() -> str:
    """
    Generate next PV number like: PV-2025-0001
    based on the highest existing PV for the current year.
    """
    today = datetime.today()
    prefix = f"PV-{today.year}-"

    last = payment_vouchers_col.find_one(
        {"pv_number": {"$regex": f"^{prefix}"}},
        sort=[("pv_number", -1)]
    )

    if last and isinstance(last.get("pv_number"), str):
        try:
            last_seq = int(last["pv_number"].split("-")[-1])
        except Exception:
            last_seq = 0
    else:
        last_seq = 0

    return f"{prefix}{last_seq + 1:04d}"


@payment_voucher_bp.get("/")
def form_page():
    """
    Payment Voucher main page:
    - Shows the create-new form
    - Lists recent vouchers (for quick access).
    """
    pv_number = _next_pv_number()
    today = datetime.today().strftime("%Y-%m-%d")

    recent = list(
        payment_vouchers_col.find({})
        .sort("created_at", -1)
        .limit(25)
    )

    return render_template(
        "payment_voucher_form.html",   # file in templates/accounting/
        pv_number=pv_number,
        today=today,
        vouchers=recent,
    )


@payment_voucher_bp.post("/")
def create_voucher():
    """Handle submission of a new payment voucher."""
    form = request.form

    pv_number = form.get("pv_number") or _next_pv_number()
    date_str = form.get("date") or datetime.today().strftime("%Y-%m-%d")
    to_name = form.get("to_name", "").strip()
    pay_method = form.get("pay_method", "tfr")  # 'cash' or 'tfr'
    tfr_no = form.get("tfr_no", "").strip()
    bank_name = form.get("bank_name", "").strip()

    # Line items
    desc_list = form.getlist("line_description[]")
    amt_list = form.getlist("line_amount[]")

    line_items = []
    subtotal = Decimal("0")

    for desc, amt in zip(desc_list, amt_list):
        desc = (desc or "").strip()
        if not desc and not amt:
            continue
        amount = _to_decimal(amt)
        line_items.append({"description": desc, "amount": float(amount)})
        subtotal += amount

    # Tax percentages
    vat_pct = _to_decimal(form.get("vat_pct"), "0")
    wht_pct = _to_decimal(form.get("wht_pct"), "0")

    vat_amount = (subtotal * vat_pct / Decimal("100")).quantize(Decimal("0.01"))
    wht_amount = (subtotal * wht_pct / Decimal("100")).quantize(Decimal("0.01"))
    total_payable = (subtotal + vat_amount - wht_amount).quantize(Decimal("0.01"))

    amount_in_words = form.get("amount_in_words", "").strip()

    prepared_by = form.get("prepared_by", "").strip()
    authorised_by = form.get("authorised_by", "").strip()
    approved_by = form.get("approved_by", "").strip()
    received_by = form.get("received_by", "").strip()
    recipient_details = form.get("recipient_details", "").strip()

    try:
        date_obj = datetime.fromisoformat(date_str)
    except Exception:
        date_obj = datetime.today()

    doc = {
        "pv_number": pv_number,
        "date": date_obj,
        "to_name": to_name,
        "pay_method": pay_method,   # 'cash' or 'tfr'
        "tfr_no": tfr_no,
        "bank_name": bank_name,
        "line_items": line_items,
        "subtotal": float(subtotal),
        "vat_pct": float(vat_pct),
        "vat_amount": float(vat_amount),
        "wht_pct": float(wht_pct),
        "wht_amount": float(wht_amount),
        "total_payable": float(total_payable),
        "amount_in_words": amount_in_words,
        "prepared_by": prepared_by,
        "authorised_by": authorised_by,
        "approved_by": approved_by,
        "received_by": received_by,
        "recipient_details": recipient_details,
        "created_at": datetime.utcnow(),
    }

    res = payment_vouchers_col.insert_one(doc)
    flash("Payment voucher created.", "success")

    return redirect(
        url_for("payment_voucher.view_voucher", voucher_id=str(res.inserted_id))
    )


@payment_voucher_bp.get("/<voucher_id>")
def view_voucher(voucher_id: str):
    """Display one voucher in printable layout."""
    try:
        oid = ObjectId(voucher_id)
    except Exception:
        abort(404)

    voucher = payment_vouchers_col.find_one({"_id": oid})
    if not voucher:
        abort(404)

    return render_template("payment_voucher_view.html", v=voucher)

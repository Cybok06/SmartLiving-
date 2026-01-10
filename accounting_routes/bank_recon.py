# accounting_routes/bank_recon.py
from __future__ import annotations

from flask import (
    Blueprint,
    render_template,
    request,
    url_for,
    Response,
    jsonify,
    redirect,
)
from datetime import datetime, date
from bson import ObjectId
import io, csv
from typing import Any, Dict, List

from db import db

bank_recon_bp = Blueprint("bank_recon", __name__, template_folder="../templates")

# Collections
bank_accounts_col = db["bank_accounts"]
bank_lines_col    = db["bank_statement_lines"]   # imported + manual lines

# Live bank metrics collections (SmartLiving)
payments_col = db["payments"]        # confirmed inbound cash-ins
tax_col      = db["tax_records"]     # P-Tax outflows (source_bank_id)
# NOTE: Old BDC/OMC collection removed for SmartLiving
# sbdc_col  = db["s_bdc_payment"]


# ----------------- helpers -----------------
def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        if v is None or v == "":
            return default
        return float(v)
    except Exception:
        return default


def _currency_symbol(code: str) -> str:
    code = (code or "").upper()
    if code in ("GHS", "GH₵", "GHC"):
        return "GH₵"
    if code == "USD":
        return "$"
    if code == "EUR":
        return "€"
    if code == "GBP":
        return "£"
    return ""


def _iso(d: str | None):
    """Parse YYYY-MM-DD into datetime or return None."""
    if not d:
        return None
    try:
        return datetime.fromisoformat(d)
    except Exception:
        return None


def _signed_amount(amount: float, direction: str) -> float:
    """
    Convert amount + direction ('debit'/'credit') into signed value.
    Simplified rule: credits = +, debits = -.
    """
    direction = (direction or "debit").lower()
    if direction == "credit":
        return amount
    return -amount


def _last4(acc_number: str | None) -> str:
    s = str(acc_number or "")
    return s[-4:] if len(s) >= 4 else s


def _build_account_label(acc_doc: Dict[str, Any]) -> str:
    """
    Build a human-friendly label for a bank or mobile wallet, e.g.:
      - "GCB • Main Current (…1234)"
      - "MTN • Main Wallet (…9876)"
    """
    if not acc_doc:
        return "Account / Wallet"

    account_type = (acc_doc.get("account_type") or "bank").lower().strip()

    raw_acc_no = acc_doc.get("account_no") or acc_doc.get("account_number") or ""
    last4 = _last4(raw_acc_no)

    if account_type == "mobile":
        network = acc_doc.get("network") or acc_doc.get("bank_name") or ""
        wallet_name = acc_doc.get("account_name") or ""
        parts = [p for p in (network, wallet_name) if p]
        label = " • ".join(parts) if parts else "Mobile Money Wallet"
    else:
        bank_name = acc_doc.get("bank_name") or ""
        account_name = acc_doc.get("account_name") or ""
        parts = [p for p in (bank_name, account_name) if p]
        label = " • ".join(parts) if parts else (bank_name or account_name or "Bank Account")

    if last4:
        label = f"{label} (…{last4})"

    return label or "Account / Wallet"


def _sum_confirmed_in(bank_name: str, last4: str) -> float:
    """
    Sum of confirmed inbound payments for this bank (by name + last4).

    NOTE: This is still using bank_name + account_last4 for compatibility with
    existing SmartLiving payment records.
    """
    try:
        pipe = [
            {
                "$match": {
                    "bank_name": bank_name,
                    "account_last4": last4,
                    "status": "confirmed",
                }
            },
            {"$group": {"_id": None, "total": {"$sum": "$amount"}}},
        ]
        row = next(payments_col.aggregate(pipe), None)
        return _safe_float(row["total"]) if row else 0.0
    except Exception:
        return 0.0


def _sum_ptax_out(bank_oid: ObjectId) -> float:
    """Sum of P-Tax payments made from this bank (tax_records.source_bank_id)."""
    try:
        pipe = [
            {
                "$match": {
                    "source_bank_id": bank_oid,
                    "type": {"$regex": r"^p[\s_-]*tax$", "$options": "i"},
                }
            },
            {"$group": {"_id": None, "total": {"$sum": "$amount"}}},
        ]
        row = next(tax_col.aggregate(pipe), None)
        return _safe_float(row["total"]) if row else 0.0
    except Exception:
        return 0.0


# --------------------------------------------------------------------
# Index helper: /bank-recon → redirect to first account
# --------------------------------------------------------------------
@bank_recon_bp.get("/bank-recon")
def bank_recon_index():
    """
    If user opens /bank-recon without specifying an account,
    redirect them to the first available bank OR mobile wallet.

    If there are no accounts yet, send them back to the Bank & Cash page.
    """
    acc = bank_accounts_col.find_one(
        {
            "$or": [
                {"account_type": {"$in": ["bank", "mobile"]}},
                {"account_type": {"$exists": False}},  # backwards-compatible
            ]
        }
    )
    if not acc:
        # No bank accounts yet → go back to the accounts dashboard
        return redirect(url_for("bank_accounts.list_accounts"))

    return redirect(url_for("bank_recon.view", account_id=str(acc["_id"])))


# --------------------------------------------------------------------
# Main reconciliation workspace for a specific account
# --------------------------------------------------------------------
@bank_recon_bp.get("/bank-recon/<account_id>")
def view(account_id):
    """
    Bank Reconciliation workspace for a single bank account / mobile wallet.

    Shows:
    - Live bank metrics (opening, inflow, P-Tax, net flow, live balance)
    - Summary (cleared, statement, difference)
    - GL side (placeholder – can be wired to real ledger later)
    - Bank statement side (imported + manual)
    """
    try:
        oid = ObjectId(account_id)
    except Exception:
        return "Invalid account id", 404

    acc_doc = bank_accounts_col.find_one({"_id": oid})
    if not acc_doc:
        return "Bank account not found", 404

    # ==== Live bank metrics (SmartLiving) ====
    bank_name = acc_doc.get("bank_name") or ""
    raw_acc_no = acc_doc.get("account_no") or acc_doc.get("account_number") or ""
    last4 = _last4(raw_acc_no)

    opening = _safe_float(acc_doc.get("opening_balance"))
    total_in = _sum_confirmed_in(bank_name, last4)
    ptax_out = _sum_ptax_out(oid)

    # For now, SmartLiving bank recon omits the old BDC/OMC outflows.
    total_out = ptax_out
    live_balance = opening + total_in - total_out

    bank_metrics = {
        "opening_balance": round(opening, 2),
        "total_in": round(total_in, 2),
        "ptax_out": round(ptax_out, 2),
        "net_flow": round(total_in - total_out, 2),
        "live_balance": round(live_balance, 2),
    }

    # All accounts (bank + mobile) to populate the select dropdown
    accounts_all = list(
        bank_accounts_col.find(
            {
                "$or": [
                    {"account_type": {"$in": ["bank", "mobile"]}},
                    {"account_type": {"$exists": False}},  # backwards-compatible
                ]
            }
        )
    )
    accounts_for_select: List[Dict[str, Any]] = []
    for a in accounts_all:
        oid_a = a.get("_id")
        if not isinstance(oid_a, ObjectId):
            continue
        label = _build_account_label(a)
        raw_no = a.get("account_no") or a.get("account_number") or ""
        accounts_for_select.append(
            {
                "id": str(oid_a),
                "label": label,
                "account_type": (a.get("account_type") or "bank").lower(),
                "last4": _last4(raw_no),
            }
        )

    selected_account = {
        "id": str(acc_doc.get("_id")),
        "label": _build_account_label(acc_doc),
        "account_type": (acc_doc.get("account_type") or "bank").lower(),
        "account_no": raw_acc_no,
        "last4": last4,
    }

    # --- Filter period (optional) ---
    from_date = _iso(request.args.get("from"))
    to_date   = _iso(request.args.get("to"))

    q: Dict[str, Any] = {"account_id": oid}
    if from_date or to_date:
        date_filter: Dict[str, Any] = {}
        if from_date:
            date_filter["$gte"] = from_date
        if to_date:
            date_filter["$lte"] = to_date
        q["date"] = date_filter

    # --- Bank statement lines ---
    all_lines = list(bank_lines_col.find(q))

    bank_entries: List[Dict[str, Any]] = []
    statement_balance = 0.0

    for ln in all_lines:
        amt = _safe_float(ln.get("amount"))
        direction = ln.get("direction", "debit")
        signed = _signed_amount(amt, direction)
        statement_balance += signed

        dt = ln.get("date")
        if isinstance(dt, datetime):
            dt_str = dt.strftime("%Y-%m-%d")
        else:
            dt_str = ""

        bank_entries.append(
            {
                "id": str(ln.get("_id")),
                "date": dt_str,
                "description": ln.get("description", ""),
                "amount": amt,
                "matched": bool(ln.get("matched", False)),
            }
        )

    bank_total    = len(bank_entries)
    bank_matched  = sum(1 for b in bank_entries if b["matched"])
    bank_counts   = {
        "total": bank_total,
        "matched": bank_matched,
        "unmatched": bank_total - bank_matched,
    }

    # --- GL side (placeholder: empty for now) ---
    gl_entries: List[Dict[str, Any]] = []
    gl_counts = {"total": 0, "matched": 0, "unmatched": 0}

    cleared_balance = 0.0  # When you wire actual GL matches, update this
    difference       = statement_balance - cleared_balance

    currency         = acc_doc.get("currency", "GHS")
    currency_symbol  = acc_doc.get("currency_symbol") or _currency_symbol(currency)

    # Period label
    statement_period = ""
    if from_date or to_date:
        ftxt = from_date.strftime("%d %b %Y") if from_date else "Start"
        ttxt = to_date.strftime("%d %b %Y") if to_date else "Today"
        statement_period = f"{ftxt} – {ttxt}"
    elif all_lines:
        dates = [
            ln.get("date") for ln in all_lines
            if isinstance(ln.get("date"), datetime)
        ]
        if dates:
            ftxt = min(dates).strftime("%d %b %Y")
            ttxt = max(dates).strftime("%d %b %Y")
            statement_period = f"{ftxt} – {ttxt}"

    export_url = url_for("bank_recon.export_excel", account_id=account_id)
    today      = date.today().isoformat()

    return render_template(
        "accounting/bank_reconciliation.html",
        accounts=accounts_for_select,
        selected_account=selected_account,
        currency_symbol=currency_symbol,
        cleared_balance=cleared_balance,
        statement_balance=statement_balance,
        difference=difference,
        gl_entries=gl_entries,
        bank_entries=bank_entries,
        gl_counts=gl_counts,
        bank_counts=bank_counts,
        export_url=export_url,
        statement_period=statement_period,
        today=today,
        bank_metrics=bank_metrics,  # 🔥 live metrics for the template
    )


@bank_recon_bp.post("/bank-recon/<account_id>/import")
def import_statement(account_id):
    """
    Import a statement file (CSV for now) and create bank lines.
    Expected headers (flexible):
      - date / Date
      - description / Details
      - amount   OR (debit + credit)
    """
    try:
        oid = ObjectId(account_id)
    except Exception:
        return "Invalid account id", 404

    f = request.files.get("statement_file")
    if not f:
        return redirect(url_for("bank_recon.view", account_id=account_id))

    filename = f.filename or ""
    ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""

    rows = []

    if ext == "csv":
        text = f.stream.read().decode("utf-8", errors="ignore")
        reader = csv.DictReader(io.StringIO(text))
        for r in reader:
            rows.append(r)
    else:
        # For now only CSV; you can extend to XLSX later.
        return redirect(url_for("bank_recon.view", account_id=account_id))

    for r in rows:
        desc = (r.get("description") or r.get("Details") or "").strip()

        # Amount logic: either single column or debit/credit pair
        amt = 0.0
        direction = "debit"

        if "amount" in r and r.get("amount") not in (None, ""):
            amt = _safe_float(r.get("amount"))
            direction = "credit" if amt >= 0 else "debit"
            amt = abs(amt)
        else:
            debit  = _safe_float(r.get("debit"))
            credit = _safe_float(r.get("credit"))
            if credit:
                amt = credit
                direction = "credit"
            else:
                amt = debit
                direction = "debit"

        dt_raw = r.get("date") or r.get("Date")
        dt = None
        if dt_raw:
            for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y"):
                try:
                    dt = datetime.strptime(dt_raw, fmt)
                    break
                except Exception:
                    continue

        doc = {
            "account_id": oid,
            "date": dt or datetime.utcnow(),
            "description": desc,
            "amount": amt,
            "direction": direction,   # debit / credit
            "matched": False,
            "created_at": datetime.utcnow(),
            "source": "import",
        }
        bank_lines_col.insert_one(doc)

    return redirect(url_for("bank_recon.view", account_id=account_id))


@bank_recon_bp.post("/bank-recon/<account_id>/manual")
def add_manual_entry(account_id):
    """
    Handle the slide-over 'Add Bank Amount' manual entry form.
    Returns JSON {ok: bool, message?}
    """
    try:
        oid = ObjectId(account_id)
    except Exception:
        return jsonify(ok=False, message="Invalid account id."), 400

    date_str = request.form.get("date") or ""
    try:
        dt = datetime.fromisoformat(date_str)
    except Exception:
        dt = datetime.utcnow()

    doc = {
        "account_id": oid,
        "date": dt,
        "line_type": request.form.get("line_type") or "other",
        "description": (request.form.get("description") or "").strip(),
        "amount": _safe_float(request.form.get("amount")),
        "direction": (request.form.get("direction") or "debit").lower(),
        "notes": (request.form.get("notes") or "").strip(),
        "matched": False,
        "created_at": datetime.utcnow(),
        "source": "manual",
    }

    bank_lines_col.insert_one(doc)
    return jsonify(ok=True)


@bank_recon_bp.get("/bank-recon/<account_id>/export")
def export_excel(account_id):
    """
    Export the current bank lines for this account as CSV (Excel-compatible).
    """
    try:
        oid = ObjectId(account_id)
    except Exception:
        return "Invalid account id", 404

    lines = list(bank_lines_col.find({"account_id": oid}))

    out = io.StringIO()
    w = csv.writer(out)

    w.writerow(["Date", "Description", "Amount", "Direction", "Matched", "Source"])

    for ln in lines:
        dt = ln.get("date")
        if isinstance(dt, datetime):
            dt = dt.strftime("%Y-%m-%d")

        w.writerow(
            [
                dt or "",
                ln.get("description", ""),
                f"{_safe_float(ln.get('amount')):0.2f}",
                ln.get("direction", ""),
                "Yes" if ln.get("matched") else "No",
                ln.get("source", ""),
            ]
        )

    return Response(
        out.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": 'attachment; filename="bank_reconciliation.csv"'},
    )


@bank_recon_bp.post("/bank-recon/<account_id>/finalize")
def finalize(account_id):
    """
    Finalize reconciliation for this account:
    - Marks last_reconciled on bank account.
    - (Later) you can also store snapshots / lock period.
    """
    try:
        oid = ObjectId(account_id)
    except Exception:
        return jsonify(ok=False, message="Invalid account id."), 400

    bank_accounts_col.update_one(
        {"_id": oid},
        {"$set": {"last_reconciled": datetime.utcnow()}},
    )

    return jsonify(ok=True)

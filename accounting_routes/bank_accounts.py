from __future__ import annotations

from flask import Blueprint, render_template, request, url_for, Response, jsonify
from datetime import datetime, date
import io, csv
from typing import Any, Dict, List

from bson import ObjectId
from db import db
from accounting_services import post_withdrawal

# NOTE: blueprint name matches the endpoints Flask suggested: acc_bank_accounts.*
bank_accounts_bp = Blueprint("acc_bank_accounts", __name__, template_folder="../templates")

# Collections
accounts_col           = db["bank_accounts"]
payments_col           = db["payments"]            # confirmed inbound cash-ins
tax_col                = db["tax_records"]         # P-Tax outflows (source_bank_id)
sbdc_col               = db["s_bdc_payment"]       # BDC bank payments (bank_paid_history)
manager_deposits_col   = db["manager_deposits"]    # manager deposits linked by bank_account_id
withdrawals_col        = db["withdrawals"]         # unified withdrawals (bank/momo/cash)


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


def _last4(acc_number: str | None) -> str:
    s = str(acc_number or "")
    return s[-4:] if len(s) >= 4 else s


def _fmt(amount: float) -> str:
    """
    Format a float with thousand separators and 2 decimals,
    e.g. 1234567.8 -> '1,234,567.80'
    """
    return f"{_safe_float(amount):,.2f}"


def _sum_confirmed_in(bank_name: str, last4: str) -> float:
    """Sum of confirmed inbound payments for this bank (by name + last4)."""
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


def _sum_manager_deposits_in(bank_oid: ObjectId) -> float:
    """
    Sum of manager deposits linked to this bank account.

    We aggregate on manager_deposits.bank_account_id == str(bank_oid),
    and only include non-rejected statuses (submitted/approved).
    """
    try:
        bank_id_str = str(bank_oid)
        pipe = [
            {
                "$match": {
                    "bank_account_id": bank_id_str,
                    "status": {"$in": ["submitted", "approved"]},
                }
            },
            {"$group": {"_id": None, "total": {"$sum": "$amount"}}},
        ]
        row = next(manager_deposits_col.aggregate(pipe), None)
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


def _sum_bdc_out(bank_oid: ObjectId) -> float:
    """Sum of BDC bank payments (sum bank_paid_history.amount where bank_id==bank_oid)."""
    try:
        pipe = [
            {"$match": {"bank_paid_history": {"$exists": True, "$ne": []}}},
            {"$unwind": "$bank_paid_history"},
            {"$match": {"bank_paid_history.bank_id": bank_oid}},
            {"$group": {"_id": None, "total": {"$sum": "$bank_paid_history.amount"}}},
        ]
        row = next(sbdc_col.aggregate(pipe), None)
        return _safe_float(row["total"]) if row else 0.0
    except Exception:
        return 0.0


def _sum_withdrawals_out(bank_oid: ObjectId) -> float:
    """Sum of withdrawals from this account."""
    try:
        pipe = [
            {"$match": {"account_id": bank_oid}},
            {"$group": {"_id": None, "total": {"$sum": "$amount"}}},
        ]
        row = next(withdrawals_col.aggregate(pipe), None)
        return _safe_float(row["total"]) if row else 0.0
    except Exception:
        return 0.0


def _account_type_label(account_type: str) -> str:
    t = (account_type or "bank").lower()
    if t == "mobile_money":
        return "Mobile Money"
    if t == "cash":
        return "Cash"
    return "Bank"


# ----------------- pages -----------------
@bank_accounts_bp.get("/bank-accounts")
def list_accounts():
    """
    Bank & Cash Accounts dashboard.
    Includes:
      - Bank accounts
      - Mobile money wallets
      - Cash accounts

    Each has:
      - Opening balance
      - Confirmed inflows (customer payments)
      - Manager deposits linked to bank accounts
      - P-Tax outflows
      - BDC outflows
      - Manual withdrawals
    """
    docs = list(accounts_col.find({}).sort("bank_name", 1))

    accounts: List[Dict[str, Any]] = []
    total_live_balance = 0.0

    bank_total = 0.0
    momo_total = 0.0
    cash_total = 0.0

    for d in docs:
        bank_oid = d.get("_id")
        if not isinstance(bank_oid, ObjectId):
            continue

        # New: classify account type (default = bank for old records)
        acc_type = (d.get("account_type") or "bank").lower().strip()
        if acc_type not in ("bank", "mobile_money", "cash"):
            acc_type = "bank"

        bank_name = d.get("bank_name") or ""
        raw_acc_no = d.get("account_no") or d.get("account_number") or ""
        last4 = _last4(raw_acc_no)

        opening = _safe_float(d.get("opening_balance"))

        # Inflows
        # For mobile money + cash, payments aggregation may be zero (which is fine),
        # but we keep the same logic for consistency.
        confirmed_in = _sum_confirmed_in(bank_name, last4)
        manager_in = _sum_manager_deposits_in(bank_oid)
        total_in = confirmed_in + manager_in

        # Outflows
        ptax_out = _sum_ptax_out(bank_oid)
        bdc_out = _sum_bdc_out(bank_oid)
        withdraw_out = _sum_withdrawals_out(bank_oid)
        total_out = ptax_out + bdc_out + withdraw_out

        # Live balance
        live_balance = opening + total_in - total_out
        total_live_balance += live_balance

        # Totals by type
        if acc_type == "mobile_money":
            momo_total += live_balance
        elif acc_type == "cash":
            cash_total += live_balance
        else:
            bank_total += live_balance

        cur = (d.get("currency") or "GHS").upper()
        sym = d.get("currency_symbol") or _currency_symbol(cur)

        account_no_masked = f"…{last4}" if last4 else ""

        metrics: Dict[str, Any] = {
            "confirmed_in": confirmed_in,
            "manager_in": manager_in,
            "total_in": total_in,
            "ptax_out": ptax_out,
            "bdc_out": bdc_out,
            "withdraw_out": withdraw_out,
            "total_out": total_out,
            "net_flow": total_in - total_out,
        }
        # Preformatted strings with thousand separators
        metrics["confirmed_in_display"] = _fmt(metrics["confirmed_in"])
        metrics["manager_in_display"] = _fmt(metrics["manager_in"])
        metrics["total_in_display"] = _fmt(metrics["total_in"])
        metrics["ptax_out_display"] = _fmt(metrics["ptax_out"])
        metrics["bdc_out_display"] = _fmt(metrics["bdc_out"])
        metrics["withdraw_out_display"] = _fmt(metrics["withdraw_out"])
        metrics["total_out_display"] = _fmt(metrics["total_out"])
        metrics["net_flow_display"] = _fmt(metrics["net_flow"])

        acc_dict: Dict[str, Any] = dict(d)
        acc_dict["id"] = str(bank_oid)
        acc_dict["opening_balance"] = opening
        acc_dict["opening_balance_display"] = _fmt(opening)
        acc_dict["balance"] = live_balance
        acc_dict["balance_display"] = _fmt(live_balance)
        acc_dict["currency"] = cur
        acc_dict["currency_symbol"] = sym
        acc_dict["account_no_masked"] = account_no_masked
        acc_dict["last_reconciled"] = d.get("last_reconciled")
        acc_dict["metrics"] = metrics
        acc_dict["account_type"] = acc_type
        acc_dict["account_type_label"] = _account_type_label(acc_type)

        accounts.append(acc_dict)

    today = date.today().isoformat()

    if accounts:
        first = accounts[0]
        sym = first.get("currency_symbol") or _currency_symbol(first.get("currency", "GHS"))
    else:
        sym = "GH₵"

    total_live_balance = float(total_live_balance)
    total_display = f"{sym}{_fmt(total_live_balance)}"
    bank_total_display = f"{sym}{_fmt(bank_total)}"
    momo_total_display = f"{sym}{_fmt(momo_total)}"
    cash_total_display = f"{sym}{_fmt(cash_total)}"

    # recent manager cash deposits for dashboard section
    accounts_by_id = {acc["id"]: acc for acc in accounts}
    recent_deposits = list(
        manager_deposits_col.find({"status": {"$in": ["submitted", "approved"]}})
        .sort("created_at", -1)
        .limit(12)
    )

    enriched_deposits: List[Dict[str, Any]] = []
    for d in recent_deposits:
        bank_id_str = d.get("bank_account_id") or ""
        acc_info = accounts_by_id.get(bank_id_str)
        doc = dict(d)
        doc["_id"] = str(doc.get("_id"))
        doc["bank_name"] = doc.get("bank_name") or (acc_info.get("bank_name") if acc_info else "")
        doc["account_name"] = doc.get("account_name") or (acc_info.get("account_name") if acc_info else "")
        doc["currency_symbol"] = (
            acc_info.get("currency_symbol") if acc_info else _currency_symbol(doc.get("currency") or "GHS")
        )
        doc["amount_display"] = _fmt(_safe_float(doc.get("amount")))
        enriched_deposits.append(doc)

    # endpoint uses the blueprint name acc_bank_accounts.*
    export_url = url_for("acc_bank_accounts.export_excel")

    return render_template(
        "accounting/bank_accounts.html",
        accounts=accounts,
        total_display=total_display,
        bank_total_display=bank_total_display,
        momo_total_display=momo_total_display,
        cash_total_display=cash_total_display,
        today=today,
        export_url=export_url,
        recent_deposits=enriched_deposits,
    )


@bank_accounts_bp.post("/bank-accounts/quick-create")
def quick_create():
    """
    Handles the slide-over 'Add Account' form.
    Supports:
      - Bank accounts
      - Mobile money wallets
      - Cash accounts
    """
    account_type = (request.form.get("account_type") or "bank").strip().lower()
    if account_type not in ("bank", "mobile_money", "cash"):
        account_type = "bank"

    account_name = (request.form.get("account_name") or "").strip()
    raw_bank_name = (request.form.get("bank_name") or "").strip()
    network = (request.form.get("network") or "").strip()
    account_no = (request.form.get("account_no") or "").strip()

    errors: list[str] = []

    if not account_name:
        errors.append("Account name is required.")

    # For bank, bank_name is required
    if account_type == "bank" and not raw_bank_name:
        errors.append("Bank name is required for a bank account.")

    # For mobile money, network is required
    if account_type == "mobile_money" and not network:
        errors.append("Network is required for a mobile money wallet.")

    if errors:
        return jsonify(ok=False, message="; ".join(errors)), 400

    if account_type == "mobile_money":
        bank_name = network
    else:
        bank_name = raw_bank_name

    data = {
        "account_type": account_type,  # "bank" | "mobile_money" | "cash"
        "account_name": account_name,
        "bank_name": bank_name,
        "account_no": account_no,
        "currency": (request.form.get("currency") or "GHS").upper(),
        "opening_balance": _safe_float(request.form.get("opening_balance")),
        "as_of_date": request.form.get("as_of_date") or None,
        "notes": (request.form.get("notes") or "").strip(),
        "created_at": datetime.utcnow(),
    }

    if data["as_of_date"]:
        try:
            data["as_of_date"] = datetime.fromisoformat(data["as_of_date"])
        except Exception:
            data["as_of_date"] = None

    # Optional: store network separately for mobile money
    if account_type == "mobile_money":
        data["network"] = network

    data["balance"] = data["opening_balance"]
    data["currency_symbol"] = _currency_symbol(data["currency"])
    data["last_reconciled"] = None

    res = accounts_col.insert_one(data)
    return jsonify(ok=True, id=str(res.inserted_id))


@bank_accounts_bp.get("/bank-accounts/export")
def export_excel():
    """
    Export bank accounts as CSV (Excel-compatible).
    (Totals here are kept numeric without thousand separators.)
    """
    docs = list(accounts_col.find({}).sort("bank_name", 1))

    out = io.StringIO()
    w = csv.writer(out)

    w.writerow([
        "Account Type",
        "Bank / Network / Cash",
        "Account Name",
        "Account Number",
        "Currency",
        "Opening Balance",
        "Confirmed Inflow (payments)",
        "Manager Deposits",
        "Total Inflow",
        "P-Tax Out",
        "BDC Out",
        "Withdrawals Out",
        "Net Flow (In - Out)",
        "Live Balance",
        "Last Reconciled",
    ])

    for d in docs:
        bank_oid = d.get("_id")
        if not isinstance(bank_oid, ObjectId):
            continue

        acc_type = (d.get("account_type") or "bank").lower().strip()
        if acc_type not in ("bank", "mobile_money", "cash"):
            acc_type = "bank"

        bank_name = d.get("bank_name") or ""
        raw_acc_no = d.get("account_no") or d.get("account_number") or ""
        last4 = _last4(raw_acc_no)

        opening = _safe_float(d.get("opening_balance"))

        confirmed_in = _sum_confirmed_in(bank_name, last4)
        manager_in = _sum_manager_deposits_in(bank_oid)
        total_in = confirmed_in + manager_in

        ptax_out = _sum_ptax_out(bank_oid)
        bdc_out = _sum_bdc_out(bank_oid)
        withdraw_out = _sum_withdrawals_out(bank_oid)
        total_out = ptax_out + bdc_out + withdraw_out

        net_flow = total_in - total_out
        live_balance = opening + net_flow

        cur = (d.get("currency") or "GHS").upper()
        last = d.get("last_reconciled")
        if isinstance(last, datetime):
            last = last.strftime("%Y-%m-%d")

        w.writerow([
            _account_type_label(acc_type),
            bank_name,
            d.get("account_name", ""),
            raw_acc_no,
            cur,
            f"{opening:0.2f}",
            f"{confirmed_in:0.2f}",
            f"{manager_in:0.2f}",
            f"{total_in:0.2f}",
            f"{ptax_out:0.2f}",
            f"{bdc_out:0.2f}",
            f"{withdraw_out:0.2f}",
            f"{net_flow:0.2f}",
            f"{live_balance:0.2f}",
            last or "",
        ])

    return Response(
        out.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": 'attachment; filename="bank_accounts.csv"'},
    )


@bank_accounts_bp.get("/bank-accounts/<bank_id>/profile")
def bank_profile(bank_id: str):
    """
    Account profile page (Bank / Mobile Money / Cash):
    - Shows summary (opening, inflows, outflows, live balance)
    - Lists manager deposits into this account
    - Lists payment inflows & tax/BDC/withdrawals outflows
    """
    try:
        bank_oid = ObjectId(bank_id)
    except Exception:
        return "Invalid bank id", 400

    account = accounts_col.find_one({"_id": bank_oid})
    if not account:
        return "Bank account not found", 404

    acc_type = (account.get("account_type") or "bank").lower().strip()
    if acc_type not in ("bank", "mobile_money", "cash"):
        acc_type = "bank"

    bank_name = account.get("bank_name") or ""
    raw_acc_no = account.get("account_no") or account.get("account_number") or ""
    last4 = _last4(raw_acc_no)
    cur = (account.get("currency") or "GHS").upper()
    sym = account.get("currency_symbol") or _currency_symbol(cur)

    opening = _safe_float(account.get("opening_balance"))

    # Totals
    confirmed_in = _sum_confirmed_in(bank_name, last4)
    manager_in = _sum_manager_deposits_in(bank_oid)
    total_in = confirmed_in + manager_in
    ptax_out = _sum_ptax_out(bank_oid)
    bdc_out = _sum_bdc_out(bank_oid)
    withdraw_out = _sum_withdrawals_out(bank_oid)
    total_out = ptax_out + bdc_out + withdraw_out

    live_balance = opening + total_in - total_out

    # Manager deposits list
    deposits = list(
        manager_deposits_col.find({"bank_account_id": str(bank_oid)}).sort("created_at", -1)
    )

    # Customer payment inflows
    payments = list(
        payments_col.find({
            "bank_name": bank_name,
            "account_last4": last4,
            "status": "confirmed",
        }).sort("date", -1)
    )

    # P-Tax entries
    ptax_entries = list(
        tax_col.find({"source_bank_id": bank_oid}).sort("date", -1)
    )

    # BDC entries (flatten bank_paid_history for this account)
    bdc_entries: list[dict[str, Any]] = []
    bdc_cursor = sbdc_col.aggregate([
        {"$match": {"bank_paid_history": {"$exists": True, "$ne": []}}},


        {"$unwind": "$bank_paid_history"},
        {"$match": {"bank_paid_history.bank_id": bank_oid}},
        {
            "$project": {
                "_id": 1,
                "vendor": 1,
                "reference": 1,
                "created_at": 1,
                "bank_paid_history": 1,
            }
        },
    ])
    for row in bdc_cursor:
        hist = row.get("bank_paid_history") or {}
        bdc_entries.append(
            {
                "amount": _safe_float(hist.get("amount")),
                "when": hist.get("date") or row.get("created_at"),
                "reference": hist.get("reference") or row.get("reference") or "",
                "vendor": row.get("vendor") or "",
            }
        )

    # Manual withdrawals list
    withdrawals = list(
        withdrawals_col.find({"account_id": bank_oid}).sort("date_dt", -1)
    )

    # For header display
    account_type_label = _account_type_label(acc_type)
    network = account.get("network") if acc_type == "mobile_money" else None

    # Choose header display name nicely
    if acc_type == "mobile_money":
        display_title = account.get("account_name") or bank_name or "Mobile Money Wallet"
        display_subtitle = []
        if network:
            display_subtitle.append(network)
        if raw_acc_no:
            display_subtitle.append(raw_acc_no)
        subtitle_str = " • ".join(display_subtitle)
    elif acc_type == "cash":
        display_title = account.get("account_name") or "Cash Account"
        subtitle_str = bank_name or ""
    else:
        # Bank
        display_title = bank_name or account.get("account_name") or "Bank Account"
        subtitle_bits = []
        if account.get("account_name"):
            subtitle_bits.append(account.get("account_name"))
        if raw_acc_no:
            subtitle_bits.append(raw_acc_no)
        subtitle_str = " • ".join(subtitle_bits)

    today_iso = date.today().isoformat()

    context = {
        "account": account,
        "bank_id": bank_id,
        "bank_name": bank_name,
        "account_no": raw_acc_no,
        "account_last4": last4,
        "currency": cur,
        "currency_symbol": sym,
        "opening": opening,
        "opening_display": _fmt(opening),
        "confirmed_in": confirmed_in,
        "confirmed_in_display": _fmt(confirmed_in),
        "manager_in": manager_in,
        "manager_in_display": _fmt(manager_in),
        "total_in": total_in,
        "total_in_display": _fmt(total_in),
        "ptax_out": ptax_out,
        "ptax_out_display": _fmt(ptax_out),
        "bdc_out": bdc_out,
        "bdc_out_display": _fmt(bdc_out),
        "withdraw_out": withdraw_out,
        "withdraw_out_display": _fmt(withdraw_out),
        "total_out": total_out,
        "total_out_display": _fmt(total_out),
        "live_balance": live_balance,
        "live_balance_display": _fmt(live_balance),
        "deposits": deposits,
        "payments": payments,
        "ptax_entries": ptax_entries,
        "bdc_entries": bdc_entries,
        "withdrawals": withdrawals,
        "account_type": acc_type,
        "account_type_label": account_type_label,
        "network": network,
        "display_title": display_title,
        "display_subtitle": subtitle_str,
        "today_iso": today_iso,
    }

    return render_template("accounting/bank_profile.html", **context)


@bank_accounts_bp.post("/bank-accounts/<bank_id>/withdraw")
def withdraw(bank_id: str):
    """
    Record a manual withdrawal from this account (bank / MoMo / cash).
    This will reduce the computed live balance (via bank_withdrawals).
    """
    try:
        bank_oid = ObjectId(bank_id)
    except Exception:
        return jsonify(ok=False, message="Invalid account id."), 400

    account = accounts_col.find_one({"_id": bank_oid})
    if not account:
        return jsonify(ok=False, message="Account not found."), 404

    amount = _safe_float(request.form.get("amount"))
    if amount <= 0:
        return jsonify(ok=False, message="Amount must be greater than zero."), 400

    txn_date_str = request.form.get("txn_date") or ""
    try:
        if txn_date_str:
            txn_date = datetime.fromisoformat(txn_date_str)
        else:
            txn_date = datetime.utcnow()
    except Exception:
        txn_date = datetime.utcnow()

    purpose = (request.form.get("purpose") or "").strip().lower()
    purpose_note = (request.form.get("purpose_note") or "").strip()
    expense_category = (request.form.get("expense_category") or "").strip()
    expense_description = (request.form.get("expense_description") or "").strip()
    asset_category = (request.form.get("asset_category") or "").strip()
    asset_description = (request.form.get("asset_description") or "").strip()
    counterparty = (request.form.get("counterparty") or "").strip()

    result = post_withdrawal({
        "amount": amount,
        "account_type": account.get("account_type") or "bank",
        "account_id": bank_id,
        "purpose": purpose,
        "purpose_note": purpose_note,
        "expense_category": expense_category,
        "expense_description": expense_description,
        "asset_category": asset_category,
        "asset_description": asset_description,
        "counterparty": counterparty,
        "date_dt": txn_date,
        "created_by": None,
    })

    if not result.get("ok"):
        return jsonify(ok=False, message=result.get("message") or "Failed to post withdrawal."), 400

    return jsonify(ok=True, message="Withdrawal recorded successfully.")

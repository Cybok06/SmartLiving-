# routes/manager_deposits.py
from __future__ import annotations

from flask import (
    Blueprint, render_template, request, redirect, url_for,
    flash, session, current_app, jsonify
)
from werkzeug.utils import secure_filename
from bson import ObjectId
from datetime import datetime, timezone
import os, uuid
from typing import Any, Dict, List

from db import db
from services.deposit_analytics import compute_deposit_analytics

manager_deposits_bp = Blueprint(
    "manager_deposits",
    __name__,
    url_prefix="/manager/deposits",
)

# Collections
users_collection       = db["users"]
manager_deposits_col   = db["manager_deposits"]   # collection for deposit proofs
accounts_col           = db["bank_accounts"]      # bank, mobile money & cash accounts

# ---------- File config ----------
ALLOWED_EXTENSIONS = {"png", "jpg", "jpeg", "webp", "pdf"}


def _allowed_file(filename: str) -> bool:
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS


def _receipts_dir() -> str:
    """
    Absolute path to <UPLOADS_ROOT>/receipts; ensures the folder exists.
    """
    uploads_root = current_app.config.get("UPLOADS_ROOT")
    if not uploads_root:
        uploads_root = os.path.join(
            os.path.abspath(os.path.dirname(__file__)), "..", "uploads"
        )
        uploads_root = os.path.abspath(uploads_root)
    receipts = os.path.join(uploads_root, "receipts")
    os.makedirs(receipts, exist_ok=True)
    return receipts


def _save_receipt(file_storage):
    """
    Save upload to <UPLOADS_ROOT>/receipts with a unique safe name.
    Returns the relative path used by /uploads/<path>, e.g. 'receipts/abc.jpg'
    """
    if not file_storage or not file_storage.filename.strip():
        return None
    if not _allowed_file(file_storage.filename):
        return None

    base, ext = os.path.splitext(file_storage.filename)
    ext = ext.lower().lstrip(".")
    safe_base = (secure_filename(base) or "receipt")[:50]
    unique_name = f"{safe_base}-{uuid.uuid4().hex[:8]}.{ext}"
    absolute_path = os.path.join(_receipts_dir(), unique_name)
    file_storage.save(absolute_path)
    return f"receipts/{unique_name}"


# ---------- Auth helpers ----------
def _require_manager_session():
    manager_id = session.get("manager_id")
    if not manager_id:
        flash("Please log in as a manager to continue.", "error")
        return None, None

    try:
        q = {"_id": ObjectId(manager_id)}
    except Exception:
        q = {"_id": manager_id}

    manager_doc = users_collection.find_one({**q, "role": "manager"})
    if not manager_doc:
        session.clear()
        flash("Access denied. Please log in as a manager.", "error")
        return None, None

    status = str(manager_doc.get("status", "")).lower()
    if status in ("not active", "inactive", "disabled"):
        session.clear()
        flash("Your account is not active. Contact an administrator.", "error")
        return None, None

    return (str(manager_doc["_id"]), manager_doc)


# ---------- Normalization / helpers ----------
_ALLOWED_METHODS = {
    "bank": "Bank",
    "mobile money": "Mobile Money",
    "mobile_money": "Mobile Money",
    "momo": "Mobile Money",
    "cash": "Cash",
}


def _normalize_method_type(val: str) -> str | None:
    key = (val or "").strip().lower().replace("_", " ")
    return _ALLOWED_METHODS.get(key)


def _last4(acc_number: str | None) -> str:
    s = str(acc_number or "")
    return s[-4:] if len(s) >= 4 else s


def _build_bank_label(bank_doc: Dict[str, Any]) -> str:
    """
    Builds a human-friendly label for an account, e.g.
    - Bank:  "GCB • Main Current (…1234)"
    - MoMo:  "MTN • Collections Wallet (…1234)"
    - Cash:  "Main Cash (…1234)"
    """
    if not bank_doc:
        return "Account / Wallet"

    account_type = (bank_doc.get("account_type") or "").lower().strip()

    raw_acc_no = bank_doc.get("account_no") or bank_doc.get("account_number") or ""
    last4 = _last4(raw_acc_no)

    if account_type == "mobile_money":
        network = bank_doc.get("network") or bank_doc.get("bank_name") or ""
        wallet_name = bank_doc.get("account_name") or ""
        parts = [p for p in [network, wallet_name] if p]
        label = " • ".join(parts) if parts else "Mobile Money Wallet"
    elif account_type == "cash":
        cash_name = bank_doc.get("account_name") or bank_doc.get("bank_name") or "Cash Account"
        label = cash_name
    else:
        bank_name = bank_doc.get("bank_name") or ""
        account_name = bank_doc.get("account_name") or ""
        parts = [p for p in [bank_name, account_name] if p]
        label = " • ".join(parts) if parts else (bank_name or account_name or "Bank Account")

    if last4:
        label = f"{label} (…{last4})"

    return label or "Account / Wallet"


def _is_ajax() -> bool:
    return (request.headers.get("X-Requested-With") == "XMLHttpRequest")


# ---------- Routes ----------
@manager_deposits_bp.route("/", methods=["GET"])
def form_and_list():
    manager_id, manager_doc = _require_manager_session()
    if not manager_id:
        return redirect(url_for("login.login"))

    manager_name = manager_doc.get("name") or manager_doc.get("username") or "Unknown"
    branch_name = manager_doc.get("branch") or manager_doc.get("branch_name") or "Unassigned"

    recent = list(
        manager_deposits_col.find({"manager_id": manager_id})
        .sort("created_at", -1)
        .limit(30)
    )

    # Fetch available bank, mobile money & cash accounts
    bank_accounts: List[Dict[str, str]] = []
    cursor = accounts_col.find(
        {
            "$or": [
                {"account_type": {"$in": ["bank", "mobile_money", "cash"]}},
                {"account_type": {"$exists": False}},  # backwards-compatible
            ]
        }
    ).sort("bank_name", 1)

    for d in cursor:
        oid = d.get("_id")
        if not isinstance(oid, ObjectId):
            continue
        label = _build_bank_label(d)
        acc_type = (d.get("account_type") or "bank").lower()
        if acc_type not in ("bank", "mobile_money", "cash"):
            acc_type = "bank"

        bank_accounts.append(
            {
                "id": str(oid),
                "label": label,
                "account_type": acc_type,
            }
        )

    custom_start = (request.args.get("start") or "").strip()
    custom_end = (request.args.get("end") or "").strip()
    analytics = compute_deposit_analytics(
        branch_name=branch_name,
        custom_start=custom_start,
        custom_end=custom_end,
    )

    return render_template(
        "manager/deposits.html",
        manager_name=manager_name,
        branch_name=branch_name,
        recent=recent,
        bank_accounts=bank_accounts,
        analytics=analytics,
        analytics_json=analytics,
        custom_start=custom_start,
        custom_end=custom_end,
    )


@manager_deposits_bp.route("/submit", methods=["POST"])
def submit_deposit():
    manager_id, manager_doc = _require_manager_session()
    if not manager_id:
        if _is_ajax():
            return jsonify({
                "ok": False,
                "errors": ["Session expired. Please log in again."]
            }), 401
        return redirect(url_for("login.login"))

    manager_name = manager_doc.get("name") or manager_doc.get("username") or "Unknown"
    branch_name = manager_doc.get("branch") or manager_doc.get("branch_name") or "Unassigned"

    amount_raw = (request.form.get("amount") or "").strip()
    method_type_in = (request.form.get("method_type") or "").strip()
    method_type = _normalize_method_type(method_type_in)  # "Bank" | "Mobile Money" | "Cash" | None
    method_name_input = (request.form.get("method_name") or "").strip()
    bank_account_id_raw = (request.form.get("bank_account_id") or "").strip()
    reference = (request.form.get("reference") or "").strip()
    notes = (request.form.get("notes") or "").strip()

    errors: List[str] = []

    # Amount
    try:
        amount = float(amount_raw.replace(",", ""))
        if amount <= 0:
            errors.append("Amount must be greater than zero.")
    except Exception:
        amount = None
        errors.append("Invalid amount.")

    # Method
    if not method_type:
        errors.append("Select a valid method: Bank, Mobile Money, or Cash.")

    bank_doc = None
    bank_account_id_str: str | None = None

    # For all three methods, we now require linking to a specific account
    if method_type in ("Bank", "Mobile Money", "Cash"):
        if not bank_account_id_raw:
            if method_type == "Bank":
                errors.append("Please select the bank account you deposited into.")
            elif method_type == "Mobile Money":
                errors.append("Please select the mobile money wallet you deposited into.")
            elif method_type == "Cash":
                errors.append("Please select the cash account you deposited into.")
        else:
            try:
                bank_oid = ObjectId(bank_account_id_raw)
            except Exception:
                bank_oid = None
                errors.append("Invalid account / wallet selected.")
            if bank_oid:
                bank_doc = accounts_col.find_one({"_id": bank_oid})
                if not bank_doc:
                    errors.append("Selected account / wallet was not found. Please try again.")
                else:
                    bank_account_id_str = str(bank_oid)
                    acct_type = (bank_doc.get("account_type") or "bank").lower()
                    if acct_type not in ("bank", "mobile_money", "cash"):
                        acct_type = "bank"

                    if method_type == "Bank" and acct_type != "bank":
                        errors.append("Selected account is not a bank account.")
                    if method_type == "Mobile Money" and acct_type != "mobile_money":
                        errors.append("Selected account is not a mobile money wallet.")
                    if method_type == "Cash" and acct_type != "cash":
                        errors.append("Selected account is not a cash account.")

    # Receipt
    file = request.files.get("receipt")
    receipt_rel_path = None
    if not file or not file.filename.strip():
        errors.append("Please upload a receipt image (or PDF).")
    else:
        if not _allowed_file(file.filename):
            errors.append("Unsupported file type. Allowed: png, jpg, jpeg, webp, pdf.")
        else:
            receipt_rel_path = _save_receipt(file)
            if not receipt_rel_path:
                errors.append("Could not save file. Try a different image/PDF.")

    if errors:
        if _is_ajax():
            return jsonify({"ok": False, "errors": errors}), 400
        for e in errors:
            flash(e, "danger")
        return redirect(url_for("manager_deposits.form_and_list"))

    # Final method_name to store
    if method_type in ("Bank", "Mobile Money"):
        # Use the account label so it matches the bank-accounts page
        method_name_final = _build_bank_label(bank_doc) if bank_doc else None
    elif method_type == "Cash":
        # Use user-provided text if any, otherwise the account label
        if method_name_input:
            method_name_final = method_name_input
        else:
            method_name_final = _build_bank_label(bank_doc) if bank_doc else None
    else:
        method_name_final = method_name_input or None

    doc: Dict[str, Any] = {
        "manager_id": manager_id,
        "manager_name": manager_name,
        "branch_name": branch_name,
        "amount": amount,
        "method_type": method_type,
        "method_name": method_name_final,
        "reference": reference or None,
        "notes": notes or None,
        "receipt_path": receipt_rel_path,
        "created_at": datetime.now(timezone.utc),
        "status": "submitted",
    }

    if bank_account_id_str:
        doc["bank_account_id"] = bank_account_id_str
        if bank_doc:
            doc["bank_name"] = bank_doc.get("bank_name")
            doc["account_name"] = bank_doc.get("account_name")
            doc["account_type"] = bank_doc.get("account_type")

    manager_deposits_col.insert_one(doc)

    if _is_ajax():
        return jsonify({
            "ok": True,
            "message": "Deposit submitted successfully."
        }), 200

    flash("Deposit submitted successfully.", "success")
    return redirect(url_for("manager_deposits.form_and_list"))

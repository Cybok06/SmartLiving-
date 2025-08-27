# routes/manager_deposits.py
from flask import Blueprint, render_template, request, redirect, url_for, flash, session, current_app
from werkzeug.utils import secure_filename
from bson import ObjectId
from datetime import datetime, timezone
import os, uuid

from db import db

manager_deposits_bp = Blueprint(
    "manager_deposits",
    __name__,
    url_prefix="/manager/deposits",
)

# Collections
users_collection     = db["users"]
manager_deposits_col = db["manager_deposits"]  # collection for deposit proofs

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
        # Fallback: local 'uploads' beside app.py if not configured
        uploads_root = os.path.join(os.path.abspath(os.path.dirname(__file__)), "..", "uploads")
        uploads_root = os.path.abspath(uploads_root)
    receipts = os.path.join(uploads_root, "receipts")
    os.makedirs(receipts, exist_ok=True)
    return receipts

def _save_receipt(file_storage):
    """
    Save upload to <UPLOADS_ROOT>/receipts with a unique safe name.
    Returns the relative path used by the /uploads/<path> route, e.g. 'receipts/abc.jpg'
    """
    if not file_storage or not file_storage.filename.strip():
        return None
    if not _allowed_file(file_storage.filename):
        return None

    base, ext = os.path.splitext(file_storage.filename)
    ext = ext.lower().lstrip(".")
    safe_base = secure_filename(base)[:50] or "receipt"
    unique_name = f"{safe_base}-{uuid.uuid4().hex[:8]}.{ext}"

    absolute_path = os.path.join(_receipts_dir(), unique_name)
    file_storage.save(absolute_path)

    # Public route serves from UPLOADS_ROOT, so return a path relative to that root:
    return f"receipts/{unique_name}"

def _require_manager_session():
    """
    Ensure a manager is 'logged in' via your session scheme.
    Returns (manager_id_str, manager_doc) or (None, None).
    """
    manager_id = session.get("manager_id")
    if not manager_id or not ObjectId.is_valid(manager_id):
        flash("Please log in as a manager to continue.", "error")
        return None, None

    manager_doc = users_collection.find_one({"_id": ObjectId(manager_id), "role": "manager"})
    if not manager_doc:
        session.clear()
        flash("Access denied. Please log in as a manager.", "error")
        return None, None

    status = str(manager_doc.get("status", "")).lower()
    if status in ("not active", "inactive", "disabled"):
        session.clear()
        flash("Your account is not active. Contact an administrator.", "error")
        return None, None

    return manager_id, manager_doc

# ---------- Routes ----------
@manager_deposits_bp.route("/", methods=["GET"])
def form_and_list():
    manager_id, manager_doc = _require_manager_session()
    if not manager_id:
        return redirect(url_for("login.login"))

    manager_name = manager_doc.get("name") or manager_doc.get("username") or "Unknown"
    branch_name  = manager_doc.get("branch") or manager_doc.get("branch_name") or "Unassigned"

    recent = list(
        manager_deposits_col.find({"manager_id": manager_id})
        .sort("created_at", -1)
        .limit(30)
    )

    return render_template(
        "manager/deposits.html",
        manager_name=manager_name,
        branch_name=branch_name,
        recent=recent
    )

@manager_deposits_bp.route("/submit", methods=["POST"])
def submit_deposit():
    manager_id, manager_doc = _require_manager_session()
    if not manager_id:
        return redirect(url_for("login.login"))

    manager_name = manager_doc.get("name") or manager_doc.get("username") or "Unknown"
    branch_name  = manager_doc.get("branch") or manager_doc.get("branch_name") or "Unassigned"

    amount_raw  = (request.form.get("amount") or "").strip()
    method_type = (request.form.get("method_type") or "").strip()     # "Bank" or "Mobile Money"
    method_name = (request.form.get("method_name") or "").strip()     # e.g., "GCB", "MTN MoMo"
    reference   = (request.form.get("reference") or "").strip()
    notes       = (request.form.get("notes") or "").strip()

    errors = []
    try:
        amount = float(amount_raw)
        if amount <= 0:
            errors.append("Amount must be greater than zero.")
    except Exception:
        amount = None
        errors.append("Invalid amount.")

    if method_type not in ("Bank", "Mobile Money"):
        errors.append("Select a valid method: Bank or Mobile Money.")
    if not method_name:
        errors.append("Please provide the name of the method (e.g., Bank name or MoMo provider).")

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
        for e in errors:
            flash(e, "danger")
        return redirect(url_for("manager_deposits.form_and_list"))

    doc = {
        "manager_id": manager_id,
        "manager_name": manager_name,
        "branch_name": branch_name,
        "amount": amount,
        "method_type": method_type,    # "Bank" | "Mobile Money"
        "method_name": method_name,    # e.g., "GCB", "MTN"
        "reference": reference,
        "notes": notes,
        "receipt_path": receipt_rel_path,  # e.g., 'receipts/<file>'
        "created_at": datetime.now(timezone.utc),
        "status": "submitted"
    }

    manager_deposits_col.insert_one(doc)
    flash("Deposit submitted successfully.", "success")
    return redirect(url_for("manager_deposits.form_and_list"))

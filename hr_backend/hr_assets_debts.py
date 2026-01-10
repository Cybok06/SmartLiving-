# hr_backend/hr_assets_debts.py

from datetime import datetime
from typing import Any, Dict, List

from flask import jsonify, request
from bson import ObjectId

from db import db
from hr_backend.hr_dashboard import hr_bp, _hr_access_guard

users_col = db["users"]


# -------------------------------
# Helpers
# -------------------------------
def _today_str() -> str:
    return datetime.utcnow().strftime("%Y-%m-%d")


def _serialize_asset(raw: Any) -> Dict[str, Any]:
    """
    Accepts either:
    - dict (new format), or
    - string / other (legacy format, e.g. "Laptop").
    Normalizes to a consistent dict for the frontend.
    """
    if isinstance(raw, dict):
        doc = raw
    else:
        # Legacy: just a string name
        doc = {"name": str(raw) if raw is not None else ""}

    given = doc.get("given_date") or ""
    returned = doc.get("returned_date") or ""

    if isinstance(given, datetime):
        given_str = given.strftime("%Y-%m-%d")
    else:
        given_str = str(given)[:10] if given else ""

    if isinstance(returned, datetime):
        returned_str = returned.strftime("%Y-%m-%d")
    else:
        returned_str = str(returned)[:10] if returned else ""

    return {
        "id": str(doc.get("_id")) if doc.get("_id") else None,
        "name": doc.get("name", ""),
        "given_date": given_str,
        "status": doc.get("status", "Issued"),  # Issued / Returned
        "returned_date": returned_str,
    }


def _serialize_debt(raw: Any) -> Dict[str, Any]:
    """
    Accepts either:
    - dict (new format), or
    - string / other (legacy format, description only).
    """
    if isinstance(raw, dict):
        doc = raw
    else:
        doc = {
            "description": str(raw) if raw is not None else "",
            "total_amount": 0,
            "amount_paid": 0,
        }

    total = float(doc.get("total_amount", 0) or 0)
    paid = float(doc.get("amount_paid", 0) or 0)

    return {
        "id": str(doc.get("_id")) if doc.get("_id") else None,
        "description": doc.get("description", ""),
        "total_amount": total,
        "amount_paid": paid,
    }


# -------------------------------
# GET: Assets & Debts (JSON)
# -------------------------------
@hr_bp.route(
    "/employee/<employee_id>/assets_debts",
    methods=["GET"],
    endpoint="get_assets_debts",
)
def get_assets_debts(employee_id):
    if not _hr_access_guard():
        return jsonify(ok=False, message="Unauthorized"), 401

    try:
        oid = ObjectId(employee_id)
    except Exception:
        return jsonify(ok=False, message="Invalid employee ID"), 400

    emp = users_col.find_one({"_id": oid}, {"assets": 1, "debts": 1})
    if not emp:
        return jsonify(ok=False, message="Employee not found"), 404

    # Normalize fields
    assets_raw = emp.get("assets") or []
    if not isinstance(assets_raw, list):
        assets_raw = [assets_raw]

    debts_raw = emp.get("debts") or []
    if not isinstance(debts_raw, list):
        debts_raw = [debts_raw]

    assets = [_serialize_asset(a) for a in assets_raw]
    debts = [_serialize_debt(d) for d in debts_raw]

    return jsonify(ok=True, assets=assets, debts=debts)


# -------------------------------
# POST: Add Asset
# -------------------------------
@hr_bp.route(
    "/employee/<employee_id>/assets/add",
    methods=["POST"],
    endpoint="add_asset",
)
def add_asset(employee_id):
    if not _hr_access_guard():
        return jsonify(ok=False, message="Unauthorized"), 401

    try:
        oid = ObjectId(employee_id)
    except Exception:
        return jsonify(ok=False, message="Invalid employee ID"), 400

    payload = request.get_json(silent=True) or {}
    name = (payload.get("name") or "").strip()
    given_date_raw = (payload.get("given_date") or "").strip()

    if not name:
        return jsonify(ok=False, message="Asset name is required."), 400

    given_date = given_date_raw[:10] if given_date_raw else _today_str()
    now = datetime.utcnow()

    asset_doc: Dict[str, Any] = {
        "_id": ObjectId(),
        "name": name,
        "given_date": given_date,
        "status": "Issued",
        "returned_date": "",
        "created_at": now,
    }

    result = users_col.update_one(
        {"_id": oid},
        {
            "$push": {"assets": asset_doc},
            "$set": {"updated_at": now},
        },
    )
    if not result.matched_count:
        return jsonify(ok=False, message="Employee not found"), 404

    return jsonify(ok=True, message="Asset recorded.")


# -------------------------------
# POST: Mark Asset Returned
# -------------------------------
@hr_bp.route(
    "/employee/<employee_id>/assets/<asset_id>/returned",
    methods=["POST"],
    endpoint="mark_asset_returned",
)
def mark_asset_returned(employee_id, asset_id):
    if not _hr_access_guard():
        return jsonify(ok=False, message="Unauthorized"), 401

    try:
        oid = ObjectId(employee_id)
        asset_oid = ObjectId(asset_id)
    except Exception:
        return jsonify(ok=False, message="Invalid IDs supplied"), 400

    now = datetime.utcnow()
    returned_date = _today_str()

    result = users_col.update_one(
        {"_id": oid, "assets._id": asset_oid},
        {
            "$set": {
                "assets.$.status": "Returned",
                "assets.$.returned_date": returned_date,
                "updated_at": now,
            }
        },
    )
    if not result.matched_count:
        return jsonify(ok=False, message="Asset or employee not found"), 404

    return jsonify(ok=True, message="Asset marked as returned.")


# -------------------------------
# POST: Add Debt
# -------------------------------
@hr_bp.route(
    "/employee/<employee_id>/debts/add",
    methods=["POST"],
    endpoint="add_debt",
)
def add_debt(employee_id):
    if not _hr_access_guard():
        return jsonify(ok=False, message="Unauthorized"), 401

    try:
        oid = ObjectId(employee_id)
    except Exception:
        return jsonify(ok=False, message="Invalid employee ID"), 400

    payload = request.get_json(silent=True) or {}
    description = (payload.get("description") or "").strip()
    amount_raw = (str(payload.get("amount") or "")).strip()

    if not description or not amount_raw:
        return jsonify(ok=False, message="Description and amount are required."), 400

    try:
        total_amount = float(amount_raw)
    except Exception:
        return jsonify(ok=False, message="Amount must be a number."), 400

    now = datetime.utcnow()
    debt_doc: Dict[str, Any] = {
        "_id": ObjectId(),
        "description": description,
        "total_amount": total_amount,
        "amount_paid": 0.0,
        "created_at": now,
    }

    result = users_col.update_one(
        {"_id": oid},
        {
            "$push": {"debts": debt_doc},
            "$set": {"updated_at": now},
        },
    )
    if not result.matched_count:
        return jsonify(ok=False, message="Employee not found"), 404

    return jsonify(ok=True, message="Debt recorded.")


# -------------------------------
# POST: Add Payment to Debt
# -------------------------------
@hr_bp.route(
    "/employee/<employee_id>/debts/<debt_id>/add_payment",
    methods=["POST"],
    endpoint="add_debt_payment",
)
def add_debt_payment(employee_id, debt_id):
    if not _hr_access_guard():
        return jsonify(ok=False, message="Unauthorized"), 401

    try:
        oid = ObjectId(employee_id)
        debt_oid = ObjectId(debt_id)
    except Exception:
        return jsonify(ok=False, message="Invalid IDs supplied"), 400

    payload = request.get_json(silent=True) or {}
    amount_raw = (str(payload.get("amount") or "")).strip()

    if not amount_raw:
        return jsonify(ok=False, message="Payment amount is required."), 400

    try:
        payment_amount = float(amount_raw)
        if payment_amount <= 0:
            raise ValueError()
    except Exception:
        return jsonify(ok=False, message="Payment amount must be positive."), 400

    now = datetime.utcnow()

    result = users_col.update_one(
        {"_id": oid, "debts._id": debt_oid},
        {
            "$inc": {"debts.$.amount_paid": payment_amount},
            "$set": {"updated_at": now},
        },
    )
    if not result.matched_count:
        return jsonify(ok=False, message="Debt or employee not found"), 404

    return jsonify(ok=True, message="Payment recorded.")

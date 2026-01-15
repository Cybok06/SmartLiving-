# hr_backend/hr_wages.py

from datetime import datetime
from typing import Any, Dict, List

from flask import request, jsonify
from bson import ObjectId

from db import db
from hr_backend.hr_dashboard import hr_bp, _hr_access_guard

users_col = db["users"]


def _serialize_wage_entry(entry: Dict[str, Any]) -> Dict[str, Any]:
  """Convert Mongo subdocument into JSON-friendly dict."""
  date_val = entry.get("date")
  if isinstance(date_val, datetime):
      date_str = date_val.strftime("%Y-%m-%d")
  elif isinstance(date_val, str):
      date_str = date_val[:10]
  else:
      date_str = ""
  return {
      "id": str(entry.get("_id")) if entry.get("_id") else None,
      "amount": float(entry.get("amount", 0) or 0),
      "reason": entry.get("reason", ""),
      "date": date_str,
  }


# -------------------------------
# GET: wages & tips history
# -------------------------------
@hr_bp.route("/employee/<employee_id>/wages_tips", methods=["GET"])
def get_wages_tips(employee_id):
    if not _hr_access_guard():
        return jsonify(ok=False, message="Unauthorized"), 401

    try:
        oid = ObjectId(employee_id)
    except Exception:
        return jsonify(ok=False, message="Invalid employee ID"), 400

    emp = users_col.find_one({"_id": oid}, {"wages_tips": 1})
    if not emp:
        return jsonify(ok=False, message="Employee not found"), 404

    raw_list: List[Dict[str, Any]] = emp.get("wages_tips") or []
    items = [_serialize_wage_entry(e) for e in raw_list]

    # Most recent first
    items.sort(key=lambda x: x.get("date") or "", reverse=True)

    return jsonify(ok=True, items=items)


# -------------------------------
# POST: add wage / tip entry
# -------------------------------
@hr_bp.route("/employee/<employee_id>/wages_tips/add", methods=["POST"])
def add_wage_tip(employee_id):
    if not _hr_access_guard():
        return jsonify(ok=False, message="Unauthorized"), 401

    try:
        oid = ObjectId(employee_id)
    except Exception:
        return jsonify(ok=False, message="Invalid employee ID"), 400

    payload = request.get_json(silent=True) or {}
    amount_raw = (str(payload.get("amount") or "")).strip()
    reason = (payload.get("reason") or "").strip()
    date_raw = (payload.get("date") or "").strip()

    if not amount_raw:
        return jsonify(ok=False, message="Amount is required."), 400

    try:
        amount = float(amount_raw)
    except Exception:
        return jsonify(ok=False, message="Amount must be a number."), 400

    # Parse date if provided, else use today
    if date_raw:
        try:
            date_obj = datetime.strptime(date_raw[:10], "%Y-%m-%d")
        except Exception:
            date_obj = datetime.utcnow()
    else:
        date_obj = datetime.utcnow()

    now = datetime.utcnow()
    entry: Dict[str, Any] = {
        "_id": ObjectId(),
        "amount": amount,
        "reason": reason,
        "date": date_obj,
        "created_at": now,
    }

    result = users_col.update_one(
        {"_id": oid},
        {
            "$push": {"wages_tips": entry},
            "$set": {"updated_at": now},
        },
    )

    if not result.matched_count:
        return jsonify(ok=False, message="Employee not found."), 404

    return jsonify(ok=True, entry=_serialize_wage_entry(entry))

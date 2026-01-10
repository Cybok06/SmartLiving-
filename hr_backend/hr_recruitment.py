# hr_backend/hr_recruitment.py

from datetime import datetime
from typing import Dict, Any, List

from flask import request, jsonify
from bson import ObjectId

from db import db
from hr_backend.hr_dashboard import hr_bp, _hr_access_guard
from hr_backend.hr_employee_profile import (
    RECRUITMENT_DEFAULT_STEPS,
    RECRUITMENT_STAGES,
)

users_col = db["users"]


# -------------------------------
# Helper functions
# -------------------------------
def _save_recruitment_checklist(emp: dict, new_step: dict) -> List[Dict[str, Any]]:
    """
    Update or insert a single step inside recruitment_checklist and return full list.
    new_step: {key, status?, completed_on?, completed_by?, training_review?}
    """
    checklist = emp.get("recruitment_checklist") or []
    key = new_step.get("key")
    if not key:
        return checklist

    idx = next((i for i, s in enumerate(checklist) if s.get("key") == key), None)
    if idx is None:
        # If we are inserting afresh, try to populate label from default config
        label = new_step.get("label")
        if not label:
            for st in RECRUITMENT_DEFAULT_STEPS:
                if st.get("key") == key:
                    label = st.get("label")
                    break
        base = {
            "key": key,
            "label": label or key,
            "status": new_step.get("status", "Pending"),
            "completed_on": new_step.get("completed_on"),
            "completed_by": new_step.get("completed_by"),
        }
        # Include training_review if supplied
        if "training_review" in new_step:
            base["training_review"] = new_step["training_review"]
        checklist.append(base)
    else:
        # Update only provided fields
        for k, v in new_step.items():
            if k == "key":
                continue
            checklist[idx][k] = v
    return checklist


def _recalc_recruitment_stage_from_checklist(
    checklist: List[Dict[str, Any]]
) -> str:
    """
    Decide highest completed stage based on RECRUITMENT_DEFAULT_STEPS order.
    If none completed, default to first stage.
    """
    completed_keys = {s.get("key") for s in checklist if s.get("status") == "Completed"}
    last_completed_label = None
    for step in RECRUITMENT_DEFAULT_STEPS:
        if step["key"] in completed_keys:
            last_completed_label = step["label"]
    return last_completed_label or RECRUITMENT_STAGES[0]


# -------------------------------
# AJAX: update a single recruitment step
# -------------------------------
@hr_bp.route("/employee/<employee_id>/recruitment_step", methods=["POST"])
def update_recruitment_step(employee_id):
    if not _hr_access_guard():
        return jsonify(ok=False, message="Unauthorized"), 401

    try:
        oid = ObjectId(employee_id)
    except Exception:
        return jsonify(ok=False, message="Invalid employee ID"), 400

    payload = request.get_json(silent=True) or {}
    key = (payload.get("key") or "").strip()
    status = (payload.get("status") or "").strip() or "Pending"

    allowed_keys = {s["key"] for s in RECRUITMENT_DEFAULT_STEPS}
    if key not in allowed_keys:
        return jsonify(ok=False, message="Unknown recruitment step"), 400

    if status not in {"Pending", "Completed", "In Progress"}:
        return jsonify(ok=False, message="Invalid status"), 400

    emp = users_col.find_one({"_id": oid})
    if not emp:
        return jsonify(ok=False, message="Employee not found"), 404

    now = datetime.utcnow()
    completed_on = None
    if status == "Completed":
        completed_on = now.strftime("%Y-%m-%d")

    completed_by = (payload.get("completed_by") or "HR").strip()

    new_step = {
        "key": key,
        "status": status,
        "completed_on": completed_on,
        "completed_by": completed_by,
    }

    checklist = _save_recruitment_checklist(emp, new_step)
    stage = _recalc_recruitment_stage_from_checklist(checklist)

    users_col.update_one(
        {"_id": oid},
        {
            "$set": {
                "recruitment_checklist": checklist,
                "recruitment_stage": stage,
                "updated_at": now,
            }
        },
    )

    return jsonify(ok=True, stage=stage, checklist=checklist)


# -------------------------------
# AJAX: update training review (Training 1 / Training 2)
# -------------------------------
@hr_bp.route("/employee/<employee_id>/training_review", methods=["POST"])
def update_training_review(employee_id):
    """
    Save detailed training review for 'training1' or 'training2'.
    Payload:
      {
        "key": "training1" | "training2",
        "overall_rating": int (0–5),
        "notes": str,
        "values": [
          {"label": "Honesty", "rating": 4},
          ...
        ]
      }
    Stored under recruitment_checklist[*].training_review
    """
    if not _hr_access_guard():
        return jsonify(ok=False, message="Unauthorized"), 401

    try:
        oid = ObjectId(employee_id)
    except Exception:
        return jsonify(ok=False, message="Invalid employee ID"), 400

    payload = request.get_json(silent=True) or {}
    key = (payload.get("key") or "").strip()

    if key not in {"training1", "training2"}:
        return jsonify(ok=False, message="Invalid training key"), 400

    # Sanitize overall rating
    try:
        overall_rating = int(payload.get("overall_rating") or 0)
    except Exception:
        overall_rating = 0
    if overall_rating < 0:
        overall_rating = 0
    if overall_rating > 5:
        overall_rating = 5

    notes = (payload.get("notes") or "").strip()

    raw_values = payload.get("values") or []
    values: List[Dict[str, Any]] = []
    for v in raw_values:
        label = (v.get("label") or "").strip()
        if not label:
            continue
        try:
            rating = int(v.get("rating") or 0)
        except Exception:
            rating = 0
        if rating <= 0:
            continue
        if rating > 5:
            rating = 5
        values.append({"label": label, "rating": rating})

    training_review: Dict[str, Any] = {
        "overall_rating": overall_rating,
        "notes": notes,
        "values": values,
    }

    emp = users_col.find_one({"_id": oid})
    if not emp:
        return jsonify(ok=False, message="Employee not found"), 404

    now = datetime.utcnow()

    # Insert/update this training step inside recruitment_checklist
    new_step = {
        "key": key,
        "training_review": training_review,
    }

    checklist = _save_recruitment_checklist(emp, new_step)
    # Stage itself doesn't change from training review, but keep it consistent
    stage = _recalc_recruitment_stage_from_checklist(checklist)

    users_col.update_one(
        {"_id": oid},
        {
            "$set": {
                "recruitment_checklist": checklist,
                "recruitment_stage": stage,
                "updated_at": now,
            }
        },
    )

    return jsonify(ok=True, training_review=training_review, stage=stage)


# -------------------------------
# AJAX: update probation info
# -------------------------------
@hr_bp.route("/employee/<employee_id>/probation", methods=["POST"])
def update_probation_info(employee_id):
    if not _hr_access_guard():
        return jsonify(ok=False, message="Unauthorized"), 401

    try:
        oid = ObjectId(employee_id)
    except Exception:
        return jsonify(ok=False, message="Invalid employee ID"), 400

    payload = request.get_json(silent=True) or {}

    start_date_raw = (payload.get("start_date") or "").strip()
    end_date_raw = (payload.get("end_date") or "").strip()

    probation: Dict[str, Any] = {}

    if start_date_raw:
        try:
            probation["start_date"] = datetime.strptime(
                start_date_raw, "%Y-%m-%d"
            ).strftime("%Y-%m-%d")
        except Exception:
            pass

    if end_date_raw:
        try:
            probation["end_date"] = datetime.strptime(
                end_date_raw, "%Y-%m-%d"
            ).strftime("%Y-%m-%d")
        except Exception:
            pass

    probation["manager_comments"] = (payload.get("manager_comments") or "").strip()
    probation["executive_comments"] = (payload.get("executive_comments") or "").strip()

    emp = users_col.find_one({"_id": oid}) or {}
    existing_status = ((emp.get("probation") or {}).get("status")) or emp.get(
        "employment_status"
    )
    probation["status"] = existing_status or "Probation"

    users_col.update_one(
        {"_id": oid},
        {"$set": {"probation": probation, "updated_at": datetime.utcnow()}},
    )

    return jsonify(ok=True, probation=probation)


# -------------------------------
# AJAX: reject applicant
# -------------------------------
@hr_bp.route("/employee/<employee_id>/reject", methods=["POST"])
def reject_applicant(employee_id):
    if not _hr_access_guard():
        return jsonify(ok=False, message="Unauthorized"), 401

    try:
        oid = ObjectId(employee_id)
    except Exception:
        return jsonify(ok=False, message="Invalid employee ID"), 400

    emp = users_col.find_one({"_id": oid})
    if not emp:
        return jsonify(ok=False, message="Employee not found"), 404

    stage = emp.get("recruitment_stage") or ""
    if stage == "Full Staff (After 3 Months)" or emp.get("employment_status") in [
        "Active",
        "Exited",
        "Rejected",
    ]:
        return jsonify(ok=False, message="Cannot reject a fully confirmed staff."), 400

    payload = request.get_json(silent=True) or {}
    reason = (payload.get("reason") or "").strip()

    now = datetime.utcnow()
    exit_info = emp.get("exit_info") or {}
    exit_info.update(
        {
            "exit_date": now.strftime("%Y-%m-%d"),
            "exit_reason": reason or "Rejected during recruitment stage",
            "type": "Rejected",
        }
    )

    users_col.update_one(
        {"_id": oid},
        {
            "$set": {
                "employment_status": "Rejected",
                "recruitment_stage": "Rejected",
                "exit_info": exit_info,
                "updated_at": now,
            }
        },
    )

    return jsonify(ok=True)

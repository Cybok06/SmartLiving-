# routes/admin_complaints.py
from __future__ import annotations
from flask import Blueprint, jsonify
from db import db
from datetime import datetime, timedelta

complaints_col = db["complaints"]

admin_complaints_bp = Blueprint("admin_complaints", __name__, url_prefix="/admin-complaints")

@admin_complaints_bp.get("/unresolved_count")
def unresolved_count():
    """Return unresolved + breaching complaints for admin badge."""
    try:
        now = datetime.utcnow()
        # adjust threshold if SLA hours differ in your app
        sla_limit = timedelta(hours=48)

        unresolved = complaints_col.count_documents({"status": {"$ne": "Resolved"}})
        breaching = complaints_col.count_documents({
            "status": {"$ne": "Resolved"},
            "created_at": {"$lt": now - sla_limit}
        })
        return jsonify(ok=True, open=unresolved, breaching=breaching)
    except Exception as e:
        return jsonify(ok=False, message=str(e)), 500

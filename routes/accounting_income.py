from __future__ import annotations

from flask import (
    Blueprint,
    render_template,
    request,
    jsonify,
    session,
    redirect,
    url_for,
    Response,
)
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
import csv
import io

from db import db

income_bp = Blueprint(
    "accounting_income",
    __name__,
    url_prefix="/accounting/income",
    template_folder="../templates",
)

income_col = db["income_entries"]
users_col = db["users"]

ALLOWED_CATEGORIES = ["Discount Received", "Investment Income", "Other Incomes"]


def _safe_float(value, default=0.0):
    try:
        return float(value or 0)
    except Exception:
        return default


def _require_accounting_role():
    role = (session.get("role") or "").lower()
    if session.get("admin_id") or session.get("executive_id"):
        return True
    return role == "accounting"


def _parse_date(val: Optional[str]) -> Optional[datetime]:
    if not val:
        return None
    try:
        return datetime.strptime(val, "%Y-%m-%d")
    except Exception:
        return None


def _build_filters(args):
    start = _parse_date(args.get("from"))
    end = _parse_date(args.get("to"))
    if not start and not end:
        end = datetime.utcnow()
        start = end - timedelta(days=30)
    elif start and not end:
        end = datetime.utcnow()
    elif end and not start:
        start = end - timedelta(days=30)
    if end:
        end = end.replace(hour=23, minute=59, second=59, microsecond=999999)
    query: Dict[str, Any] = {"date_dt": {"$gte": start, "$lte": end}}
    category = (args.get("category") or "").strip()
    status = (args.get("status") or "posted").strip().lower()
    search = (args.get("q") or "").strip()
    if category and category != "All":
        query["category"] = category
    if status != "all":
        query["status"] = status
    if search:
        query["$or"] = [
            {"description": {"$regex": search, "$options": "i"}},
            {"subcategory": {"$regex": search, "$options": "i"}},
        ]
    return query, start, end, category or "All", status or "posted", search


def _serialize_entry(doc):
    return {
        "id": str(doc["_id"]),
        "date": doc.get("date") or doc.get("date_dt", "").strftime("%Y-%m-%d") if isinstance(doc.get("date_dt"), datetime) else "",
        "category": doc.get("category", ""),
        "subcategory": doc.get("subcategory", ""),
        "amount": float(doc.get("amount", 0) or 0),
        "description": doc.get("description", ""),
        "status": doc.get("status", ""),
        "created_by": doc.get("created_by", ""),
    }


@income_bp.route("/", methods=["GET"])
def page():
    if not _require_accounting_role():
        return redirect(url_for("login.login"))
    query, start, end, category, status, search = _build_filters(request.args)
    rows = list(income_col.find(query).sort("date_dt", -1).limit(200))
    total = sum(_safe_float(row.get("amount")) for row in rows)
    counts = {cat: 0.0 for cat in ALLOWED_CATEGORIES}
    for row in rows:
        cat = row.get("category")
        if cat in counts:
            counts[cat] += _safe_float(row.get("amount"))
    return render_template(
        "accounting/income.html",
        rows=[_serialize_entry(row) for row in rows],
        totals={
            "total": total,
            "discount": counts["Discount Received"],
            "investment": counts["Investment Income"],
            "other": counts["Other Incomes"],
        },
        start=start.strftime("%Y-%m-%d") if start else "",
        end=end.strftime("%Y-%m-%d") if end else "",
        category=category,
        status=status,
        search=search,
        categories=ALLOWED_CATEGORIES,
    )


@income_bp.route("/list", methods=["GET"])
def list_income():
    if not _require_accounting_role():
        return jsonify(ok=False, message="Unauthorized"), 401
    query, *_ = _build_filters(request.args)
    docs = list(income_col.find(query).sort("date_dt", -1).limit(500))
    return jsonify(ok=True, rows=[_serialize_entry(doc) for doc in docs])


@income_bp.route("/create", methods=["POST"])
def create_income():
    if not _require_accounting_role():
        return jsonify(ok=False, message="Unauthorized"), 401
    data = request.get_json(silent=True) or {}
    category = (data.get("category") or "").strip()
    amount = data.get("amount")
    if category not in ALLOWED_CATEGORIES:
        return jsonify(ok=False, message="Invalid category"), 400
    try:
        amount = float(amount)
    except Exception:
        return jsonify(ok=False, message="Amount must be numeric"), 400
    if amount <= 0:
        return jsonify(ok=False, message="Amount must be greater than zero"), 400
    date_str = (data.get("date") or "").strip()
    try:
        date_dt = datetime.strptime(date_str[:10], "%Y-%m-%d") if date_str else datetime.utcnow()
    except Exception:
        date_dt = datetime.utcnow()
    now = datetime.utcnow()
    doc = {
        "date_dt": date_dt,
        "date": date_dt.strftime("%Y-%m-%d"),
        "category": category,
        "subcategory": (data.get("subcategory") or "").strip(),
        "amount": amount,
        "description": (data.get("description") or "").strip(),
        "source_account_type": (data.get("source_account_type") or "").strip(),
        "source_account_id": data.get("source_account_id"),
        "reference_no": (data.get("reference_no") or "").strip(),
        "status": "posted",
        "created_at": now,
        "updated_at": now,
        "created_by": session.get("user_id") or session.get("admin_id") or session.get("executive_id"),
    }
    income_col.insert_one(doc)
    return jsonify(ok=True, entry=_serialize_entry(doc))


@income_bp.route("/stats", methods=["GET"])
def stats():
    if not _require_accounting_role():
        return jsonify(ok=False, message="Unauthorized"), 401
    query, start, end, _, _, _ = _build_filters(request.args)
    pipeline = [
        {"$match": query},
        {
            "$group": {
                "_id": "$category",
                "total": {"$sum": {"$ifNull": ["$amount", 0]}},
            }
        },
    ]
    agg = list(income_col.aggregate(pipeline))
    by_category = [{"name": doc["_id"], "total": float(doc["total"] or 0)} for doc in agg]
    daily_pipeline = [
        {"$match": query},
        {
            "$group": {
                "_id": {"$dateToString": {"format": "%Y-%m-%d", "date": "$date_dt"}},
                "sum": {"$sum": {"$ifNull": ["$amount", 0]}},
            }
        },
        {"$sort": {"_id": 1}},
    ]
    daily = list(income_col.aggregate(daily_pipeline))
    labels = [doc["_id"] for doc in daily]
    values = [float(doc["sum"] or 0) for doc in daily]
    total = sum(v for v in values)
    return jsonify(ok=True, total=round(total, 2), daily={"labels": labels, "values": values}, by_category=by_category)


@income_bp.route("/export", methods=["GET"])
def export():
    if not _require_accounting_role():
        return jsonify(ok=False, message="Unauthorized"), 401
    query, start, end, category, status, search = _build_filters(request.args)
    docs = income_col.find(query).sort("date_dt", -1)
    out = io.StringIO()
    writer = csv.writer(out)
    writer.writerow(["Date", "Category", "Subcategory", "Amount", "Description", "Status"])
    for doc in docs:
        writer.writerow([
            doc.get("date"),
            doc.get("category"),
            doc.get("subcategory"),
            f"{float(doc.get('amount', 0) or 0):0.2f}",
            doc.get("description"),
            doc.get("status"),
        ])
    return Response(out.getvalue(), mimetype="text/csv", headers={"Content-Disposition": "attachment; filename=income_entries.csv"})

from __future__ import annotations

from flask import Blueprint, render_template, request, jsonify
from datetime import datetime, date, time, timedelta
from typing import Any, Dict, List, Optional, Tuple

from bson import ObjectId
from db import db
from services.activity_audit import audit_action

acc_expenses = Blueprint(
    "acc_expenses",
    __name__,
    template_folder="../templates",
)

# Main accounting expenses
expenses_col = db["expenses"]
expense_categories_col = db["expense_categories"]

# Manager / branch expenses (same collection used by executive_expense.py)
manager_expenses_col = db["manager_expenses"]


# ---------- helpers ----------

def _safe_float(v: Any, default: Optional[float] = 0.0) -> float:
    """
    Safely cast a value to float. Returns `default` if casting fails.
    `default` may be None if caller wants to detect absence separately.
    """
    try:
        if v is None or v == "":
            # type: ignore[return-value]
            return default
        return float(v)
    except Exception:
        # type: ignore[return-value]
        return default


def _parse_date(s: str | None) -> Optional[datetime]:
    """
    Parse ISO date string (YYYY-MM-DD) into a datetime at local midnight.
    Returns None if parsing fails.
    """
    if not s:
        return None
    try:
        d = datetime.strptime(s, "%Y-%m-%d").date()
        return datetime.combine(d, time.min)
    except Exception:
        return None


def _serialize_expense(doc: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalize an expense-like document into a JSON-friendly structure for the UI.
    Expects keys:
      - _id
      - date (datetime/date)
      - amount
      - category
      - description
      - payment_method (optional)
    """
    _id = doc.get("_id")
    dt = doc.get("date")

    if isinstance(dt, datetime):
        d = dt.date()
    elif isinstance(dt, date):
        d = dt
    else:
        d = None

    return {
        "id": str(_id) if isinstance(_id, ObjectId) else "",
        "date": d.isoformat() if isinstance(d, date) else "",
        "amount": float(doc.get("amount", 0.0) or 0.0),
        "category": (doc.get("category") or "").strip() or "Uncategorized",
        "description": doc.get("description") or "",
        "payment_method": doc.get("payment_method") or "",
    }


def _compute_totals(docs: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Compute total amount + totals per category for a list of expense docs.
    """
    total = 0.0
    cat_map: Dict[str, float] = {}

    for d in docs:
        amt = float(d.get("amount", 0.0) or 0.0)
        total += amt
        cat = (d.get("category") or "").strip() or "Uncategorized"
        cat_map[cat] = cat_map.get(cat, 0.0) + amt

    cat_totals = [
        {"category": k, "total": float(v)}
        for k, v in sorted(cat_map.items(), key=lambda kv: kv[1], reverse=True)
    ]

    return {
        "count": len(docs),
        "total_amount": float(total),
        "category_totals": cat_totals,
    }


def _date_range_for_month(today: date) -> Tuple[datetime, datetime]:
    """
    Default range: first day of this month 00:00 up to end-of-today.
    """
    start_d = today.replace(day=1)
    start = datetime.combine(start_d, time.min)
    end = datetime.combine(today, time.max)
    return start, end


def _normalize_manager_expense(doc: Dict[str, Any]) -> Dict[str, Any]:
    """
    Convert a manager_expenses document into an 'expense-like' document
    that can be consumed by _serialize_expense and _compute_totals.

    We:
      - Use 'date' field if parsable (YYYY-MM-DD), otherwise fallback to created_at or now
      - Map amount/category/description
      - Add a synthetic payment_method label so accounting can recognise the source
    """
    # Amount
    amount = _safe_float(doc.get("amount"), 0.0)

    # Date
    dt: Optional[datetime] = None
    raw_date = doc.get("date")
    if isinstance(raw_date, (datetime, date)):
        dt = datetime.combine(raw_date, time.min) if isinstance(raw_date, date) else raw_date
    elif isinstance(raw_date, str):
        try:
            # assuming format YYYY-MM-DD from manager UI
            d = datetime.strptime(raw_date, "%Y-%m-%d").date()
            dt = datetime.combine(d, time.min)
        except Exception:
            dt = None

    if dt is None:
        created_at = doc.get("created_at")
        if isinstance(created_at, datetime):
            dt = created_at
        else:
            dt = datetime.utcnow()

    return {
        "_id": doc.get("_id"),
        "date": dt,
        "amount": amount,
        "category": (doc.get("category") or "").strip(),
        "description": doc.get("description") or "",
        # Tag as coming from manager side
        "payment_method": "Manager Entry",
        # Extra metadata if we ever want to show it
        "source": "manager",
        "manager_id": doc.get("manager_id"),
        "status": doc.get("status"),
    }


# ---------- pages ----------

@acc_expenses.get("/expenses")
def expenses_page():
    """
    Initial page load for Accounting → Expense Tracker.
    Default view = current month (1st → today).
    Data source:
      - Accounting expenses from `expenses`
      - PLUS approved manager expenses from `manager_expenses`
    """
    today = date.today()
    start_dt, end_dt = _date_range_for_month(today)

    # --- Accounting expenses (main) ---
    acc_query: Dict[str, Any] = {
        "date": {"$gte": start_dt, "$lte": end_dt}
    }

    acc_docs = list(
        expenses_col.find(acc_query)
        .sort("date", -1)
        .limit(500)
    )

    # --- Manager expenses (Approved only) ---
    mgr_query: Dict[str, Any] = {
        "status": "Approved",
        "created_at": {"$gte": start_dt, "$lte": end_dt},
    }

    mgr_raw = list(
        manager_expenses_col.find(
            mgr_query,
            {
                "_id": 1,
                "manager_id": 1,
                "date": 1,
                "time": 1,
                "category": 1,
                "description": 1,
                "amount": 1,
                "status": 1,
                "created_at": 1,
            },
        )
        .sort("created_at", -1)
        .limit(500)
    )
    mgr_docs = [_normalize_manager_expense(d) for d in mgr_raw]

    # --- Combine for initial view ---
    all_docs: List[Dict[str, Any]] = acc_docs + mgr_docs

    totals_info = _compute_totals(all_docs)
    initial_data = {
        "expenses": [_serialize_expense(d) for d in all_docs],
        "totals": {
            "count": totals_info["count"],
            "total_amount": totals_info["total_amount"],
        },
        "category_totals": totals_info["category_totals"],
        "default_range": {
            "start": start_dt.date().isoformat(),
            "end": today.isoformat(),
        },
    }

    # --- distinct categories ---
    # 1) From dedicated categories collection
    cat_docs = list(expense_categories_col.find({}).sort("name", 1))
    categories: List[str] = [c.get("name") for c in cat_docs if c.get("name")]

    # 2) Merge categories from accounting docs
    seen = set(categories)
    for d in acc_docs:
        cat = (d.get("category") or "").strip()
        if cat and cat not in seen:
            seen.add(cat)
            categories.append(cat)

    # 3) Merge categories from manager docs
    for d in mgr_docs:
        cat = (d.get("category") or "").strip()
        if cat and cat not in seen:
            seen.add(cat)
            categories.append(cat)

    default_start = start_dt.date().isoformat()
    default_end = today.isoformat()

    return render_template(
        "accounting/expenses.html",
        initial_data=initial_data,
        categories=categories,
        default_start=default_start,
        default_end=default_end,
    )


# ---------- API: list with filters ----------

@acc_expenses.get("/expenses/list")
def expenses_list():
    """
    Filtered list endpoint for the Expense Tracker.
    Now returns combined data from:
      - `expenses`
      - `manager_expenses` (Approved only)

    Query params:
      - start, end       (YYYY-MM-DD)
      - category         (exact match)
      - search           (description / reference for accounting, description for manager)
      - min, max         (amount range, applied in Python)
    """
    start_str = (request.args.get("start") or "").strip()
    end_str   = (request.args.get("end") or "").strip()
    category  = (request.args.get("category") or "").strip()
    search    = (request.args.get("search") or "").strip()
    min_str   = (request.args.get("min") or "").strip()
    max_str   = (request.args.get("max") or "").strip()

    # Date range
    start_dt = _parse_date(start_str)
    end_dt   = _parse_date(end_str)
    if end_dt:
        # make end inclusive end-of-day
        end_dt = end_dt.replace(hour=23, minute=59, second=59, microsecond=999999)

    # ---------- Accounting expenses ----------
    acc_query: Dict[str, Any] = {}

    if start_dt or end_dt:
        acc_query["date"] = {}
        if start_dt:
            acc_query["date"]["$gte"] = start_dt
        if end_dt:
            acc_query["date"]["$lte"] = end_dt

    if category:
        acc_query["category"] = category

    if search:
        acc_query["$or"] = [
            {"description": {"$regex": search, "$options": "i"}},
            {"reference": {"$regex": search, "$options": "i"}},
        ]

    acc_docs = list(
        expenses_col.find(acc_query)
        .sort("date", -1)
        .limit(1000)
    )

    # ---------- Manager expenses (Approved only) ----------
    mgr_query: Dict[str, Any] = {"status": "Approved"}

    if start_dt or end_dt:
        mgr_query["created_at"] = {}
        if start_dt:
            mgr_query["created_at"]["$gte"] = start_dt
        if end_dt:
            mgr_query["created_at"]["$lte"] = end_dt

    if category:
        mgr_query["category"] = category

    if search:
        mgr_query["description"] = {"$regex": search, "$options": "i"}

    mgr_raw = list(
        manager_expenses_col.find(
            mgr_query,
            {
                "_id": 1,
                "manager_id": 1,
                "date": 1,
                "time": 1,
                "category": 1,
                "description": 1,
                "amount": 1,
                "status": 1,
                "created_at": 1,
            },
        )
        .sort("created_at", -1)
        .limit(1000)
    )
    mgr_docs = [_normalize_manager_expense(d) for d in mgr_raw]

    # ---------- Combine + amount range filter ----------
    combined_docs: List[Dict[str, Any]] = acc_docs + mgr_docs

    min_amt = _safe_float(min_str, None) if min_str else None
    max_amt = _safe_float(max_str, None) if max_str else None

    filtered_docs: List[Dict[str, Any]] = []
    for d in combined_docs:
        amt = float(d.get("amount", 0.0) or 0.0)
        if min_amt is not None and amt < min_amt:
            continue
        if max_amt is not None and amt > max_amt:
            continue
        filtered_docs.append(d)

    totals_info = _compute_totals(filtered_docs)

    data = {
        "expenses": [_serialize_expense(d) for d in filtered_docs],
        "totals": {
            "count": totals_info["count"],
            "total_amount": totals_info["total_amount"],
        },
        "category_totals": totals_info["category_totals"],
    }

    return jsonify(ok=True, data=data)


# ---------- API: create expense ----------

@acc_expenses.post("/expenses/create")
@audit_action("expense.created", "Created Expense", entity_type="expense")
def create_expense():
    """
    Create a new accounting expense entry in `expenses`.
    This does NOT touch manager_expenses; those are created from the manager UI.
    """
    try:
        data = request.get_json(force=True, silent=False)
    except Exception:
        return jsonify(ok=False, message="Invalid JSON body."), 400

    if not isinstance(data, dict):
        return jsonify(ok=False, message="Invalid payload format."), 400

    date_str       = (data.get("date") or "").strip()
    amount_str     = (data.get("amount") or "").strip()
    category       = (data.get("category") or "").strip()
    payment_method = (data.get("payment_method") or "").strip()
    description    = (data.get("description") or "").strip()

    if not date_str or not amount_str or not category:
        return jsonify(ok=False, message="Date, amount and category are required."), 400

    dt = _parse_date(date_str) or datetime.utcnow()
    amount = _safe_float(amount_str, 0.0)
    if amount <= 0:
        return jsonify(ok=False, message="Amount must be greater than zero."), 400

    now = datetime.utcnow()

    doc: Dict[str, Any] = {
        "date": dt,
        "amount": amount,
        "category": category,
        "payment_method": payment_method,
        "description": description,
        "created_at": now,
        "updated_at": now,
    }

    # optional reference if client ever sends it in future
    if "reference" in data:
        doc["reference"] = (data.get("reference") or "").strip()

    res = expenses_col.insert_one(doc)
    doc["_id"] = res.inserted_id

    # upsert category for future suggestions/autocomplete
    if category:
        expense_categories_col.update_one(
            {"name": category},
            {"$setOnInsert": {"name": category, "created_at": now}},
            upsert=True,
        )

    expense_out = _serialize_expense(doc)
    return jsonify(ok=True, expense=expense_out), 200

from __future__ import annotations

import csv
import io
import logging
from datetime import datetime, time
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple, Union

from bson import ObjectId
from flask import Blueprint, Response, jsonify, render_template, request

from db import db
from accounting_services import prepayment_amortization_for_period, accruals_for_period
from expense_categories import MANAGER_EXPENSE_CATEGORIES

logger = logging.getLogger(__name__)

profit_loss_bp = Blueprint("profit_loss", __name__, url_prefix="/accounting/profit-loss")

# ------------------ Collections ------------------
payments_col = db["payments"]
instant_sales_col = db["instant_sales"]
returns_inwards_col = db["returns_inwards"]
ap_bills_col = db["ap_bills"]
stock_entries_col = db["stock_entries"]
expenses_col = db["expenses"]
manager_expenses_col = db["manager_expenses"]
fixed_assets_col = db["fixed_assets"]
inventory_col = db["inventory"]
returns_outwards_col = db["returns_outwards"]
inventory_products_outflow_col = db["inventory_products_outflow"]
income_entries_col = db["income_entries"]
inventory_closings_col = db["inventory_closings"]
stock_closings_col = db["stock_closings"]
stock_closing_lines_col = db["stock_closing_lines"]
private_ledger_col = db["private_ledger_entries"]
payroll_records_col = db["payroll_records"]
users_col = db["users"]

# Index coverage (safe to call repeatedly)
try:
    payments_col.create_index([("date", 1), ("amount", 1)])
    payments_col.create_index([("date_dt", 1)])
    payments_col.create_index([("agent_id", 1)])
    payments_col.create_index([("manager_id", 1)])
    payments_col.create_index([("customer_id", 1)])
except Exception:
    pass


# ------------------ Constants ------------------
WAGE_ROLE_KEYWORDS = (
    "warehouse",
    "inventory",
    "production",
    "factory",
    "labor",
    "labour",
    "operations",
)

INCOME_CATEGORIES = {
    "Discount Received": "discount_received",
    "Investment Income": "investment_income",
    "Other Incomes": "other_incomes",
}

MANAGER_ALLOWED_CATEGORIES = MANAGER_EXPENSE_CATEGORIES
MANAGER_EXPENSE_EXCLUDED = {"carriage inwards", "stock (mini)", "susu withdrawal"}

OPERATING_EXACT_CATEGORY_MAP = {
    "Salary (Monthly)": "salaries",
    "Salaries": "salaries",
    "Fuel": "motor_expenses",
    "Transportation": "motor_expenses",
    "Vehicle servicing": "motor_expenses",
    "Delievery": "carriage_outwards",
    "Delivery": "carriage_outwards",
    "Carriage Outwards": "carriage_outwards",
    "Rent": "rent_rates",
    "Rent and rates": "rent_rates",
    "Rates": "rent_rates",
    "AMA": "rent_rates",
    "Insurance": "insurance",
    "Discount allowed": "discount_allowed",
    "Bad debts": "bad_debts",
}

OPERATING_EXCLUDED_CATEGORIES = {"carriage inwards", "stock (mini)", "susu withdrawal"}


# ------------------ Small helpers ------------------
def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        if v is None or v == "":
            return default
        return float(str(v).replace(",", "").strip())
    except Exception:
        return default


def _parse_date(s: str | None) -> Optional[datetime]:
    if not s:
        return None
    try:
        d = datetime.strptime(s, "%Y-%m-%d").date()
        return datetime.combine(d, time.min)
    except Exception:
        return None


def _default_period() -> Tuple[datetime, datetime, str]:
    current_year = datetime.utcnow().year
    start_dt = datetime(current_year, 1, 1)
    end_dt = datetime(current_year, 12, 31, 23, 59, 59, 999999)
    label = f"{current_year} Full Year"
    return start_dt, end_dt, label


def parse_period_from_args(args) -> Tuple[datetime, datetime, str, str, str]:
    from_str = (args.get("from") or "").strip()
    to_str = (args.get("to") or "").strip()
    year_raw = (args.get("year") or "").strip()
    range_key = (args.get("range") or "").strip().lower()
    all_time = (args.get("all_time") or "").strip().lower() in ("1", "true", "yes")

    start_dt = _parse_date(from_str) if from_str else None
    end_dt = _parse_date(to_str) if to_str else None
    now = datetime.utcnow()

    def this_month_period() -> Tuple[datetime, datetime, str]:
        start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        end = datetime.combine(now.date(), time.max)
        return start, end, f"{start.date().strftime('%B %Y')}"

    def all_time_period() -> Tuple[datetime, datetime, str]:
        start = datetime(2000, 1, 1)
        end = now
        return start, end, "All Time"

    def year_period(raw: str) -> Optional[Tuple[datetime, datetime, str]]:
        try:
            y = int(raw)
            start = datetime(y, 1, 1)
            end = datetime(y, 12, 31, 23, 59, 59, 999999)
            return start, end, f"{y} Full Year"
        except Exception:
            return None

    if range_key == "all_time" or all_time:
        s, e, label = all_time_period()
        return s, e, label, s.date().isoformat(), e.date().isoformat()

    if range_key == "this_month":
        s, e, label = this_month_period()
        return s, e, label, s.date().isoformat(), e.date().isoformat()

    if range_key == "year":
        period = year_period(year_raw)
        if period:
            s, e, label = period
        else:
            s, e, label = _default_period()
        return s, e, label, s.date().isoformat(), e.date().isoformat()

    if range_key == "custom":
        if start_dt and end_dt:
            if end_dt < start_dt:
                start_dt, end_dt = end_dt, start_dt
            end_dt = end_dt.replace(hour=23, minute=59, second=59, microsecond=999999)
            label = f"{start_dt.date().strftime('%d %b %Y')} - {end_dt.date().strftime('%d %b %Y')}"
            return start_dt, end_dt, label, start_dt.date().isoformat(), end_dt.date().isoformat()
        s, e, label = _default_period()
        return s, e, label, s.date().isoformat(), e.date().isoformat()

    if year_raw:
        period = year_period(year_raw)
        if period:
            s, e, label = period
            return s, e, label, s.date().isoformat(), e.date().isoformat()

    if start_dt and end_dt:
        if end_dt < start_dt:
            start_dt, end_dt = end_dt, start_dt
        end_dt = end_dt.replace(hour=23, minute=59, second=59, microsecond=999999)
        label = f"{start_dt.date().strftime('%d %b %Y')} - {end_dt.date().strftime('%d %b %Y')}"
        return start_dt, end_dt, label, start_dt.date().isoformat(), end_dt.date().isoformat()

    s, e, label = _default_period()
    return s, e, label, s.date().isoformat(), e.date().isoformat()


def _months_between(start_dt: datetime, end_dt: datetime) -> List[str]:
    months: List[str] = []
    y, m = start_dt.year, start_dt.month
    end_y, end_m = end_dt.year, end_dt.month
    while (y, m) <= (end_y, end_m):
        months.append(f"{y:04d}-{m:02d}")
        if m == 12:
            y += 1
            m = 1
        else:
            m += 1
    return months


def _field_exists(col, field: str) -> bool:
    try:
        return col.find_one({field: {"$exists": True}}, {field: 1}) is not None
    except Exception:
        return False


def _normalize_branch_id(branch_id: Optional[str]) -> Tuple[Optional[str], Optional[ObjectId]]:
    if not branch_id:
        return None, None
    try:
        return branch_id, ObjectId(branch_id)
    except Exception:
        return branch_id, None


def _branch_entry_matches(entry: Dict[str, Any], branch_str: Optional[str], branch_oid: Optional[ObjectId]) -> bool:
    if not branch_str:
        return True
    if branch_oid and entry.get("branch_id") == branch_oid:
        return True
    if entry.get("branch_id") == branch_str:
        return True
    if entry.get("branch_name") == branch_str:
        return True
    return False


def _branch_query_for_snapshot(col, branch_id: Optional[str]) -> Dict[str, Any]:
    branch_str, branch_oid = _normalize_branch_id(branch_id)
    if not branch_str:
        return {}
    filters: List[Dict[str, Any]] = []
    if _field_exists(col, "branch_id"):
        filters.append({"branch_id": branch_oid or branch_str})
    if _field_exists(col, "branch_name"):
        filters.append({"branch_name": branch_str})
    if not filters:
        return {}
    return {"$or": filters}


def _build_date_range_match(
    col,
    start_dt: datetime,
    end_dt: datetime,
    prefer_date_str: bool = False,
) -> Tuple[Dict[str, Any], str]:
    """
    Returns (match_dict, note)
    Tries: date_dt -> date(str) -> created_at
    """
    start_str = start_dt.date().isoformat()
    end_str = end_dt.date().isoformat()

    # Prefer explicit date string only if requested and field exists
    if prefer_date_str and _field_exists(col, "date"):
        return {"date": {"$gte": start_str, "$lte": end_str}}, "date string range"

    if _field_exists(col, "date_dt"):
        return {"date_dt": {"$gte": start_dt, "$lte": end_dt}}, "date_dt range"

    if _field_exists(col, "date"):
        return {"date": {"$gte": start_str, "$lte": end_str}}, "date string range"

    return {"created_at": {"$gte": start_dt, "$lte": end_dt}}, "created_at range"


# ------------------ Branch filter helpers (FIXED) ------------------
def _manager_ids_for_branch_filter(branch_id: Optional[str], context: str = "branch filter") -> Tuple[List[str], List[ObjectId], List[str]]:
    """
    Returns (manager_id_strings, manager_id_objectids, notes)
    This is used for linking manager-expense rows to a branch when manager_expenses doesn't store branch_id.
    """
    if not branch_id:
        return [], [], []

    branch_str, branch_oid = _normalize_branch_id(branch_id)

    filters: List[Dict[str, Any]] = []
    if branch_str and _field_exists(users_col, "branch_id"):
        filters.append({"branch_id": branch_oid or branch_str})
    if branch_str and _field_exists(users_col, "branch_name"):
        filters.append({"branch_name": branch_str})
    if branch_str and _field_exists(users_col, "branch"):
        filters.append({"branch": branch_str})

    if not filters:
        return [], [], [f"Branch filter ignored for {context}; users branch fields unavailable"]

    docs = list(users_col.find({"role": "manager", "$or": filters}, {"_id": 1}))
    if not docs:
        return [], [], [f"Branch filter applied for {context} returned no managers"]

    manager_strs: List[str] = []
    manager_oids: List[ObjectId] = []
    for d in docs:
        uid = d.get("_id")
        if not uid:
            continue
        manager_strs.append(str(uid))
        if isinstance(uid, ObjectId):
            manager_oids.append(uid)
        else:
            try:
                manager_oids.append(ObjectId(str(uid)))
            except Exception:
                pass

    return manager_strs, manager_oids, []


def _manager_expense_match(
    start_dt: datetime,
    end_dt: datetime,
    branch_id: Optional[str],
    context: str = "manager expenses",
    status: Optional[str] = "Approved",
) -> Tuple[Dict[str, Any], List[str]]:
    """
    IMPORTANT FIX:
    - manager_expenses.manager_id may be stored as string OR ObjectId
    - date may be stored as date_dt OR date string
    """
    match, date_note = _build_date_range_match(manager_expenses_col, start_dt, end_dt, prefer_date_str=False)
    notes: List[str] = [f"manager expenses filtered via {date_note}"]

    if status and _field_exists(manager_expenses_col, "status"):
        match["status"] = status

    if branch_id:
        manager_strs, manager_oids, manager_notes = _manager_ids_for_branch_filter(branch_id, context)
        notes.extend(manager_notes)

        if manager_strs or manager_oids:
            in_vals: List[Union[str, ObjectId]] = []
            in_vals.extend(manager_strs)
            in_vals.extend(manager_oids)
            match["manager_id"] = {"$in": in_vals}
        else:
            match["manager_id"] = "__none__"

    return match, notes


def _income_branch_filter(branch_id: Optional[str]) -> Tuple[Dict[str, Any], List[str]]:
    if not branch_id:
        return {}, []
    branch_str, branch_oid = _normalize_branch_id(branch_id)
    filters: List[Dict[str, Any]] = []
    notes: List[str] = []

    if branch_str and _field_exists(income_entries_col, "branch_id"):
        filters.append({"branch_id": branch_oid or branch_str})
    if branch_str and _field_exists(income_entries_col, "branch_name"):
        filters.append({"branch_name": branch_str})

    if not filters:
        notes.append("Income branch filter ignored; missing branch_id/branch_name")
        return {}, notes

    return {"$or": filters}, notes


# ------------------ Snapshots ------------------
def get_stock_snapshot_header(year: int) -> Optional[Dict[str, Any]]:
    try:
        return stock_closings_col.find_one({"closing_year": year, "status": "completed"})
    except Exception:
        return None


def snapshot_cost_total(year: int, branch_id: Optional[str] = None) -> Tuple[float, str, bool]:
    header = get_stock_snapshot_header(year)
    branch_str, branch_oid = _normalize_branch_id(branch_id)

    if header:
        if branch_id:
            for entry in header.get("branch_totals") or []:
                if _branch_entry_matches(entry, branch_str, branch_oid):
                    amount = _safe_float(entry.get("closing_cost_value"))
                    branch_name = entry.get("branch_name") or branch_str or "Branch"
                    return amount, f"Snapshot {year} closing ({branch_name})", True
        else:
            if header.get("total_closing_cost_value") is not None:
                amount = _safe_float(header.get("total_closing_cost_value"))
                return amount, f"Snapshot {year} closing", True

    # fallback: line details
    q: Dict[str, Any] = {"closing_year": year}
    branch_filter = _branch_query_for_snapshot(stock_closing_lines_col, branch_id)
    if branch_filter:
        q.update(branch_filter)

    total = 0.0
    for line in stock_closing_lines_col.find(q, {"closing_cost_value": 1}):
        total += _safe_float(line.get("closing_cost_value"))

    if total:
        src = f"Snapshot {year} closing (line details)"
        if branch_id:
            src += " — filtered branch"
        return total, src, True

    return 0.0, "", False


def snapshot_cost_breakdown_by_manager(year: int, branch_id: Optional[str] = None) -> List[Dict[str, Any]]:
    q: Dict[str, Any] = {"closing_year": year}
    branch_filter = _branch_query_for_snapshot(stock_closing_lines_col, branch_id)
    if branch_filter:
        q.update(branch_filter)

    totals: Dict[Tuple[str, str, str], float] = {}
    for line in stock_closing_lines_col.find(q, {"manager_id": 1, "manager_name": 1, "branch_name": 1, "closing_cost_value": 1}):
        amt = _safe_float(line.get("closing_cost_value"))
        manager_name = (line.get("manager_name") or "").strip() or "Unknown manager"
        manager_key = line.get("manager_id")
        manager_id_key = str(manager_key) if manager_key else manager_name
        branch_name = (line.get("branch_name") or "").strip() or "Unknown branch"
        key = (manager_id_key, manager_name, branch_name)
        totals[key] = totals.get(key, 0.0) + amt

    results: List[Dict[str, Any]] = []
    for (_, manager_name, branch_name), total in sorted(totals.items(), key=lambda kv: kv[1], reverse=True):
        results.append({"manager_name": manager_name, "branch_name": branch_name, "amount": total})
    return results


# ------------------ Sales (FIXED: uses date_dt if available + includes instant_sales) ------------------
def compute_sales_total(start_dt: datetime, end_dt: datetime, branch_id: Optional[str] = None) -> Dict[str, Any]:
    """
    FIXES:
    - Uses date_dt when present; otherwise uses date string
    - Applies status=confirmed only if field exists
    - Includes instant_sales within the same period
    - Keeps branch filtering based on payments.branch_id / users mapping (best effort)
    """
    missing: List[str] = []

    # date range match for payments
    payments_match, note = _build_date_range_match(payments_col, start_dt, end_dt, prefer_date_str=False)
    status_filter = None
    if _field_exists(payments_col, "status"):
        status_filter = "confirmed"

    if status_filter:
        payments_match["status"] = status_filter
    else:
        missing.append("payments.status not found; sales uses all payment rows")

    # branch filter on payments if it exists
    branch_str, branch_oid = _normalize_branch_id(branch_id)
    if branch_id and branch_str:
        if _field_exists(payments_col, "branch_id"):
            payments_match["branch_id"] = branch_oid or branch_str
        elif _field_exists(payments_col, "branch_name"):
            payments_match["branch_name"] = branch_str
        # else: no direct branch fields → still okay

    # exclude withdrawals from sales
    payments_match["payment_type"] = {"$ne": "WITHDRAWAL"}

    # sum payments amount
    payments_total = 0.0
    payments_count = 0
    by_method: Dict[str, float] = {}

    for d in payments_col.find(payments_match, {"amount": 1, "method": 1}):
        amt = _safe_float(d.get("amount"))
        if amt <= 0:
            continue
        payments_total += amt
        payments_count += 1
        method = (d.get("method") or "Other").strip() or "Other"
        by_method[method] = by_method.get(method, 0.0) + amt

    breakdown = [{"label": k, "amount": v} for k, v in sorted(by_method.items(), key=lambda kv: kv[1], reverse=True)]

    # instant sales
    instant_total = 0.0
    instant_count = 0
    if instant_sales_col is not None:
        inst_match, _ = _build_date_range_match(instant_sales_col, start_dt, end_dt, prefer_date_str=False)

        if branch_id and branch_str:
            if _field_exists(instant_sales_col, "branch_id"):
                inst_match["branch_id"] = branch_oid or branch_str
            elif _field_exists(instant_sales_col, "branch_name"):
                inst_match["branch_name"] = branch_str

        for s in instant_sales_col.find(inst_match, {"total_amount": 1, "amount": 1, "grand_total": 1}):
            amt = _safe_float(s.get("total_amount"))
            if amt <= 0:
                amt = _safe_float(s.get("grand_total"))
            if amt <= 0:
                amt = _safe_float(s.get("amount"))
            if amt <= 0:
                continue
            instant_total += amt
            instant_count += 1

    # excluded counts (optional insights)
    excluded_counts: Dict[str, int] = {}
    for excluded in ("SUSU", "WITHDRAWAL"):
        q = dict(payments_match)
        q["payment_type"] = excluded
        excluded_counts[excluded.lower()] = payments_col.count_documents(q)

    return {
        "sales_total": payments_total + instant_total,
        "payments_product_total": payments_total,
        "payments_count": payments_count,
        "instant_sales_total": instant_total,
        "instant_sales_count": instant_count,
        "breakdown": breakdown,
        "excluded_counts": excluded_counts,
        "notes": [f"payments filtered via {note}"] + missing,
    }


# ------------------ Returns / Purchases / Expenses helpers ------------------
def _sum_returns_inwards(start_dt: datetime, end_dt: datetime, branch_id: Optional[str]) -> Tuple[float, int, List[str]]:
    missing: List[str] = []
    q: Dict[str, Any] = {"status": "Approved", "return_date_dt": {"$gte": start_dt, "$lte": end_dt}}

    if branch_id:
        # best effort branch filtering
        branch_str, branch_oid = _normalize_branch_id(branch_id)
        or_filters: List[Dict[str, Any]] = []
        if branch_str and _field_exists(returns_inwards_col, "branch_id"):
            or_filters.append({"branch_id": branch_oid or branch_str})
        if branch_str and _field_exists(returns_inwards_col, "branch_name"):
            or_filters.append({"branch_name": branch_str})
        if or_filters:
            q["$or"] = or_filters
        else:
            missing.append("returns_inwards branch filter ignored; branch fields not found")

    total = 0.0
    count = 0
    for d in returns_inwards_col.find(q, {"sales_reduction_amount": 1}):
        amt = _safe_float(d.get("sales_reduction_amount"))
        if amt > 0:
            total += amt
            count += 1
    return total, count, missing


def _sum_ap_bills(start_dt: datetime, end_dt: datetime, branch_id: Optional[str]) -> float:
    q: Dict[str, Any] = {"bill_date_dt": {"$gte": start_dt, "$lte": end_dt}}
    if branch_id and _field_exists(ap_bills_col, "branch_id"):
        branch_str, branch_oid = _normalize_branch_id(branch_id)
        if branch_str:
            q["branch_id"] = branch_oid or branch_str

    total = 0.0
    for d in ap_bills_col.find(q, {"amount": 1, "status": 1}):
        status = (d.get("status") or "").lower()
        if status in ("draft", "voided", "cancelled"):
            continue
        total += _safe_float(d.get("amount"))
    return total


def _sum_stock_entries(start_dt: datetime, end_dt: datetime, branch_id: Optional[str]) -> Tuple[float, float]:
    q: Dict[str, Any] = {"purchased_at": {"$gte": start_dt, "$lte": end_dt}}
    if branch_id and _field_exists(stock_entries_col, "branch_id"):
        branch_str, branch_oid = _normalize_branch_id(branch_id)
        if branch_str:
            q["branch_id"] = branch_oid or branch_str

    period_total = 0.0
    for d in stock_entries_col.find(q, {"total_cost": 1, "quantity": 1, "unit_price": 1}):
        total_cost = d.get("total_cost")
        if total_cost is None:
            total_cost = _safe_float(d.get("quantity")) * _safe_float(d.get("unit_price"))
        period_total += _safe_float(total_cost)

    all_total = 0.0
    for d in stock_entries_col.find({}, {"total_cost": 1, "quantity": 1, "unit_price": 1}):
        total_cost = d.get("total_cost")
        if total_cost is None:
            total_cost = _safe_float(d.get("quantity")) * _safe_float(d.get("unit_price"))
        all_total += _safe_float(total_cost)

    return period_total, all_total


def _sum_purchases_stock_entries(start_dt: datetime, end_dt: datetime, branch_id: Optional[str]) -> Tuple[float, int, List[str]]:
    q: Dict[str, Any] = {"purchased_at": {"$gte": start_dt, "$lte": end_dt}}
    notes: List[str] = []

    branch_str, branch_oid = _normalize_branch_id(branch_id)
    if branch_id and branch_str:
        if _field_exists(stock_entries_col, "branch_id"):
            q["branch_id"] = branch_oid or branch_str
        elif _field_exists(stock_entries_col, "branch_name"):
            q["branch_name"] = branch_str
        else:
            notes.append("Purchases branch filter not available on stock_entries; missing branch fields.")

    total = 0.0
    count = 0
    for d in stock_entries_col.find(q, {"total_cost": 1, "quantity": 1, "unit_price": 1}):
        total_cost = d.get("total_cost")
        if total_cost is None:
            total_cost = _safe_float(d.get("quantity")) * _safe_float(d.get("unit_price"))
        total += _safe_float(total_cost)
        count += 1
    return total, count, notes


def _sum_purchases_stock_mini(start_dt: datetime, end_dt: datetime, branch_id: Optional[str]) -> Tuple[float, int, List[str]]:
    match, notes = _manager_expense_match(start_dt, end_dt, branch_id, "manager stock (mini)")
    # category stored as "Stock (mini)" in your DB
    match["category"] = {"$regex": r"^Stock\s*\(mini\)$", "$options": "i"}

    total = 0.0
    count = 0
    for d in manager_expenses_col.find(match, {"amount": 1}):
        amt = _safe_float(d.get("amount"))
        if amt <= 0:
            continue
        total += amt
        count += 1
    return total, count, notes


def _sum_accounting_expenses(start_dt: datetime, end_dt: datetime, branch_id: Optional[str]) -> Tuple[float, List[Dict[str, Any]], int, List[str], Dict[str, float]]:
    match, date_note = _build_date_range_match(expenses_col, start_dt, end_dt)
    notes: List[str] = [f"accounting expenses filtered via {date_note}"]

    if branch_id:
        branch_str, branch_oid = _normalize_branch_id(branch_id)
        or_filters: List[Dict[str, Any]] = []
        if branch_str and _field_exists(expenses_col, "branch_id"):
            or_filters.append({"branch_id": branch_oid or branch_str})
        if branch_str and _field_exists(expenses_col, "branch_name"):
            or_filters.append({"branch_name": branch_str})
        if or_filters:
            match["$or"] = or_filters
        else:
            notes.append("Accounting expenses branch filter ignored; branch fields missing.")

    totals: Dict[str, float] = {}
    raw_entries: List[Dict[str, Any]] = []
    count = 0

    for doc in expenses_col.find(match, {"amount": 1, "category": 1}):
        amt = _safe_float(doc.get("amount"))
        if amt <= 0:
            continue
        cat = (doc.get("category") or "Uncategorized").strip() or "Uncategorized"
        totals[cat] = totals.get(cat, 0.0) + amt
        raw_entries.append({"category": cat, "amount": amt})
        count += 1

    sorted_entries = sorted(raw_entries, key=lambda item: item["amount"], reverse=True)
    total = sum(totals.values())
    return total, sorted_entries, count, notes, totals


def _inventory_closing_value() -> float:
    total = 0.0
    for item in inventory_col.find({}, {"qty": 1, "cost_price": 1}):
        qty = _safe_float(item.get("qty"))
        cost = _safe_float(item.get("cost_price"))
        total += qty * cost
    return total


def _expense_category_map(start_dt: datetime, end_dt: datetime, branch_id: Optional[str]) -> Dict[str, float]:
    totals: Dict[str, float] = {}

    acc_match, _ = _build_date_range_match(expenses_col, start_dt, end_dt)
    if branch_id:
        branch_str, branch_oid = _normalize_branch_id(branch_id)
        filters: List[Dict[str, Any]] = []
        if branch_str and _field_exists(expenses_col, "branch_id"):
            filters.append({"branch_id": branch_oid or branch_str})
        if branch_str and _field_exists(expenses_col, "branch_name"):
            filters.append({"branch_name": branch_str})
        if filters:
            acc_match["$or"] = filters

    for d in expenses_col.find(acc_match, {"amount": 1, "category": 1}):
        amt = _safe_float(d.get("amount"))
        if amt <= 0:
            continue
        cat = (d.get("category") or "Uncategorized").strip() or "Uncategorized"
        totals[cat] = totals.get(cat, 0.0) + amt

    allowed_set = set(MANAGER_ALLOWED_CATEGORIES)
    excluded_set = MANAGER_EXPENSE_EXCLUDED
    mgr_match, _ = _manager_expense_match(start_dt, end_dt, branch_id, "manager expenses")

    for d in manager_expenses_col.find(mgr_match, {"amount": 1, "category": 1}):
        amt = _safe_float(d.get("amount"))
        if amt <= 0:
            continue
        raw_cat = (d.get("category") or "").strip() or "Miscellaneous"
        if raw_cat.lower() in excluded_set:
            continue
        cat = raw_cat if raw_cat in allowed_set else "Miscellaneous"
        totals[cat] = totals.get(cat, 0.0) + amt

    return totals


def _sum_by_keywords(expense_map: Dict[str, float], keywords: Iterable[str]) -> float:
    total = 0.0
    for cat, amt in expense_map.items():
        cat_lc = cat.lower()
        if any(k in cat_lc for k in keywords):
            total += amt
    return total


def _monthly_dep_amount(asset: Dict[str, Any]) -> float:
    method = (asset.get("method") or "SL").upper()
    useful_life_years = int(asset.get("useful_life_years") or 0)
    if useful_life_years <= 0:
        return 0.0

    cost = _safe_float(asset.get("cost"), 0.0)
    accum = _safe_float(asset.get("accum_depr"), 0.0)
    nbv = cost - accum
    if nbv <= 0:
        return 0.0

    months = useful_life_years * 12
    if months <= 0:
        return 0.0

    if method == "DB":
        annual_rate = 2.0 / useful_life_years
        monthly_rate = annual_rate / 12.0
        dep = nbv * monthly_rate
    else:
        dep = cost / months

    return min(dep, nbv)


def _calc_depreciation(start_dt: datetime, end_dt: datetime) -> float:
    docs = list(fixed_assets_col.find({"status": {"$in": ["Active", "Fully Depreciated"]}}))
    monthly_total = sum(_monthly_dep_amount(d) for d in docs)
    months = len(_months_between(start_dt, end_dt))
    return monthly_total * max(1, months)


def _build_goods_drawn_query(start_dt: datetime, end_dt: datetime, branch_id: Optional[str]) -> Tuple[Dict[str, Any], List[str]]:
    q: Dict[str, Any] = {
        "entry_type": "goods_drawn",
        "status": "posted",
        "date_dt": {"$gte": start_dt, "$lte": end_dt},
    }
    notes: List[str] = []

    if branch_id:
        branch_str, branch_oid = _normalize_branch_id(branch_id)
        if branch_str and _field_exists(private_ledger_col, "branch_id"):
            q["branch_id"] = branch_oid or branch_str
        else:
            manager_strs, manager_oids, n = _manager_ids_for_branch_filter(branch_id, "goods drawn")
            notes.extend(n)
            if manager_strs or manager_oids:
                ors: List[Dict[str, Any]] = []
                vals: List[Union[str, ObjectId]] = []
                vals.extend(manager_strs)
                vals.extend(manager_oids)

                if _field_exists(private_ledger_col, "created_by"):
                    ors.append({"created_by": {"$in": vals}})
                if _field_exists(private_ledger_col, "manager_id"):
                    ors.append({"manager_id": {"$in": vals}})
                if _field_exists(private_ledger_col, "agent_id"):
                    ors.append({"agent_id": {"$in": vals}})

                if ors:
                    q["$or"] = ors
                else:
                    notes.append("Goods drawn branch filter not available; missing linkage fields on private ledger.")
            else:
                notes.append("Goods drawn branch filter not available; no managers found for branch.")

    return q, notes


def _sum_goods_drawn(start_dt: datetime, end_dt: datetime, branch_id: Optional[str]) -> Tuple[float, int, List[str]]:
    q, notes = _build_goods_drawn_query(start_dt, end_dt, branch_id)
    total = 0.0
    count = 0
    for d in private_ledger_col.find(q, {"amount": 1}):
        amt = _safe_float(d.get("amount"))
        if amt <= 0:
            continue
        total += amt
        count += 1
    return total, count, notes


def _goods_drawn_breakdown(start_dt: datetime, end_dt: datetime, branch_id: Optional[str]) -> List[Dict[str, Any]]:
    q, _ = _build_goods_drawn_query(start_dt, end_dt, branch_id)
    cursor = private_ledger_col.find(q, {"product_id": 1, "purpose_text": 1, "memo": 1, "amount": 1})

    aggregation: Dict[str, Dict[str, Any]] = {}
    product_oids: Set[ObjectId] = set()

    for doc in cursor:
        amount = _safe_float(doc.get("amount"))
        if amount <= 0:
            continue
        product_id = doc.get("product_id")
        memo = (doc.get("purpose_text") or doc.get("memo") or "").strip()

        key = str(product_id) if product_id else memo or "Goods Drawn"
        entry = aggregation.setdefault(key, {"product_id": product_id, "memo": memo, "total": 0.0, "count": 0})
        entry["total"] += amount
        entry["count"] += 1

        if product_id:
            try:
                oid = product_id if isinstance(product_id, ObjectId) else ObjectId(str(product_id))
                product_oids.add(oid)
            except Exception:
                pass

    product_names: Dict[str, str] = {}
    if product_oids:
        for prod in inventory_col.find({"_id": {"$in": list(product_oids)}}, {"name": 1}):
            pid = prod.get("_id")
            if isinstance(pid, ObjectId):
                product_names[str(pid)] = prod.get("name") or ""

    results: List[Dict[str, Any]] = []
    for entry in sorted(aggregation.values(), key=lambda d: d["total"], reverse=True):
        product_id = entry.get("product_id")
        label = entry.get("memo") or "Goods Drawn"
        if product_id:
            label = product_names.get(str(product_id)) or label
        results.append({"label": label, "total": entry["total"], "count": entry["count"]})
    return results


def _sum_returns_outwards(start_dt: datetime, end_dt: datetime, branch_id: Optional[str]) -> Tuple[float, int, List[str]]:
    q: Dict[str, Any] = {"status": "posted", "date_dt": {"$gte": start_dt, "$lte": end_dt}}
    notes: List[str] = []

    if branch_id:
        branch_str, branch_oid = _normalize_branch_id(branch_id)
        if branch_str and _field_exists(returns_outwards_col, "branch_id"):
            q["branch_id"] = branch_oid or branch_str
        elif branch_str and _field_exists(returns_outwards_col, "branch_name"):
            q["branch_name"] = branch_str
        else:
            notes.append("Returns outwards branch filter unavailable; branch fields missing.")

    total = 0.0
    count = 0
    for doc in returns_outwards_col.find(q, {"total_cost": 1}):
        amt = _safe_float(doc.get("total_cost"))
        if amt <= 0:
            continue
        total += amt
        count += 1

    return total, count, notes


def _sum_hr_wages(start_dt: datetime, end_dt: datetime, branch_id: Optional[str]) -> Tuple[float, int, List[str]]:
    q: Dict[str, Any] = {"role": {"$nin": ["admin", "executive", "manager"]}}
    notes: List[str] = []

    if branch_id:
        bf = _branch_query_for_snapshot(users_col, branch_id)
        if bf:
            q.update(bf)
        else:
            notes.append("HR wages branch filter ignored; users.branch fields not available.")

    total = 0.0
    count = 0

    for doc in users_col.find(q, {"wages_tips": 1}):
        for entry in doc.get("wages_tips") or []:
            date_val = entry.get("date")
            date_obj = None
            if isinstance(date_val, datetime):
                date_obj = date_val
            elif isinstance(date_val, str):
                try:
                    date_obj = datetime.strptime(date_val[:10], "%Y-%m-%d")
                except Exception:
                    continue
            if not date_obj:
                continue
            if start_dt <= date_obj <= end_dt:
                amt = _safe_float(entry.get("amount"))
                if amt <= 0:
                    continue
                total += amt
                count += 1

    return total, count, notes


def _sum_income_entries(start_dt: datetime, end_dt: datetime, branch_id: Optional[str]) -> Tuple[Dict[str, float], int, List[str]]:
    q: Dict[str, Any] = {"status": "posted", "date_dt": {"$gte": start_dt, "$lte": end_dt}}
    filters, notes = _income_branch_filter(branch_id)
    if filters:
        q.update(filters)

    totals = {"discount_received": 0.0, "investment_income": 0.0, "other_incomes": 0.0}
    count = 0

    for doc in income_entries_col.find(q, {"category": 1, "amount": 1}):
        cat = doc.get("category")
        amount = _safe_float(doc.get("amount"))
        mapped = INCOME_CATEGORIES.get(cat)
        if mapped and amount > 0:
            totals[mapped] += amount
        count += 1

    totals["total"] = totals["discount_received"] + totals["investment_income"] + totals["other_incomes"]
    return totals, count, notes


def _manager_operating_expenses(
    start_dt: datetime, end_dt: datetime, branch_id: Optional[str]
) -> Tuple[float, List[Dict[str, Any]], List[Dict[str, Any]], int, List[str], Dict[str, float], Dict[str, Any]]:
    q, notes = _manager_expense_match(start_dt, end_dt, branch_id, "manager expenses")

    totals: Dict[str, float] = {}
    count = 0
    allowed_set = set(MANAGER_ALLOWED_CATEGORIES)

    for doc in manager_expenses_col.find(q, {"amount": 1, "category": 1}):
        amt = _safe_float(doc.get("amount"))
        if amt <= 0:
            continue
        raw_cat = (doc.get("category") or "").strip() or "Miscellaneous"
        if raw_cat.lower() in MANAGER_EXPENSE_EXCLUDED:
            continue
        cat = raw_cat if raw_cat in allowed_set else "Miscellaneous"
        totals[cat] = totals.get(cat, 0.0) + amt
        count += 1

    sorted_entries = sorted(totals.items(), key=lambda item: item[1], reverse=True)
    all_list = [{"category": cat, "amount": amt} for cat, amt in sorted_entries]
    top5 = all_list[:5]
    total = sum(totals.values())
    return total, all_list, top5, count, notes, totals, q


def _sum_carriage_inwards(start_dt: datetime, end_dt: datetime, branch_id: Optional[str]) -> Tuple[float, List[Dict[str, Any]], List[str], int]:
    match, notes = _manager_expense_match(start_dt, end_dt, branch_id, "carriage inwards")
    match["category"] = {"$regex": r"^Carriage\s+Inwards$", "$options": "i"}

    cursor = manager_expenses_col.find(match, {"manager_id": 1, "amount": 1})
    totals: Dict[str, float] = {}
    count = 0

    for doc in cursor:
        mid = doc.get("manager_id")
        amt = _safe_float(doc.get("amount"))
        if amt <= 0:
            continue
        key = str(mid) if mid else "unknown"
        totals[key] = totals.get(key, 0.0) + amt
        count += 1

    breakdown: List[Dict[str, Any]] = []
    manager_ids = [k for k in totals.keys() if k and k != "unknown"]

    info_map: Dict[str, Dict[str, str]] = {}
    if manager_ids:
        obj_ids: List[ObjectId] = []
        for mid in manager_ids:
            try:
                obj_ids.append(ObjectId(mid))
            except Exception:
                pass
        if obj_ids:
            for user in users_col.find({"_id": {"$in": obj_ids}}, {"_id": 1, "name": 1, "branch": 1, "branch_name": 1}):
                info_map[str(user["_id"])] = {
                    "name": user.get("name", "Manager"),
                    "branch": user.get("branch") or user.get("branch_name") or "",
                }

    total_amount = 0.0
    for manager_key, amt in sorted(totals.items(), key=lambda kv: kv[1], reverse=True):
        info = info_map.get(manager_key, {"name": "Manager", "branch": ""})
        breakdown.append({"manager_id": manager_key, "manager_name": info["name"], "branch": info["branch"], "amount": amt})
        total_amount += amt

    return total_amount, breakdown, notes, count


def _sum_payroll(start_dt: datetime, end_dt: datetime, branch_id: Optional[str]) -> Tuple[float, float, bool]:
    months = _months_between(start_dt, end_dt)
    if not months:
        return 0.0, 0.0, False

    q: Dict[str, Any] = {"month": {"$in": months}}
    branch_str, branch_oid = _normalize_branch_id(branch_id)

    if branch_str:
        if _field_exists(payroll_records_col, "branch_id"):
            q["branch_id"] = branch_oid or branch_str
        elif _field_exists(payroll_records_col, "branch"):
            q["branch"] = branch_str
        elif _field_exists(payroll_records_col, "branch_name"):
            q["branch_name"] = branch_str

    docs = list(payroll_records_col.find(q, {"total_staff_cost": 1, "basic_salary": 1, "allowances": 1, "role": 1}))
    if not docs:
        return 0.0, 0.0, False

    wages = 0.0
    salaries = 0.0

    for d in docs:
        total_cost = _safe_float(d.get("total_staff_cost"))
        if total_cost <= 0:
            total_cost = _safe_float(d.get("basic_salary")) + _safe_float(d.get("allowances"))
        role = (d.get("role") or "").lower()
        if role and any(k in role for k in WAGE_ROLE_KEYWORDS):
            wages += total_cost
        else:
            salaries += total_cost

    return wages, salaries, True


# ------------------ COGS from outflow ------------------
def _sum_cogs_from_outflow(start_dt: datetime, end_dt: datetime, branch_id: Optional[str]) -> Dict[str, Any]:
    q: Dict[str, Any] = {"created_at": {"$gte": start_dt, "$lte": end_dt}}
    notes: List[str] = []

    if branch_id:
        branch_str, branch_oid = _normalize_branch_id(branch_id)
        if branch_str and _field_exists(inventory_products_outflow_col, "branch_id"):
            q["branch_id"] = branch_oid or branch_str
        else:
            manager_strs, manager_oids, n = _manager_ids_for_branch_filter(branch_id, "outflow cogs")
            notes.extend(n)
            vals: List[Union[str, ObjectId]] = []
            vals.extend(manager_strs)
            vals.extend(manager_oids)

            if vals:
                ors: List[Dict[str, Any]] = []
                if _field_exists(inventory_products_outflow_col, "manager_id"):
                    ors.append({"manager_id": {"$in": vals}})
                if _field_exists(inventory_products_outflow_col, "by_user"):
                    ors.append({"by_user": {"$in": vals}})
                if _field_exists(inventory_products_outflow_col, "agent.id"):
                    ors.append({"agent.id": {"$in": vals}})
                if ors:
                    q["$or"] = ors
                else:
                    notes.append("branch filter ignored for outflow cogs; missing linkage fields")
            else:
                notes.append("branch filter applied for outflow cogs returned no managers")

    cogs = 0.0
    by_source: Dict[str, float] = {"instant_sale": 0.0, "agent_deliveries": 0.0, "close_card": 0.0, "other": 0.0}
    missing_cost_count = 0
    missing_cost_examples: List[Dict[str, Any]] = []
    count = 0

    for doc in inventory_products_outflow_col.find(q):
        count += 1
        src = (doc.get("source") or "other").lower().strip() or "other"

        qty = 1.0
        unit_cost = 0.0

        if src == "instant_sale":
            qty = _safe_float(doc.get("selected_qty") or (doc.get("selected_product") or {}).get("quantity") or 1)
            unit_cost = _safe_float(doc.get("unit_cost_price")) or _safe_float((doc.get("selected_product") or {}).get("cost_price"))
        elif src == "agent_deliveries":
            qty_candidate = doc.get("package_qty") or (doc.get("packaged_product") or {}).get("quantity") or 1
            qty = _safe_float(qty_candidate or 1)
            unit_cost = _safe_float(doc.get("unit_cost_price")) or _safe_float((doc.get("packaged_product") or {}).get("cost_price")) or _safe_float((doc.get("selected_product") or {}).get("cost_price"))
        elif src == "close_card":
            product = doc.get("selected_product") or {}
            qty = _safe_float(
                product.get("qty")
                or product.get("quantity")
                or (doc.get("closed_product") or {}).get("quantity")
                or 1
            )
            unit_cost = _safe_float(product.get("cost_price")) or _safe_float(doc.get("unit_cost_price"))
        else:
            unit_cost = _safe_float(doc.get("unit_cost_price"))
            qty = _safe_float(doc.get("qty") or doc.get("quantity") or 1)

        if unit_cost <= 0:
            missing_cost_count += 1
            if len(missing_cost_examples) < 20:
                missing_cost_examples.append({"_id": str(doc.get("_id")), "source": src, "note": "missing unit cost"})
            continue

        line_total = unit_cost * max(qty, 1.0)
        cogs += line_total

        key = src if src in by_source else "other"
        by_source[key] = by_source.get(key, 0.0) + line_total

    return {
        "cogs": cogs,
        "count": count,
        "by_source": by_source,
        "missing_cost_count": missing_cost_count,
        "missing_cost_examples": missing_cost_examples,
        "notes": notes,
    }


# ------------------ Profit & Loss (CORE) ------------------
def compute_profit_loss(start_dt: datetime, end_dt: datetime, branch_id: Optional[str] = None, debug: bool = False) -> Dict[str, Any]:
    missing_sources: List[str] = []

    sales_info = compute_sales_total(start_dt, end_dt, branch_id)
    sales = _safe_float(sales_info.get("sales_total"))
    returns_inwards, returns_inwards_count, missing = _sum_returns_inwards(start_dt, end_dt, branch_id)
    missing_sources.extend(missing)
    net_sales = sales - returns_inwards

    purchases_ap = _sum_ap_bills(start_dt, end_dt, branch_id)
    stock_period_total, stock_all_total = _sum_stock_entries(start_dt, end_dt, branch_id)

    stock_entries_total, stock_entries_count, stock_entries_notes = _sum_purchases_stock_entries(start_dt, end_dt, branch_id)
    stock_mini_total, stock_mini_count, stock_mini_notes = _sum_purchases_stock_mini(start_dt, end_dt, branch_id)

    missing_sources.extend(stock_entries_notes)
    missing_sources.extend(stock_mini_notes)

    purchases = stock_entries_total + stock_mini_total
    purchases_sources = {"executive_stock_entries": stock_entries_total, "manager_stock_mini": stock_mini_total}
    purchases_breakdown = [
        {"label": "Executive Stock Entries", "amount": stock_entries_total, "count": stock_entries_count},
        {"label": "Manager Stock (mini) (Approved)", "amount": stock_mini_total, "count": stock_mini_count},
    ]
    purchases_counts = {"stock_entries": stock_entries_count, "stock_mini": stock_mini_count}

    returns_outwards, returns_outwards_count, returns_outwards_notes = _sum_returns_outwards(start_dt, end_dt, branch_id)
    missing_sources.extend(returns_outwards_notes)
    returns_outwards_source = "returns_outwards (posted)"

    goods_drawn, goods_drawn_count, goods_drawn_notes = _sum_goods_drawn(start_dt, end_dt, branch_id)
    goods_drawn_breakdown = _goods_drawn_breakdown(start_dt, end_dt, branch_id)
    missing_sources.extend(goods_drawn_notes)

    hr_wages_total, hr_wages_count, hr_wages_notes = _sum_hr_wages(start_dt, end_dt, branch_id)
    missing_sources.extend(hr_wages_notes)

    income_sums, income_count, income_notes = _sum_income_entries(start_dt, end_dt, branch_id)
    missing_sources.extend(income_notes)

    expense_map = _expense_category_map(start_dt, end_dt, branch_id)

    carriage_inwards, carriage_inwards_breakdown, carriage_notes, carriage_inwards_count = _sum_carriage_inwards(start_dt, end_dt, branch_id)
    missing_sources.extend(carriage_notes)

    (
        manager_opex_total,
        manager_opex_all,
        manager_opex_top5,
        manager_opex_count,
        manager_opex_notes,
        manager_category_totals,
        manager_expense_match,
    ) = _manager_operating_expenses(start_dt, end_dt, branch_id)
    missing_sources.extend(manager_opex_notes)

    manager_expense_match_count = manager_expenses_col.count_documents(manager_expense_match)
    manager_expense_categories = manager_expenses_col.distinct("category", manager_expense_match)

    (
        accounting_total,
        _accounting_all,
        accounting_count,
        accounting_notes,
        accounting_category_totals,
    ) = _sum_accounting_expenses(start_dt, end_dt, branch_id)
    missing_sources.extend(accounting_notes)

    opex_total = manager_opex_total + accounting_total
    opex_category_totals: Dict[str, float] = dict(accounting_category_totals)
    for cat, amt in manager_category_totals.items():
        opex_category_totals[cat] = opex_category_totals.get(cat, 0.0) + amt

    opex_breakdown_all = [{"category": cat, "amount": amt} for cat, amt in sorted(opex_category_totals.items(), key=lambda item: item[1], reverse=True)]
    opex_counts = {"manager": manager_opex_count, "accounting": accounting_count}
    opex_sources = {"manager": manager_opex_total, "accounting": accounting_total}

    wages, salaries, payroll_used = _sum_payroll(start_dt, end_dt, branch_id)
    if not payroll_used:
        wages = _sum_by_keywords(expense_map, ["wages", "direct labor", "direct labour"])
        salaries = _sum_by_keywords(expense_map, ["salary", "salaries"])
        missing_sources.append("Payroll records missing for period; wages/salaries derived from expense categories")
    else:
        if _sum_by_keywords(expense_map, ["salary", "salaries", "wages"]) > 0:
            missing_sources.append("Payroll used for wages/salaries; salary-related expense categories excluded to avoid double count")

    # If HR wages tips exist, we treat it as wages source (your current logic)
    if hr_wages_count > 0:
        wages_source = "hr_wages_tips"
        wages_count = hr_wages_count
        wages_hr_total = hr_wages_total
        wages = hr_wages_total
    else:
        wages_source = "fallback_payroll_or_expenses"
        wages_count = 0
        wages_hr_total = 0.0
        missing_sources.append("HR wages not found; falling back to payroll/expense sources")

    net_purchases = purchases - returns_outwards - goods_drawn + carriage_inwards

    opening_year = start_dt.year - 1
    closing_year = start_dt.year

    opening_stock, opening_source, opening_snapshot = snapshot_cost_total(opening_year, branch_id)
    if not opening_snapshot:
        missing_sources.append("Opening stock snapshot missing; opening_stock set to 0")
        opening_stock = 0.0
        opening_source = f"No snapshot found for {opening_year}"

    closing_stock, closing_source, closing_snapshot = snapshot_cost_total(closing_year, branch_id)
    if not closing_snapshot:
        missing_sources.append("Closing stock snapshot missing; used live inventory valuation")
        closing_stock = _inventory_closing_value()
        closing_source = "Live inventory valuation"

    opening_stock_breakdown = snapshot_cost_breakdown_by_manager(opening_year, branch_id)
    closing_stock_breakdown = snapshot_cost_breakdown_by_manager(closing_year, branch_id)

    cost_goods_available = opening_stock + net_purchases

    cogs_info = _sum_cogs_from_outflow(start_dt, end_dt, branch_id)
    if cogs_info["missing_cost_count"]:
        missing_sources.append(f"{cogs_info['missing_cost_count']} outflow records missing cost")
        missing_sources.extend(cogs_info["notes"])

    cost_goods_sold = _safe_float(cogs_info["cogs"])
    cost_goods_sold_legacy = cost_goods_available - closing_stock
    if cost_goods_sold_legacy and abs(cost_goods_sold_legacy - cost_goods_sold) > 1.0:
        missing_sources.append("COGS differs from legacy opening+net-closing calculation")

    cost_of_sales = cost_goods_sold + wages
    gross_profit = net_sales - cost_of_sales

    discount_received = _safe_float(income_sums.get("discount_received", 0.0))
    investment_income = _safe_float(income_sums.get("investment_income", 0.0))
    other_incomes = _safe_float(income_sums.get("other_incomes", 0.0))

    total_expenses = opex_total
    net_profit = (gross_profit + discount_received + investment_income + other_incomes) - total_expenses

    # Optional: accruals/amortization hooks (kept available)
    try:
        _ = prepayment_amortization_for_period(start_dt, end_dt)
        _ = accruals_for_period(start_dt, end_dt)
    except Exception:
        pass

    if debug and missing_sources:
        logger.warning("P&L missing data sources: %s", "; ".join(missing_sources))

    revenue_breakdown = list(sales_info.get("breakdown", []))
    if returns_inwards:
        revenue_breakdown.append({"label": "Less: Returns Inwards (Approved)", "amount": -returns_inwards})

    return {
        "sales": sales,
        "returns_inwards": returns_inwards,
        "returns_inwards_count": returns_inwards_count,
        "net_sales": net_sales,

        "opening_stock": opening_stock,
        "opening_stock_year": opening_year,
        "opening_stock_source": opening_source,
        "opening_stock_breakdown_managers": opening_stock_breakdown,

        "purchases": purchases,
        "purchases_exec_stock_entries": stock_entries_total,
        "purchases_stock_mini": stock_mini_total,
        "purchases_components_note": "Executive stock entries + approved manager Stock (mini)",
        "purchases_sources": purchases_sources,
        "purchases_breakdown": purchases_breakdown,
        "purchases_counts": purchases_counts,

        "returns_outwards": returns_outwards,
        "returns_outwards_source": returns_outwards_source,
        "returns_outwards_count": returns_outwards_count,

        "carriage_inwards": carriage_inwards,
        "carriage_inwards_source": "manager_expenses(Approved)",
        "carriage_inwards_breakdown": carriage_inwards_breakdown,
        "carriage_inwards_count": carriage_inwards_count,

        "goods_drawn": goods_drawn,
        "goods_drawn_source": "private_ledger_entries(posted goods_drawn)",
        "goods_drawn_breakdown": goods_drawn_breakdown,
        "goods_drawn_count": goods_drawn_count,

        "net_purchases": net_purchases,
        "cost_goods_available": cost_goods_available,

        "closing_stock": closing_stock,
        "closing_stock_year": closing_year,
        "closing_stock_source": closing_source,
        "closing_stock_breakdown_managers": closing_stock_breakdown,

        "cost_goods_sold": cost_goods_sold,
        "cost_goods_sold_legacy": cost_goods_sold_legacy,
        "cogs_source": "inventory_products_outflow",
        "cogs_count": cogs_info["count"],
        "cogs_by_source": cogs_info["by_source"],
        "cogs_missing_cost_count": cogs_info["missing_cost_count"],
        "cogs_missing_cost_examples": cogs_info["missing_cost_examples"],

        "wages": wages,
        "wages_source": wages_source,
        "wages_hr_total": wages_hr_total,
        "wages_count": wages_count,

        "income_info": {"totals": income_sums, "count": income_count},

        "cost_of_sales": cost_of_sales,
        "gross_profit": gross_profit,

        "discount_received": discount_received,
        "investment_income": investment_income,
        "other_incomes": other_incomes,

        "manager_opex_total": manager_opex_total,
        "manager_opex_all": manager_opex_all,
        "manager_opex_top5": manager_opex_top5,
        "manager_opex_count": manager_opex_count,
        "manager_expense_match": manager_expense_match,
        "manager_expense_match_count": manager_expense_match_count,
        "manager_expense_categories": manager_expense_categories,

        "opex_total": opex_total,
        "opex_sources": opex_sources,
        "opex_breakdown_all": opex_breakdown_all,
        "opex_count": opex_counts,
        "opex_notes": manager_opex_notes + accounting_notes,

        "total_expenses": total_expenses,
        "net_profit": net_profit,

        "computed_checks": {
            "net_purchases_formula": "purchases - returns_outwards - goods_drawn + carriage_inwards",
            "cost_goods_available_formula": "opening_stock + net_purchases",
            "cogs_formula": "inventory outflow totals",
        },

        # For export / compatibility
        "total_revenue": sales,
        "ap_cogs": purchases_ap,
        "stock_total_period": stock_period_total,
        "stock_total_all": stock_all_total,
        "total_cogs": purchases,
        "total_opex": total_expenses,
        "total_dep": 0.0,
        "operating_profit": gross_profit - total_expenses,
        "manual_net": 0.0,
        "adjusted_net_profit": net_profit,
        "revenue_info": {"breakdown": revenue_breakdown},
        "sales_info": sales_info,
        "opex_info": {"manager_categories": manager_opex_all, "manager_total": manager_opex_total},
        "manual_info": {"items": []},

        "todo_notes": missing_sources,
    }


# ------------------ Views / APIs ------------------
@profit_loss_bp.get("")
def profit_loss_page():
    # you can pass branch_id via query later; keep simple now
    default_range = (request.args.get("range") or "all_time").strip().lower() or "all_time"
    years = list(range(datetime.utcnow().year, 2019, -1))

    export_url = request.args.get("export_url")
    if not export_url:
        export_url = "/accounting/profit-loss/export.csv"

    return render_template(
        "accounting/profit_loss.html",
        default_range=default_range,
        year_options=years,
        export_url=export_url,
    )


def _pl_payload() -> Tuple[Dict[str, Any], Tuple[datetime, datetime, str, str, str]]:
    start_dt, end_dt, label, from_iso, to_iso = parse_period_from_args(request.args)
    branch_id = (request.args.get("branch_id") or "").strip() or None
    data = compute_profit_loss(start_dt, end_dt, branch_id=branch_id, debug=False)
    meta = {"label": label, "from": from_iso, "to": to_iso, "branch_id": branch_id}
    return {"ok": True, "meta": meta, "data": data}, (start_dt, end_dt, label, from_iso, to_iso)


@profit_loss_bp.get("/api/summary")
def profit_loss_api_summary():
    payload, _ = _pl_payload()
    d = payload["data"]
    payload["data"] = {
        "net_sales": d.get("net_sales", 0.0),
        "gross_profit": d.get("gross_profit", 0.0),
        "total_expenses": d.get("total_expenses", 0.0),
        "net_profit": d.get("net_profit", 0.0),
    }
    return jsonify(payload)


@profit_loss_bp.get("/api/sales")
def profit_loss_api_sales():
    payload, _ = _pl_payload()
    d = payload["data"]
    payload["data"] = {
        "sales": d.get("sales", 0.0),
        "returns_inwards": d.get("returns_inwards", 0.0),
        "returns_inwards_count": d.get("returns_inwards_count", 0),
        "net_sales": d.get("net_sales", 0.0),
        "sales_info": d.get("sales_info", {}),
    }
    return jsonify(payload)


@profit_loss_bp.get("/api/cost-of-sales")
def profit_loss_api_cost_of_sales():
    payload, _ = _pl_payload()
    d = payload["data"]
    payload["data"] = {
        "opening_stock": d.get("opening_stock", 0.0),
        "purchases": d.get("purchases", 0.0),
        "returns_outwards": d.get("returns_outwards", 0.0),
        "carriage_inwards": d.get("carriage_inwards", 0.0),
        "goods_drawn": d.get("goods_drawn", 0.0),
        "net_purchases": d.get("net_purchases", 0.0),
        "cost_goods_available": d.get("cost_goods_available", 0.0),
        "closing_stock": d.get("closing_stock", 0.0),
        "cost_goods_sold": d.get("cost_goods_sold", 0.0),
        "wages": d.get("wages", 0.0),
        "cost_of_sales": d.get("cost_of_sales", 0.0),

        "purchases_breakdown": d.get("purchases_breakdown", []),
        "cogs_by_source": d.get("cogs_by_source", {}),
        "cogs_missing_cost_count": d.get("cogs_missing_cost_count", 0),
    }
    return jsonify(payload)


@profit_loss_bp.get("/api/income")
def profit_loss_api_income():
    payload, _ = _pl_payload()
    d = payload["data"]
    income = (d.get("income_info") or {}).get("totals") or {}
    payload["data"] = {
        "discount_received": income.get("discount_received", 0.0),
        "investment_income": income.get("investment_income", 0.0),
        "other_incomes": income.get("other_incomes", 0.0),
        "income_info": d.get("income_info", {}),
    }
    return jsonify(payload)


@profit_loss_bp.get("/api/opex")
def profit_loss_api_opex():
    payload, _ = _pl_payload()
    d = payload["data"]
    payload["data"] = {
        "manager_total": d.get("manager_opex_total", 0.0),
        "manager_count": d.get("manager_opex_count", 0),
        "manager_categories": d.get("manager_opex_all", []),
        "accounting_total": d.get("opex_sources", {}).get("accounting", 0.0),
        "total_expenses": d.get("total_expenses", 0.0),
    }
    return jsonify(payload)


@profit_loss_bp.get("/export.csv")
def profit_loss_export_csv():
    payload, _ = _pl_payload()
    d = payload["data"]
    meta = payload["meta"]

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["Trading, Profit & Loss Export"])
    writer.writerow(["Period", meta.get("label")])
    writer.writerow(["From", meta.get("from"), "To", meta.get("to")])
    writer.writerow([])

    writer.writerow(["Line Item", "Amount (GHS)"])
    rows = [
        ("Sales", d.get("sales", 0.0)),
        ("Returns Inwards", d.get("returns_inwards", 0.0)),
        ("Net Sales", d.get("net_sales", 0.0)),
        ("Opening Stock", d.get("opening_stock", 0.0)),
        ("Purchases", d.get("purchases", 0.0)),
        ("Returns Outwards", d.get("returns_outwards", 0.0)),
        ("Carriage Inwards", d.get("carriage_inwards", 0.0)),
        ("Goods Drawn", d.get("goods_drawn", 0.0)),
        ("Net Purchases", d.get("net_purchases", 0.0)),
        ("Cost Goods Available", d.get("cost_goods_available", 0.0)),
        ("Closing Stock", d.get("closing_stock", 0.0)),
        ("COGS", d.get("cost_goods_sold", 0.0)),
        ("Wages", d.get("wages", 0.0)),
        ("Cost of Sales", d.get("cost_of_sales", 0.0)),
        ("Gross Profit", d.get("gross_profit", 0.0)),
        ("Discount Received", d.get("discount_received", 0.0)),
        ("Investment Income", d.get("investment_income", 0.0)),
        ("Other Incomes", d.get("other_incomes", 0.0)),
        ("Total Expenses", d.get("total_expenses", 0.0)),
        ("Net Profit", d.get("net_profit", 0.0)),
    ]
    for label, amt in rows:
        writer.writerow([label, f"{_safe_float(amt):.2f}"])

    csv_bytes = output.getvalue().encode("utf-8-sig")
    return Response(
        csv_bytes,
        mimetype="text/csv",
        headers={"Content-Disposition": "attachment; filename=profit_loss_export.csv"},
    )


# ------------------ Backfill helpers (optional) ------------------
def backfill_payments_date_dt(limit: int = 5000) -> int:
    updated = 0
    cur = payments_col.find({"date_dt": {"$exists": False}, "date": {"$exists": True}}).limit(limit)
    for doc in cur:
        raw = doc.get("date")
        if not isinstance(raw, str):
            continue
        try:
            dt = datetime.strptime(raw[:10], "%Y-%m-%d")
        except Exception:
            continue
        payments_col.update_one({"_id": doc["_id"]}, {"$set": {"date_dt": dt}})
        updated += 1
    return updated


def backfill_manager_expenses_date_dt(limit: int = 5000) -> int:
    updated = 0
    cursor = manager_expenses_col.find({"date_dt": {"$exists": False}}).limit(limit)
    for doc in cursor:
        date_part = doc.get("date")
        time_part = (doc.get("time") or "00:00:00").strip() or "00:00:00"
        fallback = doc.get("created_at")
        if not isinstance(fallback, datetime):
            fallback = datetime.utcnow()

        if date_part:
            try:
                date_dt = datetime.strptime(f"{date_part} {time_part}", "%Y-%m-%d %H:%M:%S")
            except Exception:
                date_dt = fallback
        else:
            date_dt = fallback

        manager_expenses_col.update_one({"_id": doc["_id"]}, {"$set": {"date_dt": date_dt}})
        updated += 1
    return updated

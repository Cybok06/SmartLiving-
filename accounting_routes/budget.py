# accounting_routes/budget.py
from __future__ import annotations

from flask import (
    Blueprint, render_template, request,
    redirect, url_for, jsonify, Response
)
from datetime import datetime, date, timedelta
from typing import Any, Dict, List, Optional
from collections import defaultdict
import io
import csv

from db import db

acc_budget = Blueprint(
    "acc_budget",
    __name__,
    template_folder="../templates",
)

# === Collections ===
budgets_col           = db["expense_budgets"]     # our budget store (now holds income + expense)
acc_expenses_col      = db["expenses"]            # main accounting expenses
manager_expenses_col  = db["manager_expenses"]    # manager expenses
payments_col          = db["payments"]            # confirmed inbound cash-ins
manager_deposits_col  = db["manager_deposits"]    # manager deposits into bank/momo/cash


# === Manager allowed categories (for EXPENSE suggestions only) ===
MANAGER_ALLOWED_CATEGORIES = [
    "Vehicle servicing",
    "Transportation",
    "Creditors",
    "Eggs",
    "Delievery",
    "Fuel",
    "SUSU Withdrawal",
    "Marketing (Activations)",
    "Stock (mini)",
    "Airtime",
    "Pre Paid light",
    "Miscellaneous",
    "Salary (Monthly)",
    "Rewards (commisions)",
]

# === Suggested income categories (for INCOME budgets) ===
INCOME_SUGGESTED_CATEGORIES = [
    "Customer Payments",
    "Manager Deposits",
    "Other Income",
]


# ----------------- Helpers -----------------
def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        if v is None or v == "":
            return default
        return float(str(v).replace(",", "").strip())
    except Exception:
        return default


def _parse_date(s: str | None) -> Optional[datetime]:
    """
    Parse YYYY-MM-DD into datetime at 00:00.
    """
    if not s:
        return None
    try:
        d = datetime.strptime(s, "%Y-%m-%d").date()
        return datetime(d.year, d.month, d.day)
    except Exception:
        return None


def _year_default_range(year: int) -> (datetime, datetime):
    """
    Full year range: 1 Jan -> 31 Dec inclusive.
    """
    start = datetime(year, 1, 1)
    end = datetime(year, 12, 31, 23, 59, 59, 999999)
    return start, end


def _normalize_category(raw: Any) -> str:
    c = str(raw or "").strip()
    return c or "Uncategorized"


# ----------------- Budget loading -----------------
def _load_budgets_for_year_kind(year: int, kind: str) -> Dict[str, float]:
    """
    Return mapping {category -> budget_amount} for given year & kind.
    kind: 'income' | 'expense'

    For backward compatibility:
      - 'expense' includes docs with kind='expense' OR kind missing
      - 'income' includes docs with kind='income' only
    """
    if kind == "income":
        query = {"year": year, "kind": "income"}
    else:
        # expense: include legacy docs with no kind
        query = {"year": year, "$or": [{"kind": "expense"}, {"kind": {"$exists": False}}]}

    docs = list(budgets_col.find(query))
    out: Dict[str, float] = defaultdict(float)
    for d in docs:
        cat = _normalize_category(d.get("category"))
        amt = _safe_float(d.get("amount"))
        out[cat] += amt
    return dict(out)


# ----------------- Actual EXPENSE aggregation -----------------
def _aggregate_actual_expenses(start_dt: datetime, end_dt: datetime) -> Dict[str, float]:
    """
    Combine actual expenses from:
      - acc_expenses_col (using 'date' field)
      - manager_expenses_col (Approved only, using 'created_at')
    Returns {category -> total_amount} for the selected time window.
    """
    totals: Dict[str, float] = defaultdict(float)

    # --- Accounting expenses (main) ---
    acc_pipeline = [
        {
            "$match": {
                "date": {"$gte": start_dt, "$lte": end_dt},
            }
        },
        {
            "$group": {
                "_id": "$category",
                "sum_amount": {
                    "$sum": {
                        "$toDouble": {
                            "$ifNull": ["$amount", 0]
                        }
                    }
                },
            }
        },
    ]
    acc_agg = list(acc_expenses_col.aggregate(acc_pipeline))
    for row in acc_agg:
        cat = _normalize_category(row.get("_id"))
        totals[cat] += float(row.get("sum_amount", 0.0) or 0.0)

    # --- Manager expenses (Approved) ---
    mgr_pipeline = [
        {
            "$match": {
                "status": "Approved",
                "created_at": {"$gte": start_dt, "$lte": end_dt},
            }
        },
        {
            "$group": {
                "_id": "$category",
                "sum_amount": {
                    "$sum": {
                        "$toDouble": {
                            "$ifNull": ["$amount", 0]
                        }
                    }
                },
            }
        },
    ]
    mgr_agg = list(manager_expenses_col.aggregate(mgr_pipeline))
    for row in mgr_agg:
        cat = _normalize_category(row.get("_id"))
        totals[cat] += float(row.get("sum_amount", 0.0) or 0.0)

    return dict(totals)


# ----------------- Actual INCOME aggregation -----------------
def _aggregate_actual_income(start_dt: datetime, end_dt: datetime) -> Dict[str, float]:
    """
    Aggregate income (inflows) for the period:
      - Customer Payments (payments_col, status='confirmed', date range)
      - Manager Deposits (manager_deposits_col, status in submitted/approved, created_at range)

    Returns:
      { "Customer Payments": total_payments, "Manager Deposits": total_deposits }
    """
    totals: Dict[str, float] = defaultdict(float)

    # Customer Payments
    pay_pipeline = [
        {
            "$match": {
                "status": "confirmed",
                "date": {"$gte": start_dt, "$lte": end_dt},
            }
        },
        {
            "$group": {
                "_id": None,
                "sum_amount": {
                    "$sum": {
                        "$toDouble": {
                            "$ifNull": ["$amount", 0]
                        }
                    }
                },
            }
        },
    ]
    pay_row = next(payments_col.aggregate(pay_pipeline), None)
    customer_payments = _safe_float(pay_row["sum_amount"]) if pay_row else 0.0
    totals["Customer Payments"] += customer_payments

    # Manager Deposits
    dep_pipeline = [
        {
            "$match": {
                "status": {"$in": ["submitted", "approved"]},
                "created_at": {"$gte": start_dt, "$lte": end_dt},
            }
        },
        {
            "$group": {
                "_id": None,
                "sum_amount": {
                    "$sum": {
                        "$toDouble": {
                            "$ifNull": ["$amount", 0]
                        }
                    }
                },
            }
        },
    ]
    dep_row = next(manager_deposits_col.aggregate(dep_pipeline), None)
    manager_deposits = _safe_float(dep_row["sum_amount"]) if dep_row else 0.0
    totals["Manager Deposits"] += manager_deposits

    return dict(totals)


# ----------------- View-data builders -----------------
def _build_expense_view_data(year: int, start_dt: datetime, end_dt: datetime) -> Dict[str, Any]:
    """
    Build data structure for EXPENSE budgets:
      - rows per category
      - totals
      - chart data (for per-category expense chart)
      - combined categories_all for datalist
    """
    budgets_by_cat = _load_budgets_for_year_kind(year, "expense")
    actual_by_cat  = _aggregate_actual_expenses(start_dt, end_dt)

    # Union of all categories: budgets + actuals + manager allowed list
    all_cats = set(budgets_by_cat.keys()) | set(actual_by_cat.keys()) | set(MANAGER_ALLOWED_CATEGORIES)
    categories_all = sorted(all_cats)

    rows: List[Dict[str, Any]] = []
    total_budget = 0.0
    total_actual = 0.0

    for cat in categories_all:
        b = _safe_float(budgets_by_cat.get(cat, 0.0))
        a = _safe_float(actual_by_cat.get(cat, 0.0))
        v = b - a   # Positive = under budget, negative = overspent

        total_budget += b
        total_actual += a

        if b > 0:
            usage_pct = (a / b) * 100.0
        else:
            usage_pct = 0.0

        rows.append(
            {
                "category": cat,
                "budget_amount": round(b, 2),
                "actual_amount": round(a, 2),
                "variance": round(v, 2),
                "usage_pct": round(usage_pct, 2),
            }
        )

    total_variance = total_budget - total_actual

    # Chart data (only show categories with data)
    chart_labels: List[str] = []
    chart_budget: List[float] = []
    chart_actual: List[float] = []

    for r in rows:
        if (r["budget_amount"] != 0) or (r["actual_amount"] != 0):
            chart_labels.append(r["category"])
            chart_budget.append(r["budget_amount"])
            chart_actual.append(r["actual_amount"])

    chart_data = {
        "labels": chart_labels,
        "budget": chart_budget,
        "actual": chart_actual,
    }

    return {
        "rows": rows,
        "total_budget": round(total_budget, 2),
        "total_actual": round(total_actual, 2),
        "total_variance": round(total_variance, 2),
        "chart_data": chart_data,
        "categories_all": categories_all,
    }


def _build_income_view_data(year: int, start_dt: datetime, end_dt: datetime) -> Dict[str, Any]:
    """
    Build data structure for INCOME budgets:
      - rows per income category
      - totals
      - chart data (if needed later)
      - income_categories for datalist suggestions
    """
    budgets_by_cat = _load_budgets_for_year_kind(year, "income")
    actual_by_cat  = _aggregate_actual_income(start_dt, end_dt)

    # Union of budget categories + actual categories + suggested categories
    all_cats = set(budgets_by_cat.keys()) | set(actual_by_cat.keys()) | set(INCOME_SUGGESTED_CATEGORIES)
    categories_all = sorted(all_cats)

    rows: List[Dict[str, Any]] = []
    total_budget = 0.0
    total_actual = 0.0

    for cat in categories_all:
        b = _safe_float(budgets_by_cat.get(cat, 0.0))
        a = _safe_float(actual_by_cat.get(cat, 0.0))
        v = b - a   # Positive = under-collected vs budget, negative = above target

        total_budget += b
        total_actual += a

        if b > 0:
            usage_pct = (a / b) * 100.0
        else:
            usage_pct = 0.0

        rows.append(
            {
                "category": cat,
                "budget_amount": round(b, 2),
                "actual_amount": round(a, 2),
                "variance": round(v, 2),
                "usage_pct": round(usage_pct, 2),
            }
        )

    total_variance = total_budget - total_actual

    # Chart data (only show categories with data) – optional for now
    chart_labels: List[str] = []
    chart_budget: List[float] = []
    chart_actual: List[float] = []

    for r in rows:
        if (r["budget_amount"] != 0) or (r["actual_amount"] != 0):
            chart_labels.append(r["category"])
            chart_budget.append(r["budget_amount"])
            chart_actual.append(r["actual_amount"])

    chart_data = {
        "labels": chart_labels,
        "budget": chart_budget,
        "actual": chart_actual,
    }

    return {
        "rows": rows,
        "total_budget": round(total_budget, 2),
        "total_actual": round(total_actual, 2),
        "total_variance": round(total_variance, 2),
        "chart_data": chart_data,
        "income_categories": categories_all,
    }


# ----------------- Views -----------------
@acc_budget.route("/budget", methods=["GET"])
def budget_page():
    """
    Budget page:
      - Income + Expense budgets per year
      - Actuals for selected period:
          * Income: payments + manager deposits
          * Expense: accounting + manager expenses
      - Charts + tables
    """
    # Year (for budget)
    year_param = (request.args.get("year") or "").strip()
    try:
        year = int(year_param) if year_param else date.today().year
    except Exception:
        year = date.today().year

    # Date range for actuals
    start_str = (request.args.get("start") or "").strip()
    end_str   = (request.args.get("end") or "").strip()

    start_dt = _parse_date(start_str)
    end_dt   = _parse_date(end_str)

    if not start_dt or not end_dt:
        start_dt, end_dt = _year_default_range(year)
        start_str = start_dt.date().isoformat()
        end_str   = end_dt.date().isoformat()
    else:
        # ensure end is inclusive end-of-day
        end_dt = end_dt.replace(hour=23, minute=59, second=59, microsecond=999999)

    # Build expense + income view data
    expense_view = _build_expense_view_data(year, start_dt, end_dt)
    income_view  = _build_income_view_data(year, start_dt, end_dt)

    # Top-level totals
    total_income_budget = income_view["total_budget"]
    total_expense_budget = expense_view["total_budget"]
    total_income_actual = income_view["total_actual"]
    total_expense_actual = expense_view["total_actual"]

    net_budget = total_income_budget - total_expense_budget
    net_actual = total_income_actual - total_expense_actual

    # Summary chart totals (Income vs Expense)
    # Labels: ["Income", "Expense"]
    # Each dataset must have 2 points (one per label)
    chart_totals = {
        "labels": ["Income", "Expense"],
        "income_budget": [total_income_budget, 0.0],
        "income_actual": [total_income_actual, 0.0],
        "expense_budget": [0.0, total_expense_budget],
        "expense_actual": [0.0, total_expense_actual],
    }

    # For year dropdown (last 5 years + next 2)
    current_year = date.today().year
    year_options = list(range(current_year - 5, current_year + 3))

    return render_template(
        "accounting/budget.html",
        year=year,
        start_str=start_str,
        end_str=end_str,
        year_options=year_options,

        # Expense-side
        expense_rows=expense_view["rows"],
        total_expense_budget=expense_view["total_budget"],
        total_expense_actual=expense_view["total_actual"],
        total_variance=expense_view["total_variance"],   # for backwards compatibility if needed
        chart_exp=expense_view["chart_data"],
        categories_all=sorted(expense_view["categories_all"]),

        # Income-side
        income_rows=income_view["rows"],
        total_income_budget=income_view["total_budget"],
        total_income_actual=income_view["total_actual"],
        income_categories=income_view["income_categories"],

        # Combined net
        net_budget=net_budget,
        net_actual=net_actual,

        # Summary chart (income vs expense)
        chart_totals=chart_totals,
    )


@acc_budget.route("/budget/create", methods=["POST"])
def budget_create():
    """
    Create or update a budget for (year, kind, category).

    Fields:
      - kind      : 'income' | 'expense'  (hidden input)
      - year
      - category  (can be from suggestions or new free text)
      - amount
      - notes (optional)

    If a budget already exists for that triplet, we REPLACE amount (not add).
    """
    kind_raw = (request.form.get("kind") or "expense").strip().lower()
    kind = "income" if kind_raw == "income" else "expense"

    year_str  = (request.form.get("year") or "").strip()
    category  = _normalize_category(request.form.get("category"))
    amount    = _safe_float(request.form.get("amount"))
    notes     = (request.form.get("notes") or "").strip()

    try:
        year = int(year_str)
    except Exception:
        return jsonify(ok=False, message="Invalid year."), 400

    if not category:
        return jsonify(ok=False, message="Category is required."), 400
    if amount <= 0:
        return jsonify(ok=False, message="Amount must be greater than zero."), 400

    now = datetime.utcnow()

    budgets_col.update_one(
        {"year": year, "kind": kind, "category": category},
        {
            "$set": {
                "year": year,
                "kind": kind,
                "category": category,
                "amount": round(amount, 2),
                "notes": notes,
                "updated_at": now,
            },
            "$setOnInsert": {
                "created_at": now,
            },
        },
        upsert=True,
    )

    # Redirect back to budget page for same year
    return redirect(url_for("acc_budget.budget_page", year=year))


@acc_budget.route("/budget/export", methods=["GET"])
def budget_export():
    """
    Export current budget vs actuals (Income + Expense) to CSV (Excel-compatible).
    Query params:
      - year
      - start (YYYY-MM-DD)
      - end   (YYYY-MM-DD)
    """
    year_param = (request.args.get("year") or "").strip()
    try:
        year = int(year_param) if year_param else date.today().year
    except Exception:
        year = date.today().year

    start_str = (request.args.get("start") or "").strip()
    end_str   = (request.args.get("end") or "").strip()

    start_dt = _parse_date(start_str)
    end_dt   = _parse_date(end_str)

    if not start_dt or not end_dt:
        start_dt, end_dt = _year_default_range(year)
        start_str = start_dt.date().isoformat()
        end_str   = end_dt.date().isoformat()
    else:
        end_dt = end_dt.replace(hour=23, minute=59, second=59, microsecond=999999)

    # Build both views
    expense_view = _build_expense_view_data(year, start_dt, end_dt)
    income_view  = _build_income_view_data(year, start_dt, end_dt)

    # Build CSV
    output = io.StringIO()
    writer = csv.writer(output)

    writer.writerow([
        "Year",
        "Start Date",
        "End Date",
        "Type",              # Income or Expense
        "Category",
        "Budget Amount",
        "Actual Amount",
        "Variance (Budget - Actual)",
        "Usage % / Achieved %",
    ])

    # Income rows
    for r in income_view["rows"]:
        writer.writerow([
            year,
            start_str,
            end_str,
            "Income",
            r["category"],
            f"{r['budget_amount']:0.2f}",
            f"{r['actual_amount']:0.2f}",
            f"{r['variance']:0.2f}",
            f"{r['usage_pct']:0.2f}",
        ])

    # Expense rows
    for r in expense_view["rows"]:
        writer.writerow([
            year,
            start_str,
            end_str,
            "Expense",
            r["category"],
            f"{r['budget_amount']:0.2f}",
            f"{r['actual_amount']:0.2f}",
            f"{r['variance']:0.2f}",
            f"{r['usage_pct']:0.2f}",
        ])

    csv_data = output.getvalue()
    filename = f"budget_income_expense_{year}_{start_str}_to_{end_str}.csv"

    return Response(
        csv_data,
        mimetype="text/csv",
        headers={"Content-Disposition": f'attachment; filename=\"{filename}\"'},
    )

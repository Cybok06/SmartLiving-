# accounting_routes/profit_loss.py
from __future__ import annotations

from flask import Blueprint, render_template, request, Response, redirect, url_for, flash, jsonify
from datetime import datetime, date, time
from typing import Any, Dict, List, Optional
from bson import ObjectId
import csv
import io

import pdfkit
import time
from db import db
from services.profit_loss_service import (
    compute_profit_loss,
    parse_period_from_args,
    _manager_expense_match,
    _build_date_range_match,
    MANAGER_ALLOWED_CATEGORIES,
    MANAGER_EXPENSE_EXCLUDED,
)

profit_loss_bp = Blueprint("profit_loss", __name__, template_folder="../templates")

# Collections
ar_receipts_col       = db["ar_receipts"]        # from ar_payments
ap_bills_col          = db["ap_bills"]           # from ap_bills
expenses_col          = db["expenses"]           # accounting_expenses
manager_expenses_col  = db["manager_expenses"]   # manager / branch expenses
fixed_assets_col      = db["fixed_assets"]       # fixed assets register
pl_manual_items_col   = db["pl_manual_items"]    # manual P&L adjustments

# 🔹 MUST match executive_stock_entry.py:
#     stock_col  = db["stock_entries"]
stock_entries_col     = db["stock_entries"]      # Executive Stock Entry records
inventory_col         = db["inventory"]


# ---------- template filters ----------

@profit_loss_bp.app_template_filter("money")
def money_filter(value: Any) -> str:
    """Format numeric values with thousand separators and 2 decimals."""
    try:
        return f"{float(value):,.2f}"
    except Exception:
        return "0.00"


# ---------- helpers ----------

def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        if v is None or v == "":
            return default
        return float(v)
    except Exception:
        return default


def _parse_date(s: str | None) -> Optional[datetime]:
    """
    Parse YYYY-MM-DD into datetime at local midnight.
    """
    if not s:
        return None
    try:
        d = datetime.strptime(s, "%Y-%m-%d").date()
        return datetime.combine(d, time.min)
    except Exception:
        return None


def _default_period() -> tuple[datetime, datetime, str]:
    """
    Default P&L period = current month (1st → today).
    Returns (start_dt, end_dt, label).
    """
    today = date.today()
    start_d = today.replace(day=1)
    start_dt = datetime.combine(start_d, time.min)
    end_dt = datetime.combine(today, time.max)
    label = f"{start_d.strftime('%d %b %Y')} – {today.strftime('%d %b %Y')} (This Month)"
    return start_dt, end_dt, label


def _period_from_query(args) -> tuple[datetime, datetime, str]:
    """
    Build period from ?from=YYYY-MM-DD&to=YYYY-MM-DD query params.
    Falls back to current month if invalid or missing.
    """
    from_str = (args.get("from") or "").strip()
    to_str   = (args.get("to") or "").strip()

    start_dt = _parse_date(from_str)
    end_dt   = _parse_date(to_str)

    if not start_dt or not end_dt:
        return _default_period()

    if end_dt < start_dt:
        # Swap if reversed
        start_dt, end_dt = end_dt, start_dt

    # inclusive end-of-day
    end_dt = end_dt.replace(hour=23, minute=59, second=59, microsecond=999999)

    label = f"{start_dt.date().strftime('%d %b %Y')} – {end_dt.date().strftime('%d %b %Y')}"
    return start_dt, end_dt, label


def _months_between(start_dt: datetime, end_dt: datetime) -> int:
    """
    Approximate whole months between two dates inclusive.
    """
    y1, m1 = start_dt.year, start_dt.month
    y2, m2 = end_dt.year, end_dt.month
    months = (y2 - y1) * 12 + (m2 - m1) + 1
    return max(1, months)


# ---------- core calculators ----------

def _calc_revenue(start_dt: datetime, end_dt: datetime) -> dict:
    """
    Revenue (cash basis) from AR receipts:
      - sum of amount where date_dt is between start_dt and end_dt.
      - grouped by payment method for breakdown.
    """
    q: Dict[str, Any] = {
        "date_dt": {"$gte": start_dt, "$lte": end_dt}
    }

    docs = list(ar_receipts_col.find(q).sort("date_dt", 1))

    total_revenue = 0.0
    by_method: Dict[str, float] = {}

    for d in docs:
        amt = _safe_float(d.get("amount"))
        total_revenue += amt
        method = (d.get("method") or "Other").strip() or "Other"
        by_method[method] = by_method.get(method, 0.0) + amt

    breakdown = [
        {"label": k, "amount": v}
        for k, v in sorted(by_method.items(), key=lambda kv: kv[1], reverse=True)
    ]

    return {
        "total": total_revenue,
        "breakdown": breakdown,
        "count": len(docs),
    }


def _calc_cogs(start_dt: datetime, end_dt: datetime) -> dict:
    """
    COGS from AP Bills ONLY.
    Exec Stock Entry is added on top separately.
    """
    q: Dict[str, Any] = {
        "bill_date_dt": {"$gte": start_dt, "$lte": end_dt}
    }

    docs = list(ap_bills_col.find(q).sort("bill_date_dt", 1))

    total_cogs = 0.0
    by_vendor: Dict[str, float] = {}

    for d in docs:
        status = (d.get("status") or "draft").lower()
        if status == "draft":
            continue

        amt = _safe_float(d.get("amount"))
        total_cogs += amt

        vendor = (d.get("vendor_name") or d.get("vendor") or "Unknown Vendor").strip()
        by_vendor[vendor] = by_vendor.get(vendor, 0.0) + amt

    breakdown = [
        {"label": k, "amount": v}
        for k, v in sorted(by_vendor.items(), key=lambda kv: kv[1], reverse=True)
    ]

    return {
        "total": total_cogs,       # AP bills COGS only
        "breakdown": breakdown,
        "count": len(docs),
    }


def _calc_operating_expenses(start_dt: datetime, end_dt: datetime) -> dict:
    """
    Operating expenses from:
      - expenses (accounting)
      - manager_expenses (Approved only, created_at in range)
    Grouped by category.
    """
    # Accounting expenses
    acc_q: Dict[str, Any] = {
        "date": {"$gte": start_dt, "$lte": end_dt}
    }
    acc_docs = list(expenses_col.find(acc_q).sort("date", 1))

    # Manager expenses (Approved)
    mgr_match, _ = _build_date_range_match(
        manager_expenses_col, start_dt, end_dt, prefer_date_str=True
    )
    mgr_match["status"] = "Approved"
    mgr_docs = list(
        manager_expenses_col.find(
            mgr_match,
            {
                "_id": 1,
                "category": 1,
                "description": 1,
                "amount": 1,
                "status": 1,
            },
        ).sort("date", 1)
    )

    total = 0.0
    by_category: Dict[str, float] = {}

    # Accounting side
    for d in acc_docs:
        amt = _safe_float(d.get("amount"))
        total += amt
        cat = (d.get("category") or "").strip() or "Uncategorized"
        by_category[cat] = by_category.get(cat, 0.0) + amt

    # Manager side
    allowed_set = set(MANAGER_ALLOWED_CATEGORIES)
    excluded_set = MANAGER_EXPENSE_EXCLUDED

    for d in mgr_docs:
        amt = _safe_float(d.get("amount"))
        total += amt
        cat = (d.get("category") or "").strip()
        if not cat:
            cat = "Miscellaneous"
        if cat.lower() in excluded_set:
            continue
        category = cat if cat in allowed_set else "Miscellaneous"
        by_category[category] = by_category.get(category, 0.0) + amt

    breakdown = [
        {"label": k, "amount": v}
        for k, v in sorted(by_category.items(), key=lambda kv: kv[1], reverse=True)
    ]

    return {
        "total": total,
        "breakdown": breakdown,
        "acc_count": len(acc_docs),
        "mgr_count": len(mgr_docs),
    }


def _inventory_closing_value() -> float:
    """
    Current inventory valuation using qty * cost_price.
    Falls back to 0 if cost_price is missing.
    """
    total = 0.0
    for item in inventory_col.find({}, {"qty": 1, "cost_price": 1}):
        try:
            qty = float(item.get("qty") or 0)
            cost = float(item.get("cost_price") or 0)
            total += qty * cost
        except Exception:
            continue
    return total


def _build_expense_category_map(start_dt: datetime, end_dt: datetime) -> Dict[str, float]:
    """
    Aggregate operating expenses by category, including:
      - accounting expenses
      - approved manager expenses
      - prepayment amortization
      - accruals incurred
    """
    totals: Dict[str, float] = {}

    # Accounting expenses
    acc_docs = expenses_col.find(
        {"date": {"$gte": start_dt, "$lte": end_dt}},
        {"amount": 1, "category": 1},
    )
    for d in acc_docs:
        amt = _safe_float(d.get("amount"))
        cat = (d.get("category") or "Uncategorized").strip() or "Uncategorized"
        totals[cat] = totals.get(cat, 0.0) + amt

    # Manager expenses
    mgr_docs = manager_expenses_col.find(
        {"status": "Approved", "created_at": {"$gte": start_dt, "$lte": end_dt}},
        {"amount": 1, "category": 1},
    )
    for d in mgr_docs:
        amt = _safe_float(d.get("amount"))
        cat = (d.get("category") or "Uncategorized").strip() or "Uncategorized"
        totals[cat] = totals.get(cat, 0.0) + amt

    # Prepayment amortization
    for cat, amt in prepayment_amortization_for_period(start_dt, end_dt).items():
        totals[cat] = totals.get(cat, 0.0) + amt

    # Accruals incurred
    for cat, amt in accruals_for_period(start_dt, end_dt).items():
        totals[cat] = totals.get(cat, 0.0) + amt

    return totals


def _monthly_dep_amount(asset: Dict[str, Any]) -> float:
    """
    Simple depreciation logic:
      - Straight Line: cost / (useful_life_years * 12)
      - DB: 2 * SL rate * remaining NBV
    """
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

    if dep > nbv:
        dep = nbv
    return dep


def _calc_depreciation(start_dt: datetime, end_dt: datetime) -> dict:
    """
    Approximate depreciation for the period:
      - Get monthly depreciation for all active/fd assets.
      - Multiply by number of months in period.
    """
    docs = list(
        fixed_assets_col.find({
            "status": {"$in": ["Active", "Fully Depreciated"]}
        })
    )

    monthly_total = 0.0
    for d in docs:
        monthly_total += _monthly_dep_amount(d)

    months = _months_between(start_dt, end_dt)
    total_dep = monthly_total * months

    return {
        "total": total_dep,
        "monthly_estimate": monthly_total,
        "months": months,
        "asset_count": len(docs),
    }


def _calc_manual_adjustments(start_dt: datetime, end_dt: datetime) -> dict:
    """
    Manual adjustments stored in pl_manual_items:
      - Each item is attached to a specific period (from_date, to_date).
      - We match exactly on the same from/to dates as current period.
      - kind: 'income' or 'expense'
    """
    from_iso = start_dt.date().isoformat()
    to_iso   = end_dt.date().isoformat()

    q = {
        "from_date": from_iso,
        "to_date": to_iso,
        "active": True,
    }

    docs = list(pl_manual_items_col.find(q).sort("created_at", 1))

    total_income = 0.0
    total_expense = 0.0
    items: List[Dict[str, Any]] = []

    for d in docs:
        amt = _safe_float(d.get("amount"))
        kind = (d.get("kind") or "income").lower()
        label = (d.get("label") or "").strip() or "Adjustment"
        notes = d.get("notes") or ""

        if kind == "expense":
            total_expense += amt
        else:
            kind = "income"
            total_income += amt

        items.append(
            {
                "id": str(d.get("_id")) if isinstance(d.get("_id"), ObjectId) else "",
                "label": label,
                "kind": kind,
                "amount": amt,
                "notes": notes,
            }
        )

    net = total_income - total_expense

    return {
        "items": items,
        "total_income": total_income,
        "total_expense": total_expense,
        "net": net,
    }


def _calc_stock_entries(start_dt: datetime, end_dt: datetime) -> dict:
    """
    Stock purchases recorded via Executive Stock Entry (stock_entries).

    The collection stores documents like:
      {
        "name": "Bel Cola and Drinks",
        "description": "...",
        "quantity": 20.0,
        "unit_price": 450.0,
        "total_cost": 9000.0,
        "purchased_at": <datetime>,
        ...
      }

    We compute:
      - period_total: sum(total_cost) within the selected P&L period
      - all_total:    sum(total_cost) for ALL documents (for display check)
    """
    # --- Period stock (for P&L COGS) ---
    period_q: Dict[str, Any] = {
        "purchased_at": {"$gte": start_dt, "$lte": end_dt}
    }

    period_docs = list(
        stock_entries_col.find(
            period_q,
            {"total_cost": 1, "quantity": 1, "unit_price": 1}
        )
    )

    period_total = 0.0
    for d in period_docs:
        qty = _safe_float(d.get("quantity"))
        unit_price = _safe_float(d.get("unit_price"))
        total_cost = d.get("total_cost")
        if total_cost is None:
            total_cost = qty * unit_price
        period_total += _safe_float(total_cost)

    # --- ALL-TIME stock (so you can SEE everything you've entered) ---
    all_docs = list(
        stock_entries_col.find(
            {},
            {"total_cost": 1, "quantity": 1, "unit_price": 1}
        )
    )

    all_total = 0.0
    for d in all_docs:
        qty = _safe_float(d.get("quantity"))
        unit_price = _safe_float(d.get("unit_price"))
        total_cost = d.get("total_cost")
        if total_cost is None:
            total_cost = qty * unit_price
        all_total += _safe_float(total_cost)

    return {
        "period_total": period_total,
        "period_count": len(period_docs),
        "all_total": all_total,
        "all_count": len(all_docs),
    }


def _build_pl_context(args) -> dict:
    """
    Central function to compute everything for:
      - main view
      - CSV export
      - PDF export
    """
    start_dt, end_dt, period_label = _period_from_query(args)

    # Core pieces
    revenue_info = _calc_revenue(start_dt, end_dt)
    cogs_info    = _calc_cogs(start_dt, end_dt)           # AP bills (purchases)
    dep_info     = _calc_depreciation(start_dt, end_dt)
    manual_info  = _calc_manual_adjustments(start_dt, end_dt)
    stock_info   = _calc_stock_entries(start_dt, end_dt)  # Exec stock (informational)

    # Sales
    sales = revenue_info["total"]
    returns_inwards = 0.0
    net_sales = sales - returns_inwards

    # Purchases
    purchases = cogs_info["total"]
    returns_outwards = 0.0
    goods_drawn = goods_drawn_total(start_dt, end_dt)

    expense_map = _build_expense_category_map(start_dt, end_dt)

    def _sum_by_keywords(keywords: Iterable[str]) -> float:
        total = 0.0
        for k, v in expense_map.items():
            k_lc = k.lower()
            if any(kw in k_lc for kw in keywords):
                total += v
        return total

    carriage_inwards = _sum_by_keywords(["carriage in", "inwards"])
    wages = _sum_by_keywords(["wages", "direct labor"])

    net_purchases = purchases - returns_outwards - goods_drawn + carriage_inwards

    opening_stock = 0.0
    try:
        opening_stock = float(args.get("opening_stock") or 0)
    except Exception:
        opening_stock = 0.0

    closing_stock = _inventory_closing_value()
    cost_goods_available = opening_stock + net_purchases
    cost_goods_sold = cost_goods_available - closing_stock
    cost_of_sales = cost_goods_sold + wages

    gross_profit = net_sales - cost_of_sales

    # Other incomes (manual income folded here)
    discount_received = 0.0
    investment_income = 0.0
    other_incomes = manual_info["total_income"]

    # Operating expenses mapping
    rent_rates = _sum_by_keywords(["rent", "rates"])
    insurance = _sum_by_keywords(["insurance"])
    salaries = _sum_by_keywords(["salary", "salaries"])
    motor_expenses = _sum_by_keywords(["motor", "vehicle", "transport", "fuel"])
    discount_allowed = _sum_by_keywords(["discount allowed"])
    carriage_outwards = _sum_by_keywords(["carriage out", "delivery", "delievery", "outwards"])
    bad_debts = _sum_by_keywords(["bad debt"])

    categorized = (
        rent_rates
        + insurance
        + salaries
        + motor_expenses
        + discount_allowed
        + carriage_outwards
        + bad_debts
        + wages
        + carriage_inwards
    )
    other_operating = max(sum(expense_map.values()) - categorized, 0.0) + manual_info["total_expense"]

    depreciation = dep_info["total"]
    total_expenses = (
        rent_rates
        + insurance
        + salaries
        + motor_expenses
        + discount_allowed
        + carriage_outwards
        + bad_debts
        + depreciation
        + other_operating
    )

    net_profit = (gross_profit + discount_received + investment_income + other_incomes) - total_expenses

    from_str = start_dt.date().isoformat()
    to_str   = end_dt.date().isoformat()

    context = {
        "period_label": period_label,
        "from_str": from_str,
        "to_str": to_str,

        "sales": sales,
        "returns_inwards": returns_inwards,
        "net_sales": net_sales,
        "opening_stock": opening_stock,
        "purchases": purchases,
        "returns_outwards": returns_outwards,
        "carriage_inwards": carriage_inwards,
        "goods_drawn": goods_drawn,
        "net_purchases": net_purchases,
        "cost_goods_available": cost_goods_available,
        "closing_stock": closing_stock,
        "cost_goods_sold": cost_goods_sold,
        "wages": wages,
        "cost_of_sales": cost_of_sales,
        "gross_profit": gross_profit,

        "discount_received": discount_received,
        "investment_income": investment_income,
        "other_incomes": other_incomes,

        "rent_rates": rent_rates,
        "insurance": insurance,
        "salaries": salaries,
        "motor_expenses": motor_expenses,
        "discount_allowed": discount_allowed,
        "carriage_outwards": carriage_outwards,
        "bad_debts": bad_debts,
        "depreciation": depreciation,
        "other_operating": other_operating,
        "total_expenses": total_expenses,
        "net_profit": net_profit,

        "revenue_info": revenue_info,
        "dep_info": dep_info,
        "manual_info": manual_info,
        "stock_info": stock_info,
    }
    return context



# ---------- cache helpers ----------

_SECTION_CACHE: Dict[str, Dict[str, Any]] = {}
_COMPUTE_CACHE: Dict[str, Dict[str, Any]] = {}
CACHE_TTL_SECONDS = 60


def _build_cache_key(section: str) -> str:
    params = request.query_string.decode("utf-8", "ignore")
    return f"{section}|{params}"


def _cache_get(section: str) -> Optional[Dict[str, Any]]:
    key = _build_cache_key(section)
    entry = _SECTION_CACHE.get(key)
    if entry and (time.time() - entry["ts"]) < CACHE_TTL_SECONDS:
        return entry["data"]
    _SECTION_CACHE.pop(key, None)
    return None


def _cache_set(section: str, payload: Dict[str, Any]) -> None:
    key = _build_cache_key(section)
    _SECTION_CACHE[key] = {"ts": time.time(), "data": payload}


def _compute_cache_key() -> str:
    params = request.query_string.decode("utf-8", "ignore")
    return f"compute|{params}"


def _compute_cache_get(start_dt: datetime, end_dt: datetime, branch_id: Optional[str], debug: bool) -> Dict[str, Any]:
    if debug:
        return compute_profit_loss(start_dt, end_dt, branch_id=branch_id, debug=True)
    key = _compute_cache_key()
    entry = _COMPUTE_CACHE.get(key)
    if entry and (time.time() - entry["ts"]) < CACHE_TTL_SECONDS:
        return entry["data"]
    data = compute_profit_loss(start_dt, end_dt, branch_id=branch_id)
    _COMPUTE_CACHE[key] = {"ts": time.time(), "data": data}
    return data

def _parse_section_period():
    start_dt, end_dt, period_label, from_str, to_str = parse_period_from_args(request.args)
    branch_id = (request.args.get("branch_id") or request.args.get("branch") or "").strip() or None
    debug_sources = (request.args.get("debug_sources") or "").strip().lower() in ("1", "true", "yes")
    return start_dt, end_dt, period_label, from_str, to_str, branch_id, debug_sources


def _build_section_response(
    section: str,
    payload: Dict[str, Any],
    period_label: str,
    from_str: str,
    to_str: str,
) -> Dict[str, Any]:
    return {
        "ok": True,
        "section": section,
        "period_label": period_label,
        "from_str": from_str,
        "to_str": to_str,
        "data": payload,
    }


# ---------- routes ----------

@profit_loss_bp.get("/profit-loss")
def profit_loss():
    """
    Instant shell page for Profit & Loss.
    """
    year_options = list(range(datetime.utcnow().year, datetime.utcnow().year - 6, -1))
    export_url = url_for("profit_loss.export_pl_csv")
    return render_template(
        "accounting/profit_loss.html",
        default_range="all_time",
        year_options=year_options,
        export_url=export_url,
    )


@profit_loss_bp.get("/profit-loss-legacy")
def profit_loss_legacy():
    """
    Legacy server-rendered Profit & Loss for backward compatibility.
    """
    start_dt, end_dt, period_label, from_str, to_str, branch_id, debug_sources = _parse_section_period()
    year_raw = (request.args.get("year") or "").strip()
    all_time = (request.args.get("all_time") or "").strip().lower() in ("1", "true", "yes")
    ctx = compute_profit_loss(start_dt, end_dt, branch_id=branch_id, debug=debug_sources)
    debug_sources = (request.args.get("debug_sources") or "").strip().lower() in ("1", "true", "yes")
    year_options = list(range(datetime.utcnow().year, datetime.utcnow().year - 6, -1))
    export_args = request.args.to_dict(flat=True)
    export_url = url_for("profit_loss.export_pl_csv", **export_args)
    requested_range = (request.args.get("range") or "").strip().lower()
    if not requested_range:
        if all_time:
            requested_range = "all_time"
        elif year_raw:
            requested_range = "year"
        elif request.args.get("from") or request.args.get("to"):
            requested_range = "custom"
        else:
            requested_range = "year"

    ctx.update({
        "period_label": period_label,
        "from_str": from_str,
        "to_str": to_str,
        "branch_id": branch_id,
        "year": int(year_raw) if year_raw.isdigit() else None,
        "all_time": all_time,
        "year_options": year_options,
        "default_range": requested_range,
        "export_url": export_url,
        "debug_sources": debug_sources,
    })
    return render_template("accounting/profit_loss.html", **ctx)


@profit_loss_bp.post("/profit-loss/manual")
def add_manual_item():
    """
    Add a manual P&L adjustment for the current period.
    Fields:
      - from (YYYY-MM-DD)
      - to   (YYYY-MM-DD)
      - label
      - kind ('income'|'expense')
      - amount
      - notes
    """
    from_str = (request.form.get("from") or "").strip()
    to_str   = (request.form.get("to") or "").strip()
    label    = (request.form.get("label") or "").strip()
    kind     = (request.form.get("kind") or "income").strip().lower()
    amount_s = (request.form.get("amount") or "").strip()
    notes    = (request.form.get("notes") or "").strip()

    if kind not in ("income", "expense"):
        kind = "income"

    if not from_str or not to_str or not amount_s:
        flash("From, To and Amount are required for manual adjustment.", "warning")
        return redirect(url_for("profit_loss.profit_loss", **{"from": from_str, "to": to_str}))

    amount = _safe_float(amount_s, 0.0)
    if amount <= 0:
        flash("Amount must be greater than zero.", "warning")
        return redirect(url_for("profit_loss.profit_loss", **{"from": from_str, "to": to_str}))

    now = datetime.utcnow()

    doc = {
        "from_date": from_str,
        "to_date": to_str,
        "label": label or "Adjustment",
        "kind": kind,
        "amount": amount,
        "notes": notes,
        "active": True,
        "created_at": now,
        "updated_at": now,
    }

    pl_manual_items_col.insert_one(doc)
    flash("Manual adjustment added.", "success")

    return redirect(url_for("profit_loss.profit_loss", **{"from": from_str, "to": to_str}))


@profit_loss_bp.get("/profit-loss/export/csv")
def export_pl_csv():
    """
    Export the current P&L view as CSV (vertical format).
    """
    start_dt, end_dt, period_label, from_str, to_str, branch_id, _ = _parse_section_period()
    ctx = compute_profit_loss(start_dt, end_dt, branch_id=branch_id)
    ctx.update({
        "period_label": period_label,
        "from_str": from_str,
        "to_str": to_str,
        "branch_id": branch_id,
    })

    output = io.StringIO()
    writer = csv.writer(output)

    writer.writerow(["Trading, Profit & Loss Statement"])
    writer.writerow([ctx["period_label"]])
    writer.writerow([])

    writer.writerow(["Sales", ctx["sales"]])
    writer.writerow(["Less Returns Inwards", -ctx["returns_inwards"]])
    writer.writerow(["Net Sales", ctx["net_sales"]])
    writer.writerow([])

    writer.writerow(["Less Cost of Sales", ""])
    writer.writerow(["Opening stock", ctx["opening_stock"]])
    writer.writerow(["Add Purchases", ctx["purchases"]])
    writer.writerow(["Less Returns Outwards", -ctx["returns_outwards"]])
    writer.writerow(["Add Carriage Inwards", ctx["carriage_inwards"]])
    writer.writerow(["Less Goods Drawn", -ctx["goods_drawn"]])
    writer.writerow(["Net Purchases", ctx["net_purchases"]])
    writer.writerow(["Cost of goods available for sale", ctx["cost_goods_available"]])
    writer.writerow(["Less Closing stock", -ctx["closing_stock"]])
    writer.writerow(["Cost of goods sold", ctx["cost_goods_sold"]])
    writer.writerow(["Wages", ctx["wages"]])
    writer.writerow(["Cost of sales", ctx["cost_of_sales"]])
    writer.writerow([])

    writer.writerow(["Gross profit (loss)", ctx["gross_profit"]])
    writer.writerow([])

    writer.writerow(["Add: Discount received", ctx["discount_received"]])
    writer.writerow(["Add: Investment income", ctx["investment_income"]])
    writer.writerow(["Add: Other incomes", ctx["other_incomes"]])
    writer.writerow([])

    writer.writerow(["Less Operating Expenses", ""])
    writer.writerow(["Rent and rates", ctx["rent_rates"]])
    writer.writerow(["Insurance", ctx["insurance"]])
    writer.writerow(["Salaries", ctx["salaries"]])
    writer.writerow(["Motor expenses", ctx["motor_expenses"]])
    writer.writerow(["Discount allowed", ctx["discount_allowed"]])
    writer.writerow(["Carriage outwards", ctx["carriage_outwards"]])
    writer.writerow(["Bad debts", ctx["bad_debts"]])
    writer.writerow(["Depreciation on fixed assets", ctx["depreciation"]])
    if ctx.get("other_operating", 0) > 0:
        writer.writerow(["Other operating expenses", ctx["other_operating"]])
    writer.writerow(["Total expenses", ctx["total_expenses"]])
    writer.writerow([])

    writer.writerow(["Net Profit (loss)", ctx["net_profit"]])

    output.seek(0)
    filename = f"profit_loss_{ctx['from_str']}_to_{ctx['to_str']}.csv"

    return Response(
        output.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )



@profit_loss_bp.get("/profit-loss/export/pdf")
def export_pl_pdf():
    """
    Export P&L as PDF using pdfkit (wkhtmltopdf).
    Uses a simplified export template for clean printing.
    """
    start_dt, end_dt, period_label, from_str, to_str, branch_id, _ = _parse_section_period()
    ctx = compute_profit_loss(start_dt, end_dt, branch_id=branch_id)
    ctx.update({
        "period_label": period_label,
        "from_str": from_str,
        "to_str": to_str,
        "branch_id": branch_id,
    })

    html = render_template("accounting/profit_loss_export.html", **ctx)
    pdf = pdfkit.from_string(html, False)

    filename = f"profit_loss_{ctx['from_str']}_to_{ctx['to_str']}.pdf"
    return Response(
        pdf,
        mimetype="application/pdf",
        headers={"Content-Disposition": f'attachment; filename=\"{filename}\"'},
    )


@profit_loss_bp.get("/profit-loss/api/summary")
def profit_loss_api_summary():
    section = "summary"
    cached = _cache_get(section)
    if cached:
        return jsonify(cached)
    start_dt, end_dt, period_label, from_str, to_str, branch_id, debug_sources = _parse_section_period()
    compute_ctx = _compute_cache_get(start_dt, end_dt, branch_id, debug_sources)
    payload = {
        "net_sales": compute_ctx["net_sales"],
        "gross_profit": compute_ctx["gross_profit"],
        "total_expenses": compute_ctx["total_expenses"],
        "net_profit": compute_ctx["net_profit"],
    }
    response = _build_section_response(section, payload, period_label, from_str, to_str)
    _cache_set(section, response)
    return jsonify(response)


@profit_loss_bp.get("/profit-loss/api/sales")
def profit_loss_api_sales():
    section = "sales"
    cached = _cache_get(section)
    if cached:
        return jsonify(cached)
    start_dt, end_dt, period_label, from_str, to_str, branch_id, debug_sources = _parse_section_period()
    compute_ctx = _compute_cache_get(start_dt, end_dt, branch_id, debug_sources)
    payload = {
        "sales": compute_ctx["sales"],
        "returns_inwards": compute_ctx["returns_inwards"],
        "net_sales": compute_ctx["net_sales"],
        "returns_inwards_count": compute_ctx.get("returns_inwards_count", 0),
    }
    response = _build_section_response(section, payload, period_label, from_str, to_str)
    _cache_set(section, response)
    return jsonify(response)


@profit_loss_bp.get("/profit-loss/api/manager-expenses/reconcile")
def profit_loss_api_manager_expenses_reconcile():
    section = "manager_expenses_reconcile"
    start_dt, end_dt, period_label, from_str, to_str, branch_id, debug_sources = _parse_section_period()
    status_param = (request.args.get("status") or "Approved").title()
    status_filter = status_param if status_param in ("Approved", "Unapproved") else "Approved"
    stats_status = None if status_param == "All" else status_filter

    pnl_match, pnl_notes = _manager_expense_match(
        start_dt, end_dt, branch_id, "manager expenses reconciliation", status="Approved"
    )
    stats_match, stats_notes = _manager_expense_match(
        start_dt, end_dt, branch_id, "manager expenses reconciliation stats", status=stats_status
    )

    def _aggregate_total(match: Dict[str, Any]) -> float:
        pipeline = [
            {"$match": match},
            {
                "$group": {
                    "_id": None,
                    "total": {
                        "$sum": {
                            "$toDouble": {"$ifNull": ["$amount", 0]}
                        }
                    },
                }
            },
        ]
        result = next(manager_expenses_col.aggregate(pipeline), {})
        return _safe_float(result.get("total"))

    pnl_total = _aggregate_total(dict(pnl_match))
    stats_total = _aggregate_total(dict(stats_match))

    diff_match = {
        "$and": [
            dict(stats_match),
            {"$nor": [dict(pnl_match)]},
        ]
    }
    diff_pipeline = [
        {"$match": diff_match},
        {
            "$project": {
                "_id": {"$toString": "$_id"},
                "date_dt": 1,
                "status": 1,
                "category": 1,
                "amount": {
                    "$toDouble": {"$ifNull": ["$amount", 0]}
                },
            }
        },
        {"$sort": {"amount": -1}},
        {"$limit": 10},
    ]
    diff_records = list(manager_expenses_col.aggregate(diff_pipeline))

    payload = {
        "pnl_total": pnl_total,
        "stats_total": stats_total,
        "status": status_param,
        "difference": stats_total - pnl_total,
        "notes": {"pnl": pnl_notes, "stats": stats_notes},
        "top_differences": diff_records,
    }
    response = _build_section_response(section, payload, period_label, from_str, to_str)
    _cache_set(section, response)
    return jsonify(response)


@profit_loss_bp.get("/profit-loss/api/cost-of-sales")
def profit_loss_api_cost_of_sales():
    section = "cost_of_sales"
    cached = _cache_get(section)
    if cached:
        return jsonify(cached)
    start_dt, end_dt, period_label, from_str, to_str, branch_id, debug_sources = _parse_section_period()
    compute_ctx = _compute_cache_get(start_dt, end_dt, branch_id, debug_sources)
    payload = {
        "opening_stock": compute_ctx["opening_stock"],
        "purchases": compute_ctx["purchases"],
        "returns_outwards": compute_ctx["returns_outwards"],
        "carriage_inwards": compute_ctx["carriage_inwards"],
        "goods_drawn": compute_ctx["goods_drawn"],
        "net_purchases": compute_ctx["net_purchases"],
        "cost_goods_available": compute_ctx["cost_goods_available"],
        "closing_stock": compute_ctx["closing_stock"],
        "cost_goods_sold": compute_ctx["cost_goods_sold"],
        "wages": compute_ctx["wages"],
        "cost_of_sales": compute_ctx["cost_of_sales"],
        "purchases_breakdown": compute_ctx["purchases_breakdown"],
        "purchases_sources": compute_ctx["purchases_sources"],
        "cogs_by_source": compute_ctx.get("cogs_by_source", {}),
        "cogs_count": compute_ctx.get("cogs_count", 0),
    }
    response = _build_section_response(section, payload, period_label, from_str, to_str)
    _cache_set(section, response)
    return jsonify(response)


@profit_loss_bp.get("/profit-loss/api/income")
def profit_loss_api_income():
    section = "income"
    cached = _cache_get(section)
    if cached:
        return jsonify(cached)
    start_dt, end_dt, period_label, from_str, to_str, branch_id, debug_sources = _parse_section_period()
    compute_ctx = _compute_cache_get(start_dt, end_dt, branch_id, debug_sources)
    payload = {
        "discount_received": compute_ctx["discount_received"],
        "investment_income": compute_ctx["investment_income"],
        "other_incomes": compute_ctx["other_incomes"],
        "income_info": compute_ctx["income_info"],
    }
    response = _build_section_response(section, payload, period_label, from_str, to_str)
    _cache_set(section, response)
    return jsonify(response)


@profit_loss_bp.get("/profit-loss/api/opex")
def profit_loss_api_opex():
    section = "opex"
    cached = _cache_get(section)
    if cached:
        return jsonify(cached)
    start_dt, end_dt, period_label, from_str, to_str, branch_id, debug_sources = _parse_section_period()
    compute_ctx = _compute_cache_get(start_dt, end_dt, branch_id, debug_sources)
    payload = {
        "manager_total": compute_ctx["manager_opex_total"],
        "manager_categories": compute_ctx.get("manager_opex_all", []),
        "manager_count": compute_ctx.get("manager_opex_count", 0),
        "total_expenses": compute_ctx["total_expenses"],
        "net_profit": compute_ctx["net_profit"],
        "opex_total": compute_ctx.get("opex_total", 0.0),
        "opex_sources": compute_ctx.get("opex_sources", {}),
        "opex_breakdown_all": compute_ctx.get("opex_breakdown_all", []),
        "opex_count": compute_ctx.get("opex_count", {}),
    }
    response = _build_section_response(section, payload, period_label, from_str, to_str)
    _cache_set(section, response)
    return jsonify(response)

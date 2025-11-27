# accounting_routes/profit_loss.py
from __future__ import annotations

from flask import Blueprint, render_template, request, Response, redirect, url_for, flash
from datetime import datetime, date, time
from typing import Any, Dict, List, Optional
from bson import ObjectId
import csv
import io

import pdfkit
from db import db

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
    mgr_q: Dict[str, Any] = {
        "status": "Approved",
        "created_at": {"$gte": start_dt, "$lte": end_dt},
    }
    mgr_docs = list(
        manager_expenses_col.find(
            mgr_q,
            {
                "_id": 1,
                "category": 1,
                "description": 1,
                "amount": 1,
                "status": 1,
                "created_at": 1,
            },
        ).sort("created_at", 1)
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
    for d in mgr_docs:
        amt = _safe_float(d.get("amount"))
        total += amt
        cat = (d.get("category") or "").strip() or "Uncategorized"
        by_category[cat] = by_category.get(cat, 0.0) + amt

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
    cogs_info    = _calc_cogs(start_dt, end_dt)           # AP bills COGS
    opex_info    = _calc_operating_expenses(start_dt, end_dt)
    dep_info     = _calc_depreciation(start_dt, end_dt)
    manual_info  = _calc_manual_adjustments(start_dt, end_dt)
    stock_info   = _calc_stock_entries(start_dt, end_dt)  # Exec stock

    total_revenue = revenue_info["total"]

    # 🔹 Separate AP-bills COGS and Exec Stock COGS
    ap_cogs           = cogs_info["total"]
    stock_total_period = stock_info["period_total"]   # for THIS period
    stock_total_all    = stock_info["all_total"]      # all-time, for display

    total_cogs    = ap_cogs + stock_total_period     # THIS hits P&L
    gross_profit  = total_revenue - total_cogs

    total_opex    = opex_info["total"]
    total_dep     = dep_info["total"]
    operating_profit = gross_profit - total_opex - total_dep
    net_profit    = operating_profit

    # Manual adjustments
    manual_net = manual_info["net"]
    adjusted_net_profit = net_profit + manual_net

    from_str = start_dt.date().isoformat()
    to_str   = end_dt.date().isoformat()

    context = {
        "period_label": period_label,
        "from_str": from_str,
        "to_str": to_str,

        "total_revenue": total_revenue,
        "total_cogs": total_cogs,                # combined COGS (AP + stock period)
        "ap_cogs": ap_cogs,                      # AP bills only
        "stock_total_period": stock_total_period,
        "stock_total_all": stock_total_all,
        # alias so your current HTML {{ stock_total }} shows ALL-TIME total:
        "stock_total": stock_total_all,

        "gross_profit": gross_profit,
        "total_opex": total_opex,
        "total_dep": total_dep,
        "operating_profit": operating_profit,
        "net_profit": net_profit,

        "revenue_info": revenue_info,
        "cogs_info": cogs_info,
        "opex_info": opex_info,
        "dep_info": dep_info,

        "manual_info": manual_info,
        "manual_net": manual_net,
        "adjusted_net_profit": adjusted_net_profit,

        "stock_info": stock_info,
    }
    return context


# ---------- routes ----------

@profit_loss_bp.get("/profit-loss")
def profit_loss():
    """
    General (non-branch) Management Profit & Loss (Income Statement) view.
    """
    ctx = _build_pl_context(request.args)
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
    Export the current P&L view (including manual adjustments) as CSV (Excel-friendly).
    """
    ctx = _build_pl_context(request.args)

    output = io.StringIO()
    writer = csv.writer(output)

    # Header rows
    writer.writerow(["Profit & Loss Statement"])
    writer.writerow([ctx["period_label"]])
    writer.writerow([])

    # Summary section
    writer.writerow(["Section", "Amount (GHS)"])
    writer.writerow(["Revenue", ctx["total_revenue"]])
    writer.writerow(["Cost of Sales (AP Bills)", -ctx["ap_cogs"]])
    writer.writerow(["Stock Purchases (Exec Stock Entry - Period)", -ctx["stock_total_period"]])
    writer.writerow(["Total Cost of Sales", -ctx["total_cogs"]])
    writer.writerow(["Gross Profit", ctx["gross_profit"]])
    writer.writerow(["Operating Expenses", -ctx["total_opex"]])
    writer.writerow(["Depreciation", -ctx["total_dep"]])
    writer.writerow(["Operating Profit", ctx["operating_profit"]])
    writer.writerow(["Manual Adjustments (Net)", ctx["manual_net"]])
    writer.writerow(["Adjusted Net Profit", ctx["adjusted_net_profit"]])
    writer.writerow([])
    writer.writerow(["Info", "Value"])
    writer.writerow(["Stock Purchases (Exec Stock Entry - All Time)", ctx["stock_total_all"]])
    writer.writerow([])

    # Revenue breakdown
    writer.writerow(["Revenue Breakdown (by Method)"])
    writer.writerow(["Method", "Amount (GHS)"])
    for row in ctx["revenue_info"]["breakdown"]:
        writer.writerow([row["label"], row["amount"]])
    writer.writerow([])

    # Expenses breakdown
    writer.writerow(["Operating Expenses Breakdown (by Category)"])
    writer.writerow(["Category", "Amount (GHS)"])
    for row in ctx["opex_info"]["breakdown"]:
        writer.writerow([row["label"], row["amount"]])
    writer.writerow([])

    # Manual adjustments detail
    writer.writerow(["Manual Adjustments"])
    writer.writerow(["Label", "Type", "Amount (GHS)", "Notes"])
    for row in ctx["manual_info"]["items"]:
        writer.writerow([row["label"], row["kind"], row["amount"], row["notes"]])

    output.seek(0)
    filename = f"profit_loss_{ctx['from_str']}_to_{ctx['to_str']}.csv"

    return Response(
        output.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": f'attachment; filename=\"{filename}\"'},
    )


@profit_loss_bp.get("/profit-loss/export/pdf")
def export_pl_pdf():
    """
    Export P&L as PDF using pdfkit (wkhtmltopdf).
    Uses a simplified export template for clean printing.
    """
    ctx = _build_pl_context(request.args)

    html = render_template("accounting/profit_loss_export.html", **ctx)
    pdf = pdfkit.from_string(html, False)

    filename = f"profit_loss_{ctx['from_str']}_to_{ctx['to_str']}.pdf"
    return Response(
        pdf,
        mimetype="application/pdf",
        headers={"Content-Disposition": f'attachment; filename=\"{filename}\"'},
    )

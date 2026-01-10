# accounting_routes/dashboard.py
from __future__ import annotations

from flask import Blueprint, render_template, request
from datetime import datetime, date, timedelta
from typing import Any, Dict, List, Tuple
from collections import defaultdict

from db import db

acc_dashboard = Blueprint(
    "acc_dashboard",
    __name__,
    template_folder="../templates",
)

# --- Collections (adjust names if yours differ) ---
ar_invoices_col   = db["ar_invoices"]      # invoices to customers
ar_payments_col   = db["ar_payments"]      # customer payments
ap_bills_col      = db["ap_bills"]         # bills from suppliers
expenses_col      = db["expenses"]         # your expense tracker collection
bank_accounts_col = db["bank_accounts"]    # bank & cash accounts (including MoMo)
journals_col      = db["journals"]         # journal headers
fixed_assets_col  = db["fixed_assets"]     # fixed asset register
bank_recon_col    = db["bank_recon_items"] # or "bank_recon" – adjust if needed


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        if v is None or v == "":
            return default
        return float(v)
    except Exception:
        return default


def _get_doc_date(doc: Dict[str, Any], keys: List[str]) -> datetime | None:
    """
    Try multiple date keys and return the first valid datetime.
    You can adjust the priority list depending on your schema.
    """
    for k in keys:
        val = doc.get(k)
        if isinstance(val, datetime):
            return val
        if isinstance(val, date):
            return datetime.combine(val, datetime.min.time())
        if isinstance(val, str):
            try:
                # Accept ISO or YYYY-MM-DD
                return datetime.fromisoformat(val)
            except Exception:
                continue
    return None


def _period_range_from_key(key: str) -> Tuple[datetime, datetime, str]:
    """
    Convert a simple range key into (start_dt, end_dt, human_label).
    end_dt is exclusive.
    """
    today = date.today()
    now = datetime.utcnow()

    if key == "last_30":
        start = now - timedelta(days=30)
        label = "Last 30 days"
    elif key == "this_year":
        start = datetime(today.year, 1, 1)
        label = f"Year to date ({today.year})"
    elif key == "last_90":
        start = now - timedelta(days=90)
        label = "Last 90 days"
    else:
        # default: this month
        start = datetime(today.year, today.month, 1)
        label = "This month"

    end = now + timedelta(seconds=1)
    return start, end, label


def _month_key(dt: datetime) -> str:
    """Return YYYY-MM label used on charts."""
    return dt.strftime("%Y-%m")


@acc_dashboard.route("/dashboard", methods=["GET"])
def accounting_dashboard() -> str:
    """
    Accounting overview dashboard.
    Aggregates key data from AR, AP, expenses, bank, fixed assets, etc.
    """
    range_key = request.args.get("range", "this_month")
    start_dt, end_dt, range_label = _period_range_from_key(range_key)

    # ------------- KPIs INIT -------------
    cash_balance = 0.0
    ar_total = 0.0
    ap_total = 0.0
    ar_overdue_total = 0.0
    unreconciled_count = 0
    draft_journals = 0
    total_expenses_period = 0.0
    net_revenue_period = 0.0  # approximated from invoices
    net_profit_period = 0.0

    # For charts
    rev_by_month: Dict[str, float] = defaultdict(float)
    exp_by_month: Dict[str, float] = defaultdict(float)
    cash_in_by_month: Dict[str, float] = defaultdict(float)
    cash_out_by_month: Dict[str, float] = defaultdict(float)

    # Aging buckets
    ar_aging_buckets = {
        "current": 0.0,
        "1_30": 0.0,
        "31_60": 0.0,
        "61_90": 0.0,
        "90_plus": 0.0,
    }

    ap_due_buckets = {
        "due_today": 0.0,
        "next_7": 0.0,
        "next_30": 0.0,
        "overdue": 0.0,
    }

    # Top customers/suppliers
    customer_outstanding: Dict[str, float] = defaultdict(float)
    supplier_outstanding: Dict[str, float] = defaultdict(float)

    # Recent activity (we will collect from a few sources)
    recent_events: List[Dict[str, Any]] = []

    # --- Bank & cash breakdown (including Mobile Money) ---
    bank_breakdown = {
        "bank": 0.0,
        "mobile_money": 0.0,
        "cash": 0.0,
    }
    bank_accounts_list: List[Dict[str, Any]] = []

    now = datetime.utcnow()
    today = date.today()

    # ------------- CASH & BANK -------------
    try:
        for acc in bank_accounts_col.find({}):
            # Balance: adjust field names for your schema as needed
            bal = _safe_float(
                acc.get("current_balance")
                or acc.get("balance")
                or acc.get("available_balance"),
                0.0,
            )
            cash_balance += bal

            raw_type = (acc.get("type") or acc.get("account_type") or "").lower()
            category = (acc.get("category") or "").lower()
            channel = (acc.get("channel") or acc.get("wallet_type") or "").lower()

            # Try to detect MoMo / mobile wallets
            is_momo = any(
                kw in raw_type
                for kw in ["momo", "mobile money", "wallet", "mtm", "mtn", "airteltigo", "vodafone cash"]
            ) or any(
                kw in category
                for kw in ["momo", "mobile money", "wallet"]
            ) or any(
                kw in channel
                for kw in ["momo", "mobile money", "wallet"]
            ) or bool(acc.get("is_mobile_money") or acc.get("is_momo"))

            # Try to detect cash-on-hand / petty cash
            is_cash = any(
                kw in raw_type for kw in ["cash", "petty"]
            ) or any(
                kw in category for kw in ["cash", "petty"]
            )

            if is_momo:
                type_key = "mobile_money"
                bank_breakdown["mobile_money"] += bal
                type_label = "Mobile Money Wallet"
            elif is_cash:
                type_key = "cash"
                bank_breakdown["cash"] += bal
                type_label = "Cash / Petty Cash"
            else:
                type_key = "bank"
                bank_breakdown["bank"] += bal
                type_label = "Bank Account"

            name = (
                acc.get("display_name")
                or acc.get("name")
                or acc.get("account_name")
                or acc.get("label")
                or "Account"
            )
            number = acc.get("account_number") or acc.get("number") or acc.get("iban") or ""
            provider = (
                acc.get("bank_name")
                or acc.get("provider")
                or acc.get("wallet_provider")
                or acc.get("institution")
                or ""
            )

            bank_accounts_list.append(
                {
                    "name": name,
                    "number": number,
                    "provider": provider,
                    "type_key": type_key,
                    "type_label": type_label,
                    "balance": round(bal, 2),
                }
            )
    except Exception:
        pass

    # ------------- AR (INVOICES) -------------
    try:
        for inv in ar_invoices_col.find({"status": {"$ne": "cancelled"}}):
            total = _safe_float(
                inv.get("outstanding_amount")
                or inv.get("grand_total")
                or inv.get("total")
                or inv.get("amount"),
                0.0,
            )
            if total <= 0:
                continue

            # Outstanding AR total
            ar_total += total

            # Revenue series – invoices IN the selected period
            issue_dt = _get_doc_date(inv, ["issue_date", "invoice_date", "created_at"])
            if issue_dt and start_dt <= issue_dt <= end_dt:
                month_key = _month_key(issue_dt)
                rev_by_month[month_key] += total
                net_revenue_period += total

            # Aging – based on due_date (fallback: issue_date)
            due_dt = _get_doc_date(inv, ["due_date", "invoice_date", "issue_date"])
            if due_dt:
                days = (today - due_dt.date()).days
                if days <= 0:
                    ar_aging_buckets["current"] += total
                elif days <= 30:
                    ar_aging_buckets["1_30"] += total
                elif days <= 60:
                    ar_aging_buckets["31_60"] += total
                elif days <= 90:
                    ar_aging_buckets["61_90"] += total
                else:
                    ar_aging_buckets["90_plus"] += total
                    ar_overdue_total += total

            # Top customers
            cust_name = (
                inv.get("customer_name")
                or inv.get("customer_display_name")
                or "Unknown"
            )
            customer_outstanding[cust_name] += total

            # Activity
            ev_created = _get_doc_date(inv, ["created_at", "issue_date"])
            if ev_created:
                recent_events.append(
                    {
                        "ts": ev_created,
                        "type": "invoice",
                        "label": f"Invoice for {cust_name}",
                        "amount": total,
                        "link": None,
                    }
                )
    except Exception:
        pass

    # ------------- AR PAYMENTS (CASH IN) -------------
    try:
        for pay in ar_payments_col.find({}):
            amt = _safe_float(pay.get("amount") or pay.get("paid_amount"), 0.0)
            if amt <= 0:
                continue

            pay_dt = _get_doc_date(pay, ["payment_date", "created_at"])
            if not pay_dt:
                continue

            month_key = _month_key(pay_dt)
            cash_in_by_month[month_key] += amt

            if start_dt <= pay_dt <= end_dt:
                # You can extend this for period-level cash metrics
                pass

            recent_events.append(
                {
                    "ts": pay_dt,
                    "type": "payment",
                    "label": "Customer payment received",
                    "amount": amt,
                    "link": None,
                }
            )
    except Exception:
        pass

    # ------------- AP BILLS -------------
    try:
        for bill in ap_bills_col.find({"status": {"$ne": "cancelled"}}):
            total = _safe_float(
                bill.get("outstanding_amount")
                or bill.get("total")
                or bill.get("amount"),
                0.0,
            )
            if total <= 0:
                continue

            # Outstanding AP total
            status = (bill.get("status") or "").lower()
            if status in ("unpaid", "open", "partially_paid", ""):
                ap_total += total

                due_dt = _get_doc_date(bill, ["due_date", "bill_date"])
                if due_dt:
                    days_diff = (due_dt.date() - today).days
                    if days_diff < 0:
                        ap_due_buckets["overdue"] += total
                    elif days_diff == 0:
                        ap_due_buckets["due_today"] += total
                    elif days_diff <= 7:
                        ap_due_buckets["next_7"] += total
                    elif days_diff <= 30:
                        ap_due_buckets["next_30"] += total

            # Cash out (if paid) – approximate
            paid_amt = _safe_float(
                bill.get("paid_amount") if status == "paid" else 0.0, 0.0
            )
            if paid_amt > 0:
                bill_dt = _get_doc_date(bill, ["payment_date", "paid_at", "updated_at"])
                if bill_dt:
                    month_key = _month_key(bill_dt)
                    cash_out_by_month[month_key] += paid_amt

            # Top suppliers
            supp_name = bill.get("supplier_name") or bill.get("vendor_name") or "Unknown"
            supplier_outstanding[supp_name] += total

            # Activity
            ev_created = _get_doc_date(bill, ["created_at", "bill_date"])
            if ev_created:
                recent_events.append(
                    {
                        "ts": ev_created,
                        "type": "bill",
                        "label": f"Bill from {supp_name}",
                        "amount": total,
                        "link": None,
                    }
                )
    except Exception:
        pass

    # ------------- EXPENSES (TRACKER) -------------
    try:
        for exp in expenses_col.find({}):
            amt = _safe_float(exp.get("amount"), 0.0)
            if amt <= 0:
                continue

            exp_dt = _get_doc_date(exp, ["date", "expense_date", "created_at"])
            if not exp_dt:
                continue

            month_key = _month_key(exp_dt)
            exp_by_month[month_key] += amt
            cash_out_by_month[month_key] += amt

            if start_dt <= exp_dt <= end_dt:
                total_expenses_period += amt

            recent_events.append(
                {
                    "ts": exp_dt,
                    "type": "expense",
                    "label": exp.get("description") or "Expense recorded",
                    "amount": amt,
                    "link": None,
                }
            )
    except Exception:
        pass

    # ------------- FIXED ASSETS (NET BOOK VALUE) -------------
    net_book_value = 0.0
    try:
        for fa in fixed_assets_col.find({}):
            cost = _safe_float(fa.get("cost"), 0.0)
            acc_dep = _safe_float(
                fa.get("accumulated_depreciation")
                or fa.get("acc_dep")
                or fa.get("depreciation"),
                0.0,
            )
            nbv = max(cost - acc_dep, 0.0)
            net_book_value += nbv
    except Exception:
        pass

    # ------------- BANK RECON (UNRECONCILED ITEMS) -------------
    try:
        unreconciled_count = bank_recon_col.count_documents(
            {"status": {"$in": ["unmatched", "unreconciled", None, ""]}}
        )
    except Exception:
        unreconciled_count = 0

    # ------------- JOURNALS (DRAFTS) -------------
    try:
        draft_journals = journals_col.count_documents(
            {"status": {"$in": ["draft", "pending_review"]}}
        )
    except Exception:
        draft_journals = 0

    # ------------- NET PROFIT (APPROX) -------------
    net_profit_period = net_revenue_period - total_expenses_period

    # ------------- AR RISK PCT -------------
    ar_overdue_pct = 0.0
    if ar_total > 0 and ar_overdue_total > 0:
        ar_overdue_pct = round((ar_overdue_total / ar_total) * 100.0, 1)

    # ------------- TOP CUSTOMERS / SUPPLIERS -------------
    top_customers = sorted(
        [
            {"name": name, "outstanding": amt}
            for name, amt in customer_outstanding.items()
        ],
        key=lambda x: x["outstanding"],
        reverse=True,
    )[:5]

    top_suppliers = sorted(
        [
            {"name": name, "outstanding": amt}
            for name, amt in supplier_outstanding.items()
        ],
        key=lambda x: x["outstanding"],
        reverse=True,
    )[:5]

    # ------------- RECENT ACTIVITY -------------
    recent_events_sorted = sorted(
        recent_events, key=lambda e: e["ts"], reverse=True
    )[:20]

    recent_activity = [
        {
            "type": e["type"],
            "label": e["label"],
            "amount": _safe_float(e.get("amount"), 0.0),
            "ts": e["ts"].isoformat(),
        }
        for e in recent_events_sorted
    ]

    # ------------- BUILD SERIES (LAST 6 MONTHS) -------------
    today_dt = datetime.utcnow()
    months_labels: List[str] = []
    for i in range(5, -1, -1):
        # approximate month stepping – good enough for a visual dashboard
        m = today_dt.replace(day=1) - timedelta(days=30 * i)
        label = m.strftime("%b %Y")
        months_labels.append(label)

    # Map from label back to YYYY-MM key
    key_by_label = {
        label: datetime.strptime(label, "%b %Y").strftime("%Y-%m")
        for label in months_labels
    }

    revenue_series: List[float] = []
    expense_series: List[float] = []
    cash_in_series: List[float] = []
    cash_out_series: List[float] = []

    for label in months_labels:
        key = key_by_label[label]
        revenue_series.append(round(rev_by_month.get(key, 0.0), 2))
        expense_series.append(round(exp_by_month.get(key, 0.0), 2))
        cash_in_series.append(round(cash_in_by_month.get(key, 0.0), 2))
        cash_out_series.append(round(cash_out_by_month.get(key, 0.0), 2))

    dashboard_data: Dict[str, Any] = {
        "range_key": range_key,
        "range_label": range_label,
        "kpis": {
            "cash_balance": round(cash_balance, 2),
            "ar_total": round(ar_total, 2),
            "ap_total": round(ap_total, 2),
            "net_profit": round(net_profit_period, 2),
            "expenses_total": round(total_expenses_period, 2),
            "ar_overdue_pct": ar_overdue_pct,
            "unreconciled_count": int(unreconciled_count),
            "draft_journals": int(draft_journals),
            "net_book_value": round(net_book_value, 2),
        },
        "revenue_expense": {
            "labels": months_labels,
            "revenue": revenue_series,
            "expenses": expense_series,
        },
        "cash_flow": {
            "labels": months_labels,
            "cash_in": cash_in_series,
            "cash_out": cash_out_series,
        },
        "ar_aging": ar_aging_buckets,
        "ap_due": ap_due_buckets,
        "top_customers": top_customers,
        "top_suppliers": top_suppliers,
        "recent_activity": recent_activity,
        "bank_cash": {
            "breakdown": {
                "bank": round(bank_breakdown["bank"], 2),
                "mobile_money": round(bank_breakdown["mobile_money"], 2),
                "cash": round(bank_breakdown["cash"], 2),
            },
            "accounts": bank_accounts_list,
        },
    }

    return render_template(
        "accounting/dashboard.html",
        dashboard_data=dashboard_data,
    )

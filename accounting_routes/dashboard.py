# accounting_routes/dashboard.py
from __future__ import annotations

from flask import Blueprint, render_template, request, session, redirect, url_for
from datetime import datetime, date, timedelta
from typing import Any, Dict, List, Tuple
from collections import defaultdict

from db import db

acc_dashboard = Blueprint(
    "acc_dashboard",
    __name__,
    template_folder="../templates",
)

# --- Collections (aligned to accounting_routes) ---
ar_invoices_col = db["ar_invoices"]
ar_receipts_col = db["ar_receipts"]
ap_bills_col = db["ap_bills"]
expenses_col = db["expenses"]
manager_expenses_col = db["manager_expenses"]

bank_accounts_col = db["bank_accounts"]
payments_col = db["payments"]
tax_col = db["tax_records"]
sbdc_col = db["s_bdc_payment"]
manager_deposits_col = db["manager_deposits"]
withdrawals_col = db["withdrawals"]

journals_col = db["journals"]
fixed_assets_col = db["fixed_assets"]
bank_lines_col = db["bank_statement_lines"]


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        if v is None or v == "":
            return default
        return float(v)
    except Exception:
        return default


def _get_doc_date(doc: Dict[str, Any], keys: List[str]) -> datetime | None:
    for k in keys:
        val = doc.get(k)
        if isinstance(val, datetime):
            return val
        if isinstance(val, date):
            return datetime.combine(val, datetime.min.time())
        if isinstance(val, str):
            try:
                return datetime.fromisoformat(val)
            except Exception:
                try:
                    return datetime.strptime(val, "%Y-%m-%d")
                except Exception:
                    continue
    return None


def _last4(acc_number: str | None) -> str:
    s = str(acc_number or "")
    return s[-4:] if len(s) >= 4 else s


def _sum_confirmed_in(bank_name: str, last4: str) -> float:
    try:
        pipe = [
            {
                "$match": {
                    "bank_name": bank_name,
                    "account_last4": last4,
                    "status": "confirmed",
                }
            },
            {"$group": {"_id": None, "total": {"$sum": "$amount"}}},
        ]
        row = next(payments_col.aggregate(pipe), None)
        return _safe_float(row["total"]) if row else 0.0
    except Exception:
        return 0.0


def _sum_manager_deposits_in(bank_oid) -> float:
    try:
        bank_id_str = str(bank_oid)
        pipe = [
            {"$match": {"bank_account_id": bank_id_str, "status": {"$in": ["submitted", "approved"]}}},
            {"$group": {"_id": None, "total": {"$sum": "$amount"}}},
        ]
        row = next(manager_deposits_col.aggregate(pipe), None)
        return _safe_float(row["total"]) if row else 0.0
    except Exception:
        return 0.0


def _sum_ptax_out(bank_oid) -> float:
    try:
        pipe = [
            {"$match": {"source_bank_id": bank_oid, "type": {"$regex": r"^p[\\s_-]*tax$", "$options": "i"}}},
            {"$group": {"_id": None, "total": {"$sum": "$amount"}}},
        ]
        row = next(tax_col.aggregate(pipe), None)
        return _safe_float(row["total"]) if row else 0.0
    except Exception:
        return 0.0


def _sum_bdc_out(bank_oid) -> float:
    try:
        pipe = [
            {"$match": {"bank_paid_history": {"$exists": True, "$ne": []}}},
            {"$unwind": "$bank_paid_history"},
            {"$match": {"bank_paid_history.bank_id": bank_oid}},
            {"$group": {"_id": None, "total": {"$sum": "$bank_paid_history.amount"}}},
        ]
        row = next(sbdc_col.aggregate(pipe), None)
        return _safe_float(row["total"]) if row else 0.0
    except Exception:
        return 0.0


def _sum_withdrawals_out(bank_oid) -> float:
    try:
        pipe = [
            {"$match": {"account_id": bank_oid}},
            {"$group": {"_id": None, "total": {"$sum": "$amount"}}},
        ]
        row = next(withdrawals_col.aggregate(pipe), None)
        return _safe_float(row["total"]) if row else 0.0
    except Exception:
        return 0.0


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


def _accounting_access_guard() -> bool:
    role = (session.get("role") or "").lower().strip()
    if session.get("accounting_id") or role == "accounting":
        return True
    if session.get("executive_id") or session.get("admin_id"):
        return True
    return False


@acc_dashboard.route("/dashboard", methods=["GET"])
def accounting_dashboard() -> str:
    """
    Accounting overview dashboard.
    Aggregates key data from AR, AP, expenses, bank, fixed assets, etc.
    """
    if not _accounting_access_guard():
        return redirect(url_for("login.login"))
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
        "b0_30": 0.0,
        "b31_60": 0.0,
        "b61_90": 0.0,
        "b90_plus": 0.0,
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

    # ------------- CASH & BANK (match bank_accounts.py) -------------
    try:
        for acc in bank_accounts_col.find({}):
            bank_oid = acc.get("_id")
            opening = _safe_float(acc.get("opening_balance"))

            acc_type = (acc.get("account_type") or "bank").lower().strip()
            if acc_type not in ("bank", "mobile_money", "cash"):
                acc_type = "bank"

            bank_name = acc.get("bank_name") or ""
            raw_acc_no = acc.get("account_no") or acc.get("account_number") or ""
            last4 = _last4(raw_acc_no)

            confirmed_in = _sum_confirmed_in(bank_name, last4)
            manager_in = _sum_manager_deposits_in(bank_oid)
            total_in = confirmed_in + manager_in

            ptax_out = _sum_ptax_out(bank_oid)
            bdc_out = _sum_bdc_out(bank_oid)
            withdraw_out = _sum_withdrawals_out(bank_oid)
            total_out = ptax_out + bdc_out + withdraw_out

            live_balance = opening + total_in - total_out
            cash_balance += live_balance

            if acc_type == "mobile_money":
                type_key = "mobile_money"
                bank_breakdown["mobile_money"] += live_balance
                type_label = "Mobile Money"
            elif acc_type == "cash":
                type_key = "cash"
                bank_breakdown["cash"] += live_balance
                type_label = "Cash"
            else:
                type_key = "bank"
                bank_breakdown["bank"] += live_balance
                type_label = "Bank"

            name = acc.get("account_name") or acc.get("bank_name") or "Account"
            number = raw_acc_no
            provider = acc.get("bank_name") or acc.get("network") or ""

            bank_accounts_list.append(
                {
                    "name": name,
                    "number": number,
                    "provider": provider,
                    "type_key": type_key,
                    "type_label": type_label,
                    "balance": round(live_balance, 2),
                }
            )
    except Exception:
        pass

    # ------------- AR (INVOICES) -------------
    try:
        for inv in ar_invoices_col.find({}):
            status = (inv.get("status") or "draft").lower()
            balance = _safe_float(inv.get("balance"), 0.0)
            amount = _safe_float(inv.get("amount"), 0.0)

            if status == "overdue":
                ar_overdue_total += balance
            if status in ("sent", "part", "overdue"):
                ar_total += balance

            if status in ("sent", "part", "overdue", "paid"):
                issue_dt = inv.get("issue_dt") if isinstance(inv.get("issue_dt"), datetime) else _get_doc_date(inv, ["issue", "issue_date"])
                if issue_dt and start_dt <= issue_dt <= end_dt:
                    month_key = _month_key(issue_dt)
                    rev_by_month[month_key] += amount
                    net_revenue_period += amount

            cust_name = inv.get("customer_name") or inv.get("customer") or "Unknown"
            if balance > 0:
                customer_outstanding[cust_name] += balance

            ev_created = _get_doc_date(inv, ["issue_dt", "created_at"])
            if ev_created:
                recent_events.append(
                    {
                        "ts": ev_created,
                        "type": "invoice",
                        "label": f"Invoice for {cust_name}",
                        "amount": amount,
                        "link": None,
                    }
                )
    except Exception:
        pass

    # ------------- AR PAYMENTS (CASH IN) -------------
    try:
        for pay in ar_receipts_col.find({}):
            amt = _safe_float(pay.get("amount"), 0.0)
            if amt <= 0:
                continue

            pay_dt = pay.get("date_dt") if isinstance(pay.get("date_dt"), datetime) else _get_doc_date(pay, ["date", "created_at"])
            if not pay_dt:
                continue

            month_key = _month_key(pay_dt)
            cash_in_by_month[month_key] += amt

            recent_events.append(
                {
                    "ts": pay_dt,
                    "type": "payment",
                    "label": "Receipt recorded",
                    "amount": amt,
                    "link": None,
                }
            )
    except Exception:
        pass

    # ------------- AP BILLS -------------
    try:
        for bill in ap_bills_col.find({}):
            amount = _safe_float(bill.get("amount"), 0.0)
            paid = _safe_float(bill.get("paid"), 0.0)
            balance = _safe_float(bill.get("balance", amount - paid), 0.0)
            status = (bill.get("status") or "draft").lower()

            if balance > 0:
                ap_total += balance
                due_dt = bill.get("due_date_dt") if isinstance(bill.get("due_date_dt"), datetime) else _get_doc_date(bill, ["due_date"])
                if due_dt:
                    days_diff = (due_dt.date() - today).days
                    if days_diff < 0:
                        ap_due_buckets["overdue"] += balance
                    elif days_diff == 0:
                        ap_due_buckets["due_today"] += balance
                    elif days_diff <= 7:
                        ap_due_buckets["next_7"] += balance
                    elif days_diff <= 30:
                        ap_due_buckets["next_30"] += balance

            # Cash out based on payment history
            hist = bill.get("payment_history") or []
            if hist:
                for h in hist:
                    amt = _safe_float(h.get("amount"), 0.0)
                    if amt <= 0:
                        continue
                    dt = h.get("date")
                    if isinstance(dt, datetime):
                        month_key = _month_key(dt)
                        cash_out_by_month[month_key] += amt
                    elif isinstance(dt, date):
                        month_key = _month_key(datetime.combine(dt, datetime.min.time()))
                        cash_out_by_month[month_key] += amt

            supp_name = bill.get("vendor_name") or bill.get("vendor") or "Unknown"
            if balance > 0:
                supplier_outstanding[supp_name] += balance

            ev_created = bill.get("bill_date_dt") if isinstance(bill.get("bill_date_dt"), datetime) else _get_doc_date(bill, ["bill_date", "created_at"])
            if ev_created:
                recent_events.append(
                    {
                        "ts": ev_created,
                        "type": "bill",
                        "label": f"Bill from {supp_name}",
                        "amount": amount,
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
            exp_dt = _get_doc_date(exp, ["date"])
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

        for exp in manager_expenses_col.find({"status": "Approved"}):
            amt = _safe_float(exp.get("amount"), 0.0)
            if amt <= 0:
                continue
            exp_dt = exp.get("created_at") if isinstance(exp.get("created_at"), datetime) else _get_doc_date(exp, ["created_at"])
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
                    "label": exp.get("description") or "Manager expense approved",
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
            entry_type = (fa.get("entry_type") or "asset").lower()
            if entry_type == "rent":
                continue
            cost = _safe_float(fa.get("cost"), 0.0)
            acc_dep = _safe_float(fa.get("accum_depr"), 0.0)
            nbv = max(cost - acc_dep, 0.0)
            net_book_value += nbv
    except Exception:
        pass

    # ------------- BANK RECON (UNRECONCILED ITEMS) -------------
    try:
        unreconciled_count = bank_lines_col.count_documents(
            {"$or": [{"matched": False}, {"matched": {"$exists": False}}]}
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

    # ------------- AR AGING (MATCH ar_aging.py FIFO LOGIC) -------------
    try:
        as_of = today
        invoices_by_cust: Dict[str, Dict[str, Any]] = {}
        for inv in ar_invoices_col.find({}):
            code = (inv.get("customer") or "").strip() or "UNKNOWN"
            name = (inv.get("customer_name") or code).strip() or code
            amount = _safe_float(inv.get("amount"), 0.0)
            if amount <= 0:
                continue
            raw_due = inv.get("due") or inv.get("due_date")
            due_date = None
            if isinstance(raw_due, datetime):
                due_date = raw_due.date()
            elif isinstance(raw_due, date):
                due_date = raw_due
            elif isinstance(raw_due, str):
                try:
                    due_date = datetime.fromisoformat(raw_due).date()
                except Exception:
                    due_date = as_of
            if not due_date:
                due_date = as_of

            cust_block = invoices_by_cust.setdefault(code, {"name": name, "invoices": []})
            cust_block["invoices"].append({"amount": amount, "due_date": due_date})

        payments_by_cust: Dict[str, float] = {}
        rec_q = {
            "date_dt": {
                "$lte": datetime(as_of.year, as_of.month, as_of.day, 23, 59, 59, 999999)
            }
        }
        for r in ar_receipts_col.find(rec_q):
            cust = (r.get("customer") or "").strip()
            if not cust:
                continue
            paid_val = _safe_float(r.get("allocated", r.get("amount")), 0.0)
            if paid_val <= 0:
                continue
            payments_by_cust[cust] = payments_by_cust.get(cust, 0.0) + paid_val

        ar_aging_buckets = {"b0_30": 0.0, "b31_60": 0.0, "b61_90": 0.0, "b90_plus": 0.0}
        for code, data in invoices_by_cust.items():
            invs = data["invoices"]
            invs.sort(key=lambda x: x["due_date"])
            remaining_pay = payments_by_cust.get(code, 0.0)
            for inv in invs:
                amt = inv["amount"]
                applied = min(remaining_pay, amt)
                remaining_pay -= applied
                outstanding = amt - applied
                if outstanding <= 0:
                    continue
                age_days = (as_of - inv["due_date"]).days
                if age_days < 0:
                    age_days = 0
                if age_days <= 30:
                    ar_aging_buckets["b0_30"] += outstanding
                elif age_days <= 60:
                    ar_aging_buckets["b31_60"] += outstanding
                elif age_days <= 90:
                    ar_aging_buckets["b61_90"] += outstanding
                else:
                    ar_aging_buckets["b90_plus"] += outstanding
    except Exception:
        ar_aging_buckets = ar_aging_buckets

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

from __future__ import annotations

from datetime import datetime, date, timedelta
from typing import Dict, Any, List, Optional
from collections import defaultdict

from flask import (
    Blueprint, render_template, session, redirect,
    url_for, request
)
from bson.objectid import ObjectId

from db import db

executive_susu_bp = Blueprint("executive_susu", __name__)

# Collections
users_col     = db.users
customers_col = db.customers
payments_col  = db.payments


# ---------- Helpers ----------

def _require_executive_or_admin() -> bool:
    """
    Ensure only EXECUTIVE / ADMIN can view this page.
    Returns True if allowed, else False.
    """
    return bool(session.get("executive_id") or session.get("admin_id"))


def _safe_date_from_payment(p: Dict[str, Any]) -> Optional[date]:
    """
    Try to get a date from payment doc:
      - prefer `timestamp` (datetime)
      - fallback to parsing `date` (YYYY-MM-DD)
    """
    ts = p.get("timestamp")
    if isinstance(ts, datetime):
        return ts.date()

    date_str = p.get("date")
    if isinstance(date_str, str):
        try:
            return datetime.strptime(date_str[:10], "%Y-%m-%d").date()
        except Exception:
            return None
    return None


def _normalize_id(v: Any) -> Optional[str]:
    if not v:
        return None
    if isinstance(v, ObjectId):
        return str(v)
    try:
        return str(v)
    except Exception:
        return None


def _classify_susu_withdraw(p: Dict[str, Any]) -> Optional[str]:
    """
    Classify a WITHDRAWAL payment as:
      - "cash"   -> money paid to customer
      - "profit" -> company SUSU profit
      - None     -> not SUSU-related (ignore)
    """
    if p.get("payment_type") != "WITHDRAWAL":
        return None

    method_raw = (p.get("method") or "").strip()
    method_lc = method_raw.lower()
    note_lc = (p.get("note") or "").strip().lower()

    is_cash = False
    is_profit = False

    # --- Cash to customer variants ---
    if method_lc in ("susu withdrawal", "manual", "cash", "withdrawal", "susu cash"):
        is_cash = True

    # --- Profit / deduction variants ---
    if method_lc in ("susu profit", "deduction", "susu deduction"):
        is_profit = True

    # --- Infer from note text for old data ---
    if "susu" in note_lc:
        if "profit" in note_lc or "deduction" in note_lc:
            is_profit = True
        if "withdraw" in note_lc or "cash" in note_lc or "payout" in note_lc:
            is_cash = True

    if is_profit and not is_cash:
        return "profit"
    if is_cash and not is_profit:
        return "cash"
    if is_cash and is_profit:
        # If both signs appear, prioritise profit if strong signals
        if ("profit" in method_lc or "deduction" in method_lc or
                "profit" in note_lc or "deduction" in note_lc):
            return "profit"
        return "cash"

    return None


def _parse_date(s: str) -> Optional[date]:
    if not s:
        return None
    try:
        return datetime.strptime(s[:10], "%Y-%m-%d").date()
    except Exception:
        return None


# ---------- Executive SUSU Overview ----------

@executive_susu_bp.route("/executive/susu")
def executive_susu_dashboard():
    """
    Executive SUSU overview:

    - Global SUSU metrics (all branches, filtered by date range)
    - Branch-level SUSU breakdown
    - Snapshot metrics (Today / This Week / This Month)
    - Customer search (respecting date range + branch filter)
    - Date range filters:
        range = today | week | month | all | custom
        For custom: start_date=YYYY-MM-DD & end_date=YYYY-MM-DD
    """
    if not _require_executive_or_admin():
        return redirect(url_for("login.login"))

    # -------- Query params --------
    branch_filter = (request.args.get("branch") or "all").strip()
    customer_search_term = (request.args.get("search") or "").strip()

    range_key = (request.args.get("range") or "month").lower()
    start_param = (request.args.get("start_date") or "").strip()
    end_param = (request.args.get("end_date") or "").strip()

    # -------- Load managers (for branch mapping) --------
    manager_map: Dict[str, Dict[str, Any]] = {}
    branches: set[str] = set()

    for u in users_col.find({"role": "manager"}, {"_id": 1, "name": 1, "branch": 1}):
        mid_str = str(u["_id"])
        branch = (u.get("branch") or "Unassigned").strip() or "Unassigned"
        manager_map[mid_str] = {
            "name": u.get("name", "Manager"),
            "branch": branch,
        }
        branches.add(branch)

    sorted_branches = sorted(branches)

    # -------- Date setup --------
    today = datetime.utcnow().date()
    start_of_week = today - timedelta(days=today.weekday())  # Monday
    start_of_month = today.replace(day=1)

    # Range for filtering totals / branch / customers
    start_range: Optional[date] = None
    end_range: Optional[date] = None
    filter_label = ""

    if range_key == "today":
        start_range = today
        end_range = today
        filter_label = "Today"
    elif range_key == "week":
        start_range = start_of_week
        end_range = today
        filter_label = "This Week"
    elif range_key == "month":
        start_range = start_of_month
        end_range = today
        filter_label = "This Month"
    elif range_key == "custom":
        sd = _parse_date(start_param)
        ed = _parse_date(end_param)
        if sd and ed and sd <= ed:
            start_range = sd
            end_range = ed
            filter_label = f"{sd.isoformat()} to {ed.isoformat()}"
        else:
            # Fallback to month if invalid
            start_range = start_of_month
            end_range = today
            filter_label = "This Month (invalid custom dates)"
            range_key = "month"
    else:
        # "all" or unknown
        start_range = None
        end_range = None
        filter_label = "All Time"

    def _in_selected_range(d: Optional[date]) -> bool:
        if start_range is None or end_range is None:
            return True  # no filter (all time)
        if not d:
            return False
        return start_range <= d <= end_range

    # -------- Global & branch metrics --------
    global_totals = {
        "total_susu": 0.0,
        "total_withdrawals": 0.0,
        "total_profit": 0.0,
    }

    branch_totals: Dict[str, Dict[str, float]] = defaultdict(
        lambda: {
            "total_susu": 0.0,
            "total_withdrawals": 0.0,
            "total_profit": 0.0,
        }
    )

    # Snapshot metrics (NOT affected by date filter; true picture)
    time_metrics = {
        "today": {"susu": 0.0, "withdraw": 0.0, "profit": 0.0, "net": 0.0},
        "week":  {"susu": 0.0, "withdraw": 0.0, "profit": 0.0, "net": 0.0},
        "month": {"susu": 0.0, "withdraw": 0.0, "profit": 0.0, "net": 0.0},
    }

    today_withdrawals_count = 0  # for snapshot

    # -------- Scan all SUSU-related payments --------
    payments_cursor = payments_col.find({
        "payment_type": {"$in": ["SUSU", "WITHDRAWAL"]}
    })

    for p in payments_cursor:
        p_type = p.get("payment_type")
        amt = float(p.get("amount", 0) or 0)

        # Manager / branch mapping
        mid_norm = _normalize_id(p.get("manager_id"))
        manager_info = manager_map.get(mid_norm, {})
        branch_name = manager_info.get("branch", "Unassigned")

        # Date
        d = _safe_date_from_payment(p)

        # ---------- Snapshot (today/week/month) ----------
        if p_type == "SUSU":
            if d:
                # today
                if d == today:
                    time_metrics["today"]["susu"] += amt
                    time_metrics["today"]["net"]  += amt
                # week
                if d >= start_of_week:
                    time_metrics["week"]["susu"] += amt
                    time_metrics["week"]["net"]  += amt
                # month
                if d >= start_of_month:
                    time_metrics["month"]["susu"] += amt
                    time_metrics["month"]["net"]  += amt

        elif p_type == "WITHDRAWAL":
            kind = _classify_susu_withdraw(p)
            if not kind:
                # Not SUSU-related
                continue

            if d:
                # today
                if d == today:
                    if kind == "profit":
                        time_metrics["today"]["profit"] += amt
                    else:
                        time_metrics["today"]["withdraw"] += amt
                        today_withdrawals_count += 1
                    time_metrics["today"]["net"] -= amt

                # week
                if d >= start_of_week:
                    if kind == "profit":
                        time_metrics["week"]["profit"] += amt
                    else:
                        time_metrics["week"]["withdraw"] += amt
                    time_metrics["week"]["net"] -= amt

                # month
                if d >= start_of_month:
                    if kind == "profit":
                        time_metrics["month"]["profit"] += amt
                    else:
                        time_metrics["month"]["withdraw"] += amt
                    time_metrics["month"]["net"] -= amt

        # ---------- Date-filtered totals (global + branch) ----------
        if not _in_selected_range(d):
            continue  # outside selected range for totals

        if p_type == "SUSU":
            global_totals["total_susu"] += amt
            branch_totals[branch_name]["total_susu"] += amt

        elif p_type == "WITHDRAWAL":
            kind = _classify_susu_withdraw(p)
            if not kind:
                continue

            if kind == "profit":
                global_totals["total_profit"] += amt
                branch_totals[branch_name]["total_profit"] += amt
            else:
                global_totals["total_withdrawals"] += amt
                branch_totals[branch_name]["total_withdrawals"] += amt

    # Compute global available (for selected range)
    global_available = (
        global_totals["total_susu"]
        - global_totals["total_withdrawals"]
        - global_totals["total_profit"]
    )

    # Compute per-branch available & build ordered list
    branch_rows: List[Dict[str, Any]] = []
    for branch_name, bt in branch_totals.items():
        available = bt["total_susu"] - bt["total_withdrawals"] - bt["total_profit"]
        branch_rows.append({
            "branch": branch_name,
            "total_susu": round(bt["total_susu"], 2),
            "total_withdrawals": round(bt["total_withdrawals"], 2),
            "total_profit": round(bt["total_profit"], 2),
            "available": round(available, 2),
        })

    branch_rows.sort(key=lambda x: x["branch"].lower())

    # Selected branch summary (if filter applied)
    selected_branch_stats = None
    if branch_filter != "all":
        for row in branch_rows:
            if row["branch"] == branch_filter:
                selected_branch_stats = row
                break

    # -------- Customer search (by name / phone, date-filtered) --------
    customer_results: List[Dict[str, Any]] = []
    if customer_search_term:
        customer_filter: Dict[str, Any] = {
            "$or": [
                {"name": {"$regex": customer_search_term, "$options": "i"}},
                {"phone_number": {"$regex": customer_search_term, "$options": "i"}},
            ]
        }
        customers_cursor = customers_col.find(
            customer_filter,
            {
                "name": 1,
                "phone_number": 1,
                "location": 1,
                "image_url": 1,
            }
        ).limit(50)

        for cust in customers_cursor:
            cid = cust["_id"]
            name = cust.get("name", "Customer")
            phone = cust.get("phone_number", "N/A")
            location = cust.get("location", "")
            image_url = cust.get("image_url", "")

            payments_for_cust = list(payments_col.find({"customer_id": cid}))

            total_susu = 0.0
            total_withdraw = 0.0
            total_profit = 0.0

            # Last known branch (overall, not limited by date)
            last_branch = "Unknown"

            for p in payments_for_cust:
                p_type = p.get("payment_type")
                amt = float(p.get("amount", 0) or 0)

                mid_norm = _normalize_id(p.get("manager_id"))
                manager_info = manager_map.get(mid_norm, {})
                if manager_info.get("branch"):
                    last_branch = manager_info["branch"]

                d = _safe_date_from_payment(p)
                if not _in_selected_range(d):
                    # Do not count this payment in totals if outside selected period
                    continue

                if p_type == "SUSU":
                    total_susu += amt
                elif p_type == "WITHDRAWAL":
                    kind = _classify_susu_withdraw(p)
                    if not kind:
                        continue
                    if kind == "profit":
                        total_profit += amt
                    else:
                        total_withdraw += amt

            available = total_susu - total_withdraw - total_profit
            if available < 0:
                available = 0.0

            # If branch filter is applied, only show matching customers
            if branch_filter != "all" and last_branch != branch_filter:
                continue

            customer_results.append({
                "id": str(cid),
                "name": name,
                "phone": phone,
                "location": location,
                "image_url": image_url,
                "branch": last_branch,
                "total_susu": round(total_susu, 2),
                "total_withdraw": round(total_withdraw, 2),
                "total_profit": round(total_profit, 2),
                "available": round(available, 2),
            })

    # -------- Summary object for template --------
    summary = {
        "global": {
            "total_susu": round(global_totals["total_susu"], 2),
            "total_withdrawals": round(global_totals["total_withdrawals"], 2),
            "total_profit": round(global_totals["total_profit"], 2),
            "available": round(global_available, 2),
            "filter_label": filter_label,
        },
        "time": {
            "today": {k: round(v, 2) for k, v in time_metrics["today"].items()},
            "week": {k: round(v, 2) for k, v in time_metrics["week"].items()},
            "month": {k: round(v, 2) for k, v in time_metrics["month"].items()},
        },
        "today_withdrawals_count": today_withdrawals_count,
    }

    start_date_str = start_range.isoformat() if start_range else ""
    end_date_str = end_range.isoformat() if end_range else ""

    return render_template(
        "executive_susu.html",
        summary=summary,
        branch_rows=branch_rows,
        branches=sorted_branches,
        branch_filter=branch_filter,
        selected_branch_stats=selected_branch_stats,
        customer_search_term=customer_search_term,
        customer_results=customer_results,
        range_key=range_key,
        start_date=start_date_str,
        end_date=end_date_str,
    )

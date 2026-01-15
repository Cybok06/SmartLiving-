# routes/manager_susu.py
from __future__ import annotations

from datetime import datetime, date, timedelta
from typing import Dict, Any, List, Optional
from collections import defaultdict
import math

from flask import (
    Blueprint, render_template, session, redirect,
    url_for, request, flash, jsonify
)
from bson.objectid import ObjectId

from db import db
from services.activity_audit import audit_action

manager_susu_bp = Blueprint("manager_susu", __name__)

# Collections
users_col     = db.users
customers_col = db.customers
payments_col  = db.payments
expenses_col  = db.manager_expenses   # for auto SUSU Withdrawal expense (customer cash only)


# ---------------- Helpers ----------------

def _require_manager_oid() -> Optional[ObjectId]:
    """Return manager ObjectId if logged in, else None."""
    mid = session.get("manager_id")
    if not mid:
        return None
    try:
        return ObjectId(mid)
    except Exception:
        return None


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


def _is_ajax(req) -> bool:
    return (req.headers.get("X-Requested-With") or "").lower() == "xmlhttprequest"


def _manager_id_match(manager_oid: ObjectId) -> Dict[str, Any]:
    """
    Many old records store manager_id as STRING; new ones may use ObjectId.
    This matcher works for both.
    """
    return {"$in": [manager_oid, str(manager_oid)]}


def _classify_susu_withdraw(p: Dict[str, Any]) -> Optional[str]:
    """
    Classify a WITHDRAWAL payment as:
      - "cash"   -> money paid to customer
      - "profit" -> company SUSU profit
      - None     -> not SUSU-related (ignore for SUSU dashboard)

    This supports BOTH old and new styles:

    NEW:
      method = "SUSU Withdrawal" / "SUSU Profit"

    OLD:
      method = "Manual"  (cash)
      method = "Deduction" + note "SUSU deduction" (profit)
    """
    if p.get("payment_type") != "WITHDRAWAL":
        return None

    method_raw = (p.get("method") or "").strip()
    method_lc = method_raw.lower()
    note_lc = (p.get("note") or "").strip().lower()

    is_cash = False
    is_profit = False

    # --- Cash to customer (various method names) ---
    if method_lc in ("susu withdrawal", "manual", "cash", "withdrawal", "susu cash"):
        is_cash = True

    # --- Company SUSU profit (deduction / profit) ---
    if method_lc in ("susu profit", "deduction", "susu deduction"):
        is_profit = True

    # --- Fallback based on note text for older records ---
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
        # If both signs appear, prioritise profit if any strong signals
        if ("profit" in method_lc or "deduction" in method_lc or
                "profit" in note_lc or "deduction" in note_lc):
            return "profit"
        return "cash"

    return None


def _infer_susu_rate_for_customer(
    customer: Dict[str, Any],
    payments_for_cust: List[Dict[str, Any]]
) -> tuple[Optional[float], List[str]]:
    """
    Infer SUSU daily rate for a customer.

    Priority:
      1) If susu_default_rate is set on the customer and > 0, use it.
      2) Else, derive from SUSU contributions using GCD of amounts.

    Returns: (rate_or_None, logs)
    """
    logs: List[str] = []

    default_rate = customer.get("susu_default_rate")
    if default_rate is not None:
        try:
            rate = float(default_rate)
            if rate > 0:
                logs.append(f"Using stored susu_default_rate: GH₵{rate:.2f}")
                return rate, logs
        except (TypeError, ValueError):
            logs.append("Stored susu_default_rate is invalid; will infer from payments.")

    # Collect contribution amounts (payment_type == "SUSU")
    contrib_amounts: List[float] = []
    for p in payments_for_cust:
        if p.get("payment_type") == "SUSU":
            try:
                amt = float(p.get("amount", 0) or 0)
            except (TypeError, ValueError):
                continue
            if amt > 0:
                contrib_amounts.append(amt)

    if not contrib_amounts:
        logs.append("No SUSU contributions found for this customer; cannot infer rate.")
        return None, logs

    logs.append(f"Found {len(contrib_amounts)} SUSU contributions for rate inference.")

    # Convert to integer pesewas and compute GCD
    int_amounts = [int(round(a * 100)) for a in contrib_amounts if a > 0]
    if not int_amounts:
        logs.append("All SUSU contribution amounts are invalid for GCD; cannot infer rate.")
        return None, logs

    gcd_val = int_amounts[0]
    for v in int_amounts[1:]:
        gcd_val = math.gcd(gcd_val, v)

    if gcd_val <= 0:
        logs.append("Computed GCD is zero; cannot infer rate safely.")
        return None, logs

    rate = gcd_val / 100.0
    logs.append(f"Inferred SUSU daily rate from contributions: GH₵{rate:.2f}")

    return rate, logs


# ---------------- Dashboard ----------------

@manager_susu_bp.route("/manager/susu")
def susu_dashboard():
    """
    Manager SUSU overview page.

    Features:
      - Top metrics:
          * SUSU collections TODAY / THIS WEEK / THIS MONTH
          * Withdrawals to customers TODAY / THIS WEEK / THIS MONTH
          * SUSU profit (company) TODAY / THIS MONTH
          * Net movement today (collections - withdrawals - profit)
          * Count of withdrawals today
          * Active vs Dormant SUSU customers (dormant = 2 weeks no SUSU)
          * Expected daily SUSU & today's performance vs expectation
      - SUSU customers list: contributions, withdrawals (SUSU), profit, balance
      - Search by name / phone.
      - Pagination: 10 customers per page.
      - Tracks stored default SUSU rate (if any) per customer.
    """
    manager_oid = _require_manager_oid()
    if not manager_oid:
        return redirect(url_for("login.login"))

    # -------- Query params: search + pagination --------
    search_term = (request.args.get("search") or "").strip()
    try:
        page = int(request.args.get("page", 1) or 1)
    except ValueError:
        page = 1
    if page < 1:
        page = 1
    per_page = 10

    # Use UTC date to match utcnow() used when saving withdrawals
    today: date = datetime.utcnow().date()
    start_of_week = today - timedelta(days=today.weekday())  # Monday
    start_of_month = today.replace(day=1)
    dormancy_threshold = today - timedelta(days=14)          # 2 weeks for dormant

    # Metrics per period
    metrics: Dict[str, Dict[str, Any]] = {
        "today": {
            "withdraw_total": 0.0,
            "profit_total": 0.0,
            "net_profit": 0.0,
            "susu_inflow": 0.0,
            "withdraw_count": 0,
            "net_movement": 0.0,
        },
        "week": {
            "withdraw_total": 0.0,
            "profit_total": 0.0,
            "net_profit": 0.0,
            "susu_inflow": 0.0,
            "withdraw_count": 0,
        },
        "month": {
            "withdraw_total": 0.0,
            "profit_total": 0.0,
            "net_profit": 0.0,
            "susu_inflow": 0.0,
            "withdraw_count": 0,
        },
    }

    # High-level summary for extra cards
    summary: Dict[str, Any] = {
        "today_susu_collections": 0.0,
        "week_susu_collections": 0.0,
        "month_susu_collections": 0.0,
        "today_withdrawals_count": 0,
        "today_net_movement": 0.0,
        "expected_daily_susu": 0.0,
        "today_collection_percent": 0.0,
        "month_susu_profit": 0.0,
        "active_customers": 0,
        "dormant_customers": 0,
        "total_customers": 0,
    }

    # -------- 1) Find all SUSU customers (by SUSU payments) + inflow metrics --------
    susu_payments_cursor = payments_col.find({
        "manager_id": _manager_id_match(manager_oid),
        "payment_type": "SUSU",
    })

    susu_customer_ids: set[ObjectId] = set()
    susu_payments_map: Dict[ObjectId, List[Dict[str, Any]]] = defaultdict(list)

    for p in susu_payments_cursor:
        cid = p.get("customer_id")
        if not cid:
            continue
        if not isinstance(cid, ObjectId):
            try:
                cid = ObjectId(cid)
            except Exception:
                continue

        susu_customer_ids.add(cid)
        susu_payments_map[cid].append(p)

        # SUSU inflow metrics (collections)
        d = _safe_date_from_payment(p)
        if not d:
            continue
        amt = float(p.get("amount", 0) or 0)

        if d == today:
            metrics["today"]["susu_inflow"] += amt
        if d >= start_of_week:
            metrics["week"]["susu_inflow"] += amt
        if d >= start_of_month:
            metrics["month"]["susu_inflow"] += amt

    # If no SUSU customers at all
    if not susu_customer_ids:
        summary["today_susu_collections"] = metrics["today"]["susu_inflow"]
        summary["week_susu_collections"] = metrics["week"]["susu_inflow"]
        summary["month_susu_collections"] = metrics["month"]["susu_inflow"]
        summary["month_susu_profit"] = metrics["month"]["profit_total"]
        summary["today_withdrawals_count"] = metrics["today"]["withdraw_count"]
        summary["today_net_movement"] = 0.0

        return render_template(
            "manager_susu.html",
            susu_customers=[],
            metrics=metrics,
            summary=summary,
            search_term=search_term,
            page=1,
            total_pages=1,
            total_customers=0,
            start_index=0,
            end_index=0,
            overall_withdrawals=0.0,
            overall_available=0.0,
        )

    # -------- 2) Top metrics: SUSU withdrawals & SUSU profit (today/week/month) --------
    metric_payments_cursor = payments_col.find({
        "manager_id": _manager_id_match(manager_oid),
        "customer_id": {"$in": list(susu_customer_ids)},
        "payment_type": "WITHDRAWAL",
    })

    for p in metric_payments_cursor:
        d = _safe_date_from_payment(p)
        if not d:
            continue

        kind = _classify_susu_withdraw(p)
        if not kind:
            # Non-SUSU withdrawals (e.g., other modules) are ignored
            continue

        amt = float(p.get("amount", 0) or 0)

        def _add_to_bucket(bucket_key: str):
            if kind == "profit":
                metrics[bucket_key]["profit_total"] += amt
            else:
                metrics[bucket_key]["withdraw_total"] += amt

        # Today
        if d == today:
            _add_to_bucket("today")
            if kind == "cash":
                metrics["today"]["withdraw_count"] += 1

        # Week
        if d >= start_of_week:
            _add_to_bucket("week")

        # Month
        if d >= start_of_month:
            _add_to_bucket("month")

    # finalize net_profit (for now same as company SUSU profit)
    for key in ("today", "week", "month"):
        metrics[key]["net_profit"] = metrics[key]["profit_total"]

    # Net movement today = collections - withdrawals - profit
    metrics["today"]["net_movement"] = (
        metrics["today"]["susu_inflow"]
        - metrics["today"]["withdraw_total"]
        - metrics["today"]["profit_total"]
    )

    # Populate summary from metrics for top cards
    summary["today_susu_collections"] = metrics["today"]["susu_inflow"]
    summary["week_susu_collections"] = metrics["week"]["susu_inflow"]
    summary["month_susu_collections"] = metrics["month"]["susu_inflow"]
    summary["today_withdrawals_count"] = metrics["today"]["withdraw_count"]
    summary["month_susu_profit"] = metrics["month"]["profit_total"]
    summary["today_net_movement"] = metrics["today"]["net_movement"]

    # -------- 3) Load customers (with search filters) --------
    customer_filter: Dict[str, Any] = {"_id": {"$in": list(susu_customer_ids)}}
    if search_term:
        customer_filter["$or"] = [
            {"name": {"$regex": search_term, "$options": "i"}},
            {"phone_number": {"$regex": search_term, "$options": "i"}},
        ]

    customers_cursor = customers_col.find(
        customer_filter,
        {
            "name": 1,
            "phone_number": 1,
            "location": 1,
            "agent_id": 1,
            "image_url": 1,
            "susu_default_rate": 1,
            "susu_rate_last": 1,
            "susu_rate_streak": 1,
        }
    )
    customers_list = list(customers_cursor)

    if not customers_list:
        return render_template(
            "manager_susu.html",
            susu_customers=[],
            metrics=metrics,
            summary=summary,
            search_term=search_term,
            page=1,
            total_pages=1,
            total_customers=0,
            start_index=0,
            end_index=0,
            overall_withdrawals=0.0,
            overall_available=0.0,
        )

    # -------- 4) Preload agents --------
    agent_ids_raw = [c.get("agent_id") for c in customers_list if c.get("agent_id")]
    agent_oid_set = set()
    for aid in agent_ids_raw:
        try:
            if isinstance(aid, ObjectId):
                agent_oid_set.add(aid)
            else:
                agent_oid_set.add(ObjectId(aid))
        except Exception:
            continue

    agent_map: Dict[str, str] = {}
    if agent_oid_set:
        for a in users_col.find(
            {"_id": {"$in": list(agent_oid_set)}},
            {"_id": 1, "name": 1}
        ):
            agent_map[str(a["_id"])] = a.get("name", "Agent")

    # -------- 5) Payments for these customers (for balances) --------
    shown_customer_ids = [c["_id"] for c in customers_list]

    all_payments_cursor = payments_col.find({
        "manager_id": _manager_id_match(manager_oid),
        "customer_id": {"$in": shown_customer_ids},
    })

    all_payments_map: Dict[ObjectId, List[Dict[str, Any]]] = defaultdict(list)
    for p in all_payments_cursor:
        cid = p.get("customer_id")
        if not cid:
            continue
        if not isinstance(cid, ObjectId):
            try:
                cid = ObjectId(cid)
            except Exception:
                continue
        all_payments_map[cid].append(p)

    # -------- 6) Build UI list + expected SUSU + active/dormant --------
    susu_customers_ui: List[Dict[str, Any]] = []

    expected_daily_susu = 0.0
    active_customers = 0
    dormant_customers = 0

    for cust in customers_list:
        cid = cust["_id"]
        name = cust.get("name", "Unknown")
        phone = cust.get("phone_number", "N/A")
        location = cust.get("location", "")
        raw_agent_id = cust.get("agent_id")

        agent_name = "Unassigned"
        if raw_agent_id:
            try:
                agent_name = agent_map.get(str(ObjectId(raw_agent_id)), "Unassigned")
            except Exception:
                agent_name = agent_map.get(str(raw_agent_id), "Unassigned")

        payments_for_cust = all_payments_map.get(cid, [])

        total_susu = 0.0
        total_withdraw_to_customer = 0.0
        total_susu_profit = 0.0
        withdrawals_for_ui: List[Dict[str, Any]] = []

        # SUSU contributions
        last_susu_amount = 0.0
        last_susu_date: Optional[date] = None

        susu_payments_for_cust = susu_payments_map.get(cid, [])
        for sp in susu_payments_for_cust:
            amt = float(sp.get("amount", 0) or 0)
            total_susu += amt

            d = _safe_date_from_payment(sp)
            if d and (last_susu_date is None or d > last_susu_date):
                last_susu_date = d

        if susu_payments_for_cust:
            susu_sorted = sorted(
                susu_payments_for_cust,
                key=lambda p: p.get("timestamp") or p.get("date", ""),
                reverse=True
            )
            try:
                last_susu_amount = float(susu_sorted[0].get("amount", 0) or 0)
            except (TypeError, ValueError):
                last_susu_amount = 0.0

        # Withdrawals / profit - support old & new structures
        for p in payments_for_cust:
            kind = _classify_susu_withdraw(p)
            if not kind:
                continue

            amt = float(p.get("amount", 0) or 0)
            if kind == "profit":
                total_susu_profit += amt
            else:
                total_withdraw_to_customer += amt

            withdrawals_for_ui.append(p)

        available_balance = round(total_susu - total_withdraw_to_customer - total_susu_profit, 2)
        if available_balance < 0:
            available_balance = 0.0

        default_rate = cust.get("susu_default_rate")

        # Expected daily SUSU for this customer:
        # prefer stored default_rate, else fall back to last SUSU amount
        expected_rate = 0.0
        try:
            if default_rate is not None:
                expected_rate = float(default_rate) or 0.0
            elif last_susu_amount > 0:
                expected_rate = float(last_susu_amount) or 0.0
        except (TypeError, ValueError):
            expected_rate = 0.0

        if expected_rate > 0:
            expected_daily_susu += expected_rate

        # Active vs Dormant: dormant = no SUSU contribution for >= 14 days
        if last_susu_date is None or last_susu_date < dormancy_threshold:
            dormant_customers += 1
        else:
            active_customers += 1

        susu_customers_ui.append({
            "id": str(cid),
            "name": name,
            "phone": phone,
            "location": location,
            "agent_name": agent_name,
            "image_url": cust.get("image_url", ""),
            "total_susu": round(total_susu, 2),
            "withdraw_to_customer": round(total_withdraw_to_customer, 2),
            "susu_profit": round(total_susu_profit, 2),
            "available_balance": available_balance,
            "withdrawals": withdrawals_for_ui,
            "rate_hint": round(last_susu_amount, 2) if last_susu_amount else "",
            "default_rate": default_rate if default_rate is not None else "",
        })

    susu_customers_ui.sort(key=lambda x: x["name"].lower())

    # -------- 6b) Overall totals (ALL customers, not just current page) --------
    overall_withdrawals = sum(c["withdraw_to_customer"] for c in susu_customers_ui)
    overall_available = sum(c["available_balance"] for c in susu_customers_ui)

    # Fill summary counts & expected / performance
    total_customers = len(susu_customers_ui)
    summary["total_customers"] = total_customers
    summary["active_customers"] = active_customers
    summary["dormant_customers"] = dormant_customers
    summary["expected_daily_susu"] = round(expected_daily_susu, 2)

    if expected_daily_susu > 0:
        summary["today_collection_percent"] = round(
            (metrics["today"]["susu_inflow"] / expected_daily_susu) * 100.0, 1
        )
    else:
        summary["today_collection_percent"] = 0.0

    # -------- 7) Pagination --------
    total_pages = max((total_customers + per_page - 1) // per_page, 1)
    if page > total_pages:
        page = total_pages

    start = (page - 1) * per_page
    end = start + per_page
    paginated_customers = susu_customers_ui[start:end]

    if total_customers == 0:
        start_index = 0
        end_index = 0
    else:
        start_index = start + 1
        end_index = min(end, total_customers)

    return render_template(
        "manager_susu.html",
        susu_customers=paginated_customers,
        metrics=metrics,
        summary=summary,
        search_term=search_term,
        page=page,
        total_pages=total_pages,
        total_customers=total_customers,
        start_index=start_index,
        end_index=end_index,
        overall_withdrawals=overall_withdrawals,
        overall_available=overall_available,
    )


# ---------------- Withdraw (AJAX + preview + normal) ----------------

@manager_susu_bp.route("/manager/susu/<customer_id>/withdraw", methods=["POST"])
@audit_action("susu.withdrawn", "Withdrew SUSU", entity_type="susu", entity_id_from="customer_id")
def susu_withdraw(customer_id):
    """
    SUSU withdrawal flow (supports:
      - preview mode for live calculation (no DB changes),
      - normal save (records payments + expense).

    Preview:
      - request.form['preview'] == "1"
      - returns: { ok: True, preview_summary: { withdraw_amount, company_profit, balance_after, customer_rate } }
      - ignores confirm_called and does NOT insert records.
    """
    manager_oid = _require_manager_oid()
    if not manager_oid:
        if _is_ajax(request):
            return jsonify(ok=False, message="Please log in again."), 401
        return redirect(url_for("login.login"))

    logs: List[str] = []
    is_preview = (request.form.get("preview") == "1")

    try:
        customer_oid = ObjectId(customer_id)
    except Exception:
        msg = "Invalid customer ID."
        if _is_ajax(request):
            # for preview, send ok=False so frontend falls back
            if is_preview:
                return jsonify(ok=False)
            return jsonify(ok=False, message=msg), 400
        flash(msg, "danger")
        return redirect(url_for("manager_susu.susu_dashboard"))

    customer = customers_col.find_one({"_id": customer_oid})
    if not customer:
        msg = "Customer not found."
        if _is_ajax(request):
            if is_preview:
                return jsonify(ok=False)
            return jsonify(ok=False, message=msg), 404
        flash(msg, "danger")
        return redirect(url_for("manager_susu.susu_dashboard"))

    # Step 1: confirm call (NOT required for preview)
    if (not is_preview) and (not request.form.get("confirm_called")):
        msg = "Please confirm you have called the customer before withdrawing."
        if _is_ajax(request):
            return jsonify(ok=False, message=msg), 400
        flash(msg, "warning")
        return redirect(url_for("manager_susu.susu_dashboard"))

    def _to_float(field: str) -> float:
        try:
            return float(request.form.get(field, 0) or 0)
        except (TypeError, ValueError):
            return 0.0

    withdraw_amount = _to_float("withdraw_amount")
    note = (request.form.get("note") or "").strip()
    manual_rate_raw = (request.form.get("manual_rate") or "").strip()
    manual_rate: Optional[float] = None

    if manual_rate_raw:
        try:
            manual_rate = float(manual_rate_raw)
        except (TypeError, ValueError):
            manual_rate = None
        if manual_rate is None or manual_rate <= 0:
            msg = "Customer daily rate must be a positive number."
            if _is_ajax(request):
                if is_preview:
                    return jsonify(ok=False)
                return jsonify(ok=False, message=msg), 400
            flash(msg, "danger")
            return redirect(url_for("manager_susu.susu_dashboard"))

    if withdraw_amount <= 0:
        msg = "Withdraw amount must be greater than 0."
        logs.append(f"Invalid withdraw_amount={withdraw_amount:.2f}.")
        if _is_ajax(request):
            if is_preview:
                # let frontend fallback to simple estimate
                return jsonify(ok=False)
            return jsonify(ok=False, message=msg), 400
        flash(msg, "danger")
        return redirect(url_for("manager_susu.susu_dashboard"))

    logs.append(f"Requested withdrawal: GH₵{withdraw_amount:.2f}")

    # Compute available balance (support old & new manager_id types)
    payments_for_cust = list(payments_col.find({
        "manager_id": _manager_id_match(manager_oid),
        "customer_id": customer_oid,
    }))

    logs.append(f"Loaded {len(payments_for_cust)} total payments for this customer.")

    total_susu = 0.0
    total_withdraw_to_customer = 0.0
    total_susu_profit = 0.0

    for p in payments_for_cust:
        p_type = p.get("payment_type")
        try:
            amt = float(p.get("amount", 0) or 0)
        except (TypeError, ValueError):
            continue

        if p_type == "SUSU":
            total_susu += amt
        elif p_type == "WITHDRAWAL":
            kind = _classify_susu_withdraw(p)
            if not kind:
                continue
            if kind == "profit":
                total_susu_profit += amt
            else:
                total_withdraw_to_customer += amt

    logs.append(f"Total SUSU contributed so far: GH₵{total_susu:.2f}")
    logs.append(f"Total withdrawn to customer so far: GH₵{total_withdraw_to_customer:.2f}")
    logs.append(f"Total SUSU profit taken so far: GH₵{total_susu_profit:.2f}")

    available_balance_before = total_susu - total_withdraw_to_customer - total_susu_profit
    if available_balance_before < 0:
        available_balance_before = 0.0

    logs.append(f"Available SUSU balance before this withdrawal: GH₵{available_balance_before:.2f}")

    # ---- Rate selection (manual override or inferred) ----
    if manual_rate is not None and manual_rate > 0:
        rate = manual_rate
        logs.append(f"Using manager override rate: GHƒ,æ{rate:.2f}")
    else:
        rate, rate_logs = _infer_susu_rate_for_customer(customer, payments_for_cust)
        logs.extend(rate_logs)

    if rate is None or rate <= 0:
        msg = "Unable to determine SUSU daily rate for this customer. Please check their SUSU payments."
        if _is_ajax(request):
            if is_preview:
                # front-end will fallback to rough estimate
                return jsonify(ok=False)
            return jsonify(ok=False, message=msg), 400
        flash(msg, "danger")
        return redirect(url_for("manager_susu.susu_dashboard"))

    # ---- Calculate profit based on withdrawal pages ----
    # 1 page = 30 days/boxes
    page_days = 30.0

    withdraw_days = withdraw_amount / rate
    logs.append(f"Withdrawal amount in days at rate GH₵{rate:.2f}: {withdraw_days:.2f} days")

    # Profit boxes logic:
    #  - 30 days or less     -> 1 box
    #  - crosses 30 days     -> 2 boxes
    #  - crosses 60 days     -> 3 boxes, etc.
    # So boxes = max(1, ceil(withdraw_days / 30))
    raw_boxes = max(1, math.ceil(withdraw_days / page_days))
    logs.append(f"Raw profit boxes computed from withdrawal days: {raw_boxes}")

    profit_boxes = raw_boxes
    profit_amount = profit_boxes * rate
    total_to_deduct = withdraw_amount + profit_amount

    logs.append(f"Initial profit_amount = GH₵{profit_amount:.2f} "
                f"({profit_boxes} boxes × GH₵{rate:.2f}).")
    logs.append(f"Total to deduct (withdrawal + profit) = GH₵{total_to_deduct:.2f}.")

    # If total_to_deduct > available balance, try reduce boxes down to 1.
    if total_to_deduct > available_balance_before + 0.0001:
        logs.append("Total to deduct is more than available balance; trying to reduce profit boxes.")
        while profit_boxes > 1 and (withdraw_amount + profit_boxes * rate) > available_balance_before + 0.0001:
            profit_boxes -= 1
            logs.append(f"Reduced profit boxes to {profit_boxes} due to balance limit.")
        profit_amount = profit_boxes * rate
        total_to_deduct = withdraw_amount + profit_amount
        logs.append(f"After adjustment: profit_amount = GH₵{profit_amount:.2f}, "
                    f"total_to_deduct = GH₵{total_to_deduct:.2f}.")

        # Still not enough even with minimum 1 box
        if total_to_deduct > available_balance_before + 0.0001:
            msg = (
                f"Requested amount plus minimum SUSU profit (GH₵{rate:.2f}) "
                f"is more than available SUSU balance (GH₵{available_balance_before:.2f})."
            )
            logs.append("Even with minimum profit box, balance is insufficient; aborting withdrawal.")
            if _is_ajax(request):
                if is_preview:
                    # let UI fallback to simple estimated preview
                    return jsonify(ok=False)
                return jsonify(ok=False, message=msg), 400
            flash(msg, "danger")
            return redirect(url_for("manager_susu.susu_dashboard"))

    logs.append(f"Final profit boxes to charge: {profit_boxes} "
                f"-> profit_amount: GH₵{profit_amount:.2f}")

    # ---------- Preview mode: NO DB changes, just return calculated summary ----------
    if is_preview and _is_ajax(request):
        balance_after = available_balance_before - total_to_deduct
        if balance_after < 0:
            balance_after = 0.0

        return jsonify(
            ok=True,
            preview_summary={
                "withdraw_amount": round(withdraw_amount, 2),
                "company_profit": round(profit_amount, 2),
                "balance_after": round(balance_after, 2),
                "customer_rate": round(rate, 2),
            },
        )

    # ---------- Record withdrawals (normal save) ----------
    now = datetime.utcnow()
    now_str = now.strftime("%Y-%m-%d")
    time_str = now.strftime("%H:%M:%S")
    agent_id = customer.get("agent_id")

    # 1) Money to customer (SUSU Withdrawal) - store manager_id as STRING
    payments_col.insert_one({
        "manager_id": str(manager_oid),
        "agent_id": agent_id,
        "customer_id": customer_oid,
        "amount": withdraw_amount,
        "payment_type": "WITHDRAWAL",
        "method": "SUSU Withdrawal",
        "note": note or f"SUSU withdrawal (auto-rate GH₵{rate:.2f})",
        "date": now_str,
        "timestamp": now,
    })
    logs.append(f"Recorded SUSU Withdrawal payment of GH₵{withdraw_amount:.2f}.")

    # Auto-create manager expense for SUSU Withdrawal (ONLY money paid to customer)
    expenses_col.insert_one({
        "manager_id": str(manager_oid),
        "category": "SUSU Withdrawal",
        "amount": float(round(withdraw_amount, 2)),
        "description": note or "Auto SUSU withdrawal expense",
        "date": now_str,     # YYYY-MM-DD
        "time": time_str,    # HH:MM:SS
        "status": "Unapproved",
        "created_at": now,
        "updated_at": now,
        "approved_at": None,
        "approved_by": None,
    })
    logs.append("Auto-created manager expense row for SUSU Withdrawal cash paid to customer.")

    # 2) Company SUSU profit (NOT an expense)
    if profit_amount > 0:
        payments_col.insert_one({
            "manager_id": str(manager_oid),
            "agent_id": agent_id,
            "customer_id": customer_oid,
            "amount": float(round(profit_amount, 2)),
            "payment_type": "WITHDRAWAL",
            "method": "SUSU Profit",
            "note": "Auto SUSU profit collection based on pages crossed.",
            "date": now_str,
            "timestamp": now,
        })
        logs.append(f"Recorded SUSU Profit payment of GH₵{profit_amount:.2f}.")

    # ---------- Store / refresh default SUSU rate on customer ----------
    new_default_rate = rate
    customers_col.update_one(
        {"_id": customer_oid},
        {
            "$set": {
                "susu_default_rate": rate,
                "susu_rate_last": rate,
                "susu_rate_streak": 3,
            }
        }
    )
    logs.append(f"Updated customer default SUSU rate to GH₵{rate:.2f}.")

    # ---------- Build updated stats for this customer ----------
    new_withdraw_to_customer = total_withdraw_to_customer + withdraw_amount
    new_susu_profit = total_susu_profit + profit_amount
    new_available = total_susu - new_withdraw_to_customer - new_susu_profit
    if new_available < 0:
        new_available = 0.0

    logs.append(f"New total withdrawn to customer: GH₵{new_withdraw_to_customer:.2f}")
    logs.append(f"New total SUSU profit: GH₵{new_susu_profit:.2f}")
    logs.append(f"New available SUSU balance: GH₵{new_available:.2f}")

    if _is_ajax(request):
        return jsonify(
            ok=True,
            message="SUSU withdrawal recorded successfully.",
            customer_id=str(customer_oid),
            stats={
                "total_susu": round(total_susu, 2),
                "withdraw_to_customer": round(new_withdraw_to_customer, 2),
                "susu_profit": round(new_susu_profit, 2),
                "available_balance": round(new_available, 2),
                "default_rate": float(new_default_rate) if new_default_rate is not None else None,
            },
        )

    flash("✅ SUSU withdrawal recorded successfully.", "success")
    return redirect(url_for("manager_susu.susu_dashboard"))


# ---------------- Withdrawal History (for slide-in modal) ----------------

@manager_susu_bp.route("/manager/susu/withdrawals/history", methods=["GET"])
def susu_withdrawals_history():
    """
    Return recent SUSU withdrawals (JSON) for this manager.
    Used by the 'View Withdrawal History' slide-in modal at the top.

    Includes BOTH:
      - cash to customer (Manual / SUSU Withdrawal / etc.)
      - company SUSU profit (Deduction / SUSU Profit / SUSU deduction).

    Params (optional):
      limit  : int (default 50)
    """
    manager_oid = _require_manager_oid()
    if not manager_oid:
        return jsonify(ok=False, message="Please log in."), 401

    try:
        limit = int(request.args.get("limit", 50) or 50)
    except ValueError:
        limit = 50
    if limit < 1:
        limit = 1
    if limit > 200:
        limit = 200

    # Fetch ALL withdrawals for this manager; filter SUSU-related in Python
    cursor = payments_col.find(
        {
            "manager_id": _manager_id_match(manager_oid),
            "payment_type": "WITHDRAWAL",
        },
        {
            "customer_id": 1,
            "amount": 1,
            "method": 1,
            "note": 1,
            "date": 1,
            "timestamp": 1,
        }
    ).sort([("timestamp", -1), ("date", -1)]).limit(limit)

    withdrawals_raw = list(cursor)

    # Filter to only SUSU-related withdrawals (cash + profit)
    withdrawals: List[Dict[str, Any]] = []
    for p in withdrawals_raw:
        kind = _classify_susu_withdraw(p)
        if not kind:
            continue
        # annotate kind so we can clean up method label
        p["_susu_kind"] = kind
        withdrawals.append(p)

    # preload customer names
    customer_ids = {
        c["customer_id"] for c in withdrawals
        if c.get("customer_id")
    }
    cust_map: Dict[str, str] = {}
    if customer_ids:
        customer_ids_clean = []
        for cid in customer_ids:
            if isinstance(cid, ObjectId):
                customer_ids_clean.append(cid)
            else:
                try:
                    customer_ids_clean.append(ObjectId(cid))
                except Exception:
                    continue

        for c in customers_col.find(
            {"_id": {"$in": customer_ids_clean}},
            {"_id": 1, "name": 1, "phone_number": 1}
        ):
            cust_map[str(c["_id"])] = f"{c.get('name', 'Customer')} ({c.get('phone_number', 'N/A')})"

    def _serialize_withdraw(p: Dict[str, Any]) -> Dict[str, Any]:
        ts = p.get("timestamp")
        if isinstance(ts, datetime):
            date_str = ts.strftime("%Y-%m-%d")
            time_str = ts.strftime("%H:%M:%S")
        else:
            # fallback to date field
            date_str = (p.get("date") or "")[:10]
            time_str = ""

        cid = p.get("customer_id")
        cust_label = ""
        if cid:
            if not isinstance(cid, ObjectId):
                try:
                    cid = ObjectId(cid)
                except Exception:
                    pass
            cust_label = cust_map.get(str(cid), "")

        kind = p.get("_susu_kind")
        # Normalise method for UI
        method_raw = (p.get("method") or "").strip()
        method_lc = method_raw.lower()

        if kind == "cash":
            display_method = "SUSU Withdrawal"
        elif kind == "profit":
            display_method = "SUSU Profit"
        else:
            display_method = method_raw or ""

        # If old data method was already clean, keep it
        if method_lc in ("susu withdrawal", "susu profit"):
            display_method = method_raw

        return {
            "customer": cust_label,
            "amount": float(p.get("amount", 0) or 0),
            "method": display_method,
            "note": p.get("note", ""),
            "date": date_str,
            "time": time_str,
        }

    data = [_serialize_withdraw(w) for w in withdrawals]

    return jsonify(ok=True, items=data)

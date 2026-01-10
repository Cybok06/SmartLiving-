from __future__ import annotations

from datetime import datetime, date
from typing import Any, Dict, List, Optional
import csv
import io
import re

from bson import ObjectId
from flask import (
    Blueprint,
    render_template,
    request,
    jsonify,
    Response,
    session,
)

from db import db

loans_bp = Blueprint("acc_loans", __name__, template_folder="../templates")

loans_col = db["loans"]
loan_schedules_col = db["loan_schedules"]
loan_postings_col = db["loan_postings"]
journal_entries_col = db["journal_entries"]

LOAN_LIABILITY_ACCOUNT = {"code": "LL-001", "name": "Loan Liability"}
INTEREST_EXPENSE_ACCOUNT = {"code": "EXP-INT", "name": "Interest Expense"}
BANK_CASH_ACCOUNT = {"code": "BANK-001", "name": "Bank / Cash"}


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        normalized = str(value).replace(",", "").strip()
        if normalized == "":
            return default
        return float(normalized)
    except Exception:
        return default


def _format_currency(value: float) -> str:
    return f"{value:,.2f}"


def _add_months(dt: datetime, months: int) -> datetime:
    month = dt.month - 1 + months
    year = dt.year + month // 12
    month = month % 12 + 1
    day = min(dt.day, [31, 29 if year % 4 == 0 and (year % 100 != 0 or year % 400 == 0) else 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31][month - 1])
    return datetime(year, month, day, dt.hour, dt.minute, dt.second, dt.microsecond)


def _require_accounting_role() -> bool:
    role = (session.get("role") or "").lower()
    if session.get("admin_id") or session.get("executive_id"):
        return True
    return role == "accounting"


def _is_admin_role() -> bool:
    role = (session.get("role") or "").lower()
    return bool(session.get("admin_id") or session.get("executive_id") or role == "accounting")


def _generate_loan_no() -> str:
    last = loans_col.find_one({}, sort=[("created_at", -1)])
    if not last:
        return "LN-0001"
    last_no = last.get("loan_no", "")
    match = re.search(r"(\d+)$", last_no)
    if not match:
        return "LN-0001"
    next_num = int(match.group(1)) + 1
    return f"LN-{next_num:04d}"


def _build_amortization_schedule(
    principal: float,
    annual_rate: float,
    term_months: int,
    start_date: datetime,
    amortization_method: str = "reducing_balance",
) -> tuple[list[Dict[str, Any]], float, float, float]:
    if term_months <= 0:
        raise ValueError("Term must be at least one month.")

    monthly_rate = annual_rate / 100 / 12
    outstanding = principal
    total_interest = 0.0
    total_payment = 0.0
    entries: list[Dict[str, Any]] = []

    for period in range(1, term_months + 1):
        due_date = _add_months(start_date, period - 1)
        interest = 0.0
        principal_paid = 0.0
        payment = 0.0

        if amortization_method == "flat":
            interest = round(principal * monthly_rate, 2)
            principal_paid = round(principal / term_months, 2)
            payment = round(interest + principal_paid, 2)
            if period == term_months:
                principal_paid = round(outstanding, 2)
                payment = round(interest + principal_paid, 2)
                outstanding = 0.0
            else:
                outstanding = round(outstanding - principal_paid, 2)
        elif amortization_method == "interest_only":
            interest = round(outstanding * monthly_rate, 2)
            if period == term_months:
                principal_paid = round(outstanding, 2)
                payment = round(interest + principal_paid, 2)
                outstanding = 0.0
            else:
                payment = interest
        else:  # reducing balance
            if monthly_rate == 0:
                payment = round(principal / term_months, 2)
            else:
                factor = (1 + monthly_rate) ** term_months
                payment = round(principal * monthly_rate * factor / (factor - 1), 2)
            interest = round(outstanding * monthly_rate, 2)
            principal_paid = round(payment - interest, 2)
            if period == term_months:
                principal_paid = round(outstanding, 2)
                payment = round(interest + principal_paid, 2)
                outstanding = 0.0
            else:
                outstanding = round(outstanding - principal_paid, 2)

        total_interest += interest
        total_payment += payment
        entries.append(
            {
                "period_no": period,
                "period_key": f"{due_date.year}-{due_date.month:02d}",
                "period_date_dt": due_date,
                "opening_balance": round(outstanding + principal_paid, 2),
                "payment": round(payment, 2),
                "interest": round(interest, 2),
                "principal": round(principal_paid, 2),
                "closing_balance": round(outstanding, 2),
                "status": "due",
                "created_at": datetime.utcnow(),
            }
        )

    monthly_payment = entries[0]["payment"] if entries else 0.0
    return entries, round(monthly_payment, 2), round(total_interest, 2), round(total_payment, 2)


def _ensure_schedule_created(loan_id: ObjectId, schedule_entries: list[Dict[str, Any]]) -> None:
    if not schedule_entries:
        return
    for entry in schedule_entries:
        entry["loan_id"] = loan_id
    loan_schedules_col.insert_many(schedule_entries)


def _format_loan_summary(doc: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "id": str(doc.get("_id")),
        "loan_no": doc.get("loan_no", ""),
        "lender_name": doc.get("lender_name", ""),
        "lender_type": doc.get("lender_type", ""),
        "reference": doc.get("reference", ""),
        "principal": _safe_float(doc.get("principal")),
        "outstanding": _safe_float(doc.get("outstanding_principal")),
        "monthly_payment": _safe_float(doc.get("monthly_payment")),
        "annual_interest_rate": _safe_float(doc.get("annual_interest_rate")),
        "term_months": int(doc.get("term_months") or 0),
        "status": (doc.get("status") or "active").lower(),
        "start_date_dt": doc.get("start_date_dt"),
        "maturity_date_dt": doc.get("maturity_date_dt"),
        "total_repaid": _safe_float(doc.get("total_repaid")),
        "last_posted_period": doc.get("last_posted_period"),
        "notes": doc.get("notes", ""),
        "currency": doc.get("currency", "GHS").upper(),
        "monthly_payment_display": _format_currency(_safe_float(doc.get("monthly_payment"))),
    }


def _next_due_schedule(loan_id: ObjectId, as_of: datetime) -> Optional[Dict[str, Any]]:
    return loan_schedules_col.find_one(
        {"loan_id": loan_id, "status": "due", "period_date_dt": {"$gte": as_of}},
        sort=[("period_date_dt", 1)],
    )


def _get_interest_due_for_month(as_of: date) -> float:
    start = datetime(as_of.year, as_of.month, 1)
    end = _add_months(start, 1)
    match = {
        "status": "due",
        "period_date_dt": {"$gte": start, "$lt": end},
    }
    pipeline = [
        {"$match": match},
        {"$group": {"_id": None, "total": {"$sum": "$interest"}}},
    ]
    row = next(loan_schedules_col.aggregate(pipeline), None)
    return round(_safe_float(row["total"]) if row else 0.0, 2)


def _create_journal_entry(
    ref: str,
    memo: str,
    date_dt: datetime,
    lines: List[Dict[str, Any]],
) -> ObjectId:
    entry = {
        "date_dt": date_dt,
        "ref": ref,
        "memo": memo,
        "lines": lines,
        "created_at": datetime.utcnow(),
    }
    result = journal_entries_col.insert_one(entry)
    return result.inserted_id


def get_loans_outstanding(as_of: date | None = None) -> float:
    cutoff = datetime.combine(as_of or date.today(), datetime.max.time())
    pipeline = [
        {"$match": {"status": {"$in": ["active", "closed"]}}},
        {"$project": {"outstanding_principal": 1}},
    ]
    total = 0.0
    for doc in loans_col.aggregate(pipeline):
        total += _safe_float(doc.get("outstanding_principal"))
    return round(total, 2)


@loans_bp.get("/loans")
def loans_page():
    if not _require_accounting_role():
        return jsonify(ok=False, message="Unauthorized"), 403

    qtxt = (request.args.get("q") or "").strip()
    status = (request.args.get("status") or "").strip().lower()
    as_of_str = (request.args.get("as_of") or "").strip()
    as_of_date = None
    if as_of_str:
        try:
            as_of_date = datetime.combine(datetime.fromisoformat(as_of_str).date(), datetime.max.time())
        except Exception:
            as_of_date = None

    query: Dict[str, Any] = {}
    if qtxt:
        regex = {"$regex": qtxt, "$options": "i"}
        query["$or"] = [
            {"loan_no": regex},
            {"lender_name": regex},
            {"reference": regex},
        ]
    if status in ("active", "closed"):
        query["status"] = status

    docs = list(loans_col.find(query).sort("created_at", -1))
    loans = [_format_loan_summary(doc) for doc in docs]

    active_loans = [loan for loan in loans if loan["status"] == "active"]
    active_count = len(active_loans)
    total_outstanding = round(sum(loan["outstanding"] for loan in loans), 2)
    bonding_date = as_of_date or datetime.utcnow()
    interest_due_month = _get_interest_due_for_month(bonding_date.date())
    next_due_doc = loan_schedules_col.find_one(
        {"status": "due", "period_date_dt": {"$gte": bonding_date}},
        sort=[("period_date_dt", 1)],
    )
    next_due_date = next_due_doc.get("period_date_dt") if next_due_doc else None

    if request.args.get("export") == "1":
        out = io.StringIO()
        writer = csv.writer(out)
        writer.writerow(
            [
                "Loan No",
                "Lender Name",
                "Type",
                "Principal",
                "Outstanding",
                "Monthly Payment",
                "Rate (%)",
                "Term (months)",
                "Status",
                "Start Date",
            ]
        )
        for loan in loans:
            writer.writerow(
                [
                    loan["loan_no"],
                    loan["lender_name"],
                    loan["lender_type"],
                    f"{loan['principal']:0.2f}",
                    f"{loan['outstanding']:0.2f}",
                    f"{loan['monthly_payment']:0.2f}",
                    f"{loan['annual_interest_rate']:0.2f}",
                    loan["term_months"],
                    loan["status"],
                    loan["start_date_dt"].strftime("%Y-%m-%d") if loan["start_date_dt"] else "",
                ]
            )
        return Response(
            out.getvalue(),
            mimetype="text/csv",
            headers={"Content-Disposition": 'attachment; filename="loans.csv"'},
        )

    return render_template(
        "accounting/loans.html",
        loans=loans,
        active_count=active_count,
        total_outstanding=total_outstanding,
        total_interest_due=interest_due_month,
        next_due_date=next_due_date,
        denied=False,
        as_of=as_of_str,
    )


@loans_bp.post("/loans/create")
def create_loan():
    if not _require_accounting_role():
        return jsonify(ok=False, message="Unauthorized"), 401

    def _q(key: str) -> str:
        return (request.form.get(key) or "").strip()

    lender_name = _q("lender_name")
    principal = _safe_float(request.form.get("principal"))
    annual_rate = _safe_float(request.form.get("interest_rate"))
    term_months = int(_safe_float(request.form.get("term_months"), 0))
    start_date = _q("start_date")
    lender_type = _q("lender_type") or "bank"
    reference = _q("reference")
    notes = _q("notes")
    currency = (request.form.get("currency") or "GHS").upper()
    amortization_method = _q("amortization_method") or "reducing_balance"
    repayment_type = _q("repayment_type") or "equal_installment"

    if not lender_name or principal <= 0 or term_months <= 0 or not start_date:
        return jsonify(ok=False, message="Please provide lender, principal, term and start date."), 400

    try:
        start_dt = datetime.fromisoformat(start_date)
    except Exception:
        return jsonify(ok=False, message="Start date is invalid."), 400

    schedule, monthly_payment, total_interest, total_payable = _build_amortization_schedule(
        principal=principal,
        annual_rate=annual_rate,
        term_months=term_months,
        start_date=start_dt,
        amortization_method=amortization_method,
    )

    loan_doc = {
        "loan_no": _generate_loan_no(),
        "lender_name": lender_name,
        "lender_type": lender_type,
        "reference": reference,
        "principal": principal,
        "annual_interest_rate": annual_rate,
        "term_months": term_months,
        "start_date_dt": start_dt,
        "payment_frequency": "monthly",
        "amortization_method": amortization_method,
        "repayment_type": repayment_type,
        "currency": currency,
        "status": "active",
        "notes": notes,
        "monthly_payment": monthly_payment,
        "total_interest_estimate": total_interest,
        "total_payable_estimate": total_payable,
        "outstanding_principal": principal,
        "total_repaid": 0.0,
        "last_posted_period": None,
        "created_at": datetime.utcnow(),
        "updated_at": datetime.utcnow(),
    }
    result = loans_col.insert_one(loan_doc)
    loan_id = result.inserted_id
    _ensure_schedule_created(loan_id, schedule)

    return jsonify(ok=True, loan_id=str(loan_id))


@loans_bp.get("/loans/<loan_id>")
def loan_detail(loan_id: str):
    if not _require_accounting_role():
        return jsonify(ok=False, message="Unauthorized"), 403

    try:
        oid = ObjectId(loan_id)
    except Exception:
        return jsonify(ok=False, message="Invalid loan id."), 400

    loan = loans_col.find_one({"_id": oid})
    if not loan:
        return jsonify(ok=False, message="Loan not found."), 404

    status_filter = (request.args.get("schedule_status") or "all").lower()
    schedule_query = {"loan_id": oid}
    if status_filter in {"due", "posted", "paid"}:
        schedule_query["status"] = status_filter
    schedule_cursor = loan_schedules_col.find(schedule_query).sort("period_no", 1)
    schedule = list(schedule_cursor)

    selected_period_key = request.args.get("period_key") or (schedule[0]["period_key"] if schedule else "")
    selected_schedule = next((row for row in schedule if row["period_key"] == selected_period_key), schedule[0] if schedule else {})

    postings = list(
        loan_postings_col.find({"loan_id": oid}).sort("posted_at", -1).limit(10)
    )

    next_due_doc = loan_schedules_col.find_one(
        {"loan_id": oid, "status": "due"}, sort=[("period_date_dt", 1)]
    )
    auto_next_period = next_due_doc.get("period_key") if next_due_doc else None
    last_posted = loan.get("last_posted_period")

    interest_due_month = _get_interest_due_for_month(datetime.utcnow().date())

    return render_template(
        "accounting/loan_detail.html",
        loan=_format_loan_summary(loan),
        schedule=schedule,
        schedule_status=status_filter,
        selected_period=selected_schedule,
        postings=postings,
        auto_next_period=auto_next_period,
        last_posted_period=last_posted,
        interest_due_month=interest_due_month,
        denied=False,
    )


@loans_bp.post("/loans/<loan_id>/post-interest")
def post_interest(loan_id: str):
    if not _require_accounting_role():
        return jsonify(ok=False, message="Unauthorized"), 401

    period_key = (request.form.get("period_key") or "").strip()
    if not period_key:
        return jsonify(ok=False, message="Period key required."), 400

    try:
        oid = ObjectId(loan_id)
    except Exception:
        return jsonify(ok=False, message="Invalid loan id."), 400

    loan = loans_col.find_one({"_id": oid})
    if not loan:
        return jsonify(ok=False, message="Loan not found."), 404

    schedule_row = loan_schedules_col.find_one(
        {"loan_id": oid, "period_key": period_key}
    )
    if not schedule_row or schedule_row.get("status") != "due":
        return jsonify(ok=False, message="Period already posted or invalid."), 400

    interest = _safe_float(schedule_row.get("interest"))
    ref = f"{loan.get('loan_no')}-INT-{period_key}"
    journal_id = _create_journal_entry(
        ref=ref,
        memo=f"Interest posting for {period_key}",
        date_dt=datetime.utcnow(),
        lines=[
            {
                "account_code": INTEREST_EXPENSE_ACCOUNT["code"],
                "account_name": INTEREST_EXPENSE_ACCOUNT["name"],
                "debit": interest,
                "credit": 0.0,
            },
            {
                "account_code": LOAN_LIABILITY_ACCOUNT["code"],
                "account_name": LOAN_LIABILITY_ACCOUNT["name"],
                "debit": 0.0,
                "credit": interest,
            },
        ],
    )

    loan_postings_col.insert_one(
        {
            "loan_id": oid,
            "period_key": period_key,
            "posted_at": datetime.utcnow(),
            "amount_interest": interest,
            "amount_principal": 0.0,
            "journal_ref": str(journal_id),
            "created_by": session.get("admin_id") or session.get("executive_id") or session.get("user_id"),
        }
    )

    loan_schedules_col.update_one(
        {"_id": schedule_row["_id"]},
        {"$set": {"status": "posted"}},
    )
    loans_col.update_one(
        {"_id": oid},
        {"$set": {"last_posted_period": period_key, "updated_at": datetime.utcnow()}},
    )

    return jsonify(ok=True, journal_ref=str(journal_id), next_period=period_key)


@loans_bp.post("/loans/<loan_id>/record-payment")
def record_payment(loan_id: str):
    if not _require_accounting_role():
        return jsonify(ok=False, message="Unauthorized"), 401

    amount = _safe_float(request.form.get("amount"))
    period_key = (request.form.get("period_key") or "").strip()
    account_name = (request.form.get("account_name") or "Bank / Cash").strip()
    account_code = (request.form.get("account_code") or BANK_CASH_ACCOUNT["code"]).strip()

    if amount <= 0:
        return jsonify(ok=False, message="Payment amount required."), 400

    try:
        oid = ObjectId(loan_id)
    except Exception:
        return jsonify(ok=False, message="Invalid loan id."), 400

    loan = loans_col.find_one({"_id": oid})
    if not loan:
        return jsonify(ok=False, message="Loan not found."), 404

    remaining_amount = amount
    interest_paid = 0.0
    principal_paid = 0.0

    if period_key:
        schedule_row = loan_schedules_col.find_one(
            {"loan_id": oid, "period_key": period_key}
        )
        if schedule_row:
            interest_due = _safe_float(schedule_row.get("interest"))
            principal_due = _safe_float(schedule_row.get("principal"))
            interest_paid = min(remaining_amount, interest_due)
            remaining_amount -= interest_paid
            principal_paid = min(remaining_amount, principal_due)
            remaining_amount -= principal_paid
            if interest_paid + principal_paid >= interest_due + principal_due:
                loan_schedules_col.update_one(
                    {"_id": schedule_row["_id"]}, {"$set": {"status": "paid"}}
                )
    principal_paid += remaining_amount
    remaining_amount = 0.0

    if principal_paid < 0:
        principal_paid = 0.0
    if interest_paid < 0:
        interest_paid = 0.0

    total_debit = round(principal_paid + interest_paid, 2)
    ref = f"{loan.get('loan_no')}-PAY-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"
    journal_id = _create_journal_entry(
        ref=ref,
        memo=f"Payment for {loan.get('loan_no')}",
        date_dt=datetime.utcnow(),
        lines=[
            {
                "account_code": LOAN_LIABILITY_ACCOUNT["code"],
                "account_name": LOAN_LIABILITY_ACCOUNT["name"],
                "debit": total_debit,
                "credit": 0.0,
            },
            {
                "account_code": account_code,
                "account_name": account_name,
                "debit": 0.0,
                "credit": total_debit,
            },
        ],
    )

    loan_postings_col.insert_one(
        {
            "loan_id": oid,
            "period_key": period_key or "",
            "posted_at": datetime.utcnow(),
            "amount_interest": interest_paid,
            "amount_principal": principal_paid,
            "journal_ref": str(journal_id),
            "created_by": session.get("admin_id") or session.get("executive_id") or session.get("user_id"),
        }
    )

    outstanding = max(_safe_float(loan.get("outstanding_principal")) - principal_paid, 0.0)
    total_repaid = _safe_float(loan.get("total_repaid")) + total_debit
    status = "closed" if outstanding <= 0 else "active"

    loans_col.update_one(
        {"_id": oid},
        {
            "$set": {
                "outstanding_principal": outstanding,
                "total_repaid": total_repaid,
                "status": status,
                "updated_at": datetime.utcnow(),
            }
        },
    )

    return jsonify(
        ok=True,
        journal_ref=str(journal_id),
        outstanding=round(outstanding, 2),
        status=status,
    )

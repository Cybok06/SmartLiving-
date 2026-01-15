# accounting_routes/balance_sheet.py
from __future__ import annotations

from flask import Blueprint, render_template, request, jsonify, Response
from datetime import datetime, date, time
from typing import Any, Dict, List
import io
import csv
import json

from bson import ObjectId
from db import db
from accounting_routes.loans import get_loans_outstanding

acc_balance_sheet = Blueprint(
    "acc_balance_sheet",
    __name__,
    template_folder="../templates",
)

balance_sheets_col = db["balance_sheets"]
fixed_assets_col = db["fixed_assets"]
stock_closings_col = db["stock_closings"]
ar_invoices_col = db["ar_invoices"]
ap_bills_col = db["ap_bills"]
accruals_col = db["accruals"]
bank_accounts_col = db["bank_accounts"]
payments_col = db["payments"]
manager_deposits_col = db["manager_deposits"]
tax_col = db["tax_records"]
sbdc_col = db["s_bdc_payment"]
withdrawals_col = db["withdrawals"]
FIXED_ASSET_CATEGORIES = [
    "Land and Building",
    "Furniture and Fittings",
    "Motor Vehicles",
    "Plant and Machinery",
]

CURRENT_ASSET_LINES = ["Stock", "Debtors", "Bank", "Cash"]
CURRENT_LIAB_LINES = ["Creditors", "Expenses Creditors"]
EQUITY_LINES = ["Capital", "Add Net profit", "Less Drawings"]
LT_LIAB_LINES = ["Loan"]


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        if v is None or v == "":
            return default
        return float(v)
    except Exception:
        return default


def _last4(acc_number: str | None) -> str:
    s = str(acc_number or "")
    return s[-4:] if len(s) >= 4 else s


def _parse_iso_date(value: str | None) -> date | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value).date()
    except Exception:
        return None


def _get_fixed_asset_totals(as_of: date | None = None) -> Dict[str, float]:
    query: Dict[str, Any] = {
        "entry_type": "asset",
        "status": {"$ne": "Disposed"},
    }
    if as_of:
        cutoff = datetime.combine(as_of, time.max)
        query["acquisition_date"] = {"$lte": cutoff}

    totals: Dict[str, float] = {category: 0.0 for category in FIXED_ASSET_CATEGORIES}

    cursor = fixed_assets_col.find(
        query,
        {"category": 1, "cost": 1, "accum_depr": 1},
    )
    for doc in cursor:
        category = doc.get("category") or ""
        if category not in totals:
            continue
        cost = _safe_float(doc.get("cost"), 0.0)
        accum = _safe_float(doc.get("accum_depr"), 0.0)
        nbv = cost - accum
        if nbv < 0:
            nbv = 0.0
        totals[category] += nbv

    return {category: round(totals[category], 2) for category in FIXED_ASSET_CATEGORIES}


def _zero_totals(lines: List[str]) -> Dict[str, float]:
    return {line: 0.0 for line in lines}


def _ensure_liability_line(lines: List[Dict[str, Any]], section: str, label: str, amount: float | None):
    if amount is None:
        amount = 0.0
    amt = _safe_float(amount)
    sec_key = section.strip().lower()
    lbl_key = label.strip().lower()
    for line in lines:
        if (
            (line.get("section") or "").strip().lower() == sec_key
            and (line.get("label") or "").strip().lower() == lbl_key
        ):
            line["amount"] = amt
            return
    lines.append({"type": "liability", "section": section, "label": label, "amount": amt})


def _get_current_asset_totals(as_of: date | None = None) -> Dict[str, float]:
    totals = _zero_totals(CURRENT_ASSET_LINES)
    totals["Stock"] = _get_closing_stock_value(as_of)
    totals["Debtors"] = _get_debtors_value(as_of)
    bank_cash_totals = _get_bank_and_cash_totals(as_of)
    totals["Bank"] = bank_cash_totals.get("bank", 0.0)
    totals["Cash"] = bank_cash_totals.get("cash", 0.0)
    return totals


def _get_closing_stock_value(as_of: date | None = None) -> float:
    if as_of is None:
        as_of = date.today()
    cutoff = datetime.combine(as_of, time.max)
    doc = stock_closings_col.find_one(
        {"status": "completed", "closed_at": {"$lte": cutoff}},
        sort=[("closed_at", -1), ("created_at", -1)],
    )
    if not doc:
        return 0.0
    try:
        val = float(doc.get("total_closing_cost_value") or 0.0)
    except Exception:
        val = 0.0
    return round(val, 2)


def _sum_confirmed_in_asof(bank_name: str, last4: str, cutoff: datetime) -> float:
    try:
        # payments use their 'date' field if provided; otherwise fallback to 'created_at'
        match = {
            "bank_name": bank_name,
            "account_last4": last4,
            "status": "confirmed",
            "$or": [
                {"date": {"$lte": cutoff}},
                {"date": {"$exists": False}, "created_at": {"$lte": cutoff}},
            ],
        }
        pipe = [{"$match": match}, {"$group": {"_id": None, "total": {"$sum": "$amount"}}}]
        row = next(payments_col.aggregate(pipe), None)
        return _safe_float(row["total"]) if row else 0.0
    except Exception:
        return 0.0


def _sum_manager_deposits_in_asof(bank_oid: ObjectId, cutoff: datetime) -> float:
    try:
        # manager deposits always use 'created_at' for cutoff filtering
        bank_id_str = str(bank_oid)
        pipe = [
            {
                "$match": {
                    "bank_account_id": bank_id_str,
                    "status": {"$in": ["submitted", "approved"]},
                    "created_at": {"$lte": cutoff},
                }
            },
            {"$group": {"_id": None, "total": {"$sum": "$amount"}}},
        ]
        row = next(manager_deposits_col.aggregate(pipe), None)
        return _safe_float(row["total"]) if row else 0.0
    except Exception:
        return 0.0


def _sum_ptax_out_asof(bank_oid: ObjectId, cutoff: datetime) -> float:
    try:
        # tax records prefer their 'date' field; fallback to 'created_at'
        pipe = [
            {
                "$match": {
                    "source_bank_id": bank_oid,
                    "type": {"$regex": r"^p[\s_-]*tax$", "$options": "i"},
                    "$or": [
                        {"date": {"$lte": cutoff}},
                        {"date": {"$exists": False}, "created_at": {"$lte": cutoff}},
                    ],
                }
            },
            {"$group": {"_id": None, "total": {"$sum": "$amount"}}},
        ]
        row = next(tax_col.aggregate(pipe), None)
        return _safe_float(row["total"]) if row else 0.0
    except Exception:
        return 0.0


def _sum_bdc_out_asof(bank_oid: ObjectId, cutoff: datetime) -> float:
    try:
        # BDC histories use bank_paid_history.date when available; else use the parent created_at
        pipe = [
            {"$match": {"bank_paid_history": {"$exists": True, "$ne": []}}},
            {"$unwind": "$bank_paid_history"},
            {
                "$match": {
                    "bank_paid_history.bank_id": bank_oid,
                    "$or": [
                        {"bank_paid_history.date": {"$lte": cutoff}},
                        {
                            "bank_paid_history.date": {"$exists": False},
                            "created_at": {"$lte": cutoff},
                        },
                    ],
                }
            },
            {"$group": {"_id": None, "total": {"$sum": "$bank_paid_history.amount"}}},
        ]
        row = next(sbdc_col.aggregate(pipe), None)
        return _safe_float(row["total"]) if row else 0.0
    except Exception:
        return 0.0


def _sum_withdrawals_out_asof(bank_oid: ObjectId, cutoff: datetime) -> float:
    try:
        # withdrawals use 'date_dt' when present; fallback to 'created_at'
        pipe = [
            {
                "$match": {
                    "account_id": bank_oid,
                    "$or": [
                        {"date_dt": {"$lte": cutoff}},
                        {"date_dt": {"$exists": False}, "created_at": {"$lte": cutoff}},
                    ],
                }
            },
            {"$group": {"_id": None, "total": {"$sum": "$amount"}}},
        ]
        row = next(withdrawals_col.aggregate(pipe), None)
        return _safe_float(row["total"]) if row else 0.0
    except Exception:
        return 0.0


def _get_bank_and_cash_totals(as_of: date | None = None) -> Dict[str, float]:
    cutoff = datetime.combine(as_of or date.today(), time.max)
    bank_total = 0.0
    cash_total = 0.0
    for doc in bank_accounts_col.find({}):
        bank_oid = doc.get("_id")
        if not bank_oid:
            continue

        acc_type = (doc.get("account_type") or "bank").lower().strip()
        if acc_type not in ("bank", "cash"):
            acc_type = "bank"

        bank_name = doc.get("bank_name") or ""
        raw_acc_no = doc.get("account_number") or doc.get("account_no") or ""
        last4 = _last4(raw_acc_no)
        opening = _safe_float(doc.get("opening_balance"))

        confirmed_in = _sum_confirmed_in_asof(bank_name, last4, cutoff)
        manager_in = _sum_manager_deposits_in_asof(bank_oid, cutoff)
        ptax_out = _sum_ptax_out_asof(bank_oid, cutoff)
        bdc_out = _sum_bdc_out_asof(bank_oid, cutoff)
        withdraw_out = _sum_withdrawals_out_asof(bank_oid, cutoff)

        live_balance = opening + confirmed_in + manager_in - (
            ptax_out + bdc_out + withdraw_out
        )

        if acc_type == "cash":
            cash_total += live_balance
        else:
            bank_total += live_balance

    return {"bank": round(bank_total, 2), "cash": round(cash_total, 2)}


def _get_debtors_value(as_of: date | None = None) -> float:
    if as_of is None:
        as_of = date.today()
    cutoff = datetime.combine(as_of, time.max)
    # invoice_date preferred; fallback to created_at when invoice_date missing
    match = {
        "balance": {"$gt": 0},
        "$and": [
            {
                "$or": [
                    {"invoice_date": {"$lte": cutoff}},
                    {"invoice_date": {"$exists": False}, "created_at": {"$lte": cutoff}},
                ]
            },
            {
                "$or": [
                    {"status": {"$exists": False}},
                    {"status": {"$regex": "^owing$", "$options": "i"}},
                ]
            },
        ],
    }
    pipeline = [
        {"$match": match},
        {"$group": {"_id": None, "total": {"$sum": "$balance"}}},
    ]
    res = list(ar_invoices_col.aggregate(pipeline, allowDiskUse=False))
    total = res[0]["total"] if res else 0.0
    try:
        val = float(total or 0.0)
    except Exception:
        val = 0.0
    return round(val, 2)


def _get_creditors_total(as_of: date | None = None) -> float:
    if as_of is None:
        as_of = date.today()
    cutoff = datetime.combine(as_of, time.max)
    match = {
        "balance": {"$gt": 0},
        "$and": [
            {
                "$or": [
                    {"bill_date_dt": {"$lte": cutoff}},
                    {"bill_date_dt": {"$exists": False}, "created_at": {"$lte": cutoff}},
                ]
            },
            {
                "$or": [
                    {"status": {"$exists": False}},
                    {"status": {"$not": {"$regex": "^paid$", "$options": "i"}}},
                ]
            },
        ],
    }
    total = 0.0
    for doc in ap_bills_col.find(match, {"amount": 1, "paid": 1, "balance": 1}):
        bal = doc.get("balance")
        if bal is not None:
            amt = _safe_float(bal)
        else:
            amount = _safe_float(doc.get("amount"))
            paid = _safe_float(doc.get("paid"))
            amt = max(amount - paid, 0.0)
        total += amt
    return round(total, 2)


def _get_expenses_creditors_total(as_of: date | None = None) -> float:
    if as_of is None:
        as_of = date.today()
    cutoff = datetime.combine(as_of, time.max)
    match = {
        "$and": [
            {
                "$or": [
                    {"date_dt": {"$lte": cutoff}},
                    {"date_dt": {"$exists": False}, "created_at": {"$lte": cutoff}},
                ]
            },
            {
                "$or": [
                    {"status": {"$exists": False}},
                    {"status": {"$regex": "^owing$", "$options": "i"}},
                ]
            },
        ]
    }
    total = 0.0
    for doc in accruals_col.find(match, {"amount": 1}):
        total += _safe_float(doc.get("amount"))
    return round(total, 2)


def _get_current_liability_totals(creditors: float, expenses_creditors: float) -> Dict[str, float]:
    totals = _zero_totals(CURRENT_LIAB_LINES)
    totals["Creditors"] = creditors
    totals["Expenses Creditors"] = expenses_creditors
    return totals


def _get_equity_totals(as_of: date | None = None) -> Dict[str, float]:
    # TODO: replace with actual equity totals (e.g. capital accounts + retained earnings)
    return _zero_totals(EQUITY_LINES)


def _get_long_term_liability_totals(loans_total: float) -> Dict[str, float]:
    totals = {line: 0.0 for line in LT_LIAB_LINES}
    totals["Loan"] = loans_total
    return totals


@acc_balance_sheet.route("/balance-sheet", methods=["GET"])
def balance_sheet_page():
    sheet_id_str = request.args.get("sheet_id") or ""
    sheet_doc: Dict[str, Any] | None = None

    if sheet_id_str:
        try:
            oid = ObjectId(sheet_id_str)
            sheet_doc = balance_sheets_col.find_one({"_id": oid})
        except Exception:
            sheet_doc = None

    if sheet_doc is None:
        sheet_doc = balance_sheets_col.find_one(
            {},
            sort=[("as_of_date", -1), ("created_at", -1)],
        )

    today = date.today().strftime("%Y-%m-%d")

    # If no saved sheet exists, start with empty sheet (no demo data)
    if not sheet_doc:
        sheet = {
            "id": "",
            "name": "",
            "as_of_date": today,
            "currency": "GHS",
            "lines": [],
            "totals": {"assets": 0, "liabilities": 0, "equity": 0, "liab_plus_equity": 0},
            "is_demo": False,
        }
    else:
        sheet = dict(sheet_doc)
        sheet["id"] = str(sheet.pop("_id", ""))

        as_of = sheet.get("as_of_date")
        if isinstance(as_of, datetime):
            sheet["as_of_date"] = as_of.strftime("%Y-%m-%d")
        elif isinstance(as_of, date):
            sheet["as_of_date"] = as_of.strftime("%Y-%m-%d")
        else:
            sheet["as_of_date"] = ""

        sheet["is_demo"] = False

        if "totals" not in sheet or not isinstance(sheet["totals"], dict):
            sheet["totals"] = {"assets": 0, "liabilities": 0, "equity": 0, "liab_plus_equity": 0}

        if "lines" not in sheet or not isinstance(sheet["lines"], list):
            sheet["lines"] = []

    sheet_as_of_date = _parse_iso_date(sheet.get("as_of_date"))
    fixed_asset_totals = _get_fixed_asset_totals(sheet_as_of_date)
    fixed_asset_total = round(sum(fixed_asset_totals.values()), 2)

    current_asset_totals = _get_current_asset_totals(sheet_as_of_date)
    current_asset_total = round(sum(current_asset_totals.values()), 2)

    creditors_total = _get_creditors_total(sheet_as_of_date)
    expenses_creditors_total = _get_expenses_creditors_total(sheet_as_of_date)
    current_liab_totals = _get_current_liability_totals(creditors_total, expenses_creditors_total)
    current_liab_total = round(sum(current_liab_totals.values()), 2)

    sheet_lines = sheet.get("lines") if isinstance(sheet.get("lines"), list) else []
    _ensure_liability_line(sheet_lines, "Current Liabilities", "Creditors", creditors_total)
    _ensure_liability_line(sheet_lines, "Current Liabilities", "Expenses Creditors", expenses_creditors_total)
    sheet["lines"] = sheet_lines

    equity_totals = _get_equity_totals(sheet_as_of_date)
    equity_total = round(sum(equity_totals.values()), 2)

    loans_total = get_loans_outstanding(sheet_as_of_date)
    lt_liab_totals = _get_long_term_liability_totals(loans_total)
    lt_liab_total = loans_total

    working_capital = round(current_asset_total - current_liab_total, 2)
    net_total_assets = round(fixed_asset_total + working_capital, 2)
    capital_employed = round(equity_total + lt_liab_total, 2)

    # Build options
    options: List[Dict[str, Any]] = []
    cursor = balance_sheets_col.find({}, {"name": 1, "as_of_date": 1}).sort("as_of_date", -1)

    for d in cursor:
        oid = d.get("_id")
        name = d.get("name") or ""
        as_of = d.get("as_of_date")

        if isinstance(as_of, datetime):
            as_of_str = as_of.strftime("%Y-%m-%d")
        elif isinstance(as_of, date):
            as_of_str = as_of.strftime("%Y-%m-%d")
        else:
            as_of_str = ""

        label_parts = []
        if name:
            label_parts.append(name)
        if as_of_str:
            label_parts.append(f"As at {as_of_str}")
        label = " • ".join(label_parts) if label_parts else "Unnamed Sheet"

        options.append({"id": str(oid), "name": name, "as_of_date": as_of_str, "label": label})

    return render_template(
        "accounting/balance_sheet_vertical.html",
        sheet=sheet,
        sheet_options=options,
        today=today,
        fixed_asset_categories=FIXED_ASSET_CATEGORIES,
        fixed_asset_totals=fixed_asset_totals,
        fixed_asset_total=fixed_asset_total,
        current_asset_lines=CURRENT_ASSET_LINES,
        current_asset_totals=current_asset_totals,
        current_asset_total=current_asset_total,
        current_liab_lines=CURRENT_LIAB_LINES,
        current_liab_totals=current_liab_totals,
        current_liab_total=current_liab_total,
        equity_lines=EQUITY_LINES,
        equity_totals=equity_totals,
        equity_total=equity_total,
        lt_liab_lines=LT_LIAB_LINES,
        lt_liab_totals=lt_liab_totals,
        lt_liab_total=lt_liab_total,
        working_capital=working_capital,
        net_total_assets=net_total_assets,
        capital_employed=capital_employed,
    )


@acc_balance_sheet.route("/balance-sheet/save", methods=["POST"])
def balance_sheet_save():
    try:
        data = request.get_json(force=True, silent=False)
    except Exception:
        return jsonify(ok=False, message="Invalid JSON body"), 400

    if not isinstance(data, dict):
        return jsonify(ok=False, message="Invalid payload."), 400

    sheet_id_str = (data.get("id") or "").strip()
    name = (data.get("name") or "").strip()
    as_of_date_str = (data.get("as_of_date") or "").strip()
    currency = (data.get("currency") or "GHS").upper()
    lines = data.get("lines") or []

    if not lines:
        return jsonify(ok=False, message="No balance sheet lines to save."), 400

    as_of_date_only = _parse_iso_date(as_of_date_str)
    as_of_dt: datetime | None = None
    if as_of_date_only:
        as_of_dt = datetime.combine(as_of_date_only, time.min)

    now = datetime.utcnow()

    norm_lines: List[Dict[str, Any]] = []
    total_assets = 0.0
    total_liab = 0.0
    total_equity = 0.0

    for line in lines:
        if not isinstance(line, dict):
            continue

        l_type = (line.get("type") or "").lower()
        if l_type not in ("asset", "liability", "equity"):
            continue

        label = (line.get("label") or "").strip()
        if not label:
            continue

        section = (line.get("section") or "").strip()
        amount = _safe_float(line.get("amount"), 0.0)

        if amount == 0.0:
            continue

        if l_type == "asset":
            total_assets += amount
        elif l_type == "liability":
            total_liab += amount
        elif l_type == "equity":
            total_equity += amount

        norm_lines.append({"type": l_type, "section": section, "label": label, "amount": amount})

    if not norm_lines:
        return jsonify(ok=False, message="All rows are empty or invalid."), 400

    totals = {
        "assets": round(total_assets, 2),
        "liabilities": round(total_liab, 2),
        "equity": round(total_equity, 2),
        "liab_plus_equity": round(total_liab + total_equity, 2),
    }

    doc: Dict[str, Any] = {
        "name": name,
        "as_of_date": as_of_dt,
        "currency": currency,
        "lines": norm_lines,
        "totals": totals,
        "updated_at": now,
    }

    if sheet_id_str:
        try:
            oid = ObjectId(sheet_id_str)
        except Exception:
            return jsonify(ok=False, message="Invalid sheet id."), 400

        balance_sheets_col.update_one(
            {"_id": oid},
            {"$set": doc, "$setOnInsert": {"created_at": now}},
            upsert=True,
        )
        sheet_id = sheet_id_str
    else:
        doc["created_at"] = now
        res = balance_sheets_col.insert_one(doc)
        sheet_id = str(res.inserted_id)

    return jsonify(ok=True, id=sheet_id, totals=totals), 200


@acc_balance_sheet.route("/balance-sheet/export/csv", methods=["POST"])
def balance_sheet_export_csv():
    payload = request.form.get("payload")
    if not payload:
        return jsonify(ok=False, message="No data to export"), 400

    try:
        data = json.loads(payload)
    except json.JSONDecodeError:
        return jsonify(ok=False, message="Invalid JSON payload"), 400

    name = (data.get("name") or "").strip()
    as_of_date_str = (data.get("as_of_date") or "").strip()
    currency = (data.get("currency") or "GHS").upper()
    lines = data.get("lines") or []

    out = io.StringIO()
    w = csv.writer(out)

    title = "Balance Sheet"
    if name:
        title += f" - {name}"
    if as_of_date_str:
        title += f" (As at {as_of_date_str})"

    w.writerow([title])
    w.writerow([])
    w.writerow(["Type", "Section", "Account", f"Amount ({currency})"])

    for line in lines:
        t = (line.get("type") or "").lower()
        sec = line.get("section") or ""
        lab = line.get("label") or ""
        amt = _safe_float(line.get("amount"), 0.0)
        w.writerow([t, sec, lab, f"{amt:0.2f}"])

    filename_date = (as_of_date_str or date.today().strftime("%Y-%m-%d")).replace("-", "")
    filename = f"balance_sheet_{filename_date}.csv"

    return Response(
        out.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )

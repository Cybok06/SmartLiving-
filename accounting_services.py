from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, date
from typing import Any, Dict, Iterable, List, Optional, Tuple

from bson import ObjectId

from db import db

# Collections
private_ledger_col = db["private_ledger_entries"]
withdrawals_col = db["withdrawals"]
prepayments_col = db["prepayments"]
accruals_col = db["accruals"]
expenses_col = db["expenses"]
asset_purchases_col = db["asset_purchases"]
inventory_col = db["inventory"]
inventory_logs_col = db["inventory_logs"]
inventory_outflows_col = db["inventory_outflows"]


# -----------------------------
# Date helpers
# -----------------------------

def _dt(val: Any) -> Optional[datetime]:
    if isinstance(val, datetime):
        return val
    if isinstance(val, date):
        return datetime(val.year, val.month, val.day)
    if isinstance(val, str):
        try:
            return datetime.fromisoformat(val[:19])
        except Exception:
            return None
    return None


def _parse_month(value: str) -> Optional[Tuple[int, int]]:
    if not value or len(value) != 7 or value[4] != "-":
        return None
    try:
        year = int(value[:4])
        month = int(value[5:7])
    except Exception:
        return None
    if month < 1 or month > 12:
        return None
    return year, month


def _iter_months(start_dt: datetime, end_dt: datetime) -> List[Tuple[int, int]]:
    """
    Inclusive month list for the given datetime range.
    Returns list of (year, month).
    """
    months: List[Tuple[int, int]] = []
    y = start_dt.year
    m = start_dt.month
    end_y = end_dt.year
    end_m = end_dt.month
    while (y, m) <= (end_y, end_m):
        months.append((y, m))
        if m == 12:
            y += 1
            m = 1
        else:
            m += 1
    return months


def _month_count(start_period: str, end_period: str) -> int:
    start = _parse_month(start_period)
    end = _parse_month(end_period)
    if not start or not end:
        return 0
    sy, sm = start
    ey, em = end
    return max(1, (ey - sy) * 12 + (em - sm) + 1)


# -----------------------------
# Posting services
# -----------------------------

def post_withdrawal(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Create a unified withdrawal record and post related entries.
    Required payload keys:
      - amount, account_type, account_id (optional for cash), purpose, date_dt
    Optional:
      - purpose_note, counterparty, created_by, branch_id
      - expense_category, expense_description
      - asset_category, asset_description
    """
    amount = float(payload.get("amount") or 0)
    if amount <= 0:
        return {"ok": False, "message": "Amount must be greater than zero."}

    account_type = (payload.get("account_type") or "").strip().lower()
    purpose = (payload.get("purpose") or "").strip().lower()
    if account_type not in ("cash", "bank", "momo", "mobile_money"):
        return {"ok": False, "message": "Invalid account type."}
    if purpose not in ("drawings", "asset", "expense", "other"):
        return {"ok": False, "message": "Invalid withdrawal purpose."}

    date_dt = _dt(payload.get("date_dt")) or datetime.utcnow()

    account_id = payload.get("account_id")
    account_oid = None
    if account_id:
        try:
            account_oid = ObjectId(account_id)
        except Exception:
            account_oid = None

    doc = {
        "date_dt": date_dt,
        "amount": amount,
        "account_type": "mobile_money" if account_type == "momo" else account_type,
        "account_id": account_oid,
        "purpose": purpose,
        "purpose_note": (payload.get("purpose_note") or "").strip(),
        "counterparty": (payload.get("counterparty") or "").strip() or None,
        "posted": True,
        "journal_ref": None,
        "created_by": payload.get("created_by"),
        "branch_id": payload.get("branch_id"),
        "created_at": datetime.utcnow(),
    }

    res = withdrawals_col.insert_one(doc)
    withdrawal_id = res.inserted_id

    # Drawings: post into private ledger (equity only)
    if purpose == "drawings":
        private_ledger_col.insert_one({
            "entry_type": "cash_drawing",
            "source_account_type": doc["account_type"],
            "source_account_id": account_oid,
            "date_dt": date_dt,
            "amount": amount,
            "purpose_text": doc["purpose_note"],
            "created_by": payload.get("created_by"),
            "branch_id": payload.get("branch_id"),
            "status": "posted",
            "link": {
                "related_collection": "withdrawals",
                "related_id": withdrawal_id,
            },
            "created_at": datetime.utcnow(),
        })

    # Expense: create accounting expense
    if purpose == "expense":
        category = (payload.get("expense_category") or "").strip() or "Uncategorized"
        description = (payload.get("expense_description") or doc["purpose_note"] or "Withdrawal expense").strip()
        expenses_col.insert_one({
            "date": date_dt,
            "amount": amount,
            "category": category,
            "description": description,
            "payment_method": f"{doc['account_type']} withdrawal",
            "reference": str(withdrawal_id),
            "created_at": datetime.utcnow(),
            "updated_at": datetime.utcnow(),
        })

    # Asset: create holding entry for asset purchases
    if purpose == "asset":
        asset_purchases_col.insert_one({
            "date_dt": date_dt,
            "amount": amount,
            "category": (payload.get("asset_category") or "").strip() or "Other Fixed Assets",
            "description": (payload.get("asset_description") or doc["purpose_note"] or "Asset purchase").strip(),
            "account_type": doc["account_type"],
            "account_id": account_oid,
            "created_by": payload.get("created_by"),
            "branch_id": payload.get("branch_id"),
            "status": "posted",
            "created_at": datetime.utcnow(),
        })

    return {"ok": True, "withdrawal_id": str(withdrawal_id)}


def post_goods_drawn(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Record goods drawn by owner:
      - Reduce inventory qty
      - Log inventory outflow
      - Post private ledger entry
    """
    product_id = payload.get("product_id")
    qty = float(payload.get("quantity") or 0)
    if not product_id or qty <= 0:
        return {"ok": False, "message": "Product and quantity are required."}

    try:
        prod_oid = ObjectId(product_id)
    except Exception:
        return {"ok": False, "message": "Invalid product id."}

    product = inventory_col.find_one({"_id": prod_oid})
    if not product:
        return {"ok": False, "message": "Inventory item not found."}

    current_qty = float(product.get("qty") or 0)
    if qty > current_qty:
        return {"ok": False, "message": "Quantity exceeds available stock."}

    unit_cost = payload.get("unit_cost")
    if unit_cost is None:
        unit_cost = product.get("cost_price") or product.get("price") or 0
    unit_cost = float(unit_cost or 0)
    total_value = float(payload.get("total_value") or (qty * unit_cost))

    date_dt = _dt(payload.get("date_dt")) or datetime.utcnow()
    memo = (payload.get("memo") or "").strip()

    new_qty = max(current_qty - qty, 0)
    inventory_col.update_one(
        {"_id": prod_oid},
        {"$set": {"qty": new_qty, "updated_at": datetime.utcnow()}},
    )

    log_doc = {
        "product_id": prod_oid,
        "product_name": product.get("name") or "",
        "action": "goods_drawn",
        "log_type": "withdrawal",
        "qty_moved": qty,
        "old_qty": current_qty,
        "new_qty": new_qty,
        "updated_by": payload.get("created_by") or "accounting",
        "updated_at": datetime.utcnow(),
    }
    inventory_logs_col.insert_one(log_doc)

    outflow = {
        "product_id": prod_oid,
        "product_name": product.get("name") or "",
        "quantity": qty,
        "unit_cost": unit_cost,
        "total_value": total_value,
        "date_dt": date_dt,
        "memo": memo,
        "source": "goods_drawn",
        "created_by": payload.get("created_by"),
        "created_at": datetime.utcnow(),
    }
    outflow_res = inventory_outflows_col.insert_one(outflow)

    private_ledger_col.insert_one({
        "entry_type": "goods_drawn",
        "source_account_type": None,
        "source_account_id": None,
        "date_dt": date_dt,
        "amount": total_value,
        "purpose_text": memo,
        "created_by": payload.get("created_by"),
        "branch_id": payload.get("branch_id"),
        "status": "posted",
        "link": {
            "related_collection": "inventory_outflows",
            "related_id": outflow_res.inserted_id,
        },
        "created_at": datetime.utcnow(),
    })

    return {"ok": True, "total_value": total_value}


def post_prepayment(payload: Dict[str, Any]) -> Dict[str, Any]:
    amount_total = float(payload.get("amount_total") or 0)
    if amount_total <= 0:
        return {"ok": False, "message": "Amount must be greater than zero."}

    start_period = (payload.get("start_period") or "").strip()
    end_period = (payload.get("end_period") or "").strip()
    months = _month_count(start_period, end_period)
    if months <= 0:
        return {"ok": False, "message": "Invalid start/end period."}

    monthly = round(amount_total / months, 2)

    doc = {
        "date_dt": _dt(payload.get("date_dt")) or datetime.utcnow(),
        "category": (payload.get("category") or "").strip() or "Prepayment",
        "vendor": (payload.get("vendor") or "").strip(),
        "amount_total": amount_total,
        "start_period": start_period,
        "end_period": end_period,
        "monthly_expense_amount": monthly,
        "remaining_balance": amount_total,
        "status": "active",
        "created_by": payload.get("created_by"),
        "branch_id": payload.get("branch_id"),
        "created_at": datetime.utcnow(),
    }
    res = prepayments_col.insert_one(doc)
    return {"ok": True, "id": str(res.inserted_id)}


def post_accrual(payload: Dict[str, Any]) -> Dict[str, Any]:
    amount = float(payload.get("amount") or 0)
    if amount <= 0:
        return {"ok": False, "message": "Amount must be greater than zero."}

    doc = {
        "date_dt": _dt(payload.get("date_dt")) or datetime.utcnow(),
        "category": (payload.get("category") or "").strip() or "Accrual",
        "vendor": (payload.get("vendor") or "").strip(),
        "amount": amount,
        "due_date": _dt(payload.get("due_date")),
        "status": (payload.get("status") or "owing").strip().lower(),
        "linked_payment_id": payload.get("linked_payment_id"),
        "created_by": payload.get("created_by"),
        "branch_id": payload.get("branch_id"),
        "created_at": datetime.utcnow(),
    }
    res = accruals_col.insert_one(doc)
    return {"ok": True, "id": str(res.inserted_id)}


# -----------------------------
# Reporting helpers
# -----------------------------

def prepayment_amortization_for_period(start_dt: datetime, end_dt: datetime) -> Dict[str, float]:
    """
    Return {category: amount} for amortized prepayments within period.
    """
    period_months = set(_iter_months(start_dt, end_dt))
    totals: Dict[str, float] = {}

    for p in prepayments_col.find({"status": {"$in": ["active", "closed"]}}):
        sp = p.get("start_period")
        ep = p.get("end_period")
        if not sp or not ep:
            continue
        start = _parse_month(sp)
        end = _parse_month(ep)
        if not start or not end:
            continue
        prepay_months = set(_iter_months(
            datetime(start[0], start[1], 1),
            datetime(end[0], end[1], 1),
        ))
        overlap = period_months.intersection(prepay_months)
        if not overlap:
            continue
        monthly = float(p.get("monthly_expense_amount") or 0)
        amt = monthly * len(overlap)
        cat = (p.get("category") or "Prepayment").strip() or "Prepayment"
        totals[cat] = totals.get(cat, 0.0) + amt

    return totals


def accruals_for_period(start_dt: datetime, end_dt: datetime) -> Dict[str, float]:
    """
    Return {category: amount} for accruals incurred in period.
    """
    totals: Dict[str, float] = {}
    q = {"date_dt": {"$gte": start_dt, "$lte": end_dt}, "status": {"$ne": "voided"}}
    for a in accruals_col.find(q):
        amt = float(a.get("amount") or 0)
        if amt <= 0:
            continue
        cat = (a.get("category") or "Accrual").strip() or "Accrual"
        totals[cat] = totals.get(cat, 0.0) + amt
    return totals


def accruals_outstanding(as_of: datetime) -> float:
    q = {"status": "owing", "date_dt": {"$lte": as_of}}
    total = 0.0
    for a in accruals_col.find(q):
        total += float(a.get("amount") or 0)
    return total


def prepayments_outstanding(as_of: datetime) -> float:
    total = 0.0
    for p in prepayments_col.find({"status": {"$in": ["active", "closed"]}}):
        start = _parse_month(p.get("start_period") or "")
        end = _parse_month(p.get("end_period") or "")
        if not start or not end:
            continue
        start_dt = datetime(start[0], start[1], 1)
        end_dt = datetime(end[0], end[1], 1)
        months_total = _month_count(p.get("start_period") or "", p.get("end_period") or "")
        if months_total <= 0:
            continue
        monthly = float(p.get("monthly_expense_amount") or 0)
        if as_of < start_dt:
            total += float(p.get("amount_total") or 0)
            continue
        if as_of > end_dt:
            continue
        elapsed = len(_iter_months(start_dt, as_of))
        remaining = max(float(p.get("amount_total") or 0) - (monthly * elapsed), 0.0)
        total += remaining
    return total


def private_drawings_total(start_dt: datetime, end_dt: datetime) -> float:
    q = {
        "entry_type": "cash_drawing",
        "status": "posted",
        "date_dt": {"$gte": start_dt, "$lte": end_dt},
    }
    total = 0.0
    for d in private_ledger_col.find(q):
        total += float(d.get("amount") or 0)
    return total


def goods_drawn_total(start_dt: datetime, end_dt: datetime) -> float:
    q = {
        "entry_type": "goods_drawn",
        "status": "posted",
        "date_dt": {"$gte": start_dt, "$lte": end_dt},
    }
    total = 0.0
    for d in private_ledger_col.find(q):
        total += float(d.get("amount") or 0)
    return total


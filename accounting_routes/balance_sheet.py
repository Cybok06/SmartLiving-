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

acc_balance_sheet = Blueprint(
    "acc_balance_sheet",
    __name__,
    template_folder="../templates",
)

balance_sheets_col = db["balance_sheets"]


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        if v is None or v == "":
            return default
        return float(v)
    except Exception:
        return default


def _parse_iso_date(value: str | None) -> date | None:
    if not value:
        return None
    try:
        # expects "YYYY-MM-DD"
        return datetime.fromisoformat(value).date()
    except Exception:
        return None


# ---------------- PAGES ----------------

@acc_balance_sheet.route("/balance-sheet", methods=["GET"])
def balance_sheet_page():
    """
    Balance sheet UI.
    You can switch between multiple named sheets (e.g. 'Dec 2025', 'As at 31-12-2024').
    """
    sheet_id_str = request.args.get("sheet_id") or ""
    sheet_doc: Dict[str, Any] | None = None

    # Try to load specific sheet
    if sheet_id_str:
        try:
            oid = ObjectId(sheet_id_str)
            sheet_doc = balance_sheets_col.find_one({"_id": oid})
        except Exception:
            sheet_doc = None

    # If no specific sheet, load the most recent one
    if sheet_doc is None:
        sheet_doc = balance_sheets_col.find_one(
            {},
            sort=[("as_of_date", -1), ("created_at", -1)],
        )

    sheet: Dict[str, Any] | None = None
    if sheet_doc:
        sheet = dict(sheet_doc)
        sheet["id"] = str(sheet.pop("_id", ""))

        # Convert as_of_date to ISO string for the input[type=date]
        as_of = sheet.get("as_of_date")
        if isinstance(as_of, datetime):
            sheet["as_of_date"] = as_of.strftime("%Y-%m-%d")
        elif isinstance(as_of, date):
            sheet["as_of_date"] = as_of.strftime("%Y-%m-%d")
        else:
            sheet["as_of_date"] = ""
    else:
        sheet = {
            "id": "",
            "name": "",
            "as_of_date": "",
            "currency": "GHS",
            "lines": [],
            "totals": {
                "assets": 0.0,
                "liabilities": 0.0,
                "equity": 0.0,
                "liab_plus_equity": 0.0,
            },
        }

    # Build options for selector
    options: List[Dict[str, Any]] = []
    cursor = balance_sheets_col.find(
        {},
        {"name": 1, "as_of_date": 1},
    ).sort("as_of_date", -1)

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

        options.append(
            {
                "id": str(oid),
                "name": name,
                "as_of_date": as_of_str,
                "label": label,
            }
        )

    today = date.today().strftime("%Y-%m-%d")

    return render_template(
        "accounting/balance_sheet.html",
        sheet=sheet or {},
        sheet_options=options,
        today=today,
    )


@acc_balance_sheet.route("/balance-sheet/save", methods=["POST"])
def balance_sheet_save():
    """
    Save (upsert) a balance sheet.

    Expected JSON:
    {
      "id": "optional existing id",
      "name": "Dec 2025",
      "as_of_date": "2025-12-31",
      "currency": "GHS",
      "lines": [
        {
          "type": "asset" | "liability" | "equity",
          "section": "Current Assets",
          "label": "Cash and cash equivalents",
          "amount": 12345.67
        },
        ...
      ]
    }
    """
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

    # Parse date string to date, then convert to datetime for MongoDB
    as_of_date_only = _parse_iso_date(as_of_date_str)
    as_of_dt: datetime | None = None
    if as_of_date_only:
        as_of_dt = datetime.combine(as_of_date_only, time.min)

    now = datetime.utcnow()

    # Normalize lines and compute totals
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
            # skip pure zero rows
            continue

        if l_type == "asset":
            total_assets += amount
        elif l_type == "liability":
            total_liab += amount
        elif l_type == "equity":
            total_equity += amount

        norm_lines.append(
            {
                "type": l_type,
                "section": section,
                "label": label,
                "amount": amount,
            }
        )

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
        "as_of_date": as_of_dt,  # NOTE: stored as datetime for Mongo
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
            {
                "$set": doc,
                "$setOnInsert": {"created_at": now},
            },
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
    """
    Export balance sheet as CSV (Excel-compatible).

    Expects form field 'payload' containing JSON with:
    {
      "name": "...",
      "as_of_date": "YYYY-MM-DD",
      "currency": "GHS",
      "lines": [...],
      "totals": {
        "assets": ...,
        "liabilities": ...,
        "equity": ...,
        "liab_plus_equity": ...
      }
    }
    """
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
    totals = data.get("totals") or {}

    out = io.StringIO()
    w = csv.writer(out)

    title = "Balance Sheet"
    if name:
        title += f" - {name}"
    if as_of_date_str:
        title += f" (As at {as_of_date_str})"

    # Title row
    w.writerow([title])
    w.writerow([])

    # Assets
    w.writerow(["ASSETS"])
    w.writerow(["Section", "Account", f"Amount ({currency})"])

    asset_lines = [l for l in lines if (l.get("type") or "").lower() == "asset"]
    current_section = None
    total_assets = 0.0

    for line in asset_lines:
        section = line.get("section") or ""
        label = line.get("label") or ""
        amount = _safe_float(line.get("amount"), 0.0)
        if section != current_section:
            current_section = section
            if section:
                w.writerow([section, "", ""])
        w.writerow(["", label, f"{amount:0.2f}"])
        total_assets += amount

    # Total assets row
    if asset_lines:
        w.writerow([])
        w.writerow(["", "Total Assets", f"{total_assets:0.2f}"])

    w.writerow([])
    w.writerow([])

    # Liabilities & Equity
    w.writerow(["LIABILITIES & EQUITY"])
    w.writerow(["Type", "Section", "Account", f"Amount ({currency})"])

    liab_lines = [l for l in lines if (l.get("type") or "").lower() == "liability"]
    eq_lines = [l for l in lines if (l.get("type") or "").lower() == "equity"]

    total_liab = 0.0
    total_eq = 0.0

    if liab_lines:
        w.writerow(["Liabilities", "", "", ""])
        current_section = None
        for line in liab_lines:
            section = line.get("section") or ""
            label = line.get("label") or ""
            amount = _safe_float(line.get("amount"), 0.0)
            if section != current_section:
                current_section = section
                if section:
                    w.writerow(["", section, "", ""])
            w.writerow(["", "", label, f"{amount:0.2f}"])
            total_liab += amount
        w.writerow(["", "", "Total Liabilities", f"{total_liab:0.2f}"])

    if eq_lines:
        w.writerow([])
        w.writerow(["Equity", "", "", ""])
        current_section = None
        for line in eq_lines:
            section = line.get("section") or ""
            label = line.get("label") or ""
            amount = _safe_float(line.get("amount"), 0.0)
            if section != current_section:
                current_section = section
                if section:
                    w.writerow(["", section, "", ""])
            w.writerow(["", "", label, f"{amount:0.2f}"])
            total_eq += amount
        w.writerow(["", "", "Total Equity", f"{total_eq:0.2f}"])

    w.writerow([])
    w.writerow(["", "", "Total Liabilities + Equity", f"{(total_liab + total_eq):0.2f}"])

    filename_date = (as_of_date_str or date.today().strftime("%Y-%m-%d")).replace("-", "")
    filename = f"balance_sheet_{filename_date}.csv"

    return Response(
        out.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )

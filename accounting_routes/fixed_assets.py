#!/usr/bin/env python3
"""
Fixed Assets Register routes for TRUEtype Services.

When registered in app.py as:
    app.register_blueprint(fixed_assets_bp, url_prefix="/accounting/fixed-assets")

Routes become:
    /accounting/fixed-assets/                        -> register()
    /accounting/fixed-assets/export                  -> export_assets()
    /accounting/fixed-assets/add                     -> add_asset_form()
    /accounting/fixed-assets/compute-depreciation    -> compute_depreciation()
    /accounting/fixed-assets/post-depreciation       -> post_depreciation()
    /accounting/fixed-assets/dispose/<asset_id>      -> dispose_asset()
"""

from flask import (
    Blueprint, render_template, request, redirect,
    url_for, flash, Response
)
from datetime import datetime, date
from db import db
import csv
import io

fixed_assets_col = db["fixed_assets"]

# NOTE: no url_prefix here – it will be applied in app.py
fixed_assets_bp = Blueprint(
    "fixed_assets",
    __name__,
)

# -------------------------------------------------------------------
# Template filter: money (thousand separator, 2dp)
# -------------------------------------------------------------------

@fixed_assets_bp.app_template_filter("money")
def money_filter(value):
    """Format a numeric value with thousand separator and 2 decimals."""
    try:
        return f"{float(value):,.2f}"
    except (TypeError, ValueError):
        return "0.00"


# -------------------------------------------------------------------
# Helpers
# -------------------------------------------------------------------

def _safe_float(doc, key, default=0.0):
    try:
        return float(doc.get(key, default) or 0)
    except (TypeError, ValueError):
        return float(default)


def _parse_date(value):
    """
    For display/formatting only – returns datetime.date or None.
    Never write this back to Mongo (use datetime.datetime when saving).
    """
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except Exception:
        return None


def _format_date(d):
    if isinstance(d, datetime):
        d = d.date()
    if isinstance(d, date):
        return d.strftime("%Y-%m-%d")
    return ""


def _auto_asset_id():
    """
    Generate next asset_id like FA-00001.
    Looks at existing records and increments highest numeric part.
    """
    last = fixed_assets_col.find_one(
        {"asset_id": {"$regex": r"^FA-\d+$"}},
        sort=[("asset_id", -1)]
    )
    if not last:
        return "FA-00001"
    try:
        num = int(str(last["asset_id"]).split("-")[1])
    except Exception:
        num = 0
    return f"FA-{num + 1:05d}"


def _compute_net_book_value(asset):
    cost = _safe_float(asset, "cost", 0)
    accum = _safe_float(asset, "accum_depr", 0)
    nbv = cost - accum
    return nbv if nbv > 0 else 0.0


# -------------------------------------------------------------------
# Main register view
# -------------------------------------------------------------------

@fixed_assets_bp.route("/", methods=["GET"])
def register():
    q = (request.args.get("q") or "").strip()
    category = (request.args.get("category") or "").strip()
    status = (request.args.get("status") or "").strip()

    query = {}
    if q:
        query["$or"] = [
            {"asset_id": {"$regex": q, "$options": "i"}},
            {"name": {"$regex": q, "$options": "i"}},
        ]
    if category:
        query["category"] = category
    if status:
        query["status"] = status

    docs = list(
        fixed_assets_col.find(query).sort("acquisition_date", -1)
    )

    assets = []
    for doc in docs:
        asset = {
            "_id": doc.get("_id"),
            "asset_id": doc.get("asset_id"),
            "name": doc.get("name"),
            "category": doc.get("category"),
            "method": doc.get("method", "SL"),
            "useful_life_years": doc.get("useful_life_years", 0),
            "status": doc.get("status", "Active"),
        }

        cost = _safe_float(doc, "cost", 0)
        accum = _safe_float(doc, "accum_depr", 0)
        asset["cost"] = cost
        asset["accum_depr"] = accum
        asset["net_book_value"] = _compute_net_book_value(doc)

        acq_date = doc.get("acquisition_date")
        asset["acquisition_date_str"] = _format_date(_parse_date(acq_date))

        assets.append(asset)

    categories = sorted([c for c in fixed_assets_col.distinct("category") if c])
    statuses = ["Active", "Fully Depreciated", "Disposed"]

    return render_template(
        "accounting/fixed_assets_register.html",
        assets=assets,
        categories=categories,
        statuses=statuses,
        currency_symbol="GHS ",
    )


# -------------------------------------------------------------------
# Export CSV
# -------------------------------------------------------------------

@fixed_assets_bp.route("/export", methods=["GET"])
def export_assets():
    docs = list(fixed_assets_col.find().sort("acquisition_date", -1))

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([
        "Asset ID",
        "Name",
        "Category",
        "Acquisition Date",
        "Cost",
        "Accumulated Depreciation",
        "Net Book Value",
        "Method",
        "Useful Life (Years)",
        "Status",
    ])

    for doc in docs:
        cost = _safe_float(doc, "cost", 0)
        accum = _safe_float(doc, "accum_depr", 0)
        nbv = _compute_net_book_value(doc)

        writer.writerow([
            doc.get("asset_id", ""),
            doc.get("name", ""),
            doc.get("category", ""),
            _format_date(_parse_date(doc.get("acquisition_date"))),
            f"{cost:,.2f}",
            f"{accum:,.2f}",
            f"{nbv:,.2f}",
            doc.get("method", "SL"),
            doc.get("useful_life_years", 0),
            doc.get("status", "Active"),
        ])

    output.seek(0)
    filename = f"fixed_assets_{date.today().isoformat()}.csv"

    return Response(
        output.getvalue(),
        mimetype="text/csv",
        headers={
            "Content-Disposition": f"attachment; filename={filename}"
        },
    )


# -------------------------------------------------------------------
# Add Asset – POST only (modal submits here)
# -------------------------------------------------------------------

@fixed_assets_bp.route("/add", methods=["POST"])
def add_asset_form():
    name = (request.form.get("name") or "").strip()
    category = (request.form.get("category") or "").strip()
    method = (request.form.get("method") or "SL").strip()
    life_years = request.form.get("useful_life_years") or "0"
    cost_raw = request.form.get("cost") or "0"
    acq_date_raw = request.form.get("acquisition_date") or ""
    notes = (request.form.get("notes") or "").strip()

    if not name:
        flash("Asset name is required.", "error")
        return redirect(url_for("fixed_assets.register"))

    asset_id = _auto_asset_id()

    # 🔧 IMPORTANT: store as datetime.datetime (never datetime.date)
    if acq_date_raw:
        try:
            acquisition_datetime = datetime.strptime(acq_date_raw, "%Y-%m-%d")
        except Exception:
            acquisition_datetime = datetime.utcnow()
    else:
        acquisition_datetime = datetime.utcnow()

    cost = _safe_float({"cost": cost_raw}, "cost", 0)
    try:
        useful_life_years = int(life_years)
    except ValueError:
        useful_life_years = 0

    doc = {
        "asset_id": asset_id,
        "name": name,
        "category": category,
        "method": method or "SL",
        "useful_life_years": useful_life_years,
        "acquisition_date": acquisition_datetime,   # ⬅️ datetime, BSON-safe
        "cost": cost,
        "accum_depr": 0.0,
        "status": "Active",
        "notes": notes,
        "created_at": datetime.utcnow(),
        "updated_at": datetime.utcnow(),
    }

    fixed_assets_col.insert_one(doc)

    flash(f"Asset {asset_id} created.", "success")
    return redirect(url_for("fixed_assets.register"))


# -------------------------------------------------------------------
# Compute & Post Depreciation
# -------------------------------------------------------------------

def _monthly_depreciation_amount(doc):
    """
    Very simple depreciation logic:
      - Straight Line: cost / (useful_life_years * 12)
      - DB: 2 * SL rate * remaining NBV
    Assumes zero salvage value.
    """
    method = (doc.get("method") or "SL").upper()
    useful_life_years = int(doc.get("useful_life_years") or 0)
    if useful_life_years <= 0:
        return 0.0

    cost = _safe_float(doc, "cost", 0)
    accum = _safe_float(doc, "accum_depr", 0)
    nbv = cost - accum
    if nbv <= 0:
        return 0.0

    months = useful_life_years * 12

    if method == "DB":
        annual_rate = 2.0 / useful_life_years
        monthly_rate = annual_rate / 12.0
        dep = nbv * monthly_rate
    else:
        dep = cost / months

    if dep > nbv:
        dep = nbv
    return dep


@fixed_assets_bp.route("/compute-depreciation", methods=["POST"])
def compute_depreciation():
    active = list(
        fixed_assets_col.find({"status": {"$in": ["Active", "Fully Depreciated"]}})
    )

    count_eligible = 0
    total_dep = 0.0

    for doc in active:
        dep = _monthly_depreciation_amount(doc)
        if dep > 0:
            count_eligible += 1
            total_dep += dep

    flash(
        f"Computed depreciation for {count_eligible} asset(s). "
        f"Estimated total for this month: GHS {total_dep:,.2f}.",
        "info",
    )
    return redirect(url_for("fixed_assets.register"))


@fixed_assets_bp.route("/post-depreciation", methods=["POST"])
def post_depreciation():
    active = list(
        fixed_assets_col.find({"status": {"$in": ["Active", "Fully Depreciated"]}})
    )

    updated_count = 0
    for doc in active:
        dep = _monthly_depreciation_amount(doc)
        if dep <= 0:
            continue
        new_accum = _safe_float(doc, "accum_depr", 0) + dep
        cost = _safe_float(doc, "cost", 0)

        status = doc.get("status", "Active")
        if new_accum >= cost:
            new_accum = cost
            status = "Fully Depreciated"

        fixed_assets_col.update_one(
            {"_id": doc["_id"]},
            {
                "$set": {
                    "accum_depr": new_accum,
                    "status": status,
                    "updated_at": datetime.utcnow(),
                }
            },
        )
        updated_count += 1

    flash(f"Posted monthly depreciation for {updated_count} asset(s).", "success")
    return redirect(url_for("fixed_assets.register"))


# -------------------------------------------------------------------
# Dispose asset
# -------------------------------------------------------------------

@fixed_assets_bp.route("/dispose/<asset_id>", methods=["POST"])
def dispose_asset(asset_id):
    doc = fixed_assets_col.find_one({"asset_id": asset_id})
    if not doc:
        flash("Asset not found.", "error")
        return redirect(url_for("fixed_assets.register"))

    fixed_assets_col.update_one(
        {"_id": doc["_id"]},
        {
            "$set": {
                "status": "Disposed",
                "updated_at": datetime.utcnow(),
            }
        },
    )
    flash(f"Asset {asset_id} marked as disposed.", "success")
    return redirect(url_for("fixed_assets.register"))

# accounting_routes/accounts.py
from __future__ import annotations
from flask import Blueprint, render_template, request, redirect, url_for, send_file, flash
from io import StringIO, BytesIO
import csv, math, datetime as dt

from db import db
from services.activity_audit import audit_action
accounts_col = db["accounts"]

# Template folder points one level up (since folder is beside app.py)
accounting_bp = Blueprint("accounting", __name__, template_folder="../templates")

# ---------- Helpers ----------
def _q(s: str | None) -> str:
    return (s or "").strip()

def _paginate(collection, query: dict, page: int, per: int):
    """Pagination using public PyMongo API (count_documents)."""
    total = collection.count_documents(query)
    pages = max(1, math.ceil(total / per))
    page = max(1, min(page, pages))  # clamp

    def _u(p):
        args = request.args.to_dict()
        args["page"] = str(p)
        return url_for("accounting.accounts", **args)

    return {
        "page": page,
        "pages": pages,
        "prev_url": _u(page - 1) if page > 1 else None,
        "next_url": _u(page + 1) if page < pages else None,
        "total": total,
    }

# ---------- Routes ----------
@accounting_bp.get("/")
def home():
    # acts as "dashboard" placeholder; redirects to accounts list
    return redirect(url_for("accounting.accounts"))

@accounting_bp.get("/reports")
def reports_home():
    # simple stub so template link resolves
    flash("Reports home coming soon.", "info")
    return redirect(url_for("accounting.accounts"))

@accounting_bp.get("/accounts")
def accounts():
    """List all accounts with filters + pagination."""
    q = _q(request.args.get("q"))
    typ = _q(request.args.get("type"))
    status = _q(request.args.get("status"))
    page = int(request.args.get("page", 1))
    per = min(50, int(request.args.get("per", 20)))

    query: dict = {}
    if q:
        query["$or"] = [
            {"name": {"$regex": q, "$options": "i"}},
            {"code": {"$regex": q, "$options": "i"}},
        ]
    if typ:
        query["type"] = typ
    if status:
        query["active"] = (status == "active")

    base_cur = "GHS"

    pager = _paginate(accounts_col, query, page, per)
    page = pager["page"]  # use clamped page

    cur = (
        accounts_col.find(query)
        .sort("code", 1)
        .skip((page - 1) * per)
        .limit(per)
    )
    accounts = [
        {
            "code": a.get("code"),
            "name": a.get("name"),
            "type": a.get("type"),
            "currency": a.get("currency", base_cur),
            "allow_post": bool(a.get("allow_post", True)),
            "active": bool(a.get("active", True)),
        }
        for a in cur
    ]

    return render_template(
        "accounting/chart_of_accounts.html",
        accounts=accounts,
        pager=pager,
        base_currency=base_cur,
    )

# ---------- Create (Modal submits here) ----------
@accounting_bp.post("/accounts")
@audit_action("account.created", "Created Account", entity_type="account")
def accounts_create():
    """Create account from modal form POST."""
    code = _q(request.form.get("code"))
    name = _q(request.form.get("name"))
    acc_type = _q(request.form.get("type")).lower()
    currency = _q(request.form.get("currency")) or "GHS"
    allow_post = request.form.get("allow_post") == "on"
    active = request.form.get("active") != "off"  # default True

    if not code or not name or acc_type not in {"asset","liability","equity","income","expense"}:
        flash("Code, Name and a valid Type are required.", "warning")
        return redirect(url_for("accounting.accounts"))

    if accounts_col.find_one({"code": code}):
        flash("Account code already exists.", "warning")
        return redirect(url_for("accounting.accounts"))

    doc = {
        "code": code,
        "name": name,
        "type": acc_type,
        "currency": currency,
        "allow_post": allow_post,
        "active": active,
        "created_at": dt.datetime.utcnow(),
        "updated_at": dt.datetime.utcnow(),
    }
    accounts_col.insert_one(doc)
    flash("Account created.", "success")
    return redirect(url_for("accounting.accounts"))

# ---------- Edit (optional placeholder so link won’t break) ----------
@accounting_bp.get("/accounts/<code>/edit")
def accounts_edit(code):
    flash(f"Edit form for {code} coming soon.", "info")
    return redirect(url_for("accounting.accounts"))

# ---------- Toggle / Import / Export ----------
@accounting_bp.post("/accounts/<code>/toggle")
@audit_action("account.toggled", "Toggled Account Status", entity_type="account", entity_id_from="code")
def accounts_toggle(code):
    """Toggle active/inactive status of an account."""
    a = accounts_col.find_one({"code": code})
    if not a:
        flash("Account not found.", "warning")
        return redirect(url_for("accounting.accounts"))

    accounts_col.update_one(
        {"code": code},
        {
            "$set": {
                "active": not bool(a.get("active", True)),
                "updated_at": dt.datetime.utcnow(),
            }
        },
    )
    flash("Status updated.", "success")
    return redirect(url_for("accounting.accounts", **request.args))

@accounting_bp.get("/accounts/export")
def accounts_export():
    """Export all accounts as CSV."""
    out = StringIO()
    w = csv.writer(out)
    w.writerow(["code", "name", "type", "currency", "allow_post", "active"])

    for a in accounts_col.find({}).sort("code", 1):
        w.writerow(
            [
                a.get("code"),
                a.get("name"),
                a.get("type"),
                a.get("currency", "GHS"),
                int(bool(a.get("allow_post", True))),
                int(bool(a.get("active", True))),
            ]
        )

    mem = BytesIO(out.getvalue().encode("utf-8"))
    mem.seek(0)
    return send_file(
        mem,
        mimetype="text/csv",
        as_attachment=True,
        download_name="chart_of_accounts.csv",
    )

@accounting_bp.post("/accounts/import")
@audit_action("account.imported", "Imported Accounts", entity_type="account")
def accounts_import():
    """Import or update chart of accounts from CSV file."""
    f = request.files.get("file")
    if not f:
        flash("Choose a CSV file.", "warning")
        return redirect(url_for("accounting.accounts"))

    try:
        rdr = csv.DictReader((b.decode("utf-8") for b in f.stream), skipinitialspace=True)
        for r in rdr:
            code = _q(r.get("code"))
            if not code:
                continue

            payload = {
                "code": code,
                "name": _q(r.get("name")),
                "type": _q(r.get("type")).lower(),
                "currency": _q(r.get("currency")) or "GHS",
                "allow_post": r.get("allow_post") in ("1", "true", "True", "yes", "Yes"),
                "active": r.get("active") not in ("0", "false", "False", "no", "No"),
                "updated_at": dt.datetime.utcnow(),
            }

            if accounts_col.find_one({"code": code}):
                accounts_col.update_one({"code": code}, {"$set": payload})
            else:
                payload["created_at"] = dt.datetime.utcnow()
                accounts_col.insert_one(payload)

        flash("Import complete.", "success")
    except Exception as e:
        flash(f"Import failed: {e}", "danger")

    return redirect(url_for("accounting.accounts"))

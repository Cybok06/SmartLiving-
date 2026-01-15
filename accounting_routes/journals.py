# accounting_routes/journals.py
from __future__ import annotations
from flask import Blueprint, render_template, request, redirect, url_for, flash
from bson import ObjectId
import math, datetime as dt
from db import db
from services.activity_audit import audit_action

journals_bp = Blueprint("journals", __name__, template_folder="../templates")
journals_col = db["journals"]

# ---------- helpers ----------
def _q(s): 
    return (s or "").strip()

def _to_float(x):
    try:
        return float(str(x).replace(",", "").strip() or "0")
    except Exception:
        return 0.0

def _as_datetime(d):
    """MongoDB can't store datetime.date; convert to datetime at midnight."""
    if isinstance(d, dt.datetime):
        return d.replace(tzinfo=None)
    if isinstance(d, dt.date):
        return dt.datetime(d.year, d.month, d.day)
    return None

def _paginate(collection, query: dict, page: int, per: int):
    total = collection.count_documents(query)
    pages = max(1, math.ceil(total / per))
    page = max(1, min(page, pages))
    def _u(p): return url_for("journals.journals", page=p, per=per)
    return {
        "total": total, "page": page, "pages": pages,
        "prev_url": _u(page-1) if page > 1 else None,
        "next_url": _u(page+1) if page < pages else None,
    }

# ---------- list ----------
@journals_bp.get("/journals")
def journals():
    page = int(request.args.get("page", 1))
    per  = min(50, int(request.args.get("per", 20)))
    query = {}

    pager = _paginate(journals_col, query, page, per)
    cur = (journals_col.find(query)
           .sort([("date", -1), ("_id", -1)])
           .skip((pager["page"]-1)*per)
           .limit(per))

    rows = []
    for j in cur:
        lines = j.get("lines", [])
        total_dr = sum(_to_float(l.get("debit", 0)) for l in lines)
        total_cr = sum(_to_float(l.get("credit", 0)) for l in lines)

        d = j.get("date")
        if isinstance(d, dt.datetime) or isinstance(d, dt.date):
            date_display = _as_datetime(d).strftime("%b %d, %Y")
        else:
            date_display = ""

        rows.append({
            "id": str(j.get("_id")),
            "date": date_display,
            "ref": j.get("ref"),
            "source": (j.get("source") or "Manual").title(),
            "memo": j.get("memo") or "",
            "total_dr": total_dr,
            "total_cr": total_cr,
            "status": (j.get("status") or "draft").lower(),
        })

    # values for modal defaults
    today = dt.date.today().isoformat()
    ref = f"JE-{dt.datetime.utcnow().strftime('%Y%m%d-%H%M%S')}"
    return render_template("accounting/journals.html", rows=rows, pager=pager, today=today, ref=ref, base_currency="GHS")

# ---------- new (standalone page still works) ----------
@journals_bp.get("/journals/new")
def journal_new():
    today = dt.date.today().isoformat()
    ref = f"JE-{dt.datetime.utcnow().strftime('%Y%m%d-%H%M%S')}"
    return render_template("accounting/journal_new.html", today=today, ref=ref, base_currency="GHS")

# ---------- create ----------
@journals_bp.post("/journals")
@audit_action("journal.created", "Created Journal", entity_type="journal")
def journal_create():
    date_str = _q(request.form.get("date"))
    ref      = _q(request.form.get("ref"))
    source   = _q(request.form.get("source")) or "Manual"
    currency = _q(request.form.get("currency")) or "GHS"
    fx_rate  = _to_float(request.form.get("fx_rate") or 1)
    memo     = _q(request.form.get("memo"))
    action   = _q(request.form.get("action"))  # "post" or "draft"

    accounts = request.form.getlist("account[]")
    partners = request.form.getlist("partner[]")
    descs    = request.form.getlist("desc[]")
    debits   = request.form.getlist("debit[]")
    credits  = request.form.getlist("credit[]")

    lines = []
    n = max(len(accounts), len(debits), len(credits), len(descs), len(partners))
    for i in range(n):
        acc = _q(accounts[i] if i < len(accounts) else "")
        dsc = _q(descs[i]    if i < len(descs)    else "")
        prt = _q(partners[i] if i < len(partners) else "")
        dr  = _to_float(debits[i]  if i < len(debits)  else 0)
        cr  = _to_float(credits[i] if i < len(credits) else 0)
        if not acc and dr == 0 and cr == 0 and not dsc:
            continue
        lines.append({"account": acc, "partner": prt, "desc": dsc, "debit": dr, "credit": cr})

    if not date_str or not ref:
        flash("Date and Reference are required.", "warning")
        return redirect(url_for("journals.journals"))

    try:
        parsed_date = dt.date.fromisoformat(date_str)
    except ValueError:
        flash("Invalid date.", "warning")
        return redirect(url_for("journals.journals"))

    if len(lines) < 1:
        flash("Add at least one journal line.", "warning")
        return redirect(url_for("journals.journals"))

    total_dr = round(sum(l["debit"] for l in lines), 2)
    total_cr = round(sum(l["credit"] for l in lines), 2)

    if action == "post":
        if total_dr != total_cr or total_dr <= 0:
            flash("Entry must be balanced and totals greater than zero to post.", "danger")
            return redirect(url_for("journals.journals"))
        status = "posted"
        posted_at = dt.datetime.utcnow()
    else:
        status = "draft"
        posted_at = None

    doc = {
        "date": _as_datetime(parsed_date),
        "ref": ref,
        "source": source.lower(),
        "currency": currency,
        "fx_rate": fx_rate,
        "memo": memo,
        "lines": lines,
        "status": status,
        "posted_at": posted_at,
        "created_at": dt.datetime.utcnow(),
        "updated_at": dt.datetime.utcnow(),
        "created_by": None,
    }

    journals_col.insert_one(doc)
    flash("Journal posted." if status == "posted" else "Journal saved.", "success")
    return redirect(url_for("journals.journals"))

# ---------- detail view ----------
@journals_bp.get("/journals/<id>")
def journal_view(id: str):
    try:
        doc = journals_col.find_one({"_id": ObjectId(id)})
    except Exception:
        doc = None
    if not doc:
        flash("Journal not found.", "warning")
        return redirect(url_for("journals.journals"))

    # display helpers
    d = doc.get("date")
    date_display = _as_datetime(d).strftime("%b %d, %Y") if d else ""
    total_dr = sum(_to_float(l.get("debit", 0)) for l in doc.get("lines", []))
    total_cr = sum(_to_float(l.get("credit", 0)) for l in doc.get("lines", []))

    return render_template("accounting/journal_view.html",
                           j=doc, date_display=date_display,
                           total_dr=total_dr, total_cr=total_cr)

# ---------- DANGER: clear all journals (POST only) ----------
@journals_bp.post("/journals/clear")
@audit_action("journal.cleared", "Cleared Journals", entity_type="journal")
def journals_clear():
    journals_col.delete_many({})
    flash("All journal entries cleared.", "warning")
    return redirect(url_for("journals.journals"))

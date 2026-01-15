# accounting_routes/ledger.py
from __future__ import annotations
from flask import Blueprint, render_template, request, url_for, Response
from datetime import datetime, date
from typing import List, Dict, Any
import io, csv, math

from db import db

ledger_bp = Blueprint("ledger", __name__, template_folder="../templates")
journals_col = db["journals"]

def _to_dt(d: str | None):
    if not d:
        return None
    try:
        return datetime.fromisoformat(d)
    except Exception:
        return None

def _paginate_url(page: int, per: int) -> str:
    args = request.args.to_dict()
    args["page"] = str(page)
    args["per"] = str(per)
    return url_for("ledger.ledger", **args)

def _fmt_date(d: Any) -> str:
    if isinstance(d, datetime):
        return d.strftime("%b %d, %Y")
    if isinstance(d, date):
        return d.strftime("%b %d, %Y")
    return ""

@ledger_bp.get("/ledger")
def ledger():
    account = (request.args.get("account") or "").strip()
    date_from = _to_dt(request.args.get("from"))
    date_to   = _to_dt(request.args.get("to"))
    status    = (request.args.get("status") or "posted").strip().lower()
    side      = (request.args.get("side") or "debit").strip().lower()
    page      = max(1, int(request.args.get("page", 1)))
    per       = min(100, max(10, int(request.args.get("per", 25))))
    export    = request.args.get("export") == "1"

    q: Dict[str, Any] = {}
    if status in ("posted", "draft"):
        q["status"] = status

    if date_from or date_to:
        q["date"] = {}
        if date_from:
            q["date"]["$gte"] = date_from.replace(hour=0, minute=0, second=0, microsecond=0)
        if date_to:
            q["date"]["$lte"] = date_to.replace(hour=23, minute=59, second=59, microsecond=999999)

    cur = journals_col.find(q).sort([("date", 1), ("_id", 1)])

    rows: List[Dict[str, Any]] = []
    for j in cur:
        j_date = j.get("date")
        ref    = j.get("ref", "")
        for ln in j.get("lines", []):
            acc_name = (ln.get("account") or "").strip()
            if account and acc_name.lower() != account.lower():
                continue
            debit  = float(ln.get("debit") or 0) or 0.0
            credit = float(ln.get("credit") or 0) or 0.0
            rows.append({
                "date": j_date,
                "date_display": _fmt_date(j_date),
                "ref": ref,
                "desc": ln.get("desc") or "",
                "debit": round(debit, 2),
                "credit": round(credit, 2),
            })

    rows.sort(key=lambda r: (r["date"] or datetime.min, r["ref"]))

    running = 0.0
    for r in rows:
        running += (r["debit"] - r["credit"]) if side == "debit" else (r["credit"] - r["debit"])
        r["running"] = round(running, 2)

    tot_deb = round(sum(r["debit"] for r in rows), 2)
    tot_crd = round(sum(r["credit"] for r in rows), 2)

    if export and rows:
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(["Date", "Ref", "Description", "Debit", "Credit", "Running"])
        for r in rows:
            writer.writerow([r["date_display"], r["ref"], r["desc"],
                             f'{r["debit"]:.2f}', f'{r["credit"]:.2f}', f'{r["running"]:.2f}'])
        return Response(output.getvalue(),
                        mimetype="text/csv",
                        headers={"Content-Disposition": 'attachment; filename="general_ledger.csv"'})

    total = len(rows)
    pages = max(1, math.ceil(total / per))
    page  = max(1, min(page, pages))
    start = (page - 1) * per
    end   = start + per
    page_rows = rows[start:end]

    pager = {
        "total": total, "page": page, "pages": pages,
        "prev_url": _paginate_url(page-1, per) if page > 1 else None,
        "next_url": _paginate_url(page+1, per) if page < pages else None,
    }

    account_info = None
    if account:
        account_info = {
            "code": account.split(" - ", 1)[0] if " - " in account else account,
            "name": account.split(" - ", 1)[1] if " - " in account else account,
            "type": "asset" if side == "debit" else "liability",
        }

    export_args = request.args.to_dict(flat=True)
    export_args["export"] = "1"
    export_url = url_for("ledger.ledger", **export_args)

    # Defaults for modal form
    today_iso = date.today().isoformat()
    ref = f"JE-{datetime.utcnow().strftime('%Y%m%d-%H%M%S')}"
    base_currency = "GHS"

    return render_template(
        "accounting/ledger.html",
        rows=page_rows,
        totals={"debit": tot_deb, "credit": tot_crd} if rows else None,
        pager=pager,
        account_info=account_info,
        running_end=(f"{rows[-1]['running']:.2f}" if rows else "0.00"),
        export_url=export_url,
        today_iso=today_iso, ref=ref, base_currency=base_currency,
    )

from __future__ import annotations
from flask import Blueprint, render_template, request, url_for, Response, jsonify
from datetime import datetime, date
import io, csv, math, re
from db import db
from services.activity_audit import audit_action

ar_invoices_bp = Blueprint("ar_invoices", __name__, template_folder="../templates")

inv_col     = db["ar_invoices"]
clients_col = db["clients"]   # 👉 accounting clients master


def _iso(d):
    try:
        return datetime.fromisoformat(d) if d else None
    except Exception:
        return None


def _paginate_url(page: int, per: int) -> str:
    args = request.args.to_dict()
    args["page"] = str(page)
    args["per"]  = str(per)
    return url_for("ar_invoices.invoices", **args)


@ar_invoices_bp.get("/ar/invoices")
def invoices():
    qtxt     = (request.args.get("q") or "").strip()
    customer = (request.args.get("customer") or "").strip()  # client code (param name kept)
    status   = (request.args.get("status") or "").strip().lower()
    dfrom    = _iso(request.args.get("from"))
    dto      = _iso(request.args.get("to"))
    page     = max(1, int(request.args.get("page", 1)))
    per      = min(100, max(25, int(request.args.get("per", 25))))
    export   = request.args.get("export") == "1"

    q: dict = {}
    if qtxt:
        rx = re.compile(re.escape(qtxt), re.IGNORECASE)
        q["$or"] = [
            {"no": rx},
            {"customer": rx},
            {"customer_name": rx},
        ]
    if customer:
        q["customer"] = customer
    if status in ("draft", "sent", "part", "overdue", "paid"):
        q["status"] = status
    if dfrom or dto:
        q["issue_dt"] = {}
        if dfrom:
            q["issue_dt"]["$gte"] = datetime(dfrom.year, dfrom.month, dfrom.day)
        if dto:
            q["issue_dt"]["$lte"] = datetime(dto.year, dto.month, dto.day, 23, 59, 59, 999999)

    cur  = inv_col.find(q).sort([("issue_dt", -1), ("_id", -1)])
    docs = list(cur)

    # quick stats
    today   = datetime.utcnow().date()
    overdue = sum(float(d.get("balance", 0) or 0) for d in docs if d.get("status") == "overdue")
    awaiting = sum(
        float(d.get("balance", 0) or 0)
        for d in docs
        if d.get("status") in ("sent", "part")
    )
    paid30 = 0.0
    for d in docs:
        if d.get("status") != "paid":
            continue
        paid_dt = d.get("paid_dt")
        if isinstance(paid_dt, datetime):
            paid_date = paid_dt.date()
        elif isinstance(paid_dt, date):
            paid_date = paid_dt
        else:
            continue
        if (today - paid_date).days <= 30:
            paid30 += float(d.get("amount", 0) or 0)

    stats = type("S", (object,), dict(overdue=overdue, awaiting=awaiting, paid30=paid30))

    # export CSV
    if export and docs:
        out = io.StringIO()
        w   = csv.writer(out)
        w.writerow(["Invoice", "Client", "Issue", "Due", "Amount (GH₵)", "Balance (GH₵)", "Status"])
        for d in docs:
            w.writerow([
                d.get("no", ""),
                d.get("customer", ""),   # client code in db
                d.get("issue", ""),
                d.get("due", ""),
                f'{float(d.get("amount", 0)):0.2f}',
                f'{float(d.get("balance", 0)):0.2f}',
                d.get("status", ""),
            ])
        return Response(
            out.getvalue(),
            mimetype="text/csv",
            headers={"Content-Disposition": 'attachment; filename="ar_invoices.csv"'},
        )

    total = len(docs)
    pages = max(1, math.ceil(total / per))
    page  = max(1, min(page, pages))
    start = (page - 1) * per
    end   = start + per
    pager = {
        "total": total,
        "page": page,
        "pages": pages,
        "prev_url": _paginate_url(page - 1, per) if page > 1 else None,
        "next_url": _paginate_url(page + 1, per) if page < pages else None,
    }

    export_args = request.args.to_dict(flat=True)
    export_args["export"] = "1"
    export_url = url_for("ar_invoices.invoices", **export_args)

    rows = []
    for d in docs[start:end]:
        rows.append({
            "no": d.get("no", ""),
            "customer": d.get("customer", ""),              # client code
            "customer_name": d.get("customer_name", ""),    # client display name
            "issue": d.get("issue", ""),
            "due":   d.get("due", ""),
            "amount":  float(d.get("amount", 0) or 0),
            "balance": float(d.get("balance", 0) or 0),
            "status":  d.get("status", "draft"),
        })

    # 👉 Clients for dropdown (kept as customers_list for template compatibility)
    client_docs = clients_col.find({}, {"code": 1, "name": 1}).sort([("name", 1)])
    customers_list = [
        {"code": c.get("code", ""), "name": c.get("name", "")}
        for c in client_docs
    ]

    return render_template(
        "accounting/ar_invoices.html",
        rows=rows,
        pager=pager,
        stats=stats,
        export_url=export_url,
        customers_list=customers_list,
    )


@ar_invoices_bp.post("/ar/invoices/quick")
@audit_action("invoice.created", "Created Invoice", entity_type="invoice")
def quick_create():
    def _q(x): return (x or "").strip()
    def _f(x):
        try:
            return float(str(x).replace(",", ""))
        except Exception:
            return 0.0

    no       = _q(request.form.get("no"))
    customer = _q(request.form.get("customer"))  # client code
    issue    = _q(request.form.get("issue"))
    due      = _q(request.form.get("due"))
    amount   = _f(request.form.get("amount"))
    status   = (_q(request.form.get("status")) or "draft").lower()

    if not no or not customer or not issue or not due or amount <= 0:
        return jsonify(ok=False, message="All fields are required and amount > 0."), 400
    if inv_col.find_one({"no": no}):
        return jsonify(ok=False, message="Invoice number exists."), 409

    # Look up client from clients collection
    client   = clients_col.find_one({"code": customer}) or {}
    issue_dt = datetime.fromisoformat(issue)
    due_dt   = datetime.fromisoformat(due)

    inv_col.insert_one({
        "no":            no,
        "customer":      customer,               # client code
        "customer_name": client.get("name", ""), # client name
        "issue":   issue,
        "due":     due,
        "issue_dt": issue_dt,
        "due_dt":   due_dt,
        "amount":   amount,
        "balance":  amount,
        "status":   status,
        "created_at": datetime.utcnow(),
    })
    return jsonify(ok=True)

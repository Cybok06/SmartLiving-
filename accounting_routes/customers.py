from __future__ import annotations
from flask import Blueprint, render_template, request, url_for, Response, jsonify
from datetime import datetime
import io, csv, math, re
from db import db

# NOTE:
# This blueprint is registered in app.py as:
# app.register_blueprint(acc_clients_bp, url_prefix="/accounting", name="acc_clients")
# So the endpoint prefix is "acc_clients", not "customers".
CLIENTS_LIST_ENDPOINT = "acc_clients.customers"
CLIENTS_QUICK_ENDPOINT = "acc_clients.quick_create"

# Keep blueprint name "customers" so import in app.py still works:
# from accounting_routes.customers import customers_bp as acc_clients_bp
customers_bp = Blueprint("customers", __name__, template_folder="../templates")

# 👉 Use a separate "clients" collection for accounting
clients_col = db["clients"]


def _paginate_url(endpoint: str, page: int, per: int) -> str:
    """
    Build a URL for the given endpoint with updated page/per,
    preserving the rest of the current query string.
    """
    args = request.args.to_dict()
    args["page"] = str(page)
    args["per"] = str(per)
    return url_for(endpoint, **args)


def _next_client_code() -> str:
    """
    Generate next client code like CL-0001, CL-0002, ...
    """
    last = clients_col.find_one(
        {"code": {"$regex": r"^CL-\d+$"}},
        sort=[("created_at", -1), ("_id", -1)],
    )
    if not last:
        return "CL-0001"
    m = re.search(r"(\d+)$", last.get("code", ""))
    if not m:
        return "CL-0001"
    num = int(m.group(1)) + 1
    return f"CL-{num:04d}"


@customers_bp.get("/customers")
def customers():
    """
    Accounting Clients master list.
    Endpoint URL name (because of app.register_blueprint name="acc_clients"):
      acc_clients.customers
    """
    qtxt = (request.args.get("q") or "").strip()
    status = (request.args.get("status") or "").strip().lower()
    page = max(1, int(request.args.get("page", 1)))
    per = min(100, max(12, int(request.args.get("per", 12))))
    export = request.args.get("export") == "1"

    q: dict = {}
    if qtxt:
        # search code/name/phone/email
        rx = re.compile(re.escape(qtxt), re.IGNORECASE)
        q["$or"] = [{"code": rx}, {"name": rx}, {"phone": rx}, {"email": rx}]
    if status in ("active", "inactive"):
        q["status"] = status

    cur = clients_col.find(q).sort([("name", 1), ("_id", 1)])
    docs = list(cur)

    # Export
    if export and docs:
        out = io.StringIO()
        w = csv.writer(out)
        w.writerow(
            [
                "Code",
                "Name",
                "Phone",
                "Email",
                "Status",
                "Balance (GH₵)",
                "Bucket",
                "Last Invoice",
                "Last Payment",
            ]
        )
        for d in docs:
            w.writerow(
                [
                    d.get("code", ""),
                    d.get("name", ""),
                    d.get("phone", ""),
                    d.get("email", ""),
                    d.get("status", ""),
                    f'{float(d.get("balance", 0)):0.2f}',
                    d.get("bucket", ""),
                    d.get("last_invoice", ""),
                    d.get("last_payment", ""),
                ]
            )
        return Response(
            out.getvalue(),
            mimetype="text/csv",
            headers={"Content-Disposition": 'attachment; filename="clients.csv"'},
        )

    total = len(docs)
    pages = max(1, math.ceil(total / per))
    page = max(1, min(page, pages))
    start = (page - 1) * per
    end = start + per

    pager = {
        "total": total,
        "page": page,
        "pages": pages,
        "prev_url": _paginate_url(CLIENTS_LIST_ENDPOINT, page - 1, per)
        if page > 1
        else None,
        "next_url": _paginate_url(CLIENTS_LIST_ENDPOINT, page + 1, per)
        if page < pages
        else None,
    }

    export_args = request.args.to_dict(flat=True)
    export_args["export"] = "1"
    export_url = url_for(CLIENTS_LIST_ENDPOINT, **export_args)

    # map to simple rows for template
    rows = []
    for d in docs[start:end]:
        rows.append(
            {
                "code": d.get("code", ""),
                "name": d.get("name", ""),
                "phone": d.get("phone", ""),
                "email": d.get("email", ""),
                "status": d.get("status", "active"),
                "balance": float(d.get("balance", 0) or 0),
                "bucket": d.get("bucket", ""),
                "last_invoice": d.get("last_invoice", ""),
                "last_payment": d.get("last_payment", ""),
            }
        )

    next_client_code = _next_client_code()

    return render_template(
        "accounting/customers.html",   # template file name kept the same
        rows=rows,
        pager=pager,
        export_url=export_url,
        next_client_code=next_client_code,
    )


@customers_bp.post("/customers/quick")
def quick_create():
    """
    Quick-create accounting client (endpoint: acc_clients.quick_create)
    """
    def _q(x: str | None) -> str:
        return (x or "").strip()

    code = _q(request.form.get("code"))
    name = _q(request.form.get("name"))
    phone = _q(request.form.get("phone"))
    email = _q(request.form.get("email"))
    status = (_q(request.form.get("status")) or "active").lower()

    if not name:
        return jsonify(ok=False, message="Name is required."), 400

    # Auto-generate code if not provided
    if not code:
        code = _next_client_code()

    if clients_col.find_one({"code": code}):
        return jsonify(ok=False, message="Code already exists."), 409

    now = datetime.utcnow()
    clients_col.insert_one(
        {
            "code": code,
            "name": name,
            "phone": phone,
            "email": email,
            "status": status,
            "balance": 0.0,
            "bucket": "0-30",
            "created_at": now,
            "updated_at": now,
        }
    )
    return jsonify(ok=True, code=code)

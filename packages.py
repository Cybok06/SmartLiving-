# routes/packages.py
from flask import Blueprint, render_template, request, redirect, url_for, flash, send_file, session, Response
from bson import ObjectId
from datetime import datetime
from io import BytesIO
from db import db

# PDF (pure-Python)
from reportlab.lib.pagesizes import A4, landscape
from reportlab.lib import colors
from reportlab.lib.units import cm
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
from reportlab.lib.styles import getSampleStyleSheet

packages_bp = Blueprint("packages", __name__, template_folder="templates")

users_collection     = db["users"]        # managers, agents, inventory users
packages_collection  = db["packages"]     # submit-for-packaging saved here
customers_collection = db["customers"]
undelivered_items_col = db["undelivered_items"]

# ---------- helpers ----------
def _oid(v):
    try:
        return ObjectId(str(v))
    except Exception:
        return None

def _as_dt(v):
    """Return datetime from possible inputs (datetime, ms/s epoch, or ISO str)."""
    if isinstance(v, datetime):
        return v
    try:
        # ints: ms vs s
        if isinstance(v, (int, float)) or (isinstance(v, str) and v.isdigit()):
            iv = int(v)
            # treat values > 10^12 as milliseconds
            if iv > 10**12:
                return datetime.fromtimestamp(iv / 1000.0)
            return datetime.fromtimestamp(iv)
        if isinstance(v, str):
            # try plain date
            try:
                return datetime.strptime(v, "%Y-%m-%d")
            except:
                # last resort: fromisoformat (may raise)
                return datetime.fromisoformat(v)
    except:
        pass
    return None

def _current_user():
    """Resolve logged-in user from typical session keys."""
    user_id = session.get("user_id") or session.get("manager_id") or session.get("agent_id") or session.get("inventory_id")
    if not user_id:
        return None
    return users_collection.find_one({"_id": _oid(user_id)}, {"password": 0})

def _manager_scope_filter():
    """
    Returns (flt, role, branch) for packages query:
      - Manager: packages where agent_id in agents managed by user._id
      - Inventory/Admin: all, optionally restricted by ?branch=
      - Agent: only their own agent_id
    NOTE: packages.agent_id is stored as STRING hex, not ObjectId.
    """
    user = _current_user()
    if not user:
        return {"_id": {"$in": []}}, None, None

    role   = (user.get("role") or "").lower()
    my_id  = user.get("_id")
    branch = user.get("branch")

    if role == "manager":
        ags = list(users_collection.find({"role": "agent", "manager_id": my_id}, {"_id": 1}))
        ids = [str(a["_id"]) for a in ags]
        return {"agent_id": {"$in": ids}}, role, branch

    if role in ("inventory", "admin"):
        req_branch = (request.args.get("branch") or "").strip()
        if req_branch:
            ags = list(users_collection.find({"role": "agent", "branch": req_branch}, {"_id": 1}))
            ids = [str(a["_id"]) for a in ags]
            return ({"agent_id": {"$in": ids}}, role, req_branch)
        return ({}, role, None)

    if role == "agent":
        return {"agent_id": str(my_id)}, role, branch

    return {"_id": {"$in": []}}, role, branch

def _attach_agent_meta(rows):
    """Attach agent_name & agent_branch to rows (not persisted)."""
    # collect distinct agent_id strings
    ids = sorted({r.get("agent_id") for r in rows if r.get("agent_id")})
    oid_map = {}
    for s in ids:
        try:
            oid_map[s] = ObjectId(s)
        except:
            continue
    if not oid_map:
        return
    agents = users_collection.find({"_id": {"$in": list(oid_map.values())}}, {"name": 1, "branch": 1})
    rev = {str(a["_id"]): {"name": a.get("name"), "branch": a.get("branch")} for a in agents}
    for r in rows:
        meta = rev.get(r.get("agent_id"))
        if meta:
            r["agent_name"]   = meta.get("name")
            r["agent_branch"] = meta.get("branch")

def _render_packages(flt, role, branch, route_name):
    user = _current_user()
    # Search by customer/product (agent name applied after we attach meta)
    q = (request.args.get("q") or "").strip()
    if q:
        flt["$or"] = [
            {"customer_name": {"$regex": q, "$options": "i"}},
            {"product.name":  {"$regex": q, "$options": "i"}},
            {"product_snapshot.name":  {"$regex": q, "$options": "i"}},
        ]

    # Date range on submitted_at
    date_from = (request.args.get("from") or "").strip()
    date_to   = (request.args.get("to")   or "").strip()
    if date_from or date_to:
        dtflt = {}
        if date_from:
            try:
                dtflt["$gte"] = datetime.strptime(date_from, "%Y-%m-%d")
            except:
                pass
        if date_to:
            try:
                dtflt["$lte"] = datetime.strptime(date_to, "%Y-%m-%d").replace(hour=23, minute=59, second=59)
            except:
                pass
        if dtflt:
            flt["submitted_at"] = dtflt

    base_flt = dict(flt)

    # Exclude delivered by default
    include_delivered = request.args.get("include_delivered") == "1"
    if not include_delivered:
        flt["status"] = {"$ne": "delivered"}

    projection = {
        "customer_name": 1,
        "customer_phone": 1,
        "product": 1,
        "product_snapshot": 1,
        "submitted_at": 1,
        "status": 1,
        "agent_id": 1,
        # schema you provided doesn't store agent_name/branch; we attach at runtime
    }
    rows = list(packages_collection.find(flt, projection).sort("submitted_at", 1))

    # Normalize submitted_at and attach agent meta
    for r in rows:
        r["submitted_at"] = _as_dt(r.get("submitted_at"))
    _attach_agent_meta(rows)

    # If user searched agent name, apply in-memory filter now that we have meta
    if q:
        q_low = q.lower()
        rows = [r for r in rows if
                (r.get("agent_name") and q_low in r["agent_name"].lower()) or
                (r.get("customer_name") and q_low in r["customer_name"].lower()) or
                ((r.get("product") or {}).get("name") and q_low in r["product"]["name"].lower()) or
                ((r.get("product_snapshot") or {}).get("name") and q_low in r["product_snapshot"]["name"].lower())
        ]

    for r in rows:
        r["product_display"] = r.get("product_snapshot") or r.get("product") or {}

    # CSV export
    if request.args.get("export") == "1":
        def _gen():
            yield "Agent,Branch,Customer,Product,Amount,Date Submitted,Status\n"
            for p in rows:
                prod = p.get("product_display") or {}
                amt = prod.get("total", "")
                ds  = p.get("submitted_at").strftime("%Y-%m-%d") if isinstance(p.get("submitted_at"), datetime) else ""
                yield f"{p.get('agent_name','')},{p.get('agent_branch','')},{p.get('customer_name','')},{prod.get('name','')},{amt},{ds},{p.get('status','')}\n"
        return Response(_gen(), mimetype="text/csv",
                        headers={"Content-Disposition": "attachment; filename=packages.csv"})

    # Stats: total submitted + confirmed + top agents (within date/search scope)
    try:
        total_submitted = packages_collection.count_documents(base_flt)
        total_confirmed = packages_collection.count_documents({**base_flt, "status": "delivered"})
    except Exception:
        total_submitted = 0
        total_confirmed = 0

    top_agents = []
    try:
        pipe = [
            {"$match": base_flt},
            {"$group": {"_id": "$agent_id", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": 5},
        ]
        agg = list(packages_collection.aggregate(pipe))
        agent_ids = [a["_id"] for a in agg if a.get("_id")]
        agents = list(users_collection.find({"_id": {"$in": [_oid(i) for i in agent_ids if _oid(i)]}}, {"name": 1, "branch": 1}))
        agent_map = {str(a["_id"]): a for a in agents}
        for a in agg:
            aid = str(a.get("_id"))
            meta = agent_map.get(aid, {})
            top_agents.append({
                "agent_id": aid,
                "name": meta.get("name", "Unknown"),
                "branch": meta.get("branch", ""),
                "count": a.get("count", 0)
            })
    except Exception:
        top_agents = []

    # Undelivered items modal list (pending, scoped)
    undelivered_rows = []
    try:
        undelivered_flt = {"status": "pending"}
        if role == "manager" and user:
            mid = user.get("_id")
            undelivered_flt["$or"] = [{"manager_id": mid}, {"manager_id": str(mid)}]
        elif role == "agent" and user:
            undelivered_flt["agent_id"] = str(user.get("_id"))
        elif role in ("inventory", "admin"):
            if branch:
                undelivered_flt["agent_branch"] = branch
        undelivered_rows = list(
            undelivered_items_col.find(undelivered_flt).sort("created_at", -1).limit(50)
        )
        for u in undelivered_rows:
            ts = u.get("created_at")
            if isinstance(ts, datetime):
                u["created_at"] = ts.strftime("%Y-%m-%d")
    except Exception:
        undelivered_rows = []

    # Branch list (inventory/admin convenience)
    branches = users_collection.distinct("branch", {"role": {"$in": ["agent", "manager"]}})
    branches = [b for b in branches if b]
    return render_template("packages.html",
                           packages=rows,
                           role=role,
                           current_branch=branch,
                           branches=branches,
                           packages_route=route_name,
                           package_stats={
                               "total_submitted": total_submitted,
                               "total_confirmed": total_confirmed
                           },
                           top_agents=top_agents,
                           undelivered_rows=undelivered_rows)

# ---------- pages ----------
@packages_bp.route("/packages", methods=["GET"])
def list_packages():
    """
    Branch-based list:
      - Manager => agents under them
      - Inventory/Admin => optional branch filter
      - Agent => self
    Supports: search, date range, include_delivered, CSV export
    """
    flt, role, branch = _manager_scope_filter()
    if flt.get("_id") == {"$in": []}:
        flash("Please log in to view packages.", "warning")
        return redirect(url_for("login.login"))

    return _render_packages(flt, role, branch, "packages.list_packages")


@packages_bp.route("/packages/manager", methods=["GET"])
def list_packages_manager():
    user = _current_user()
    if not user:
        flash("Please log in to view packages.", "warning")
        return redirect(url_for("login.login"))
    if (user.get("role") or "").lower() != "manager":
        flash("Unauthorized access.", "warning")
        return redirect(url_for("packages.list_packages"))

    flt, role, branch = _manager_scope_filter()
    if flt.get("_id") == {"$in": []}:
        flash("Please log in to view packages.", "warning")
        return redirect(url_for("login.login"))
    return _render_packages(flt, role, branch, "packages.list_packages_manager")


@packages_bp.route("/packages/admin", methods=["GET"])
def list_packages_admin():
    user = _current_user()
    if not user:
        flash("Please log in to view packages.", "warning")
        return redirect(url_for("login.login"))
    if (user.get("role") or "").lower() != "admin":
        flash("Unauthorized access.", "warning")
        return redirect(url_for("packages.list_packages"))

    flt = {}
    role = "admin"
    branch = None
    return _render_packages(flt, role, branch, "packages.list_packages_admin")

@packages_bp.route("/packages/generate_pdf", methods=["POST"])
def generate_packages_pdf():
    """
    Requires: selected package IDs + confirm_paid.
    Generates PDF; afterwards sets status='delivered' on those docs.
    """
    user = _current_user()
    if not user:
        flash("Please log in.", "warning")
        return redirect(url_for("login.login"))

    selected_ids = request.form.getlist("package_id")
    confirm_paid = request.form.get("confirm_paid") == "on"

    if not selected_ids:
        flash("No packages selected.", "warning")
        return redirect(url_for("packages.list_packages"))
    if not confirm_paid:
        flash("Please confirm that the items are fully paid before generating.", "warning")
        return redirect(url_for("packages.list_packages"))

    # Scope-limited selection; exclude already delivered
    flt, role, branch = _manager_scope_filter()
    object_ids = [_oid(i) for i in selected_ids if _oid(i)]
    flt.update({"_id": {"$in": object_ids}, "status": {"$ne": "delivered"}})

    rows = list(packages_collection.find(flt))
    if not rows:
        flash("No eligible packages found (maybe already delivered or out of scope).", "warning")
        return redirect(url_for("packages.list_packages"))

    # Normalize + meta
    for r in rows:
        r["submitted_at"] = _as_dt(r.get("submitted_at"))
    _attach_agent_meta(rows)

    # ---- Build PDF ----
    buf   = BytesIO()
    doc   = SimpleDocTemplate(buf, pagesize=landscape(A4),
                              leftMargin=1.0*cm, rightMargin=1.0*cm,
                              topMargin=1.0*cm, bottomMargin=1.0*cm)
    styles = getSampleStyleSheet()
    story  = []

    title_branch = branch or request.args.get("branch") or "All Branches"
    title = Paragraph(
        f"<b>SMART LIVING — Packaging Dispatch List</b>"
        f"<br/><font size=10>Branch: {title_branch}"
        f"&nbsp;&nbsp; Date: {datetime.now().strftime('%Y-%m-%d %H:%M')}</font>",
        styles["Title"]
    )
    story.append(title)
    story.append(Spacer(1, 0.3*cm))

    # Header
    data = [["#", "Agent", "Branch", "Customer", "Product", "Amount (GH₵)", "Date Submitted", "Status"]]

    # Sort by date
    rows.sort(key=lambda r: r.get("submitted_at") or datetime.min)

    total_amount = 0
    for i, r in enumerate(rows, start=1):
        prod = r.get("product_snapshot") or r.get("product") or {}
        amt  = prod.get("total", 0) or 0
        total_amount += float(amt)
        sub  = r.get("submitted_at")
        data.append([
            i,
            r.get("agent_name", "") or "",
            r.get("agent_branch", "") or "",
            r.get("customer_name", "") or "",
            prod.get("name", "") or "",
            amt,
            sub.strftime("%Y-%m-%d") if isinstance(sub, datetime) else "",
            r.get("status", "") or "",
        ])

    # Totals row
    data.append(["", "", "", "", "TOTAL", round(total_amount, 2), "", ""])

    table = Table(
        data,
        colWidths=[1.0*cm, 4.0*cm, 3.0*cm, 4.5*cm, 6.0*cm, 3.0*cm, 3.2*cm, 3.0*cm]
    )
    table.setStyle(TableStyle([
        ("BACKGROUND", (0,0), (-1,0), colors.HexColor("#0d6efd")),
        ("TEXTCOLOR",  (0,0), (-1,0), colors.white),
        ("ALIGN",      (0,0), (-1,-1), "LEFT"),
        ("GRID",       (0,0), (-1,-1), 0.5, colors.grey),
        ("FONTNAME",   (0,0), (-1,0), "Helvetica-Bold"),
        ("FONTNAME",   (0,1), (-1,-2), "Helvetica"),
        ("ROWBACKGROUNDS", (0,1), (-1,-2), [colors.whitesmoke, colors.lightgrey]),
        ("VALIGN",     (0,0), (-1,-1), "MIDDLE"),
        ("FONTSIZE",   (0,0), (-1,-1), 9),
        ("FONTNAME",   (0,-1), (-1,-1), "Helvetica-Bold"),
        ("BACKGROUND", (0,-1), (-1,-1), colors.HexColor("#e9ecef")),
    ]))
    story.append(table)
    doc.build(story)

    pdf_bytes = buf.getvalue()
    buf.close()

    # ---- After successful build, mark delivered ----
    now = datetime.utcnow()
    packages_collection.update_many(
        {"_id": {"$in": [r["_id"] for r in rows]}},
        {"$set": {"status": "delivered", "delivered_at": now, "delivered_by": user.get("_id")}}
    )

    for r in rows:
        customer_id = r.get("customer_id")
        product_index = r.get("product_index")
        if customer_id is None or product_index is None:
            continue
        customers_collection.update_one(
            {"_id": customer_id},
            {"$set": {
                f"purchases.{product_index}.product.status": "delivered",
                f"purchases.{product_index}.delivered_at": now
            }}
        )

    filename = f"packages_{(branch or 'all').lower()}_{datetime.now().strftime('%Y%m%d_%H%M')}.pdf"
    return send_file(BytesIO(pdf_bytes), as_attachment=True, download_name=filename, mimetype="application/pdf")

# ---------- (optional) index hints ----------
# In Mongo shell / migration:
# db.packages.createIndex({ status: 1, agent_id: 1, submitted_at: 1 })
# db.users.createIndex({ role: 1, manager_id: 1, branch: 1 })

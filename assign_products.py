# routes/assign_products.py
from flask import Blueprint, render_template, request, redirect, url_for, flash, session, Response, jsonify
from bson import ObjectId
from datetime import datetime
from db import db

assign_bp = Blueprint("assign", __name__, template_folder="templates")

users_collection         = db["users"]              # managers, agents, inventory/admin
products_collection      = db["products"]           # your existing product catalog
assigned_products_coll   = db["assigned_products"]  # NEW

# ---------- helpers ----------
def _oid(v):
    try: return ObjectId(str(v))
    except: return None

def _current_user():
    uid = session.get("inventory_id") or session.get("user_id") or session.get("manager_id")
    if not uid: return None
    return users_collection.find_one({"_id": _oid(uid)}, {"password": 0})

def _require_inventory_or_admin():
    u = _current_user()
    role = (u or {}).get("role","").lower()
    return (u if role in ("inventory","admin") else None)

def _manager_list():
    cur = users_collection.find({"role": "manager"}, {"name":1,"branch":1})
    return sorted([{"_id":str(x["_id"]), "name":x.get("name",""), "branch":x.get("branch","")} for x in cur],
                  key=lambda r: (r["branch"] or "", r["name"] or ""))

def _product_list():
    cur = products_collection.find({}, {"name":1,"sku":1,"image_url":1})
    return sorted(
        [{"_id":str(x["_id"]), "name":x.get("name",""), "sku":x.get("sku",""), "image_url":x.get("image_url")} for x in cur],
        key=lambda r: (r["name"] or "")
    )

# ---------- list page ----------
@assign_bp.route("/assignments", methods=["GET"])
def list_assignments():
    """
    View + filter the per-manager product assignments.
    Everyone can view:
      - manager sees only their rows
      - inventory/admin see all with filters
    """
    user = _current_user()
    if not user:
        flash("Please log in to continue.", "warning")
        return redirect(url_for("login.login"))

    role = (user.get("role") or "").lower()
    flt = {}
    if role == "manager":
        flt["manager_id"] = str(user["_id"])   # stored as string for easy joins
    # inventory/admin can filter
    branch    = (request.args.get("branch") or "").strip()
    manager_q = (request.args.get("manager_id") or "").strip()
    product_q = (request.args.get("product_id") or "").strip()
    q         = (request.args.get("q") or "").strip()

    if manager_q:
        flt["manager_id"] = manager_q
    if product_q:
        flt["product_id"] = product_q
    if branch:
        flt["manager_branch"] = branch
    if q:
        flt["$or"] = [
            {"product_name":   {"$regex": q, "$options": "i"}},
            {"manager_name":   {"$regex": q, "$options": "i"}},
            {"manager_branch": {"$regex": q, "$options": "i"}},
        ]

    projection = {
        "product_id":1,"product_name":1,"product_sku":1,
        "manager_id":1,"manager_name":1,"manager_branch":1,
        "assigned_total":1,"sent_total":1,"created_at":1,"updated_at":1
    }
    rows = list(assigned_products_coll.find(flt, projection).sort([("manager_branch",1),("manager_name",1),("product_name",1)]))
    # derive remaining
    for r in rows:
        r["_id"] = str(r["_id"])
        r["remaining"] = int(r.get("assigned_total",0)) - int(r.get("sent_total",0))

    # filter options
    managers = _manager_list()
    products = _product_list()
    branches = sorted({ m["branch"] for m in managers if m.get("branch") })

    # CSV export
    if request.args.get("export") == "1":
        def _gen():
            yield "Branch,Manager,Product,SKU,Assigned,Sent,Remaining,Updated At\n"
            for r in rows:
                yield f"{r.get('manager_branch','')},{r.get('manager_name','')},{r.get('product_name','')},{r.get('product_sku','')},{r.get('assigned_total',0)},{r.get('sent_total',0)},{r.get('remaining',0)},{(r.get('updated_at') or '').strftime('%Y-%m-%d %H:%M') if r.get('updated_at') else ''}\n"
        return Response(_gen(), mimetype="text/csv",
                        headers={"Content-Disposition": "attachment; filename=assigned_products.csv"})

    return render_template("assign_products.html",
                           role=role,
                           assignments=rows,
                           managers=managers,
                           products=products,
                           branches=branches)

# ---------- create / increment assignment ----------
@assign_bp.route("/assignments/create", methods=["POST"])
def create_or_increment_assignment():
    """
    Inventory/Admin only.
    Upsert per (manager_id, product_id):
      - increments assigned_total by qty
      - appends an event in history
    """
    user = _require_inventory_or_admin()
    if not user:
        flash("Only Inventory/Admin can assign products.", "danger")
        return redirect(url_for("assign.list_assignments"))

    manager_id = (request.form.get("manager_id") or "").strip()
    product_id = (request.form.get("product_id") or "").strip()
    qty        = request.form.get("qty") or "0"
    note       = (request.form.get("note") or "").strip()

    # validate
    if not (_oid(manager_id) and _oid(product_id)):
        flash("Select a valid manager and product.", "warning")
        return redirect(url_for("assign.list_assignments"))

    try:
        qty = int(qty)
        if qty <= 0: raise ValueError()
    except:
        flash("Quantity must be a positive integer.", "warning")
        return redirect(url_for("assign.list_assignments"))

    mgr = users_collection.find_one({"_id": _oid(manager_id), "role": "manager"}, {"name":1,"branch":1})
    prod = products_collection.find_one({"_id": _oid(product_id)}, {"name":1,"sku":1,"image_url":1})
    if not mgr or not prod:
        flash("Invalid manager or product selected.", "danger")
        return redirect(url_for("assign.list_assignments"))

    now = datetime.utcnow()
    assigned_products_coll.update_one(
        {"manager_id": manager_id, "product_id": product_id},
        {
            "$setOnInsert": {
                "manager_id": manager_id,
                "manager_name": mgr.get("name"),
                "manager_branch": mgr.get("branch"),
                "product_id": product_id,
                "product_name": prod.get("name"),
                "product_sku": prod.get("sku"),
                "product_image": prod.get("image_url"),
                "created_at": now,
                "sent_total": 0
            },
            "$inc": {"assigned_total": qty},
            "$push": {"history": {
                "type": "assign", "qty": qty, "by": str(user["_id"]),
                "at": now, "note": note
            }},
            "$set": {"updated_at": now}
        },
        upsert=True
    )

    flash(f"Assigned {qty} units of '{prod.get('name')}' to {mgr.get('name')} ({mgr.get('branch')}).", "success")
    return redirect(url_for("assign.list_assignments"))

# ---------- send (dispatch) from assignment ----------
@assign_bp.route("/assignments/send", methods=["POST"])
def send_from_assignment():
    """
    Inventory/Admin only.
    Increments sent_total by qty, capped by remaining.
    """
    user = _require_inventory_or_admin()
    if not user:
        flash("Only Inventory/Admin can send products.", "danger")
        return redirect(url_for("assign.list_assignments"))

    assign_id = (request.form.get("assignment_id") or "").strip()
    qty       = request.form.get("send_qty") or "0"
    note      = (request.form.get("note") or "").strip()

    if not _oid(assign_id):
        flash("Invalid assignment.", "warning")
        return redirect(url_for("assign.list_assignments"))

    try:
        qty = int(qty)
        if qty <= 0: raise ValueError()
    except:
        flash("Send quantity must be a positive integer.", "warning")
        return redirect(url_for("assign.list_assignments"))

    doc = assigned_products_coll.find_one({"_id": _oid(assign_id)})
    if not doc:
        flash("Assignment not found.", "danger")
        return redirect(url_for("assign.list_assignments"))

    remaining = int(doc.get("assigned_total",0)) - int(doc.get("sent_total",0))
    if qty > remaining:
        flash(f"Cannot send {qty}; only {remaining} remaining.", "warning")
        return redirect(url_for("assign.list_assignments"))

    now = datetime.utcnow()
    assigned_products_coll.update_one(
        {"_id": _oid(assign_id)},
        {
            "$inc": {"sent_total": qty},
            "$push": {"history": {
                "type": "send", "qty": qty, "by": str(user["_id"]),
                "at": now, "note": note
            }},
            "$set": {"updated_at": now}
        }
    )

    flash(f"Sent {qty} units.", "success")
    return redirect(url_for("assign.list_assignments"))

# ---------- optional: fetch history (AJAX) ----------
@assign_bp.route("/assignments/history/<assign_id>", methods=["GET"])
def get_assignment_history(assign_id):
    doc = assigned_products_coll.find_one({"_id": _oid(assign_id)}, {"history":1})
    if not doc: return jsonify({"history": []})
    hist = doc.get("history") or []
    # normalize for JSON
    out = []
    for h in hist[::-1]:
        out.append({
            "type": h.get("type"),
            "qty": h.get("qty"),
            "by": h.get("by"),
            "at": h.get("at").strftime("%Y-%m-%d %H:%M") if isinstance(h.get("at"), datetime) else str(h.get("at")),
            "note": h.get("note","")
        })
    return jsonify({"history": out})

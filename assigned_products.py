from flask import Blueprint, render_template, request, redirect, url_for, flash
from flask_login import login_required, current_user
from bson import ObjectId
from datetime import datetime
from db import db

assigned_products_bp = Blueprint('assigned_products', __name__, template_folder='templates')

# collections
users_collection           = db["users"]                # managers live here
products_collection        = db["products"]             # full product catalog (may contain dup name+price across branches)
assigned_products_coll     = db["assigned_products"]    # per manager x product assignment

# -------- helpers --------
def _oid(v):
    try: return ObjectId(str(v))
    except: return None

def _role(u):
    return (u.get("role") or "").lower()

def _mgr_list():
    cur = users_collection.find({"role": "manager"}, {"name":1,"branch":1})
    out = []
    for x in cur:
        out.append({
            "_id": str(x["_id"]),
            "name": x.get("name") or "",
            "branch": x.get("branch") or ""
        })
    # sort by branch then name
    out.sort(key=lambda r: (r["branch"], r["name"]))
    return out

def _dedup_products_by_name_price():
    """
    Build a unique list of products grouped by (name, price).
    Each entry carries:
      key, name, price, any sample product_id, all_product_ids (list for info)
    """
    cur = products_collection.find({}, {"name":1, "price":1, "image_url":1, "sku":1})
    groups = {}
    for p in cur:
        name = (p.get("name") or "").strip()
        price = p.get("price", 0)
        key = f"{name}__{price}"
        rec = groups.get(key)
        pid = str(p["_id"])
        if not rec:
            groups[key] = {
                "key": key,
                "name": name,
                "price": price,
                "sample_product_id": pid,
                "all_product_ids": [pid],
            }
        else:
            rec["all_product_ids"].append(pid)
    # sort by name then price
    items = list(groups.values())
    items.sort(key=lambda r: (r["name"].lower(), float(r["price"]) if r["price"] is not None else 0))
    return items

# -------- views --------
@assigned_products_bp.route('/products_sold', methods=['GET'])
@login_required
def products_sold():
    """
    Reworked page:
      - Two Select2 search selects: Product (dedup by name+price) & Manager (shows branch)
      - Optional Branch filter
      - Shows assignment rows for chosen filters (or all)
      - Inline option to edit 'assigned_total' for a manager × product
    Permissions:
      - Inventory/Admin: can view all & update
      - Manager: sees their own rows; cannot update by default
    """
    u = current_user
    role = _role(u)

    # filters
    f_key       = (request.args.get("product_key") or "").strip()     # dedup key: name__price
    f_manager   = (request.args.get("manager_id") or "").strip()
    f_branch    = (request.args.get("branch") or "").strip()
    free_text   = (request.args.get("q") or "").strip()

    # sources for filters
    managers = _mgr_list()
    branches = sorted({ m["branch"] for m in managers if m["branch"] })
    products_unique = _dedup_products_by_name_price()

    # build query for assigned_products
    flt = {}
    # role scoping
    if role == "manager":
        flt["manager_id"] = str(u.id)

    # manager filter (inventory/admin only)
    if f_manager and role in ("inventory", "admin"):
        flt["manager_id"] = f_manager

    # branch filter (inventory/admin only)
    if f_branch and role in ("inventory", "admin"):
        flt["manager_branch"] = f_branch

    # product_key → we don’t have the price on assigned doc always; we match by product_name & price using products collection
    # But assigned_products stores product_id, product_name, maybe sku. Safest path:
    #   Find all product_ids matching that (name, price) pair, then filter assigned on those product_ids.
    if f_key:
        # parse key
        try:
            name, price = f_key.split("__", 1)
        except ValueError:
            name, price = f_key, ""
        # find all products with same pair
        ids = []
        q = {"name": name}
        # handle price number
        try:
            price_val = float(price)
            q["price"] = price_val
        except:
            pass
        for p in products_collection.find(q, {"_id":1}):
            ids.append(str(p["_id"]))
        if ids:
            flt["product_id"] = {"$in": ids}
        else:
            # no matches → show nothing
            flt["product_id"] = "__none__"

    # free text search over a few fields
    if free_text:
        flt["$or"] = [
            {"manager_name": {"$regex": free_text, "$options":"i"}},
            {"manager_branch": {"$regex": free_text, "$options":"i"}},
            {"product_name": {"$regex": free_text, "$options":"i"}},
            {"product_sku": {"$regex": free_text, "$options":"i"}},
        ]

    projection = {
        "manager_id":1,"manager_name":1,"manager_branch":1,
        "product_id":1,"product_name":1,"product_sku":1,
        "assigned_total":1,"sent_total":1,"updated_at":1
    }
    rows = list(assigned_products_coll.find(flt, projection).sort([("manager_branch",1),("manager_name",1),("product_name",1)]))

    # enrich + derive remaining + attach the dedup key (name__price) for each row
    # (so editing can validate the chosen product group)
    # fetch price quickly per product_id
    pid_to_price = {}
    pids = list({ r.get("product_id") for r in rows if r.get("product_id") })
    if pids:
        for p in products_collection.find({"_id": {"$in": [ _oid(pid) for pid in pids if _oid(pid) ]}}, {"price":1}):
            pid_to_price[str(p["_id"])] = p.get("price", 0)

    for r in rows:
        r["_id"] = str(r["_id"])
        assigned = int(r.get("assigned_total", 0) or 0)
        sent     = int(r.get("sent_total", 0) or 0)
        r["remaining"] = max(0, assigned - sent)
        # dedup key for display
        price = pid_to_price.get(r.get("product_id"), 0)
        name  = (r.get("product_name") or "").strip()
        r["product_key"] = f"{name}__{price}"

    can_edit = role in ("inventory", "admin")

    return render_template(
        'products_sold.html',
        role=role,
        managers=managers,
        branches=branches,
        products_unique=products_unique,
        rows=rows,
        # current filter values
        f_key=f_key, f_manager=f_manager, f_branch=f_branch, free_text=free_text,
        can_edit=can_edit
    )

@assigned_products_bp.route('/products_sold/update_quantity', methods=['POST'])
@login_required
def update_assigned_quantity():
    """
    Update the 'assigned_total' for a specific assignment document.
    Keeps sent_total as-is and ensures assigned_total >= sent_total.
    Inventory/Admin only.
    """
    # basic perm
    role = _role(current_user)
    if role not in ("inventory", "admin"):
        flash("Only Inventory/Admin can update quantities.", "danger")
        return redirect(url_for('assigned_products.products_sold'))

    assignment_id = request.form.get("assignment_id") or ""
    new_qty       = request.form.get("new_assigned") or ""
    note          = (request.form.get("note") or "").strip()

    if not _oid(assignment_id):
        flash("Invalid assignment.", "warning")
        return redirect(url_for('assigned_products.products_sold'))

    try:
        new_qty = int(new_qty)
        if new_qty < 0: raise ValueError()
    except:
        flash("Assigned quantity must be a non-negative integer.", "warning")
        return redirect(url_for('assigned_products.products_sold'))

    doc = assigned_products_coll.find_one({"_id": _oid(assignment_id)})
    if not doc:
        flash("Assignment not found.", "danger")
        return redirect(url_for('assigned_products.products_sold'))

    sent_total = int(doc.get("sent_total", 0) or 0)
    if new_qty < sent_total:
        flash(f"Assigned cannot be less than already sent ({sent_total}).", "warning")
        return redirect(url_for('assigned_products.products_sold'))

    now = datetime.utcnow()
    assigned_products_coll.update_one(
        {"_id": _oid(assignment_id)},
        {
            "$set": {"assigned_total": new_qty, "updated_at": now},
            "$push": {"history": {
                "type": "adjust_assigned",
                "qty": new_qty,
                "by": str(current_user.id),
                "at": now,
                "note": note
            }}
        }
    )

    flash("Assigned quantity updated.", "success")
    return redirect(url_for('assigned_products.products_sold'))

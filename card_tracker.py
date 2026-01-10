
from flask import Blueprint, render_template, request, redirect, url_for, flash, Response, jsonify
from bson import ObjectId
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import csv
import io
import re
import uuid

from db import db
from card_sales_helper import sold_counts_by_name
from login import get_current_identity

card_tracker_bp = Blueprint("card_tracker", __name__, url_prefix="/cards")

products_col = db["products"]
users_col = db["users"]
card_balances_col = db["card_balances"]
card_movements_col = db["card_movements"]
card_print_batches_col = db["card_print_batches"]
customers_col = db["customers"]
instant_sales_col = db["instant_sales"]

ADMIN_POOL_ID = "pool"


def _ensure_card_indexes():
    try:
        card_balances_col.create_index(
            [("holder_type", 1), ("holder_id", 1), ("product_key", 1)],
            unique=True,
        )
        card_balances_col.create_index([("updated_at", -1)])
        card_movements_col.create_index([("product_key", 1), ("created_at", -1)])
        card_movements_col.create_index([("to_id", 1), ("created_at", -1)])
        card_movements_col.create_index([("from_id", 1), ("created_at", -1)])
        card_movements_col.create_index([("to_id", 1), ("product_name", 1), ("created_at", 1)])
        card_print_batches_col.create_index([("product_key", 1), ("created_at", -1)])
        card_print_batches_col.create_index([("batch_code", 1)], unique=True)
        customers_col.create_index([("agent_id", 1)])
        customers_col.create_index([("purchases.agent_id", 1)])
        customers_col.create_index([("manager_id", 1)])
        customers_col.create_index([("purchases.purchase_date", 1)])
        customers_col.create_index([("purchases.product.name", 1)])
        instant_sales_col.create_index([("agent_id", 1), ("purchase_date", 1)])
        instant_sales_col.create_index([("manager_id", 1)])
        instant_sales_col.create_index([("product.name", 1)])
    except Exception:
        pass


_ensure_card_indexes()


def normalize_name(name: str) -> str:
    if not name:
        return ""
    s = str(name).strip().lower()
    s = re.sub(r"\s+", " ", s)
    return s


def normalize_oid(val):
    if isinstance(val, ObjectId):
        return val
    if val is None:
        return ""
    sval = str(val).strip()
    if ObjectId.is_valid(sval):
        return ObjectId(sval)
    return sval


def id_variants(val):
    if isinstance(val, ObjectId):
        return [val, str(val)]
    if val is None:
        return []
    sval = str(val).strip()
    if ObjectId.is_valid(sval):
        return [ObjectId(sval), sval]
    return [sval]


def safe_int(val, default=0):
    try:
        return int(val)
    except Exception:
        return default


def parse_date(val):
    if not val:
        return None
    try:
        return datetime.strptime(val, "%Y-%m-%d")
    except Exception:
        return None


def _month_bounds(month_str: str):
    try:
        start = datetime.strptime(month_str, "%Y-%m")
    except Exception:
        now = datetime.utcnow()
        start = datetime(now.year, now.month, 1)
    end = start + relativedelta(months=1)
    return start, end


def _range_bounds(args):
    from_str = (args.get("from") or "").strip()
    to_str = (args.get("to") or "").strip()
    month = (args.get("month") or "").strip()

    if from_str or to_str:
        start_raw = parse_date(from_str) or parse_date(to_str)
        end_raw = parse_date(to_str) or start_raw
        if not start_raw:
            start_raw = datetime.utcnow()
        if not end_raw:
            end_raw = start_raw
        if end_raw < start_raw:
            start_raw, end_raw = end_raw, start_raw
        start_dt = datetime.combine(start_raw.date(), datetime.min.time())
        end_dt = datetime.combine(end_raw.date(), datetime.min.time()) + timedelta(days=1)
        return start_dt, end_dt, start_raw.strftime("%Y-%m-%d"), end_raw.strftime("%Y-%m-%d")

    start, end = _month_bounds(month)
    start_str = start.strftime("%Y-%m-%d")
    end_str = (end - timedelta(days=1)).strftime("%Y-%m-%d")
    return start, end, start_str, end_str


def require_role(*roles):
    def decorator(fn):
        def wrapper(*args, **kwargs):
            ident = get_current_identity()
            if not ident.get("is_authenticated"):
                return redirect(url_for("login.login", next=request.path))
            if roles and ident.get("role") not in roles:
                return "Forbidden", 403
            return fn(*args, **kwargs)
        wrapper.__name__ = fn.__name__
        return wrapper
    return decorator


def get_user_doc(user_id):
    if not user_id:
        return None
    if isinstance(user_id, ObjectId):
        return users_col.find_one({"_id": user_id})
    if ObjectId.is_valid(str(user_id)):
        return users_col.find_one({"_id": ObjectId(str(user_id))})
    return users_col.find_one({"_id": user_id})


def _dedupe_products(products):
    unique_products = {}
    for p in products:
        name = p.get("name", "")
        key = normalize_name(name)
        if not key:
            continue
        if key not in unique_products:
            unique_products[key] = p
            continue
        existing = unique_products[key]
        if not existing.get("image_url") and p.get("image_url"):
            unique_products[key] = p
        else:
            cur_dt = existing.get("created_at")
            new_dt = p.get("created_at")
            if new_dt and cur_dt and new_dt > cur_dt:
                unique_products[key] = p
    return unique_products


def _unique_product_list(products):
    unique_products = _dedupe_products(products)
    rows = []
    for key, p in unique_products.items():
        rows.append(
            {
                "product_key": key,
                "name": p.get("name", ""),
                "image_url": p.get("image_url", ""),
                "cash_price": p.get("cash_price", 0),
                "price": p.get("price", 0),
                "description": p.get("description", ""),
                "sample_product_id": p.get("_id"),
            }
        )
    rows.sort(key=lambda x: (x.get("name") or "").lower())
    return rows


def _find_product_by_key(products, product_key):
    key = normalize_name(product_key)
    if not key:
        return None
    unique_products = _dedupe_products(products)
    return unique_products.get(key)


def get_balance(holder_type, holder_id, product_key):
    variants = id_variants(holder_id)
    if not variants:
        return 0
    doc = card_balances_col.find_one(
        {"holder_type": holder_type, "holder_id": {"$in": variants}, "product_key": product_key}
    )
    return safe_int(doc.get("qty_on_hand") if doc else 0, 0)


def inc_balance(holder_type, holder_id, product_key, delta, meta=None):
    meta = meta or {}
    delta_val = safe_int(delta, 0)
    if delta_val == 0:
        return
    holder_norm = normalize_oid(holder_id)
    update = {
        "$inc": {"qty_on_hand": delta_val},
        "$set": {
            "updated_at": datetime.utcnow(),
        },
        "$setOnInsert": {
            "holder_type": holder_type,
            "holder_id": holder_norm,
            "product_key": product_key,
        },
    }
    for k in ("branch", "holder_name", "product_name", "product_image_url", "sample_product_id"):
        if meta.get(k) is not None:
            update["$set"][k] = meta.get(k)

    card_balances_col.update_one(
        {"holder_type": holder_type, "holder_id": holder_norm, "product_key": product_key},
        update,
        upsert=True,
    )


def dec_balance_if_enough(holder_type, holder_id, product_key, qty):
    qty_val = safe_int(qty, 0)
    if qty_val <= 0:
        return False
    for hid in id_variants(holder_id):
        res = card_balances_col.update_one(
            {
                "holder_type": holder_type,
                "holder_id": hid,
                "product_key": product_key,
                "qty_on_hand": {"$gte": qty_val},
            },
            {"$inc": {"qty_on_hand": -qty_val}, "$set": {"updated_at": datetime.utcnow()}},
        )
        if res.modified_count == 1:
            return True
    return False


def write_movement(from_type, from_id, to_type, to_id, product_snapshot, qty, note, created_by_user):
    product_snapshot = product_snapshot or {}
    created_by_user = created_by_user or {}
    doc = {
        "from_type": from_type,
        "from_id": from_id,
        "to_type": to_type,
        "to_id": to_id,
        "product_key": product_snapshot.get("product_key", ""),
        "product_name": product_snapshot.get("product_name", ""),
        "product_image_url": product_snapshot.get("product_image_url", ""),
        "sample_product_id": product_snapshot.get("sample_product_id"),
        "qty": safe_int(qty, 0),
        "note": note or "",
        "created_at": datetime.utcnow(),
        "created_by_id": str(created_by_user.get("_id")) if created_by_user.get("_id") else "",
        "created_by_role": (created_by_user.get("role") or ""),
        "from_name": created_by_user.get("from_name"),
        "to_name": created_by_user.get("to_name"),
        "branch": created_by_user.get("branch"),
    }
    card_movements_col.insert_one(doc)


def get_admin_pool_id():
    return ADMIN_POOL_ID


def _product_snapshot_from_row(row):
    return {
        "product_key": row.get("product_key", ""),
        "product_name": row.get("name", ""),
        "product_image_url": row.get("image_url", ""),
        "sample_product_id": row.get("sample_product_id"),
    }


def _duplicate_product_detector():
    pipeline = [
        {"$group": {"_id": {"$toLower": "$name"}, "count": {"$sum": 1}, "name": {"$first": "$name"}}},
        {"$match": {"count": {"$gt": 1}}},
        {"$sort": {"count": -1, "name": 1}},
    ]
    return list(products_col.aggregate(pipeline))

@card_tracker_bp.route("/admin/print", methods=["GET", "POST"])
@require_role("admin")
def admin_print_cards():
    ident = get_current_identity()
    admin_user = get_user_doc(ident.get("user_id")) or {}

    products = list(
        products_col.find(
            {},
            {"name": 1, "image_url": 1, "cash_price": 1, "price": 1, "description": 1, "created_at": 1},
        )
    )
    product_rows = _unique_product_list(products)
    product_lookup = {row["product_key"]: row for row in product_rows}

    if request.method == "POST":
        product_key = normalize_name(request.form.get("product_key"))
        qty_printed = safe_int(request.form.get("qty_printed"), 0)
        note = (request.form.get("note") or "").strip()

        if not product_key:
            flash("Invalid product selected.", "danger")
            return redirect(url_for("card_tracker.admin_print_cards"))

        if qty_printed <= 0:
            flash("Quantity must be at least 1.", "danger")
            return redirect(url_for("card_tracker.admin_print_cards"))

        product_row = product_lookup.get(product_key)
        if not product_row:
            flash("Product not found.", "danger")
            return redirect(url_for("card_tracker.admin_print_cards"))

        batch_code = uuid.uuid4().hex[:8].upper()
        for _ in range(3):
            if not card_print_batches_col.find_one({"batch_code": batch_code}):
                break
            batch_code = uuid.uuid4().hex[:8].upper()

        product_snapshot = _product_snapshot_from_row(product_row)

        card_print_batches_col.insert_one(
            {
                "product_key": product_snapshot["product_key"],
                "product_name": product_snapshot["product_name"],
                "product_image_url": product_snapshot["product_image_url"],
                "sample_product_id": product_snapshot["sample_product_id"],
                "qty_printed": qty_printed,
                "batch_code": batch_code,
                "note": note,
                "created_at": datetime.utcnow(),
                "created_by_id": str(admin_user.get("_id")) if admin_user.get("_id") else "",
            }
        )

        inc_balance(
            "admin",
            get_admin_pool_id(),
            product_snapshot["product_key"],
            qty_printed,
            {
                "holder_name": "Admin Pool",
                "product_name": product_snapshot["product_name"],
                "product_image_url": product_snapshot["product_image_url"],
                "sample_product_id": product_snapshot["sample_product_id"],
            },
        )

        write_movement(
            "print",
            batch_code,
            "admin",
            get_admin_pool_id(),
            product_snapshot,
            qty_printed,
            (note or f"Print batch {batch_code}"),
            {
                "_id": admin_user.get("_id"),
                "role": "admin",
                "from_name": "Print Batch",
                "to_name": "Admin Pool",
            },
        )

        flash("Print batch recorded and stock added to admin pool.", "success")
        return redirect(url_for("card_tracker.admin_print_cards"))

    batches = list(card_print_batches_col.find().sort("created_at", -1).limit(25))

    return render_template(
        "admin_print_cards.html",
        products=product_rows,
        batches=batches,
    )


@card_tracker_bp.route("/admin/distribute", methods=["GET", "POST"])
@require_role("admin")
def admin_distribute_cards():
    ident = get_current_identity()
    admin_user = get_user_doc(ident.get("user_id")) or {}

    products = list(
        products_col.find(
            {},
            {"name": 1, "image_url": 1, "cash_price": 1, "price": 1, "description": 1, "created_at": 1},
        )
    )
    product_rows = _unique_product_list(products)
    product_lookup = {row["product_key"]: row for row in product_rows}

    if request.method == "POST":
        product_key = normalize_name(request.form.get("product_key"))
        manager_id = request.form.get("manager_id")
        qty = safe_int(request.form.get("qty"), 0)
        note = (request.form.get("note") or "").strip()

        if not product_key:
            flash("Invalid product selected.", "danger")
            return redirect(url_for("card_tracker.admin_distribute_cards"))

        if not manager_id:
            flash("Please select a manager.", "danger")
            return redirect(url_for("card_tracker.admin_distribute_cards"))

        if qty <= 0:
            flash("Quantity must be at least 1.", "danger")
            return redirect(url_for("card_tracker.admin_distribute_cards"))

        product_row = product_lookup.get(product_key)
        if not product_row:
            flash("Product not found.", "danger")
            return redirect(url_for("card_tracker.admin_distribute_cards"))

        manager_doc = get_user_doc(manager_id)
        if not manager_doc or (manager_doc.get("role") or "").lower() != "manager":
            flash("Manager not found.", "danger")
            return redirect(url_for("card_tracker.admin_distribute_cards"))

        if not dec_balance_if_enough("admin", get_admin_pool_id(), product_key, qty):
            flash("Admin pool does not have enough cards.", "danger")
            return redirect(url_for("card_tracker.admin_distribute_cards"))

        product_snapshot = _product_snapshot_from_row(product_row)

        inc_balance(
            "manager",
            manager_doc.get("_id"),
            product_key,
            qty,
            {
                "holder_name": manager_doc.get("name", ""),
                "branch": manager_doc.get("branch"),
                "product_name": product_snapshot["product_name"],
                "product_image_url": product_snapshot["product_image_url"],
                "sample_product_id": product_snapshot["sample_product_id"],
            },
        )

        write_movement(
            "admin",
            get_admin_pool_id(),
            "manager",
            manager_doc.get("_id"),
            product_snapshot,
            qty,
            note,
            {
                "_id": admin_user.get("_id"),
                "role": "admin",
                "from_name": "Admin Pool",
                "to_name": manager_doc.get("name", ""),
                "branch": manager_doc.get("branch"),
            },
        )

        flash("Cards distributed to manager.", "success")
        return redirect(url_for("card_tracker.admin_distribute_cards"))

    managers = list(users_col.find({"role": "manager"}, {"name": 1, "branch": 1}).sort("name", 1))
    pool_balances = list(card_balances_col.find({"holder_type": "admin", "holder_id": get_admin_pool_id()}))

    balance_map = {b.get("product_key"): safe_int(b.get("qty_on_hand"), 0) for b in pool_balances}

    return render_template(
        "admin_distribute_cards.html",
        products=product_rows,
        managers=managers,
        balance_map=balance_map,
    )


@card_tracker_bp.route("/admin/dashboard")
@require_role("admin")
def admin_cards_dashboard():
    return redirect(url_for("admin_cards_dashboard.admin_cards_dashboard"))


def _build_history_query(args, scope=None):
    filters = []

    product_key = normalize_name(args.get("product_key"))
    if product_key:
        filters.append({"product_key": product_key})

    to_type = (args.get("to_type") or "").strip()
    if to_type:
        filters.append({"to_type": to_type})

    date_from = parse_date(args.get("date_from"))
    date_to = parse_date(args.get("date_to"))
    if date_from or date_to:
        date_filter = {}
        if date_from:
            date_filter["$gte"] = date_from
        if date_to:
            date_filter["$lt"] = date_to + timedelta(days=1)
        filters.append({"created_at": date_filter})

    manager_id = args.get("manager_id")
    if manager_id:
        variants = id_variants(manager_id)
        if variants:
            filters.append(
                {
                    "$or": [
                        {"from_type": "manager", "from_id": {"$in": variants}},
                        {"to_type": "manager", "to_id": {"$in": variants}},
                    ]
                }
            )

    agent_id = args.get("agent_id")
    if agent_id:
        variants = id_variants(agent_id)
        if variants:
            filters.append(
                {
                    "$or": [
                        {"from_type": "agent", "from_id": {"$in": variants}},
                        {"to_type": "agent", "to_id": {"$in": variants}},
                    ]
                }
            )

    if scope and scope.get("manager_id"):
        variants = id_variants(scope.get("manager_id"))
        if variants:
            filters.append({"$or": [{"from_id": {"$in": variants}}, {"to_id": {"$in": variants}}]})

    if not filters:
        return {}
    return {"$and": filters}


@card_tracker_bp.route("/admin/history")
@require_role("admin")
def admin_cards_history():
    query = _build_history_query(request.args)
    movements = list(card_movements_col.find(query).sort("created_at", -1).limit(500))

    products = list(products_col.find({}, {"name": 1, "image_url": 1, "created_at": 1}))
    product_rows = _unique_product_list(products)

    managers = list(users_col.find({"role": "manager"}, {"name": 1, "branch": 1}).sort("name", 1))
    agents = list(users_col.find({"role": "agent"}, {"name": 1}).sort("name", 1))

    return render_template(
        "admin_cards_history.html",
        movements=movements,
        products=product_rows,
        managers=managers,
        agents=agents,
    )


@card_tracker_bp.route("/admin/history.csv")
@require_role("admin")
def admin_cards_history_csv():
    query = _build_history_query(request.args)
    movements = list(card_movements_col.find(query).sort("created_at", -1))

    def row_iter():
        header = [
            "created_at",
            "product_key",
            "product_name",
            "qty",
            "from_type",
            "from_id",
            "to_type",
            "to_id",
            "note",
            "branch",
            "created_by_role",
            "created_by_id",
        ]
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(header)
        yield output.getvalue()
        output.seek(0)
        output.truncate(0)

        for m in movements:
            writer.writerow(
                [
                    m.get("created_at").isoformat() if m.get("created_at") else "",
                    m.get("product_key", ""),
                    m.get("product_name", ""),
                    m.get("qty", 0),
                    m.get("from_type", ""),
                    m.get("from_id", ""),
                    m.get("to_type", ""),
                    m.get("to_id", ""),
                    m.get("note", ""),
                    m.get("branch", ""),
                    m.get("created_by_role", ""),
                    m.get("created_by_id", ""),
                ]
            )
            yield output.getvalue()
            output.seek(0)
            output.truncate(0)

    headers = {
        "Content-Disposition": "attachment; filename=card_movements.csv",
        "Content-Type": "text/csv",
    }
    return Response(row_iter(), headers=headers)

@card_tracker_bp.route("/manager/stock")
@require_role("manager")
def manager_card_stock():
    ident = get_current_identity()
    manager_doc = get_user_doc(ident.get("user_id")) or {}
    manager_id = manager_doc.get("_id")

    variants = id_variants(manager_id)
    balances = list(card_balances_col.find({"holder_type": "manager", "holder_id": {"$in": variants}}))

    given_map = {
        row.get("_id"): row.get("total", 0)
        for row in card_movements_col.aggregate(
            [
                {"$match": {"from_type": "manager", "from_id": {"$in": variants}}},
                {"$group": {"_id": "$product_key", "total": {"$sum": "$qty"}}},
            ]
        )
    }

    rows = []
    for row in balances:
        key = row.get("product_key")
        rows.append(
            {
                "product_key": key,
                "product_name": row.get("product_name", ""),
                "product_image_url": row.get("product_image_url", ""),
                "sample_product_id": row.get("sample_product_id"),
                "qty": safe_int(row.get("qty_on_hand"), 0),
                "given_to_agents": given_map.get(key, 0),
            }
        )

    rows.sort(key=lambda x: (x.get("product_name") or "").lower())

    return render_template(
        "manager_cards_stock.html",
        rows=rows,
    )


@card_tracker_bp.route("/manager/distribute", methods=["GET", "POST"])
@require_role("manager")
def manager_distribute_cards():
    ident = get_current_identity()
    manager_doc = get_user_doc(ident.get("user_id")) or {}
    manager_id = manager_doc.get("_id")
    variants = id_variants(manager_id)

    products = list(
        products_col.find(
            {"manager_id": {"$in": variants}},
            {"name": 1, "image_url": 1, "cash_price": 1, "price": 1, "description": 1, "created_at": 1},
        )
    )
    product_rows = _unique_product_list(products)
    product_lookup = {row["product_key"]: row for row in product_rows}

    if request.method == "POST":
        product_key = normalize_name(request.form.get("product_key"))
        agent_id = request.form.get("agent_id")
        qty = safe_int(request.form.get("qty"), 0)
        note = (request.form.get("note") or "").strip()

        if not product_key:
            flash("Invalid product selected.", "danger")
            return redirect(url_for("card_tracker.manager_distribute_cards"))

        if not agent_id:
            flash("Please select an agent.", "danger")
            return redirect(url_for("card_tracker.manager_distribute_cards"))

        if qty <= 0:
            flash("Quantity must be at least 1.", "danger")
            return redirect(url_for("card_tracker.manager_distribute_cards"))

        product_row = product_lookup.get(product_key)
        if not product_row:
            flash("Product not found.", "danger")
            return redirect(url_for("card_tracker.manager_distribute_cards"))

        agent_doc = get_user_doc(agent_id)
        if not agent_doc or (agent_doc.get("role") or "").lower() != "agent":
            flash("Agent not found.", "danger")
            return redirect(url_for("card_tracker.manager_distribute_cards"))

        agent_manager_id = agent_doc.get("manager_id")
        if variants and agent_manager_id not in variants and str(agent_manager_id) not in [str(v) for v in variants]:
            flash("Agent is not assigned to your branch.", "danger")
            return redirect(url_for("card_tracker.manager_distribute_cards"))

        if not dec_balance_if_enough("manager", manager_id, product_key, qty):
            flash("You do not have enough cards for this product.", "danger")
            return redirect(url_for("card_tracker.manager_distribute_cards"))

        product_snapshot = _product_snapshot_from_row(product_row)

        inc_balance(
            "agent",
            agent_doc.get("_id"),
            product_key,
            qty,
            {
                "holder_name": agent_doc.get("name", ""),
                "branch": manager_doc.get("branch", ""),
                "product_name": product_snapshot["product_name"],
                "product_image_url": product_snapshot["product_image_url"],
                "sample_product_id": product_snapshot["sample_product_id"],
            },
        )

        write_movement(
            "manager",
            manager_id,
            "agent",
            agent_doc.get("_id"),
            product_snapshot,
            qty,
            note,
            {
                "_id": manager_id,
                "role": "manager",
                "from_name": manager_doc.get("name", ""),
                "to_name": agent_doc.get("name", ""),
                "branch": manager_doc.get("branch", ""),
            },
        )

        flash("Cards distributed to agent.", "success")
        return redirect(url_for("card_tracker.manager_distribute_cards"))

    agents = list(users_col.find({"role": "agent", "manager_id": {"$in": variants}}, {"name": 1}).sort("name", 1))
    balances = list(card_balances_col.find({"holder_type": "manager", "holder_id": {"$in": variants}}))

    balance_map = {b.get("product_key"): safe_int(b.get("qty_on_hand"), 0) for b in balances}

    return render_template(
        "manager_distribute_cards.html",
        agents=agents,
        products=product_rows,
        balance_map=balance_map,
    )


@card_tracker_bp.route("/manager/history")
@require_role("manager")
def manager_cards_history():
    ident = get_current_identity()
    manager_doc = get_user_doc(ident.get("user_id")) or {}
    manager_id = manager_doc.get("_id")

    query = _build_history_query(request.args, scope={"manager_id": manager_id})
    movements = list(card_movements_col.find(query).sort("created_at", -1).limit(300))

    products = list(
        products_col.find(
            {"manager_id": {"$in": id_variants(manager_id)}},
            {"name": 1, "image_url": 1, "created_at": 1},
        )
    )
    product_rows = _unique_product_list(products)

    agents = list(
        users_col.find({"role": "agent", "manager_id": {"$in": id_variants(manager_id)}}, {"name": 1}).sort("name", 1)
    )

    return render_template(
        "manager_cards_history.html",
        movements=movements,
        products=product_rows,
        agents=agents,
    )


@card_tracker_bp.route("/agent/my")
@require_role("agent")
def agent_my_cards():
    ident = get_current_identity()
    agent_doc = get_user_doc(ident.get("user_id")) or {}
    agent_id = agent_doc.get("_id")
    variants = id_variants(agent_id)
    default_month = datetime.utcnow().strftime("%Y-%m")

    product_names = set()
    balances = list(card_balances_col.find({"holder_type": "agent", "holder_id": {"$in": variants}}))
    for row in balances:
        name = (row.get("product_name") or "").strip()
        if name:
            product_names.add(name)

    movement_names = card_movements_col.distinct(
        "product_name",
        {"to_type": "agent", "to_id": {"$in": variants}},
    )
    for name in movement_names:
        if name:
            product_names.add(str(name).strip())

    if not product_names:
        for row in products_col.find({}, {"name": 1}):
            name = (row.get("name") or "").strip()
            if name:
                product_names.add(name)

    products = sorted(product_names, key=lambda x: x.lower())

    return render_template(
        "agent_my_cards.html",
        default_month=default_month,
        products=products,
    )


@card_tracker_bp.route("/agent/my/data")
@require_role("agent")
def agent_my_cards_data():
    ident = get_current_identity()
    agent_doc = get_user_doc(ident.get("user_id")) or {}
    agent_id = agent_doc.get("_id")
    agent_id_str = str(agent_id)
    variants = id_variants(agent_id)

    product_name = (request.args.get("product_name") or "").strip()
    product_norm = normalize_name(product_name)

    start_dt, end_dt, from_str, to_str = _range_bounds(request.args)

    def _format_dt(val):
        if not val:
            return ""
        if isinstance(val, datetime):
            return val.strftime("%Y-%m-%d")
        return str(val)

    def _merge_movements(rows):
        out = {}
        for row in rows:
            raw_name = (row.get("_id") or "").strip()
            pnorm = normalize_name(raw_name)
            if not pnorm:
                continue
            if product_norm and pnorm != product_norm:
                continue
            entry = out.setdefault(
                pnorm,
                {"name": raw_name, "qty": 0, "last_at": None, "image_url": row.get("image_url", "")},
            )
            entry["qty"] += safe_int(row.get("total", 0), 0)
            if row.get("last_at") and (entry["last_at"] is None or row.get("last_at") > entry["last_at"]):
                entry["last_at"] = row.get("last_at")
            if not entry.get("name") and raw_name:
                entry["name"] = raw_name
            if not entry.get("image_url") and row.get("image_url"):
                entry["image_url"] = row.get("image_url")
        return out

    move_match = {"to_type": "agent", "to_id": {"$in": variants}}
    move_range_rows = list(
        card_movements_col.aggregate(
            [
                {"$match": {**move_match, "created_at": {"$gte": start_dt, "$lt": end_dt}}},
                {
                    "$group": {
                        "_id": "$product_name",
                        "total": {"$sum": "$qty"},
                        "last_at": {"$max": "$created_at"},
                        "image_url": {"$first": "$product_image_url"},
                    }
                },
            ]
        )
    )
    move_all_rows = list(
        card_movements_col.aggregate(
            [
                {"$match": move_match},
                {
                    "$group": {
                        "_id": "$product_name",
                        "total": {"$sum": "$qty"},
                        "last_at": {"$max": "$created_at"},
                        "image_url": {"$first": "$product_image_url"},
                    }
                },
            ]
        )
    )

    received_range = _merge_movements(move_range_rows)
    received_all = _merge_movements(move_all_rows)

    def _sales_map(payload):
        out = {}
        for key, entry in payload.get("total", {}).items():
            if product_norm and key != product_norm:
                continue
            out[key] = {
                "name": entry.get("name", ""),
                "qty": safe_int(entry.get("count", 0), 0),
                "last_at": entry.get("last_at"),
            }
        return out

    sold_range_payload = sold_counts_by_name(
        customers_col,
        instant_sales_col,
        agent_id=agent_id_str,
        product_name=product_name,
        start_dt=start_dt,
        end_dt=end_dt,
    )
    sold_all_payload = sold_counts_by_name(
        customers_col,
        instant_sales_col,
        agent_id=agent_id_str,
        product_name=product_name,
        start_dt=None,
        end_dt=end_dt,
    )

    sold_range = _sales_map(sold_range_payload)
    sold_all = _sales_map(sold_all_payload)

    keys = set(received_all) | set(received_range) | set(sold_all) | set(sold_range)
    rows = []
    for key in keys:
        received_all_qty = received_all.get(key, {}).get("qty", 0)
        sold_all_qty = sold_all.get(key, {}).get("qty", 0)
        received_range_qty = received_range.get(key, {}).get("qty", 0)
        sold_range_qty = sold_range.get(key, {}).get("qty", 0)

        rows.append(
            {
                "product_name": received_all.get(key, {}).get("name")
                or sold_all.get(key, {}).get("name")
                or product_name,
                "image_url": received_all.get(key, {}).get("image_url", ""),
                "received_all": received_all_qty,
                "sold_all": sold_all_qty,
                "at_hand_all": max(0, received_all_qty - sold_all_qty),
                "received_range": received_range_qty,
                "sold_range": sold_range_qty,
                "at_hand_range": max(0, received_range_qty - sold_range_qty),
                "last_received_at": _format_dt(received_all.get(key, {}).get("last_at")),
                "last_sold_at": _format_dt(sold_all.get(key, {}).get("last_at")),
                "warning_over_sold": sold_all_qty > received_all_qty,
            }
        )

    rows.sort(key=lambda x: (x.get("product_name") or "").lower())

    kpis = {
        "received_all": sum(row["received_all"] for row in rows),
        "sold_all": sum(row["sold_all"] for row in rows),
        "at_hand_all": sum(row["at_hand_all"] for row in rows),
        "received_range": sum(row["received_range"] for row in rows),
        "sold_range": sum(row["sold_range"] for row in rows),
        "at_hand_range": sum(row["at_hand_range"] for row in rows),
    }

    return jsonify(
        {
            "range": {"from": from_str, "to": to_str},
            "kpis": kpis,
            "rows": rows,
        }
    )


@card_tracker_bp.route("/admin/migrate", methods=["POST"])
@require_role("admin")
def admin_migrate_cards():
    updated = {"balances": 0, "movements": 0, "batches": 0}

    for doc in card_balances_col.find({"product_key": {"$exists": False}, "product_id": {"$exists": True}}):
        product = products_col.find_one({"_id": doc.get("product_id")}) if doc.get("product_id") else None
        name = (product or {}).get("name") or doc.get("product_name")
        product_key = normalize_name(name)
        if not product_key:
            continue
        card_balances_col.update_one(
            {"_id": doc.get("_id")},
            {
                "$set": {
                    "product_key": product_key,
                    "product_name": name or "",
                    "product_image_url": (product or {}).get("image_url", ""),
                    "sample_product_id": (product or {}).get("_id"),
                }
            },
        )
        updated["balances"] += 1

    for doc in card_movements_col.find({"product_key": {"$exists": False}, "product_id": {"$exists": True}}):
        product = products_col.find_one({"_id": doc.get("product_id")}) if doc.get("product_id") else None
        name = (product or {}).get("name") or doc.get("product_name")
        product_key = normalize_name(name)
        if not product_key:
            continue
        card_movements_col.update_one(
            {"_id": doc.get("_id")},
            {
                "$set": {
                    "product_key": product_key,
                    "product_name": name or "",
                    "product_image_url": (product or {}).get("image_url", ""),
                    "sample_product_id": (product or {}).get("_id"),
                }
            },
        )
        updated["movements"] += 1

    for doc in card_print_batches_col.find({"product_key": {"$exists": False}, "product_id": {"$exists": True}}):
        product = products_col.find_one({"_id": doc.get("product_id")}) if doc.get("product_id") else None
        name = (product or {}).get("name") or doc.get("product_name")
        product_key = normalize_name(name)
        if not product_key:
            continue
        card_print_batches_col.update_one(
            {"_id": doc.get("_id")},
            {
                "$set": {
                    "product_key": product_key,
                    "product_name": name or "",
                    "product_image_url": (product or {}).get("image_url", ""),
                    "sample_product_id": (product or {}).get("_id"),
                }
            },
        )
        updated["batches"] += 1

    _ensure_card_indexes()

    return {"ok": True, "updated": updated}

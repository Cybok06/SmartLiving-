from flask import Blueprint, render_template, request, redirect, url_for, flash, send_from_directory
from bson.objectid import ObjectId
import os, requests, traceback, json
from datetime import datetime

from db import db

product_profile_bp = Blueprint('product_profile', __name__)
products_col = db.products
inventory_col = db.inventory
users_col = db.users
images_col = db.images  # keep logs consistent with your add_product module

# === File upload (local, only used for legacy serve endpoint) ===
UPLOAD_FOLDER = os.path.join(os.getcwd(), 'uploads')
ALLOWED_EXTENSIONS = {'png', 'jpg', 'jpeg', 'gif'}
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

def allowed_file(filename: str) -> bool:
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

# === Cloudflare Images config (hardcoded) ===
CF_ACCOUNT_ID   = "63e6f91eec9591f77699c4b434ab44c6"
CF_IMAGES_TOKEN = "Brz0BEfl_GqEUjEghS2UEmLZhK39EUmMbZgu_hIo"
CF_HASH         = "h9fmMoa1o2c2P55TcWJGOg"
CF_REQUIRE_SIGNED = False

def _to_oid(val):
    try:
        return ObjectId(str(val))
    except Exception:
        return None

def _to_float(x, default=0.0) -> float:
    try:
        return float(str(x).replace(",", "").strip())
    except Exception:
        return float(default)

def _calc_price(cost: float, margin_pct: float) -> float:
    if cost <= 0:
        return 0.0
    if margin_pct < 0:
        margin_pct = 0.0
    return round(cost * (1.0 + (margin_pct / 100.0)), 2)

def upload_to_cloudflare(file_storage):
    """
    Upload FileStorage to Cloudflare Images using Direct Creator Upload.
    Returns (ok, payload):
      ok=True  -> payload = {"image_id": "...", "url": "...", "variant": "public"}
      ok=False -> payload = {"stage": "...", "raw": ...}
    """
    try:
        direct_url = f"https://api.cloudflare.com/client/v4/accounts/{CF_ACCOUNT_ID}/images/v2/direct_upload"
        data = {}
        if CF_REQUIRE_SIGNED:
            data["requireSignedURLs"] = "true"

        res = requests.post(
            direct_url,
            headers={"Authorization": f"Bearer {CF_IMAGES_TOKEN}"},
            data=data,
            timeout=25,
        )
        try:
            j = res.json()
        except Exception:
            return False, {"stage": "direct_upload", "raw": {"success": False, "errors": [{"message": "Bad JSON from Cloudflare"}]}}

        if not j.get("success"):
            return False, {"stage": "direct_upload", "raw": j}

        upload_url = j["result"]["uploadURL"]
        image_id   = j["result"]["id"]

        up = requests.post(
            upload_url,
            files={"file": (file_storage.filename, file_storage.stream, file_storage.mimetype or "application/octet-stream")},
            timeout=60,
        )
        try:
            uj = up.json()
        except Exception:
            return False, {"stage": "upload", "raw": {"success": False, "errors": [{"message": "Bad JSON from Cloudflare upload"}]}}

        if not uj.get("success"):
            return False, {"stage": "upload", "raw": uj}

        variant = "public"
        url = f"https://imagedelivery.net/{CF_HASH}/{image_id}/{variant}"

        # traceability (optional but good)
        try:
            images_col.insert_one({
                "provider": "cloudflare_images",
                "image_id": image_id,
                "variant": variant,
                "url": url,
                "original_filename": file_storage.filename,
                "mimetype": file_storage.mimetype,
                "created_at": datetime.utcnow(),
                "source": "product_profile.change_image",
            })
        except Exception:
            pass

        return True, {"image_id": image_id, "url": url, "variant": variant}

    except requests.RequestException as e:
        return False, {"stage": "network", "raw": {"message": str(e)}}
    except Exception as e:
        return False, {"stage": "server", "raw": {"message": str(e), "trace": traceback.format_exc()}}


@product_profile_bp.route('/product/<product_id>')
def view_product(product_id):
    product = products_col.find_one({'_id': _to_oid(product_id)})
    if not product:
        return "Product not found", 404

    manager = users_col.find_one({'_id': product.get('manager_id')})

    # Load components from inventory (robust id formats)
    components = []
    for comp in product.get('components', []):
        comp_id = comp.get('_id')
        if isinstance(comp_id, dict) and '$oid' in comp_id:
            comp_oid = _to_oid(comp_id['$oid'])
        elif isinstance(comp_id, str):
            comp_oid = _to_oid(comp_id)
        elif isinstance(comp_id, ObjectId):
            comp_oid = comp_id
        else:
            comp_oid = None

        if not comp_oid:
            continue

        inv_item = inventory_col.find_one({'_id': comp_oid})
        if inv_item:
            components.append({
                'id': str(comp_oid),
                'name': inv_item.get('name', ''),
                'image_url': inv_item.get('image_url'),
                'description': inv_item.get('description', ''),
                'price': inv_item.get('price', ''),
                'qty': int(comp.get('quantity', 1) or 1)
            })

    # Inventory limited to same manager
    raw_inventory = list(inventory_col.find({'manager_id': product.get('manager_id')}))
    inventory_by_id = {}
    for item in raw_inventory:
        item_copy = dict(item)
        item_copy['_id'] = str(item_copy['_id'])
        inventory_by_id[item_copy['_id']] = item_copy

    all_managers = list(users_col.find({'role': 'manager'}))

    return render_template(
        "product_profile.html",
        product=product,
        manager=manager,
        components=components,
        inventory=inventory_by_id,
        all_managers=all_managers
    )


@product_profile_bp.route('/product/<product_id>/edit', methods=['POST'])
def edit_product(product_id):
    product = products_col.find_one({'_id': _to_oid(product_id)})
    if not product:
        flash("❌ Product not found.", "danger")
        return redirect(url_for('added_products.view_added_products'))

    data = request.form

    # New fields
    cost_price = _to_float(data.get("cost_price"), product.get("cost_price", 0) or 0)
    margin_price = _to_float(data.get("profit_margin_price"), product.get("profit_margin_price", 0) or 0)
    margin_cash  = _to_float(data.get("profit_margin_cash"), product.get("profit_margin_cash", 0) or 0)

    auto_calc = (data.get("auto_calc") == "1")

    if auto_calc:
        price = _calc_price(cost_price, margin_price)
        cash_price = _calc_price(cost_price, margin_cash)
    else:
        # allow manual override if user unchecks auto-calc
        price = _to_float(data.get("price"), product.get("price", 0) or 0)
        cash_price = _to_float(data.get("cash_price"), product.get("cash_price", 0) or 0)

    profit_price = round(price - cost_price, 2)
    profit_cash  = round(cash_price - cost_price, 2)

    update_fields = {
        "name": (data.get("name") or "").strip(),
        "product_type": (data.get("product_type") or "").strip(),
        "category": (data.get("category") or "").strip(),
        "package_name": (data.get("package_name") or "").strip(),
        "description": (data.get("description") or "").strip(),

        "cost_price": cost_price,
        "profit_margin_price": margin_price,
        "profit_margin_cash": margin_cash,
        "price": price,
        "cash_price": cash_price,
        "profit_price": profit_price,
        "profit_cash": profit_cash,
    }

    # Remove empty strings for some optional fields but keep numbers
    cleaned = {}
    for k, v in update_fields.items():
        if isinstance(v, str) and v == "":
            if k in ("category", "package_name", "product_type"):
                cleaned[k] = ""  # keep as empty if user wants
            else:
                continue
        else:
            cleaned[k] = v

    products_col.update_one({'_id': _to_oid(product_id)}, {'$set': cleaned})
    flash("✅ Product updated successfully.", "success")
    return redirect(url_for('product_profile.view_product', product_id=product_id))


# ✅ FIXED: Remove a single component (this is what your UI should call)
@product_profile_bp.route('/product/<product_id>/remove-component/<comp_id>', methods=['POST'])
def remove_component(product_id, comp_id):
    prod_oid = _to_oid(product_id)
    comp_oid = _to_oid(comp_id)
    if not prod_oid or not comp_oid:
        flash("❌ Invalid product/component id.", "danger")
        return redirect(url_for('product_profile.view_product', product_id=product_id))

    res = products_col.update_one(
        {'_id': prod_oid},
        {'$pull': {'components': {'_id': comp_oid}}}
    )
    if res.modified_count:
        flash("✅ Component removed successfully.", "success")
    else:
        flash("⚠ Component not found or already removed.", "warning")

    return redirect(url_for('product_profile.view_product', product_id=product_id))


# ✅ NEW: Update quantity of an existing component
@product_profile_bp.route('/product/<product_id>/update-component/<comp_id>', methods=['POST'])
def update_component_qty(product_id, comp_id):
    prod_oid = _to_oid(product_id)
    comp_oid = _to_oid(comp_id)
    if not prod_oid or not comp_oid:
        flash("❌ Invalid product/component id.", "danger")
        return redirect(url_for('product_profile.view_product', product_id=product_id))

    qty = request.form.get("qty", "1")
    try:
        qty = int(qty)
        if qty <= 0:
            qty = 1
    except Exception:
        qty = 1

    res = products_col.update_one(
        {'_id': prod_oid, 'components._id': comp_oid},
        {'$set': {'components.$.quantity': qty}}
    )
    if res.modified_count:
        flash("✅ Component quantity updated.", "success")
    else:
        flash("⚠ Component not found.", "warning")

    return redirect(url_for('product_profile.view_product', product_id=product_id))


# ✅ Add components (skip duplicates; if exists, update qty instead of skipping)
@product_profile_bp.route('/product/<product_id>/add-components', methods=['POST'], endpoint='add_components')
def add_components(product_id):
    prod_oid = _to_oid(product_id)
    product = products_col.find_one({'_id': prod_oid})
    if not product:
        flash("❌ Product not found.", "danger")
        return redirect(url_for('product_profile.view_product', product_id=product_id))

    component_ids = request.form.getlist('components')

    # Map existing component ids -> index/qty
    existing = {}
    for c in product.get("components", []):
        cid = c.get("_id")
        if isinstance(cid, ObjectId):
            existing[str(cid)] = int(c.get("quantity", 1) or 1)
        elif isinstance(cid, str):
            existing[cid] = int(c.get("quantity", 1) or 1)
        elif isinstance(cid, dict) and "$oid" in cid:
            existing[cid["$oid"]] = int(c.get("quantity", 1) or 1)

    added = 0
    updated = 0
    new_components = []

    for comp_id in component_ids:
        comp_oid = _to_oid(comp_id)
        if not comp_oid:
            continue

        qty_raw = request.form.get(f'qty_{comp_id}', '1')
        try:
            qty = int(qty_raw)
            if qty <= 0:
                qty = 1
        except Exception:
            qty = 1

        if comp_id in existing:
            # update qty for existing item
            res = products_col.update_one(
                {'_id': prod_oid, 'components._id': comp_oid},
                {'$set': {'components.$.quantity': qty}}
            )
            if res.modified_count:
                updated += 1
            continue

        inv_item = inventory_col.find_one({'_id': comp_oid})
        if inv_item:
            new_components.append({'_id': comp_oid, 'quantity': qty})
            added += 1

    if new_components:
        products_col.update_one({'_id': prod_oid}, {'$push': {'components': {'$each': new_components}}})

    if added or updated:
        msg = []
        if added: msg.append(f"✅ {added} component(s) added")
        if updated: msg.append(f"✅ {updated} component(s) updated")
        flash(" • ".join(msg), "success")
    else:
        flash("❌ No valid components selected.", "warning")

    return redirect(url_for('product_profile.view_product', product_id=product_id))


@product_profile_bp.route('/product/<product_id>/delete', methods=['POST'])
def delete_product(product_id):
    product = products_col.find_one({'_id': _to_oid(product_id)})
    if not product:
        flash("❌ Product not found.", "danger")
        return redirect(url_for('added_products.view_added_products'))

    products_col.delete_one({'_id': _to_oid(product_id)})

    delete_for_ids = request.form.getlist('delete_for[]')
    if delete_for_ids:
        oids = []
        for mid in delete_for_ids:
            oid = _to_oid(mid)
            if oid:
                oids.append(oid)

        deleted = 0
        if oids:
            deleted = products_col.delete_many({
                'name': product.get('name'),
                'manager_id': {'$in': oids},
                '_id': {'$ne': _to_oid(product_id)}
            }).deleted_count

        flash(f"✅ Product also deleted for {deleted} other manager(s).", "success")
    else:
        flash("✅ Product deleted successfully.", "success")

    return redirect(url_for('added_products.view_added_products'))


@product_profile_bp.route('/product/<product_id>/change_image', methods=['POST'])
def change_image(product_id):
    product = products_col.find_one({'_id': _to_oid(product_id)})
    if not product:
        flash("❌ Product not found.", "danger")
        return redirect(url_for('product_profile.view_product', product_id=product_id))

    file = request.files.get('image')
    if not file or file.filename == '':
        flash("❌ No image selected.", "danger")
        return redirect(url_for('product_profile.view_product', product_id=product_id))

    if not allowed_file(file.filename):
        flash("❌ Invalid file type. Please upload PNG/JPG/JPEG/GIF.", "danger")
        return redirect(url_for('product_profile.view_product', product_id=product_id))

    ok, payload = upload_to_cloudflare(file)
    if not ok:
        stage = payload.get("stage", "unknown")
        raw = payload.get("raw")
        try:
            raw_json = json.dumps(raw)[:800]
        except Exception:
            raw_json = str(raw)[:800]
        flash(f"❌ Image upload failed at '{stage}'. Details: {raw_json}", "danger")
        return redirect(url_for('product_profile.view_product', product_id=product_id))

    image_id  = payload["image_id"]
    image_url = payload["url"]
    set_fields = {"image_url": image_url, "cf_image_id": image_id}

    # propagate: same name + same price (keep your rule)
    name = product.get('name')
    price = product.get('price')
    if name is not None and price is not None:
        products_col.update_many({'name': name, 'price': price}, {'$set': set_fields})
        flash("✅ Image updated for all matching products (same name & price).", "success")
    else:
        products_col.update_one({'_id': product['_id']}, {'$set': set_fields})
        flash("✅ Image updated for this product.", "success")

    return redirect(url_for('product_profile.view_product', product_id=product_id))


@product_profile_bp.route('/uploads/<filename>')
def serve_uploaded_file(filename):
    return send_from_directory(UPLOAD_FOLDER, filename)

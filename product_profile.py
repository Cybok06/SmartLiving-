# product_profile.py
from flask import Blueprint, render_template, request, redirect, url_for, flash, send_from_directory
from bson.objectid import ObjectId
from werkzeug.utils import secure_filename
import os, requests, traceback, json

from db import db

product_profile_bp = Blueprint('product_profile', __name__)
products_col = db.products
inventory_col = db.inventory
users_col = db.users

# === File upload (local, only used for legacy serve endpoint) ===
UPLOAD_FOLDER = os.path.join(os.getcwd(), 'uploads')
ALLOWED_EXTENSIONS = {'png', 'jpg', 'jpeg', 'gif'}
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

def allowed_file(filename: str) -> bool:
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

# === Cloudflare Images config (hardcoded per your request) ===
CF_ACCOUNT_ID   = "63e6f91eec9591f77699c4b434ab44c6"
CF_IMAGES_TOKEN = "Brz0BEfl_GqEUjEghS2UEmLZhK39EUmMbZgu_hIo"
CF_HASH         = "h9fmMoa1o2c2P55TcWJGOg"
CF_REQUIRE_SIGNED = False  # set True only if you later move to private images

def upload_to_cloudflare(file_storage):
    """
    Upload a Werkzeug FileStorage to Cloudflare Images using Direct Creator Upload.
    Returns (ok, payload) where:
      ok=True  -> payload = {"image_id": "...", "url": "...", "variant": "public"}
      ok=False -> payload = {"stage": "...", "raw": <cloudflare body or message>}
    """
    try:
        # Step 1: one-time upload URL
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

        # Step 2: upload the actual file
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

        # Step 3: build delivery URL (public variant by default)
        variant = "public"
        url = f"https://imagedelivery.net/{CF_HASH}/{image_id}/{variant}"
        return True, {"image_id": image_id, "url": url, "variant": variant}

    except requests.RequestException as e:
        return False, {"stage": "network", "raw": {"message": str(e)}}
    except Exception as e:
        return False, {"stage": "server", "raw": {"message": str(e), "trace": traceback.format_exc()}}


def _to_oid(val):
    try:
        return ObjectId(val)
    except Exception:
        return None


@product_profile_bp.route('/product/<product_id>')
def view_product(product_id):
    product = products_col.find_one({'_id': _to_oid(product_id)})
    if not product:
        return "Product not found", 404

    manager = users_col.find_one({'_id': product['manager_id']})

    # Load components from inventory
    components = []
    for comp in product.get('components', []):
        comp_id = comp.get('_id')
        # Handle dict {'$oid': ...}, str, or ObjectId
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
                'qty': comp.get('quantity', 1)
            })

    # Raw inventory for selection (same manager)
    raw_inventory = list(inventory_col.find({'manager_id': product['manager_id']}))
    # ensure id is string for template usage
    inventory_by_id = {}
    for item in raw_inventory:
        item_copy = dict(item)
        item_copy['_id'] = str(item_copy['_id'])
        inventory_by_id[item_copy['_id']] = item_copy

    # Get all managers (for optional deletion)
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
    data = request.form
    update_fields = {
        "name": data.get("name"),
        "product_type": data.get("product_type"),
        "price": float(data.get("price", 0) or 0),
        "cash_price": float(data.get("cash_price", 0) or 0),
        "category": data.get("category"),
        "package_name": data.get("package_name"),
        "description": data.get("description")
    }
    update_fields = {k: v for k, v in update_fields.items() if v not in [None, ""]}

    products_col.update_one({'_id': _to_oid(product_id)}, {'$set': update_fields})
    flash("✅ Product updated successfully.", "success")
    return redirect(url_for('product_profile.view_product', product_id=product_id))


@product_profile_bp.route('/product/<product_id>/delete/<comp_id>', methods=['POST'])
def delete_component(product_id, comp_id):
    products_col.update_one(
        {'_id': _to_oid(product_id)},
        {'$pull': {'components': {'_id': _to_oid(comp_id)}}}
    )
    flash("✅ Component removed successfully.", "success")
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
        deleted = products_col.delete_many({
            'name': product.get('name'),
            'manager_id': {'$in': [ObjectId(mid) for mid in delete_for_ids]},
            '_id': {'$ne': _to_oid(product_id)}
        }).deleted_count
        flash(f"✅ Product also deleted for {deleted} other manager(s).", "success")
    else:
        flash("✅ Product deleted successfully.", "success")

    return redirect(url_for('added_products.view_added_products'))


# === ✅ Add components to a product (explicit endpoint name to match template) ===
@product_profile_bp.route(
    '/product/<product_id>/add-components',
    methods=['POST'],
    endpoint='add_components'
)
def add_components(product_id):
    product = products_col.find_one({'_id': _to_oid(product_id)})
    if not product:
        flash("❌ Product not found.", "danger")
        return redirect(url_for('product_profile.view_product', product_id=product_id))

    component_ids = request.form.getlist('components')
    existing_ids = {
        str(comp['_id']['$oid']) if isinstance(comp['_id'], dict) and '$oid' in comp['_id']
        else (str(comp['_id']) if isinstance(comp['_id'], (str, ObjectId)) else None)
        for comp in product.get('components', [])
    }
    existing_ids.discard(None)

    new_components = []
    for comp_id in component_ids:
        qty_raw = request.form.get(f'qty_{comp_id}', '1')
        try:
            qty = int(qty_raw)
            if qty <= 0:
                qty = 1
        except Exception:
            qty = 1

        if comp_id in existing_ids:
            # skip if already present
            continue

        comp_oid = _to_oid(comp_id)
        if not comp_oid:
            continue

        inv_item = inventory_col.find_one({'_id': comp_oid})
        if inv_item:
            new_components.append({'_id': comp_oid, 'quantity': qty})

    if new_components:
        products_col.update_one(
            {'_id': _to_oid(product_id)},
            {'$push': {'components': {'$each': new_components}}}
        )
        flash(f"✅ {len(new_components)} component(s) added.", "success")
    else:
        flash("❌ No valid components added or they already exist.", "warning")

    return redirect(url_for('product_profile.view_product', product_id=product_id))


# === ✅ Change Product Image via Cloudflare Images (propagate same name+price) ===
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

    # Upload to Cloudflare
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

    image_id = payload["image_id"]
    image_url = payload["url"]  # delivery URL (public variant)
    set_fields = {"image_url": image_url, "cf_image_id": image_id}

    # Propagate: update all products with same name + same price
    name = product.get('name')
    price = product.get('price')
    if name is not None and price is not None:
        products_col.update_many({'name': name, 'price': price}, {'$set': set_fields})
        flash("✅ Image updated for all matching products (same name & price).", "success")
    else:
        products_col.update_one({'_id': product['_id']}, {'$set': set_fields})
        flash("✅ Image updated for this product.", "success")

    return redirect(url_for('product_profile.view_product', product_id=product_id))


# (legacy local serve; safe to keep for older records that point to /uploads/*)
@product_profile_bp.route('/uploads/<filename>')
def serve_uploaded_file(filename):
    return send_from_directory(UPLOAD_FOLDER, filename)

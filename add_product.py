from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify, send_from_directory
from bson.objectid import ObjectId
from urllib.parse import unquote
from werkzeug.utils import secure_filename
from datetime import datetime
import json
import os
import traceback
import requests

from db import db

add_product_bp = Blueprint('add_product', __name__)
UPLOAD_FOLDER = os.path.join(os.getcwd(), 'uploads')
ALLOWED_EXTENSIONS = {'png', 'jpg', 'jpeg', 'gif'}

# ===== Cloudflare (hardcoded as requested) =====
CF_ACCOUNT_ID   = "63e6f91eec9591f77699c4b434ab44c6"
CF_IMAGES_TOKEN = "Brz0BEfl_GqEUjEghS2UEmLZhK39EUmMbZgu_hIo"
CF_HASH         = "h9fmMoa1o2c2P55TcWJGOg"
DEFAULT_VARIANT = "public"  # make sure this variant exists in Cloudflare Images

os.makedirs(UPLOAD_FOLDER, exist_ok=True)

inventory_col = db.inventory
products_col  = db.products
users_col     = db.users
images_col    = db.images

try:
    products_col.create_index([("manager_id", 1), ("name", 1)], background=True)
except Exception:
    pass

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def _to_float(val, default=0.0):
    try:
        return float(str(val).replace(",", "").strip())
    except Exception:
        return float(default)

def _profit_margin(cost_price: float, selling_price: float) -> float:
    """
    Returns margin % based on COST:
      margin% = ((selling - cost) / cost) * 100
    If cost <= 0 -> return 0.0 (avoid division errors)
    """
    try:
        if cost_price <= 0:
            return 0.0
        return round(((selling_price - cost_price) / cost_price) * 100.0, 2)
    except Exception:
        return 0.0

def _safe_int(val, default=0):
    try:
        return int(val)
    except Exception:
        return default

def _norm_str(val):
    return (str(val or "").strip().lower()) or None

def _product_identity_key(doc):
    components = []
    for c in (doc.get("components") or []):
        comp_id = c.get("_id") or c.get("id") or c.get("product_id")
        if comp_id is None:
            continue
        components.append(
            {
                "_id": str(comp_id),
                "quantity": _safe_int(c.get("quantity", 0), 0),
            }
        )
    components.sort(key=lambda x: x["_id"])

    payload = {
        "name": _norm_str(doc.get("name")),
        "image_url": _norm_str(doc.get("image_url")),
        "description": _norm_str(doc.get("description")),
        "price": round(_to_float(doc.get("price"), 0.0), 2),
        "cash_price": round(_to_float(doc.get("cash_price"), 0.0), 2),
        "cost_price": round(_to_float(doc.get("cost_price"), 0.0), 2),
        "product_type": _norm_str(doc.get("product_type")),
        "category": _norm_str(doc.get("category")),
        "package_name": _norm_str(doc.get("package_name")),
        "default_term_months": _safe_int(doc.get("default_term_months"), 0),
        "components": components,
    }
    return json.dumps(payload, separators=(",", ":"), sort_keys=True)

# =============== Upload directly to Cloudflare ===============
@add_product_bp.route('/products/upload_image', methods=['POST'])
def upload_image():
    try:
        if 'image' not in request.files:
            return jsonify({'success': False, 'error': 'No file part in request'}), 400

        image = request.files['image']
        if image.filename == '':
            return jsonify({'success': False, 'error': 'No selected file'}), 400

        if not (image and allowed_file(image.filename)):
            return jsonify({'success': False, 'error': 'File type not allowed'}), 400

        direct_url = f"https://api.cloudflare.com/client/v4/accounts/{CF_ACCOUNT_ID}/images/v2/direct_upload"
        headers    = {"Authorization": f"Bearer {CF_IMAGES_TOKEN}"}
        data = {}

        res = requests.post(direct_url, headers=headers, data=data, timeout=20)
        try:
            j = res.json()
        except Exception:
            return jsonify({'success': False, 'error': 'Cloudflare (direct_upload) returned non-JSON'}), 502

        if not j.get('success'):
            return jsonify({'success': False, 'error': 'Cloudflare direct_upload failed', 'details': j}), 400

        upload_url = j['result']['uploadURL']
        image_id   = j['result']['id']

        up = requests.post(
            upload_url,
            files={'file': (secure_filename(image.filename), image.stream, image.mimetype or 'application/octet-stream')},
            timeout=60
        )
        try:
            uj = up.json()
        except Exception:
            return jsonify({'success': False, 'error': 'Cloudflare (upload) returned non-JSON'}), 502

        if not uj.get('success'):
            return jsonify({'success': False, 'error': 'Cloudflare upload failed', 'details': uj}), 400

        variant   = request.args.get('variant', DEFAULT_VARIANT)
        image_url = f"https://imagedelivery.net/{CF_HASH}/{image_id}/{variant}"

        images_col.insert_one({
            'provider': 'cloudflare_images',
            'image_id': image_id,
            'variant': variant,
            'url': image_url,
            'original_filename': secure_filename(image.filename),
            'mimetype': image.mimetype,
            'size_bytes': request.content_length,
            'created_at': datetime.utcnow()
        })

        return jsonify({'success': True, 'image_url': image_url, 'image_id': image_id, 'variant': variant})

    except Exception as e:
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 500

# =============== (Legacy) Serve local files if ever needed ===============
@add_product_bp.route('/uploads/<filename>')
def uploaded_file(filename):
    return send_from_directory(UPLOAD_FOLDER, filename)

# =============== Main product add route ===============
@add_product_bp.route('/add_product', methods=['GET', 'POST'])
def add_product():
    managers = list(users_col.find({'role': 'manager'}))
    raw_inventory = list(inventory_col.find({}))

    grouped_inventory = {}
    for item in raw_inventory:
        key_data = {
            "name": item.get('name'),
            "image_url": item.get('image_url'),
            "price": item.get('price'),
            "description": item.get('description')
        }
        key = json.dumps(key_data, separators=(',', ':'))
        if key not in grouped_inventory:
            grouped_inventory[key] = item

    product_types = sorted(set(products_col.distinct("product_type")))
    categories    = sorted(set(products_col.distinct("category")))

    if request.method == 'POST':
        try:
            # ✅ added cost_price as required
            required_fields = ['name', 'price', 'cash_price', 'cost_price', 'description', 'image_url']
            for field in required_fields:
                if request.form.get(field) in [None, ""]:
                    flash(f"❌ '{field}' is required.", "danger")
                    return redirect(url_for('add_product.add_product'))

            name        = request.form['name'].strip()
            price       = _to_float(request.form['price'])
            cash_price  = _to_float(request.form['cash_price'])
            cost_price  = _to_float(request.form['cost_price'])
            description = request.form['description']
            image_url   = request.form['image_url']

            # cf image id from the upload response (hidden input)
            cf_image_id = (request.form.get('image_id') or '').strip() or None

            # ✅ DO NOT change your pricing logic:
            # price and cash_price are used as entered;
            # we only calculate margins & profits from cost_price.
            profit_price = round(price - cost_price, 2)
            profit_cash  = round(cash_price - cost_price, 2)
            profit_margin_price = _profit_margin(cost_price, price)
            profit_margin_cash  = _profit_margin(cost_price, cash_price)

            product_type = request.form.get('custom_product_type') or request.form.get('product_type') or ''
            category     = request.form.get('custom_category') or request.form.get('category') or ''
            package_name = request.form.get('package_name', '')
            default_term_months = (request.form.get('default_term_months') or '').strip()
            if default_term_months == "":
                default_term_months = None
            else:
                try:
                    default_term_months = int(default_term_months)
                except Exception:
                    flash("❌ Default installment term must be a valid number.", "danger")
                    return redirect(url_for('add_product.add_product'))
                if default_term_months < 1 or default_term_months > 36:
                    flash("❌ Default installment term must be between 1 and 36.", "danger")
                    return redirect(url_for('add_product.add_product'))

            selected_manager_ids = request.form.getlist('managers')
            component_keys       = request.form.getlist('components')

            components_by_key = {}
            for key in component_keys:
                qty_key = f"qty_{key}"
                try:
                    quantity = int(request.form.get(qty_key, 1))
                    if quantity <= 0:
                        flash("❌ Quantity must be at least 1.", "danger")
                        return redirect(url_for('add_product.add_product'))

                    decoded_key = unquote(key)
                    components_by_key[decoded_key] = quantity
                except (ValueError, TypeError):
                    flash("❌ Invalid quantity for a component.", "danger")
                    return redirect(url_for('add_product.add_product'))

            if not selected_manager_ids:
                flash("❌ Please select at least one manager.", "danger")
                return redirect(url_for('add_product.add_product'))

            valid_count = 0

            for manager_id in selected_manager_ids:
                manager_oid = ObjectId(manager_id)
                missing = []
                resolved_components = []

                for key_str, qty in components_by_key.items():
                    try:
                        comp_data = json.loads(key_str)
                    except json.JSONDecodeError:
                        flash("❌ Error reading component data.", "danger")
                        return redirect(url_for('add_product.add_product'))

                    match = inventory_col.find_one({
                        "name": comp_data.get('name'),
                        "image_url": comp_data.get('image_url'),
                        "price": comp_data.get('price'),
                        "description": comp_data.get('description'),
                        "manager_id": manager_oid
                    })

                    if match:
                        resolved_components.append({"_id": match["_id"], "quantity": qty})
                    else:
                        missing.append(comp_data.get('name'))

                if missing:
                    manager = users_col.find_one({'_id': manager_oid})
                    flash(f"❌ {manager.get('name', 'Manager')} is missing: {', '.join(missing)}", 'danger')
                    continue

                if resolved_components:
                    product = {
                        'name': name,
                        'price': price,
                        'cash_price': cash_price,

                        # ✅ NEW
                        'cost_price': cost_price,
                        'profit_price': profit_price,
                        'profit_cash': profit_cash,
                        'profit_margin_price': profit_margin_price,
                        'profit_margin_cash': profit_margin_cash,

                        'description': description,
                        'image_url': image_url,
                        'cf_image_id': cf_image_id,   # matches your product_profile usage
                        'product_type': product_type,
                        'category': category,
                        'package_name': package_name,
                        'default_term_months': default_term_months,
                        'components': resolved_components,
                        'manager_id': manager_oid,
                        'created_at': datetime.utcnow()
                    }

                    products_col.insert_one(product)
                    valid_count += 1

            if valid_count:
                flash(f"✅ Product added for {valid_count} manager(s).", "success")
            else:
                flash("❌ No product was added. All selected managers had missing components.", "danger")

        except Exception as e:
            print("❌ Error while adding product:", str(e))
            traceback.print_exc()
            flash("❌ An unexpected error occurred. Please check your inputs.", "danger")

        return redirect(url_for('add_product.add_product'))

    return render_template(
        'add_product.html',
        inventory=grouped_inventory,
        managers=managers,
        product_types=product_types,
        categories=categories
    )


@add_product_bp.route('/add_product/missing_products', methods=['GET'])
def missing_products():
    try:
        manager_id = (request.args.get("manager_id") or "").strip()
        if not manager_id:
            return jsonify(ok=False, message="manager_id required"), 400

        try:
            manager_oid = ObjectId(manager_id)
        except Exception:
            return jsonify(ok=False, message="Invalid manager_id"), 400

        manager = users_col.find_one({"_id": manager_oid}, {"name": 1, "branch": 1})
        if not manager:
            return jsonify(ok=False, message="Manager not found"), 404

        projection = {
            "name": 1,
            "image_url": 1,
            "description": 1,
            "price": 1,
            "cash_price": 1,
            "cost_price": 1,
            "product_type": 1,
            "category": 1,
            "package_name": 1,
            "default_term_months": 1,
            "components": 1,
            "manager_id": 1,
        }

        all_products = list(products_col.find({}, projection).limit(4000))
        target_keys = set()
        for p in all_products:
            if p.get("manager_id") == manager_oid:
                target_keys.add(_product_identity_key(p))

        missing = []
        seen = set()
        for p in all_products:
            key = _product_identity_key(p)
            if key in target_keys or key in seen:
                continue
            seen.add(key)
            missing.append(
                {
                    "_id": str(p["_id"]),
                    "name": p.get("name"),
                    "image_url": p.get("image_url"),
                    "description": p.get("description"),
                    "price": p.get("price"),
                    "cash_price": p.get("cash_price"),
                    "product_type": p.get("product_type"),
                    "category": p.get("category"),
                    "package_name": p.get("package_name"),
                    "default_term_months": p.get("default_term_months"),
                }
            )

        return jsonify(
            ok=True,
            manager_name=manager.get("name"),
            results=missing,
        )
    except Exception:
        traceback.print_exc()
        return jsonify(ok=False, message="Failed to load missing products."), 500


@add_product_bp.route('/add_product/add_missing_products', methods=['POST'])
def add_missing_products():
    try:
        data = request.get_json(silent=True) or {}
        manager_id = (data.get("manager_id") or "").strip()
        product_ids = data.get("product_ids") or []

        if not manager_id:
            return jsonify(ok=False, message="manager_id required"), 400
        if not isinstance(product_ids, list) or not product_ids:
            return jsonify(ok=False, message="product_ids required"), 400

        try:
            manager_oid = ObjectId(manager_id)
        except Exception:
            return jsonify(ok=False, message="Invalid manager_id"), 400

        manager = users_col.find_one({"_id": manager_oid}, {"name": 1})
        if not manager:
            return jsonify(ok=False, message="Manager not found"), 404

        source_oids = []
        for pid in product_ids:
            try:
                source_oids.append(ObjectId(str(pid)))
            except Exception:
                continue

        if not source_oids:
            return jsonify(ok=False, message="No valid product IDs provided"), 400

        projection = {
            "name": 1,
            "price": 1,
            "cash_price": 1,
            "cost_price": 1,
            "profit_price": 1,
            "profit_cash": 1,
            "profit_margin_price": 1,
            "profit_margin_cash": 1,
            "description": 1,
            "image_url": 1,
            "cf_image_id": 1,
            "product_type": 1,
            "category": 1,
            "package_name": 1,
            "components": 1,
            "default_term_months": 1,
        }

        source_docs = list(products_col.find({"_id": {"$in": source_oids}}, projection))

        existing_keys = set()
        for p in products_col.find({"manager_id": manager_oid}, projection).limit(4000):
            existing_keys.add(_product_identity_key(p))

        added = 0
        skipped = 0
        now = datetime.utcnow()

        for src in source_docs:
            key = _product_identity_key(src)
            if key in existing_keys:
                skipped += 1
                continue
            new_doc = {
                "name": src.get("name"),
                "price": src.get("price"),
                "cash_price": src.get("cash_price"),
                "cost_price": src.get("cost_price"),
                "profit_price": src.get("profit_price"),
                "profit_cash": src.get("profit_cash"),
                "profit_margin_price": src.get("profit_margin_price"),
                "profit_margin_cash": src.get("profit_margin_cash"),
                "description": src.get("description"),
                "image_url": src.get("image_url"),
                "cf_image_id": src.get("cf_image_id"),
                "product_type": src.get("product_type"),
                "category": src.get("category"),
                "package_name": src.get("package_name"),
                "components": src.get("components") or [],
                "default_term_months": src.get("default_term_months"),
                "manager_id": manager_oid,
                "created_at": now,
            }
            products_col.insert_one(new_doc)
            existing_keys.add(key)
            added += 1

        return jsonify(
            ok=True,
            added_count=added,
            skipped_count=skipped,
            manager_name=manager.get("name"),
        )
    except Exception:
        traceback.print_exc()
        return jsonify(ok=False, message="Failed to add missing products."), 500

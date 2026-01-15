# add_inventory.py
import os
import math
import traceback
from datetime import datetime, timedelta
from typing import Optional

import requests
from flask import (
    Blueprint, render_template, request, redirect,
    url_for, flash, jsonify, send_from_directory, session
)
from werkzeug.utils import secure_filename
from bson.objectid import ObjectId, InvalidId

from db import db

add_inventory_bp = Blueprint('add_inventory', __name__)
UPLOAD_FOLDER = os.path.join(os.getcwd(), 'uploads')
ALLOWED_EXTENSIONS = {'png', 'jpg', 'jpeg', 'gif'}

# ========= Cloudflare Images (hardcoded) =========
CF_ACCOUNT_ID   = "63e6f91eec9591f77699c4b434ab44c6"
CF_IMAGES_TOKEN = "Brz0BEfl_GqEUjEghS2UEmLZhK39EUmMbZgu_hIo"
CF_HASH         = "h9fmMoa1o2c2P55TcWJGOg"
DEFAULT_VARIANT = "public"

os.makedirs(UPLOAD_FOLDER, exist_ok=True)

inventory_col = db.inventory
users_col     = db.users
images_col    = db.images

try:
    inventory_col.create_index([("manager_id", 1), ("name", 1)], background=True)
except Exception:
    pass


def allowed_file(filename: str) -> bool:
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def parse_money(val: str) -> Optional[float]:
    if val is None:
        return None
    s = str(val).strip()
    if s == "":
        return None
    try:
        return float(s)
    except Exception:
        return None

def parse_int(val: str) -> Optional[int]:
    if val is None:
        return None
    s = str(val).strip()
    if s == "":
        return None
    try:
        return int(s)
    except Exception:
        return None

def parse_profit(profit_input: str, initial_price: float) -> Optional[float]:
    if profit_input is None:
        return None
    s = profit_input.strip()
    if s == "":
        return None
    try:
        if s.endswith('%'):
            percent = float(s[:-1].strip())
            return round(initial_price + (initial_price * percent / 100.0), 2)
        amt = float(s)
        return round(initial_price + amt, 2)
    except Exception:
        return None

def to_object_id(val: str) -> Optional[ObjectId]:
    try:
        return ObjectId(val)
    except (InvalidId, TypeError):
        return None

def money2(v: Optional[float]) -> Optional[float]:
    if v is None or (isinstance(v, float) and (math.isinf(v) or math.isnan(v))):
        return None
    try:
        return round(float(v), 2)
    except Exception:
        return None

def parse_date_yyyy_mm_dd(val: str) -> Optional[datetime]:
    """
    Parse 'YYYY-MM-DD' to a datetime at midnight.
    Returns None if empty or invalid.
    """
    if val is None:
        return None
    s = str(val).strip()
    if s == "":
        return None
    try:
        return datetime.strptime(s, "%Y-%m-%d")
    except Exception:
        return None


def _norm_str(val: Optional[str]) -> str:
    if val is None:
        return ""
    return str(val).strip().lower()


def _norm_num(val: Optional[float]) -> str:
    try:
        return f"{float(val):.2f}"
    except Exception:
        return ""


def _identity_key(doc: dict) -> str:
    return "|".join([
        _norm_str(doc.get("name")),
        _norm_str(doc.get("image_url")),
        _norm_str(doc.get("description")),
        _norm_num(doc.get("price")),
        _norm_num(doc.get("selling_price")),
        _norm_num(doc.get("cost_price")),
        _norm_num(doc.get("initial_price")),
    ])


@add_inventory_bp.route('/add_inventory/upload_image', methods=['POST'])
def upload_inventory_image():
    try:
        if 'image' not in request.files:
            return jsonify({'success': False, 'error': 'No image in request'}), 400

        image = request.files['image']
        if image.filename == '':
            return jsonify({'success': False, 'error': 'No file selected'}), 400

        if not (image and allowed_file(image.filename)):
            return jsonify({'success': False, 'error': 'Invalid file type'}), 400

        direct_url = f"https://api.cloudflare.com/client/v4/accounts/{CF_ACCOUNT_ID}/images/v2/direct_upload"
        headers    = {"Authorization": f"Bearer {CF_IMAGES_TOKEN}"}

        res = requests.post(direct_url, headers=headers, data={}, timeout=20)
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
            'created_at': datetime.utcnow(),
            'module': 'add_inventory'
        })

        return jsonify({'success': True, 'image_url': image_url, 'image_id': image_id, 'variant': variant})

    except Exception as e:
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 500


@add_inventory_bp.route('/uploads/<filename>')
def uploaded_inventory_file(filename):
    return send_from_directory(UPLOAD_FOLDER, filename)


@add_inventory_bp.route('/add_inventory', methods=['GET', 'POST'])
def add_inventory():
    if request.method == 'POST':
        try:
            request_id = (request.form.get('request_id') or '').strip()
            if request_id:
                now = datetime.utcnow()
                store = session.get('add_inventory_requests', {})
                pruned = {}
                for k, v in store.items():
                    try:
                        ts = datetime.fromisoformat(v)
                    except Exception:
                        continue
                    if now - ts <= timedelta(minutes=5):
                        pruned[k] = v
                if request_id in pruned:
                    flash("✅ Already submitted. Please wait for processing.", "success")
                    return redirect(url_for('add_inventory.add_inventory'))
                pruned[request_id] = now.isoformat()
                session['add_inventory_requests'] = pruned
                session.modified = True

            name = (request.form.get('name') or '').strip()
            description = (request.form.get('description') or '').strip()

            image_url = (request.form.get('image_url') or '').strip()
            image_id  = (request.form.get('image_id') or '').strip()

            selected_managers = request.form.getlist('manager_ids')

            initial_price_str = (request.form.get('initial_price') or '').strip()
            profit_input = (request.form.get('profit') or '').strip()

            cost_price_str = (request.form.get('cost_price') or '').strip()
            selling_price_str = (request.form.get('selling_price') or '').strip()

            qty_str = (request.form.get('qty') or '').strip()

            # ✅ NEW: expiry date (optional)
            expiry_str = (request.form.get('expiry_date') or '').strip()
            expiry_dt = parse_date_yyyy_mm_dd(expiry_str)

            # Required fields (allow qty "0")
            if name == "" or description == "" or image_url == "" or initial_price_str == "" or profit_input == "" or qty_str == "":
                flash("❌ All required fields must be provided.", "danger")
                return redirect(url_for('add_inventory.add_inventory'))

            if not selected_managers:
                flash("❌ Please select at least one manager.", "danger")
                return redirect(url_for('add_inventory.add_inventory'))

            initial_price = parse_money(initial_price_str)
            qty = parse_int(qty_str)

            if initial_price is None or initial_price < 0:
                flash("❌ Initial Price must be a valid non-negative number.", "danger")
                return redirect(url_for('add_inventory.add_inventory'))

            if qty is None or qty < 0:
                flash("❌ Quantity must be a non-negative integer (0 allowed).", "danger")
                return redirect(url_for('add_inventory.add_inventory'))

            final_price = parse_profit(profit_input, initial_price)
            if final_price is None:
                flash("❌ Profit must be a valid amount or percentage (e.g., 50 or 30%).", "danger")
                return redirect(url_for('add_inventory.add_inventory'))

            cost_price = parse_money(cost_price_str)
            if cost_price is None:
                cost_price = initial_price

            selling_price = parse_money(selling_price_str)
            if selling_price is None:
                selling_price = final_price

            margin = (selling_price - cost_price) if (selling_price is not None and cost_price is not None) else None

            initial_price = money2(initial_price)
            final_price   = money2(final_price)
            cost_price    = money2(cost_price)
            selling_price = money2(selling_price)
            margin        = money2(margin)

            valid_url = (
                image_url.startswith('/uploads/')
                or image_url.startswith('http://')
                or image_url.startswith('https://')
            )
            if not valid_url:
                flash("❌ Image URL looks invalid. Please re-upload the image.", "danger")
                return redirect(url_for('add_inventory.add_inventory'))

            # Expiry flags
            today0 = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
            is_expired = bool(expiry_dt and expiry_dt < today0)
            expiring_soon = bool(expiry_dt and (today0 <= expiry_dt <= (today0 + timedelta(days=30))))

            inserted = 0
            now = datetime.utcnow()

            for manager_id in selected_managers:
                oid = to_object_id(manager_id)
                if not oid:
                    continue

                doc = {
                    'name': name,
                    'qty': qty,
                    'is_out_of_stock': (qty == 0),

                    'expiry_date': expiry_dt,                 # ✅ datetime or None
                    'expiry_date_str': expiry_str or None,    # ✅ easy UI render
                    'is_expired': is_expired,
                    'expiring_soon': expiring_soon,

                    'description': description,
                    'image_url': image_url,
                    'image_id': image_id or None,
                    'manager_id': oid,

                    'initial_price': initial_price,
                    'profit_input': profit_input,
                    'price': final_price,

                    'cost_price': cost_price,
                    'selling_price': selling_price,
                    'margin': margin,

                    'created_at': now,
                    'updated_at': now,
                    'source': 'add_inventory_form_v3'
                }

                inventory_col.insert_one(doc)
                inserted += 1

            if inserted == 0:
                flash("❌ No items were added. Manager IDs may be invalid.", "danger")
                return redirect(url_for('add_inventory.add_inventory'))

            flash(f"✅ Inventory item added for {inserted} manager(s).", "success")
            return redirect(url_for('add_inventory.add_inventory'))

        except Exception as e:
            print("❌ Inventory add error:", str(e))
            traceback.print_exc()
            flash("❌ Something went wrong. Please try again.", "danger")
            return redirect(url_for('add_inventory.add_inventory'))

    managers = list(users_col.find({'role': 'manager'}))
    return render_template('add_inventory.html', managers=managers)


@add_inventory_bp.route('/add_inventory/missing_products', methods=['GET'])
def missing_products():
    manager_id = request.args.get('manager_id')
    manager_oid = to_object_id(manager_id)
    if not manager_oid:
        return jsonify({'ok': False, 'message': 'Invalid manager_id'}), 400

    manager = users_col.find_one({'_id': manager_oid, 'role': 'manager'}, {'name': 1})
    if not manager:
        return jsonify({'ok': False, 'message': 'Manager not found'}), 404

    projection = {
        "name": 1,
        "qty": 1,
        "is_out_of_stock": 1,
        "expiry_date": 1,
        "expiry_date_str": 1,
        "is_expired": 1,
        "expiring_soon": 1,
        "description": 1,
        "image_url": 1,
        "image_id": 1,
        "manager_id": 1,
        "initial_price": 1,
        "profit_input": 1,
        "price": 1,
        "cost_price": 1,
        "selling_price": 1,
        "margin": 1,
        "source": 1,
    }

    docs = list(inventory_col.find({"manager_id": {"$exists": True}}, projection))
    manager_keys = set()
    all_keys = {}
    for d in docs:
        key = _identity_key(d)
        if not key:
            continue
        all_keys.setdefault(key, d)
        mgr_val = d.get("manager_id")
        if mgr_val == manager_oid or str(mgr_val) == str(manager_oid):
            manager_keys.add(key)

    missing = []
    for key, doc in all_keys.items():
        if key in manager_keys:
            continue
        missing.append({
            "_id": str(doc.get("_id")),
            "name": doc.get("name"),
            "description": doc.get("description"),
            "image_url": doc.get("image_url"),
            "price": doc.get("price"),
            "selling_price": doc.get("selling_price"),
            "cost_price": doc.get("cost_price"),
            "qty": doc.get("qty"),
        })

    missing.sort(key=lambda x: (x.get("name") or "").lower())

    return jsonify({
        'ok': True,
        'manager_id': str(manager_oid),
        'manager_name': manager.get('name') or '',
        'results': missing
    })


@add_inventory_bp.route('/add_inventory/add_missing_products', methods=['POST'])
def add_missing_products():
    payload = request.get_json(silent=True) or {}
    manager_id = payload.get('manager_id')
    product_ids = payload.get('product_ids') or []

    manager_oid = to_object_id(manager_id)
    if not manager_oid:
        return jsonify({'ok': False, 'message': 'Invalid manager_id'}), 400
    if not isinstance(product_ids, list) or not product_ids:
        return jsonify({'ok': False, 'message': 'No products selected'}), 400

    manager = users_col.find_one({'_id': manager_oid, 'role': 'manager'}, {'name': 1})
    if not manager:
        return jsonify({'ok': False, 'message': 'Manager not found'}), 404

    existing_docs = list(inventory_col.find({"manager_id": manager_oid}))
    existing_keys = {_identity_key(d) for d in existing_docs if _identity_key(d)}

    added = 0
    skipped = 0
    now = datetime.utcnow()

    for pid in product_ids:
        src_oid = to_object_id(pid)
        if not src_oid:
            skipped += 1
            continue
        src = inventory_col.find_one({"_id": src_oid})
        if not src:
            skipped += 1
            continue

        key = _identity_key(src)
        if key and key in existing_keys:
            skipped += 1
            continue

        qty = int(src.get("qty", 0) or 0)
        new_doc = {
            "name": src.get("name"),
            "qty": qty,
            "is_out_of_stock": (qty == 0),

            "expiry_date": src.get("expiry_date"),
            "expiry_date_str": src.get("expiry_date_str"),
            "is_expired": src.get("is_expired"),
            "expiring_soon": src.get("expiring_soon"),

            "description": src.get("description"),
            "image_url": src.get("image_url"),
            "image_id": src.get("image_id") or None,
            "manager_id": manager_oid,

            "initial_price": src.get("initial_price"),
            "profit_input": src.get("profit_input"),
            "price": src.get("price"),
            "cost_price": src.get("cost_price"),
            "selling_price": src.get("selling_price"),
            "margin": src.get("margin"),

            "created_at": now,
            "updated_at": now,
            "source": src.get("source") or "missing_products_copy",
        }

        inventory_col.insert_one(new_doc)
        if key:
            existing_keys.add(key)
        added += 1

    return jsonify({
        'ok': True,
        'added_count': added,
        'skipped_count': skipped,
        'manager_name': manager.get('name') or ''
    })

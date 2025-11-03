import os
import json
import math
import traceback
from datetime import datetime
from typing import Optional, Tuple

from flask import (
    Blueprint, render_template, request, redirect,
    url_for, flash, jsonify, send_from_directory
)
from werkzeug.utils import secure_filename
from bson.objectid import ObjectId, InvalidId

from db import db

add_inventory_bp = Blueprint('add_inventory', __name__)
UPLOAD_FOLDER = os.path.join(os.getcwd(), 'uploads')
ALLOWED_EXTENSIONS = {'png', 'jpg', 'jpeg', 'gif'}

# Ensure uploads folder exists
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

# DB collections
inventory_col = db.inventory
users_col = db.users


# -----------------------------
# Helpers
# -----------------------------
def allowed_file(filename: str) -> bool:
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


def parse_money(val: str) -> Optional[float]:
    """
    Parse a money-like string to float. Returns None for empty/invalid.
    Accepts "123", "123.45", "  123  ".
    """
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
    """
    Returns the computed final price given initial_price and profit_input.
    profit_input can be '50' (amount) or '30%' (percent).
    Returns None if invalid.
    """
    if profit_input is None:
        return None
    s = profit_input.strip()
    if s == "":
        return None
    try:
        if s.endswith('%'):
            percent = float(s[:-1].strip())
            return round(initial_price + (initial_price * percent / 100.0), 2)
        else:
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
    """Round to 2dp if not None."""
    if v is None or (isinstance(v, float) and (math.isinf(v) or math.isnan(v))):
        return None
    try:
        return round(float(v), 2)
    except Exception:
        return None


# -----------------------------
# Image upload endpoint
# -----------------------------
@add_inventory_bp.route('/add_inventory/upload_image', methods=['POST'])
def upload_inventory_image():
    if 'image' not in request.files:
        return jsonify({'success': False, 'error': 'No image in request'})

    image = request.files['image']
    if image.filename == '':
        return jsonify({'success': False, 'error': 'No file selected'})

    if image and allowed_file(image.filename):
        # sanitize + avoid overwrite
        filename = secure_filename(image.filename)
        save_path = os.path.join(UPLOAD_FOLDER, filename)

        counter = 1
        base, ext = os.path.splitext(filename)
        while os.path.exists(save_path):
            filename = f"{base}_{counter}{ext}"
            save_path = os.path.join(UPLOAD_FOLDER, filename)
            counter += 1

        image.save(save_path)
        return jsonify({'success': True, 'image_url': f'/uploads/{filename}'})

    return jsonify({'success': False, 'error': 'Invalid file type'})


# Serve uploaded images
@add_inventory_bp.route('/uploads/<filename>')
def uploaded_inventory_file(filename):
    return send_from_directory(UPLOAD_FOLDER, filename)


# -----------------------------
# Main add_inventory route
# -----------------------------
@add_inventory_bp.route('/add_inventory', methods=['GET', 'POST'])
def add_inventory():
    if request.method == 'POST':
        try:
            # ------- Read basic fields -------
            name = (request.form.get('name') or '').strip()
            description = (request.form.get('description') or '').strip()
            image_url = (request.form.get('image_url') or '').strip()
            selected_managers = request.form.getlist('manager_ids')

            # legacy pricing inputs (kept)
            initial_price_str = (request.form.get('initial_price') or '').strip()
            profit_input = (request.form.get('profit') or '').strip()

            # new pricing inputs
            cost_price_str = (request.form.get('cost_price') or '').strip()
            selling_price_str = (request.form.get('selling_price') or '').strip()
            margin_str = (request.form.get('margin') or '').strip()  # read but we will recompute server-side

            qty_str = (request.form.get('qty') or '').strip()

            # ------- Validate presence -------
            if not all([name, description, image_url, initial_price_str, profit_input, qty_str]):
                flash("❌ All required fields must be provided.", "danger")
                return redirect(url_for('add_inventory.add_inventory'))

            if not selected_managers:
                flash("❌ Please select at least one manager.", "danger")
                return redirect(url_for('add_inventory.add_inventory'))

            # ------- Parse numbers safely -------
            initial_price = parse_money(initial_price_str)
            qty = parse_int(qty_str)
            if initial_price is None or initial_price < 0:
                flash("❌ Initial Price must be a valid non-negative number.", "danger")
                return redirect(url_for('add_inventory.add_inventory'))
            if qty is None or qty <= 0:
                flash("❌ Quantity must be a positive integer.", "danger")
                return redirect(url_for('add_inventory.add_inventory'))

            # Final price from legacy model (server-side recalculation for safety)
            final_price = parse_profit(profit_input, initial_price)
            if final_price is None:
                flash("❌ Profit must be a valid amount or percentage (e.g., 50 or 30%).", "danger")
                return redirect(url_for('add_inventory.add_inventory'))

            # ------- New pricing trio logic (safe defaults) -------
            # Default cost_price from initial_price if blank
            cost_price = parse_money(cost_price_str)
            if cost_price is None:
                cost_price = initial_price

            # Default selling_price from final_price if blank
            selling_price = parse_money(selling_price_str)
            if selling_price is None:
                selling_price = final_price

            # Margin is always recomputed server-side to avoid tampering
            margin = (selling_price - cost_price) if (selling_price is not None and cost_price is not None) else None

            # Round to 2dp
            initial_price = money2(initial_price)
            final_price = money2(final_price)
            cost_price = money2(cost_price)
            selling_price = money2(selling_price)
            margin = money2(margin)

            # ------- Basic image_url sanity check -------
            if not (image_url.startswith('/uploads/') or image_url.startswith('http://') or image_url.startswith('https://')):
                flash("❌ Image URL looks invalid. Please re-upload the image.", "danger")
                return redirect(url_for('add_inventory.add_inventory'))

            # ------- Insert per manager -------
            inserted = 0
            now = datetime.utcnow()
            for manager_id in selected_managers:
                oid = to_object_id(manager_id)
                if not oid:
                    # skip invalid ObjectId; continue with others
                    continue

                doc = {
                    'name': name,
                    'qty': qty,
                    'description': description,
                    'image_url': image_url,
                    'manager_id': oid,

                    # legacy pricing (kept for compatibility)
                    'initial_price': initial_price,
                    'profit_input': profit_input,
                    'price': final_price,  # final selling price used by legacy flow

                    # new pricing fields
                    'cost_price': cost_price,
                    'selling_price': selling_price,
                    'margin': margin,

                    # meta
                    'created_at': now,
                    'updated_at': now,
                    'source': 'add_inventory_form_v2'
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

    # GET
    managers = list(users_col.find({'role': 'manager'}))
    return render_template('add_inventory.html', managers=managers)

from flask import Blueprint, render_template, request, jsonify, redirect, url_for, flash, Response, session, send_from_directory
from pymongo import MongoClient
from pymongo.server_api import ServerApi
from bson import ObjectId
from datetime import datetime, timedelta
from db import db
import uuid
import os
from werkzeug.utils import secure_filename

view_bp = Blueprint('view', __name__)

# ✅ Fixed: Use proper uploads path
UPLOAD_FOLDER = os.path.join(os.getcwd(), 'uploads')
ALLOWED_EXTENSIONS = {'png', 'jpg', 'jpeg'}

# Ensure upload folder exists
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

customers_collection = db["customers"]
payments_collection  = db["payments"]
packages_collection  = db["packages"]   # NEW

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

# ----------------------------
# 📄 View Customers
# ----------------------------
@view_bp.route('/customers', methods=['GET'])
def view_customers():
    search_query = request.args.get('search', '').strip().lower()
    status_filter = request.args.get('status', 'all').lower()
    sort_by = request.args.get('sort_by', 'name_asc').lower()
    export = request.args.get('export', 'false').lower() == 'true'
    page = int(request.args.get('page', 1))
    per_page = 30
    skip = (page - 1) * per_page

    agent_id = session.get('agent_id')
    if not agent_id:
        flash("You must be logged in to view your customers.", "error")
        return redirect(url_for('login.login'))

    query = {'agent_id': agent_id}

    if search_query:
        query['$or'] = [
            {'name': {'$regex': search_query, '$options': 'i'}},
            {'phone_number': {'$regex': search_query}}
        ]

    projection = {'name': 1, 'phone_number': 1, 'image_url': 1}

    try:
        cursor = customers_collection.find(query, projection).skip(skip).limit(per_page)
        customers = list(cursor)
    except Exception:
        flash("Failed to fetch customer data.", "error")
        return redirect(url_for('login.login'))

    customer_data = []

    for customer in customers:
        customer_id = customer['_id']
        name = customer.get('name', '')
        phone = customer.get('phone_number', '')
        image_url = customer.get('image_url', '')

        latest_payment = payments_collection.find_one({'customer_id': customer_id}, sort=[('date', -1)])
        status = 'Active'
        last_payment_date = None

        try:
            if latest_payment and 'date' in latest_payment:
                last_payment_date = datetime.strptime(latest_payment['date'], '%Y-%m-%d')
                if last_payment_date < datetime.now() - timedelta(days=14):
                    status = 'Not Active'
            else:
                status = 'No Payment'
        except:
            status = 'Date Error'

        if status_filter == 'active' and status != 'Active':
            continue
        elif status_filter == 'not active' and status != 'Not Active':
            continue

        customer_data.append({
            '_id': str(customer_id),
            'name': name,
            'phone_number': phone,
            'image_url': image_url,
            'status': status,
            'last_payment_date': last_payment_date.strftime('%Y-%m-%d') if last_payment_date else 'N/A'
        })

    # Sort
    if sort_by == 'name_asc':
        customer_data.sort(key=lambda x: x['name'].lower())
    elif sort_by == 'name_desc':
        customer_data.sort(key=lambda x: x['name'].lower(), reverse=True)
    elif sort_by == 'payment_newest':
        customer_data.sort(key=lambda x: x['last_payment_date'], reverse=True)
    elif sort_by == 'payment_oldest':
        customer_data.sort(key=lambda x: x['last_payment_date'])

    # Export CSV if requested
    if export:
        def generate_csv():
            yield 'Name,Phone,Status,Last Payment\n'
            for c in customer_data:
                yield f"{c['name']},{c['phone_number']},{c['status']},{c['last_payment_date']}\n"

        return Response(generate_csv(), mimetype='text/csv',
                        headers={"Content-Disposition": "attachment; filename=customers.csv"})

    return render_template('view_customers.html',
                           customers=customer_data,
                           search_query=search_query,
                           status_filter=status_filter,
                           sort_by=sort_by,
                           page=page)

# ----------------------------
# 👤 View Customer Profile
# ----------------------------
@view_bp.route('/customer/<customer_id>', methods=['GET'])
def view_customer_profile(customer_id):
    try:
        customer_obj_id = ObjectId(customer_id)
    except Exception:
        return jsonify({'error': 'Invalid customer ID format'}), 400

    customer = customers_collection.find_one({'_id': customer_obj_id})
    if not customer:
        return jsonify({'error': 'Customer not found'}), 404

    payments = list(payments_collection.find({'customer_id': customer_obj_id}))
    deposits = [p for p in payments if p.get("payment_type") != "WITHDRAWAL"]
    withdrawals = [p for p in payments if p.get("payment_type") == "WITHDRAWAL"]

    total_debt = sum(p.get('product', {}).get('total', 0) for p in customer.get('purchases', []))
    deposits_sum = sum(p.get("amount", 0) for p in deposits)
    withdrawn_amount = sum(p.get("amount", 0) for p in withdrawals)
    total_paid = round(deposits_sum - withdrawn_amount, 2)
    amount_left = round(total_debt - total_paid, 2)

    current_status = customer.get("status", "payment_ongoing")
    if current_status == "payment_ongoing" and amount_left <= 0:
        customers_collection.update_one(
            {'_id': customer_obj_id},
            {'$set': {
                'status': 'completed',
                'status_updated_at': datetime.utcnow()
            }}
        )
        current_status = "completed"

    # Decorate purchases with per-product paid/left & time progress
    for index, purchase in enumerate(customer.get("purchases", [])):
        purchase_date = purchase.get("purchase_date")
        tracking = calculate_progress(purchase_date)
        purchase["progress"] = tracking["progress"]
        purchase["end_date"] = tracking["end_date"]

        product_total = float(purchase.get("product", {}).get("total", 0))
        product_payments = [p for p in deposits if p.get("product_index") == index]
        product_paid = sum(p.get("amount", 0) for p in product_payments)
        product_left = max(0, round(product_total - product_paid, 2))

        purchase["amount_paid"]  = product_paid
        purchase["amount_left"]  = product_left
        purchase["can_submit"]   = (product_left == 0)  # <- frontend uses this to show the button

    customer["status"] = current_status
    customer["_id"] = str(customer["_id"])
    penalties = customer.get("penalties", [])

    return render_template(
        'customer_profile.html',
        customer=customer,
        total_debt=total_debt,
        total_paid=total_paid,
        amount_left=amount_left,
        payments=deposits,
        withdrawals=withdrawals,
        penalties=penalties,
        withdrawn_amount=withdrawn_amount
    )

# ----------------------------
# 🆕 Submit product for Packaging (move to packages + delete payments)
# ----------------------------
@view_bp.route('/customer/<customer_id>/submit_for_packaging/<int:product_index>', methods=['POST'])
def submit_for_packaging(customer_id, product_index):
    """
    Preconditions:
      - agent must be logged in (session['agent_id'])
      - product's amount_left must be 0 (fully paid)
    Actions:
      - insert into packages collection (with agent_id, customer snapshot, product)
      - delete all payments for that product_index
      - remove the product from customer's purchases
    """
    agent_id = session.get('agent_id')
    if not agent_id:
        flash("Please log in as an agent.", "danger")
        return redirect(url_for('login.login'))

    try:
        customer_obj_id = ObjectId(customer_id)
    except Exception:
        flash("Invalid customer ID format.", "danger")
        return redirect(url_for('view.view_customer_profile', customer_id=customer_id))

    customer = customers_collection.find_one({'_id': customer_obj_id})
    if not customer:
        flash("Customer not found.", "danger")
        return redirect(url_for('view.view_customer_profile', customer_id=customer_id))

    purchases = customer.get("purchases", [])
    if product_index < 0 or product_index >= len(purchases):
        flash("Invalid product selection.", "warning")
        return redirect(url_for('view.view_customer_profile', customer_id=customer_id))

    purchase = purchases[product_index]
    product_info = purchase.get("product", {})
    product_total = float(product_info.get("total", 0))

    # Recompute pay/left for safety
    product_deposits = list(payments_collection.find({
        'customer_id': customer_obj_id,
        'payment_type': {'$ne': 'WITHDRAWAL'},
        'product_index': product_index
    }))
    product_withdrawals = list(payments_collection.find({
        'customer_id': customer_obj_id,
        'payment_type': 'WITHDRAWAL',
        'product_index': product_index
    }))

    paid_sum = sum(p.get("amount", 0) for p in product_deposits) - sum(p.get("amount", 0) for p in product_withdrawals)
    amount_left = max(0, round(product_total - paid_sum, 2))

    if amount_left != 0:
        flash("This product is not fully paid yet.", "warning")
        return redirect(url_for('view.view_customer_profile', customer_id=customer_id))

    # Build package document (snapshot)
    package_doc = {
        'customer_id': customer_obj_id,
        'customer_name': customer.get('name'),
        'customer_phone': customer.get('phone_number'),
        'agent_id': agent_id,
        'submitted_at': datetime.utcnow(),
        'source': 'customer_profile',
        'status': 'submitted',           # initial package status
        'product_index': product_index,  # original index for traceability
        'product': {
            'name': product_info.get('name'),
            'price': product_info.get('price'),
            'quantity': product_info.get('quantity'),
            'total': product_info.get('total'),
            'image_url': product_info.get('image_url'),
        },
        'purchase_meta': {
            'purchase_type': purchase.get('purchase_type'),
            'purchase_date': purchase.get('purchase_date'),
            'end_date': purchase.get('end_date'),
        },
        # Keep an internal payments snapshot for audit BEFORE deletion
        'payments_snapshot': {
            'deposit_ids': [p['_id'] for p in product_deposits],
            'withdrawal_ids': [p['_id'] for p in product_withdrawals],
            'paid_sum': paid_sum,
            'captured_at': datetime.utcnow()
        }
    }

    # Insert package first (so we don't lose info if deletion fails)
    inserted = packages_collection.insert_one(package_doc)

    # Delete payments for this product (both deposits and withdrawals with this product_index)
    payments_collection.delete_many({
        'customer_id': customer_obj_id,
        'product_index': product_index
    })

    # Remove the product from customer's purchases (unset by index then pull null)
    customers_collection.update_one(
        {'_id': customer_obj_id},
        {
            # keep overall customer status unchanged; packaging pipeline is handled by packages collection
            '$unset': {f'purchases.{product_index}': 1},
            '$set': {'updated_at': datetime.utcnow()}
        }
    )
    customers_collection.update_one(
        {'_id': customer_obj_id},
        {'$pull': {'purchases': None}}
    )

    flash("Submitted for packaging successfully.", "success")
    return redirect(url_for('view.view_customer_profile', customer_id=customer_id))

# ----------------------------
# 🔁 Update Status (existing)
# ----------------------------
@view_bp.route('/customer/<customer_id>/update_status/<next_status>', methods=['POST'])
def agent_update_status(customer_id, next_status):
    try:
        customer_obj_id = ObjectId(customer_id)
    except:
        flash("Invalid customer ID format.", "danger")
        return redirect(url_for('view.view_customer_profile', customer_id=customer_id))

    allowed_transitions = {
        "approved": "packaging",
        "packaging": "delivering",
        "delivering": "delivered"
    }

    customer = customers_collection.find_one({'_id': customer_obj_id})
    if not customer:
        flash("Customer not found.", "danger")
        return redirect(url_for('view.view_customer_profile', customer_id=customer_id))

    current_status = customer.get("status", "payment_ongoing")
    if allowed_transitions.get(current_status) != next_status:
        flash("Invalid status transition.", "warning")
        return redirect(url_for('view.view_customer_profile', customer_id=customer_id))

    customers_collection.update_one(
        {'_id': customer_obj_id},
        {'$set': {
            'status': next_status,
            'status_updated_at': datetime.utcnow()
        }}
    )
    flash(f"Customer status updated to '{next_status}'.", "success")
    return redirect(url_for('view.view_customer_profile', customer_id=customer_id))

# ----------------------------
# 🖼️ Upload Customer Image
# ----------------------------
@view_bp.route('/customer/<customer_id>/upload_image', methods=['POST'])
def upload_customer_image(customer_id):
    try:
        customer_obj_id = ObjectId(customer_id)

        image = request.files.get('image')
        if not image or not allowed_file(image.filename):
            return jsonify({'error': 'Invalid or missing image'}), 400

        filename = f"{uuid.uuid4().hex}_{secure_filename(image.filename)}"
        image_path = os.path.join(UPLOAD_FOLDER, filename)
        image.save(image_path)

        image_url = f"/uploads/{filename}"

        result = customers_collection.update_one(
            {'_id': customer_obj_id},
            {'$set': {'image_url': image_url}}
        )

        if result.modified_count == 0:
            return jsonify({'error': 'Image not updated'}), 500

        return jsonify({'success': True, 'image_url': image_url})

    except Exception as e:
        print("Upload error:", e)
        return jsonify({'error': str(e)}), 500

# ✅ Route to Serve Uploaded Images
@view_bp.route('/uploads/<filename>')
def serve_uploaded_file(filename):
    return send_from_directory(UPLOAD_FOLDER, filename)

# ----------------------------
# 📍 Update Customer Location
# ----------------------------
@view_bp.route('/customer/<customer_id>/update_location', methods=['POST'])
def update_customer_location(customer_id):
    try:
        customer_obj_id = ObjectId(customer_id)
        data = request.get_json()
        lat = float(data.get("latitude"))
        lon = float(data.get("longitude"))

        customers_collection.update_one(
            {'_id': customer_obj_id},
            {'$set': {
                'coordinates.latitude': lat,
                'coordinates.longitude': lon
            }}
        )
        return jsonify({"success": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 400

# ----------------------------
# 🧮 Helper: Progress Tracker
# ----------------------------
def calculate_progress(purchase_date_str):
    try:
        purchase_date = datetime.strptime(purchase_date_str, "%Y-%m-%d")
    except (ValueError, TypeError):
        return {"progress": 0, "end_date": "N/A"}

    end_date = purchase_date + timedelta(days=180)
    today = datetime.now()

    total_days = (end_date - purchase_date).days
    elapsed_days = (today - purchase_date).days

    progress = max(0, min(100, round((elapsed_days / total_days) * 100))) if total_days > 0 else 0

    return {
        "progress": progress,
        "end_date": end_date.strftime("%Y-%m-%d")
    }

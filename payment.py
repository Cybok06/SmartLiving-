from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify
from flask_login import login_required, current_user
from bson import ObjectId
from datetime import datetime
import requests
from urllib.parse import quote  # ✅ Correct quote function
from db import db

payment_bp = Blueprint('payment', __name__)

customers_collection = db["customers"]
payments_collection  = db["payments"]
users_collection     = db["users"]
sales_close_collection = db["sales_close"]  # ✅ NEW: daily rollup per agent

ARKESEL_API_KEY = 'b3dheEVqUWNyeVBuUGxDVWFxZ0E'


def _is_ajax(req) -> bool:
    return req.headers.get("X-Requested-With", "").lower() == "xmlhttprequest"


def _normalize_phone(raw: str) -> str | None:
    if not raw:
        return None
    p = raw.strip().replace(' ', '').replace('-', '').replace('+', '')
    if p.startswith('0') and len(p) == 10:
        p = '233' + p[1:]
    if p.startswith('233') and len(p) == 12:
        return p
    return None


def _existing_payment_info(customer_id: ObjectId, payment_date_str: str, is_susu: bool, product_index: int | None):
    """
    Look for payments already recorded for the same customer on the same date
    and within the same scope (SUSU vs specific PRODUCT index).
    Returns (count, total, scope_label).
    """
    q = {
        'customer_id': customer_id,
        'date': payment_date_str,
        'payment_type': {'$ne': 'WITHDRAWAL'}  # ignore withdrawals
    }

    scope_label = "SUSU" if is_susu else "PRODUCT"

    if is_susu:
        q['payment_type'] = 'SUSU'
    else:
        q['payment_type'] = 'PRODUCT'
        q['product_index'] = product_index if product_index is not None else -1

    docs = list(payments_collection.find(q))
    count = len(docs)
    total = sum(float(d.get('amount', 0.0)) for d in docs)

    if not is_susu:
        # Try to include product name in label (if any prior payment had it)
        for d in docs:
            name = d.get('product_name')
            if name:
                scope_label = f"PRODUCT: {name}"
                break

    return count, total, scope_label


@payment_bp.route('/add_payment', methods=['GET', 'POST'])
@login_required
def add_payment():
    if request.method == 'POST':
        customer_id = request.form.get('customer_id')
        product_index_raw = request.form.get('product_id')
        method = request.form.get('method') or "Cash"
        amount_raw = request.form.get('amount')
        date_str = request.form.get('date') or datetime.today().strftime('%Y-%m-%d')
        is_susu = request.form.get('is_susu') == 'yes'
        send_sms = request.form.get('send_sms') == 'yes'
        force_insert = request.form.get('force') == 'yes'

        # ---- Basic validation ----
        if not all([customer_id, amount_raw, date_str]):
            if _is_ajax(request):
                return jsonify(ok=False, message='All fields are required!'), 400
            flash('All fields are required!', 'danger')
            return redirect(url_for('payment.add_payment'))

        try:
            amount = float(amount_raw)
            payment_date = datetime.strptime(date_str, '%Y-%m-%d')
        except ValueError:
            if _is_ajax(request):
                return jsonify(ok=False, message='Invalid amount or date.'), 400
            flash('Invalid input for amount or date.', 'danger')
            return redirect(url_for('payment.add_payment'))

        # ---- Auth + entities ----
        try:
            cust_oid = ObjectId(customer_id)
        except Exception:
            if _is_ajax(request):
                return jsonify(ok=False, message='Invalid customer id.'), 400
            flash('Invalid customer.', 'danger')
            return redirect(url_for('payment.add_payment'))

        customer = customers_collection.find_one({
            '_id': cust_oid,
            'agent_id': str(current_user.id)
        })

        if not customer:
            if _is_ajax(request):
                return jsonify(ok=False, message='Unauthorized or customer not found.'), 403
            flash('Unauthorized access or customer not found.', 'danger')
            return redirect(url_for('payment.add_payment'))

        agent = users_collection.find_one({
            "_id": ObjectId(current_user.id),
            "role": "agent"
        })

        if not agent or "manager_id" not in agent:
            if _is_ajax(request):
                return jsonify(ok=False, message='Agent not linked to a manager. Contact admin.'), 400
            flash('Agent not linked to a manager. Contact admin.', 'danger')
            return redirect(url_for('payment.add_payment'))

        # ---- Product scope (if PRODUCT mode) ----
        product_index = None
        product_name = None
        product_total = None
        if not is_susu:
            try:
                product_index = int(product_index_raw)
            except Exception:
                if _is_ajax(request):
                    return jsonify(ok=False, message='Invalid product selected.'), 400
                flash('Invalid product selected.', 'danger')
                return redirect(url_for('payment.add_payment'))

            purchases = customer.get('purchases', [])
            if product_index < 0 or product_index >= len(purchases):
                if _is_ajax(request):
                    return jsonify(ok=False, message='Selected product not found for this customer.'), 404
                flash('Selected product not found for this customer.', 'danger')
                return redirect(url_for('payment.add_payment'))

            sel = purchases[product_index]
            prod = sel.get('product', {})
            product_name = prod.get('name', 'Unnamed Product')
            try:
                product_total = float(prod.get('total', 0))
            except Exception:
                product_total = 0.0

        # ---- Duplicate check (same customer, same date, same scope) ----
        existing_count, existing_total, scope_label = _existing_payment_info(
            cust_oid, payment_date.strftime('%Y-%m-%d'), is_susu, product_index
        )

        if existing_count > 0 and not force_insert:
            # Tell the frontend to confirm override
            if _is_ajax(request):
                return jsonify(
                    ok=False,
                    needs_confirm=True,
                    existing_count=existing_count,
                    existing_total=round(existing_total, 2),
                    scope=scope_label,
                    message=f"You already recorded {existing_count} payment(s) totaling GHS {existing_total:.2f} for this {scope_label} on {date_str}. Proceed anyway?"
                ), 409
            # Non-AJAX fallback: inform via flash (user can re-submit with force=yes)
            flash(f"You already recorded {existing_count} payment(s) totaling GHS {existing_total:.2f} for this {scope_label} on {date_str}. Resubmit to confirm.", 'warning')
            return redirect(url_for('payment.add_payment'))

        # ---- Build payment doc & insert ----
        now_utc = datetime.utcnow()
        time_str = now_utc.strftime('%H:%M:%S')     # ✅ NEW: separate time
        date_only_str = payment_date.strftime('%Y-%m-%d')

        payment_doc = {
            'customer_id': cust_oid,
            'agent_id': str(current_user.id),
            'manager_id': agent['manager_id'],
            'method': method,
            'amount': amount,
            'date': date_only_str,                   # YYYY-MM-DD
            'time': time_str,                        # ✅ NEW: HH:MM:SS
            'payment_type': 'SUSU' if is_susu else 'PRODUCT',
            'created_at': now_utc
        }

        if not is_susu:
            payment_doc.update({
                'product_index': product_index,
                'product_name': product_name,
                'product_total': product_total
            })

        payments_collection.insert_one(payment_doc)

        # ---- NEW: roll-up / daily close per agent in `sales_close` ----
        sales_close_filter = {
            'agent_id': str(current_user.id),
            'date': date_only_str
        }
        sales_close_update = {
            '$setOnInsert': {
                'agent_id': str(current_user.id),
                'manager_id': agent['manager_id'],
                'date': date_only_str,
                'created_at': now_utc
            },
            '$inc': {
                'total_amount': amount,
                'count': 1
            },
            '$set': {
                'last_payment_at': now_utc,
                'updated_at': now_utc
            }
        }
        sales_close_collection.update_one(sales_close_filter, sales_close_update, upsert=True)

        # ---- Optional SMS ----
        sms_status = None
        if send_sms:
            try:
                phone = _normalize_phone(customer.get('phone_number', ''))
                if phone is None:
                    sms_status = 'invalid_phone'
                else:
                    full_name = customer.get('name', 'Customer').strip()
                    first_name = full_name.split()[0] if full_name else 'Customer'

                    if is_susu:
                        total_paid = sum(
                            float(p.get('amount', 0))
                            for p in payments_collection.find({'customer_id': cust_oid, 'payment_type': 'SUSU'})
                        )
                        message = (
                            f"Dear {first_name}, we received GHS {amount:.2f} for your SUSU savings. "
                            f"Total saved: GHS {total_paid:.2f}. Keep saving with Smart Living!"
                        )
                    else:
                        all_payments = list(payments_collection.find({
                            'customer_id': cust_oid,
                            'product_index': product_index
                        }))
                        total_paid = sum(float(p.get('amount', 0)) for p in all_payments)
                        message = (
                            f"Dear {first_name}, We received GHS {amount:.2f} for your order. "
                            f"Total paid: GHS {total_paid:.2f}. For inquiries, visit our office. "
                            f"Call/WhatsApp 0556064611 / 0509639836 / 0598948132"
                        )

                    sms_url = (
                        f"https://sms.arkesel.com/sms/api?action=send-sms"
                        f"&api_key={ARKESEL_API_KEY}"
                        f"&to={phone}"
                        f"&from=SMARTLIVING"
                        f"&sms={quote(message)}"
                    )

                    resp = requests.get(sms_url, timeout=12)
                    if resp.status_code == 200 and '"code":"ok"' in resp.text:
                        sms_status = 'sent'
                    else:
                        sms_status = 'failed'
            except Exception as e:
                print("SMS sending error:", str(e))
                sms_status = 'error'

        # ---- Respond (AJAX vs normal) ----
        if _is_ajax(request):
            msg = 'Payment added successfully.'
            if sms_status == 'sent':
                msg = 'Payment added and SMS sent successfully.'
            elif sms_status in ('failed', 'error'):
                msg = 'Payment added, but SMS delivery failed.'
            elif sms_status == 'invalid_phone':
                msg = 'Payment added; phone number invalid for SMS.'

            return jsonify(ok=True, message=msg)

        # Non-AJAX fallback
        if sms_status == 'sent':
            flash('Payment added and SMS sent successfully.', 'success')
        elif sms_status in ('failed', 'error'):
            flash('Payment added but SMS delivery failed.', 'warning')
        elif sms_status == 'invalid_phone':
            flash('Payment added; phone number invalid for SMS.', 'warning')
        else:
            flash('Payment added successfully. (SMS not sent)', 'success')

        return redirect(url_for('payment.add_payment'))

    # ---------- GET ----------
    raw_customers = customers_collection.find(
        {'agent_id': str(current_user.id)},
        {'name': 1, 'phone_number': 1, 'purchases': 1}
    )

    customers = []
    for c in raw_customers:
        c['_id'] = str(c['_id'])
        customers.append(c)

    today = datetime.today().strftime('%Y-%m-%d')
    return render_template('add_payment.html', customers=customers, today=today)


@payment_bp.route('/all_payments', methods=['GET'])
@login_required
def view_all_payments():
    selected_date = request.args.get('date') or datetime.today().strftime('%Y-%m-%d')

    agent_customers = list(customers_collection.find({'agent_id': str(current_user.id)}))
    agent_customer_ids = [c['_id'] for c in agent_customers]

    payments_on_date = list(payments_collection.find({
        'customer_id': {'$in': agent_customer_ids},
        'date': selected_date,
        'payment_type': {'$ne': 'WITHDRAWAL'}  # ✅ Exclude withdrawals
    }))

    grouped = {}
    for p in payments_on_date:
        customer = customers_collection.find_one({'_id': p.get('customer_id')})
        if not customer:
            continue

        cid = str(p['customer_id'])
        if cid not in grouped:
            grouped[cid] = {
                'customer_name': customer.get('name', 'Unknown'),
                'phone_number': customer.get('phone_number', 'N/A'),
                'date': selected_date,
                'amounts': [],
                'total_amount': 0.0,
                'payment_count': 0
            }

        grouped[cid]['amounts'].append(f"{float(p.get('amount', 0)):.2f}")
        grouped[cid]['total_amount'] += float(p.get('amount', 0))
        grouped[cid]['payment_count'] += 1

    summaries = list(grouped.values())
    return render_template('view_all_payments.html', payments=summaries, selected_date=selected_date)


@payment_bp.route('/payment/product_paid', methods=['GET'])
def get_product_paid():
    if not current_user.is_authenticated:
        return jsonify(ok=False, message='Unauthorized.'), 401
    customer_id = request.args.get('customer_id')
    product_index_raw = request.args.get('product_index')
    if not customer_id or product_index_raw is None:
        return jsonify(ok=False, message='Missing customer or product.'), 400

    try:
        cust_oid = ObjectId(customer_id)
        product_index = int(product_index_raw)
    except Exception:
        return jsonify(ok=False, message='Invalid customer or product.'), 400

    customer = customers_collection.find_one({
        '_id': cust_oid,
        'agent_id': str(current_user.id)
    })
    if not customer:
        return jsonify(ok=False, message='Unauthorized or customer not found.'), 403

    purchases = customer.get('purchases', [])
    if product_index < 0 or product_index >= len(purchases):
        return jsonify(ok=False, message='Product not found for this customer.'), 404

    product = purchases[product_index].get('product', {})
    try:
        product_total = float(product.get('total', 0))
    except Exception:
        product_total = 0.0

    deposits = payments_collection.find({
        'customer_id': cust_oid,
        'payment_type': 'PRODUCT',
        'product_index': product_index
    })
    withdrawals = payments_collection.find({
        'customer_id': cust_oid,
        'payment_type': 'WITHDRAWAL',
        'product_index': product_index
    })

    paid_sum = sum(float(p.get('amount', 0)) for p in deposits) - sum(float(p.get('amount', 0)) for p in withdrawals)
    paid_sum = round(paid_sum, 2)
    amount_left = max(0, round(product_total - paid_sum, 2))

    return jsonify(ok=True, paid=paid_sum, total=product_total, left=amount_left)


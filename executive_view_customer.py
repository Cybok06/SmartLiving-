from flask import Blueprint, render_template, redirect, url_for, flash
from bson.objectid import ObjectId
from datetime import datetime
from db import db

executive_view_bp = Blueprint("executive_view", __name__)

customers_col = db["customers"]
payments_col = db["payments"]

@executive_view_bp.route('/executive/customer/<customer_id>')
def executive_customer_profile(customer_id):
    try:
        customer_object_id = ObjectId(customer_id)
    except Exception:
        flash("Invalid customer ID format.", "danger")
        return redirect(url_for('executive_customers.executive_customers_list'))

    customer = customers_col.find_one({'_id': customer_object_id})
    if not customer:
        return "Customer not found", 404

    purchases = customer.get('purchases', [])
    total_debt = sum(int(p['product'].get('total', 0)) for p in purchases if 'product' in p)

    all_payments = list(payments_col.find({"customer_id": customer_object_id}))
    deposits = sum(p.get("amount", 0) for p in all_payments if p.get("payment_type") != "WITHDRAWAL")
    withdrawals = [p for p in all_payments if p.get("payment_type") == "WITHDRAWAL"]
    withdrawn_amount = sum(p.get("amount", 0) for p in withdrawals)

    total_paid = round(deposits - withdrawn_amount, 2)
    withdrawn_amount = round(withdrawn_amount, 2)
    amount_left = round(total_debt - total_paid, 2)

    current_status = customer.get("status", "payment_ongoing")
    steps = ["payment_ongoing", "completed", "approved", "packaging", "delivering", "delivered"]
    penalties = customer.get('penalties', [])
    total_penalty = round(sum(p.get("amount", 0) for p in penalties), 2)

    # Add progress to each product
    for p in purchases:
        try:
            start = datetime.strptime(p.get("purchase_date", "")[:10], "%Y-%m-%d")
            end = datetime.strptime(p.get("end_date", "")[:10], "%Y-%m-%d")
            total_days = (end - start).days
            elapsed_days = (datetime.today() - start).days
            progress = int((elapsed_days / total_days) * 100) if total_days > 0 else 100
            p["product"]["progress"] = max(0, min(progress, 100))
        except:
            p["product"]["progress"] = 0

    return render_template(
        'executive_customer_profile.html',
        customer=customer,
        total_debt=total_debt,
        total_paid=total_paid,
        withdrawn_amount=withdrawn_amount,
        amount_left=amount_left,
        withdrawals=withdrawals,
        steps=steps,
        current_status=current_status,
        penalties=penalties,
        total_penalty=total_penalty
    )



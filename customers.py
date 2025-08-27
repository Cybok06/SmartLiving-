from flask import Blueprint, render_template, session, redirect, url_for, request, flash
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from bson.objectid import ObjectId
from datetime import datetime, date
from db import db

customers_bp = Blueprint('customers', __name__)


users_col = db.users
customers_col = db.customers
payments_col = db.payments
deleted_col = db.deleted




@customers_bp.route('/customers')
def customers_list():
    if 'manager_id' not in session:
        return redirect(url_for('login.login'))

    manager_id = ObjectId(session['manager_id'])

    # Pagination
    page = int(request.args.get('page', 1))
    per_page = 30
    skip = (page - 1) * per_page

    # Filters
    search_term = request.args.get('search', '').strip()
    agent_id = request.args.get('agent_id', '').strip()
    status_filter = request.args.get('status', '').strip()

    # Base filter
    base_filter = {'manager_id': manager_id}
    if agent_id and agent_id != 'all':
        try:
            base_filter['agent_id'] = ObjectId(agent_id)
        except:
            flash("Invalid agent ID", "warning")

    if search_term:
        base_filter['$or'] = [
            {'name': {'$regex': search_term, '$options': 'i'}},
            {'phone_number': {'$regex': search_term, '$options': 'i'}}
        ]

    # Fetch all matching customers
    all_customers = list(customers_col.find(base_filter))
    customer_ids = [c['_id'] for c in all_customers]
    payments = list(payments_col.find({'customer_id': {'$in': customer_ids}}))

    from collections import defaultdict
    payment_map = defaultdict(list)
    for p in payments:
        payment_map[p['customer_id']].append(p)

    total_active = 0
    total_overdue = 0
    customers_with_status = []

    for c in all_customers:
        cid = c['_id']
        cust_payments = payment_map.get(cid, [])

        total_debt = sum(int(p.get('product', {}).get('total', 0)) for p in c.get('purchases', []))
        paid = sum(p.get('amount', 0) for p in cust_payments if p.get('payment_type') != 'WITHDRAWAL')
        withdrawn = sum(p.get('amount', 0) for p in cust_payments if p.get('payment_type') == 'WITHDRAWAL')
        net_paid = paid - withdrawn

        overdue = False
        for p in c.get('purchases', []):
            end_str = p.get("end_date")
            if end_str:
                try:
                    end_date = datetime.strptime(end_str, "%Y-%m-%d").date()
                except ValueError:
                    try:
                        end_date = datetime.strptime(end_str, "%y-%m-%d").date()
                    except ValueError:
                        continue  # skip if still invalid

                if end_date < date.today() and net_paid < total_debt:
                    overdue = True
                    break

        if overdue:
            status = "Overdue"
            total_overdue += 1
        elif cust_payments:
            status = "Active"
            total_active += 1
        else:
            status = "Not Active"

        customers_with_status.append({
            'id': str(cid),
            'name': c.get('name', 'No Name'),
            'phone': c.get('phone_number', 'N/A'),
            'image_url': c.get('image_url', 'https://via.placeholder.com/80'),
            'status': status
        })

    total_customers = len(customers_with_status)
    total_not_active = total_customers - total_active

    # ✅ Apply status filter
    if status_filter:
        customers_with_status = [c for c in customers_with_status if c['status'].lower() == status_filter.lower()]

    # ✅ Paginate after filtering
    start = (page - 1) * per_page
    end = start + per_page
    paginated_customers = customers_with_status[start:end]
    total_pages = (len(customers_with_status) + per_page - 1) // per_page

    agents = list(users_col.find({'manager_id': manager_id, 'role': 'agent'}, {'_id': 1, 'name': 1}))

    return render_template(
        'customers.html',
        customers=paginated_customers,
        total_customers=total_customers,
        total_active=total_active,
        total_overdue=total_overdue,
        total_not_active=total_not_active,
        agents=agents,
        selected_agent=agent_id,
        search_term=search_term,
        page=page,
        total_pages=total_pages,
        selected_status=status_filter
    )



@customers_bp.route('/customer/<customer_id>')
def customer_profile(customer_id):
    if 'manager_id' not in session:
        return redirect(url_for('login.login'))

    try:
        customer_object_id = ObjectId(customer_id)
    except Exception:
        flash("Invalid customer ID format.", "danger")
        return redirect(url_for('customers.customers_list'))

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

    if current_status == "payment_ongoing" and amount_left <= 0:
        customers_col.update_one(
            {'_id': customer_object_id},
            {'$set': {'status': 'completed', 'status_updated_at': datetime.utcnow()}}
        )
        customer["status"] = "completed"
    else:
        customer["status"] = current_status

    steps = ["payment_ongoing", "completed", "approved", "packaging", "delivering", "delivered"]

    penalties = customer.get('penalties', [])
     
    total_penalty = round(sum(p.get("amount", 0) for p in penalties), 2)


    return render_template(
        'agent_customer_profile.html',
        customer=customer,
        total_debt=total_debt,
        total_paid=total_paid,
        withdrawn_amount=withdrawn_amount,
        amount_left=amount_left,
        withdrawals=withdrawals,
        steps=steps,
        current_status=customer["status"],
        penalties=penalties,
        total_penalty=total_penalty  # ✅ add this  
    )


@customers_bp.route('/customer/<customer_id>/approve_status', methods=['POST'])
def approve_customer_status(customer_id):
    if 'manager_id' not in session:
        return redirect(url_for('login.login'))

    try:
        customer_object_id = ObjectId(customer_id)
        manager_id = ObjectId(session['manager_id'])
    except Exception:
        flash("Invalid customer ID format.", "danger")
        return redirect(url_for('customers.customers_list'))

    customer = customers_col.find_one({'_id': customer_object_id})
    if not customer:
        flash("Customer not found.", "danger")
        return redirect(url_for('customers.customers_list'))

    current_status = customer.get("status", "payment_ongoing")
    if current_status != "completed":
        flash("Customer must be in 'completed' status to approve.", "warning")
        return redirect(url_for('customers.customer_profile', customer_id=customer_id))

    purchases = customer.get('purchases', [])

    for p in purchases:
        product = p.get("product", {})
        end_date_str = p.get("end_date")
        if end_date_str:
            try:
                end_date = datetime.strptime(end_date_str, "%Y-%m-%d").date()
                purchase_date = datetime.strptime(p.get("purchase_date", "")[:10], "%Y-%m-%d").date()
                today = date.today()
                total_days = (end_date - purchase_date).days
                elapsed_days = (today - purchase_date).days
                progress = int((elapsed_days / total_days) * 100) if total_days > 0 else 100
                product["progress"] = max(0, min(progress, 100))
            except:
                product["progress"] = None
        else:
            product["progress"] = None

    insufficient_items = []

    for purchase in purchases:
        product = purchase.get('product')
        quantity = int(product.get('quantity', 1))
        components = product.get('components', [])

        for component in components:
            comp_id = ObjectId(component['_id']) if isinstance(component['_id'], str) else component['_id']
            required_qty = int(component['quantity']) * quantity

            inventory_item = db.inventory.find_one({
                '_id': comp_id,
                'manager_id': manager_id
            })

            if not inventory_item or inventory_item.get('qty', 0) < required_qty:
                insufficient_items.append({
                    'component_id': str(comp_id),
                    'needed': required_qty,
                    'available': inventory_item.get('qty', 0) if inventory_item else 0
                })

    if insufficient_items:
        details = "; ".join(
            [f"ID: {item['component_id']} (Need: {item['needed']}, Have: {item['available']})"
             for item in insufficient_items]
        )
        flash(f"Cannot approve. Not enough inventory for components: {details}", "danger")
        return redirect(url_for('customers.customer_profile', customer_id=customer_id))

    # Deduct inventory
    for purchase in purchases:
        product = purchase.get('product')
        quantity = int(product.get('quantity', 1))
        components = product.get('components', [])

        for component in components:
            comp_id = ObjectId(component['_id']) if isinstance(component['_id'], str) else component['_id']
            used_qty = int(component['quantity']) * quantity

            db.inventory.update_one(
                {'_id': comp_id, 'manager_id': manager_id},
                {'$inc': {'qty': -used_qty}}
            )

    customers_col.update_one(
        {'_id': customer_object_id},
        {'$set': {
            'status': 'approved',
            'status_updated_at': datetime.utcnow()
        }}
    )

    flash("Customer status updated to 'approved' and inventory adjusted.", "success")
    return redirect(url_for('customers.customer_profile', customer_id=customer_id))


@customers_bp.route('/customer/<customer_id>/edit', methods=['POST'])
def edit_customer(customer_id):
    if 'manager_id' not in session:
        return redirect(url_for('login.login'))

    form_data = {
        'name': request.form.get('name'),
        'phone_number': request.form.get('phone_number'),
        'location': request.form.get('location'),
        'occupation': request.form.get('occupation'),
        'comment': request.form.get('comment'),
    }

    result = customers_col.update_one({'_id': ObjectId(customer_id)}, {'$set': form_data})
    flash('Customer updated successfully!' if result.modified_count else 'No changes made.', 'success')
    return redirect(url_for('customers.customer_profile', customer_id=customer_id))


@customers_bp.route('/customer/<customer_id>/delete', methods=['POST'])
def delete_customer(customer_id):
    if 'manager_id' not in session:
        return redirect(url_for('login.login'))

    customer = customers_col.find_one({'_id': ObjectId(customer_id)})
    if not customer:
        flash("Customer not found", "danger")
        return redirect(url_for('customers.customers_list'))

    deleted_col.insert_one(customer)
    customers_col.delete_one({'_id': ObjectId(customer_id)})
    flash('Customer deleted and archived.', 'info')
    return redirect(url_for('customers.customers_list'))


@customers_bp.route('/customer/<customer_id>/withdraw', methods=['POST'])
def withdraw_from_customer(customer_id):
    if 'manager_id' not in session:
        return redirect(url_for('login.login'))

    customer = customers_col.find_one({'_id': ObjectId(customer_id)})
    if not customer:
        flash("Customer not found.", "danger")
        return redirect(url_for('customers.customers_list'))

    agent_id = customer.get('agent_id')
    manager_id = ObjectId(session['manager_id'])

    # Combined mode (new modal with both fields)
    if request.form.get("combined_mode"):
        try:
            withdraw_amount = float(request.form.get("withdraw_amount", 0))
        except (TypeError, ValueError):
            withdraw_amount = 0

        try:
            deduction_amount = float(request.form.get("deduction_amount", 0))
        except (TypeError, ValueError):
            deduction_amount = 0

        note = request.form.get("note", "").strip()

        if withdraw_amount <= 0 and deduction_amount <= 0:
            flash("Please enter at least one valid amount.", "warning")
            return redirect(url_for('customers.customer_profile', customer_id=customer_id))

        now_str = datetime.utcnow().strftime('%Y-%m-%d')
        now_ts = datetime.utcnow()

        if withdraw_amount > 0:
            payments_col.insert_one({
                "manager_id": manager_id,
                "agent_id": agent_id,
                "customer_id": ObjectId(customer_id),
                "amount": withdraw_amount,
                "payment_type": "WITHDRAWAL",
                "method": "Manual",
                "note": note,
                "date": now_str,
                "timestamp": now_ts
            })

        if deduction_amount > 0:
            payments_col.insert_one({
                "manager_id": manager_id,
                "agent_id": agent_id,
                "customer_id": ObjectId(customer_id),
                "amount": deduction_amount,
                "payment_type": "WITHDRAWAL",
                "method": "Deduction",
                "note": "SUSU deduction" if not note else note,
                "date": now_str,
                "timestamp": now_ts
            })

        flash("✅ Withdrawal and/or Deduction recorded successfully.", "success")
        return redirect(url_for('customers.customer_profile', customer_id=customer_id))

    # Fallback: Old single withdrawal or deduction (for backward compatibility)
    try:
        amount = float(request.form.get('amount'))
    except (TypeError, ValueError):
        flash("Invalid amount entered.", "danger")
        return redirect(url_for('customers.customer_profile', customer_id=customer_id))

    if amount <= 0:
        flash("Enter a positive amount.", "danger")
        return redirect(url_for('customers.customer_profile', customer_id=customer_id))

    is_deduction = request.form.get("deduction_only") == "true"

    payment_record = {
        'manager_id': manager_id,
        'agent_id': agent_id,
        'customer_id': ObjectId(customer_id),
        'amount': amount,
        'payment_type': 'WITHDRAWAL',
        'method': "Deduction" if is_deduction else "Manual",
        'note': "SUSU deduction" if is_deduction else "",
        'date': datetime.utcnow().strftime('%Y-%m-%d'),
        'timestamp': datetime.utcnow()
    }

    payments_col.insert_one(payment_record)

    flash("✅ Deduction recorded successfully." if is_deduction else "✅ Withdrawal recorded successfully.", "success")
    return redirect(url_for('customers.customer_profile', customer_id=customer_id))

@customers_bp.route('/customer/<customer_id>/add_penalty/<int:purchase_index>', methods=['POST'])
def add_penalty(customer_id, purchase_index):
    if 'manager_id' not in session:
        return redirect(url_for('login.login'))

    try:
        customer_object_id = ObjectId(customer_id)
        customer = customers_col.find_one({'_id': customer_object_id})
        if not customer:
            flash("Customer not found.", "danger")
            return redirect(url_for('customers.customer_profile', customer_id=customer_id))

        amount = float(request.form.get('amount', 0))
        reason = request.form.get('reason')
        new_end_date = request.form.get('new_end_date')

        if amount <= 0 or not reason:
            flash("Amount and reason are required. Amount must be greater than 0.", "danger")
            return redirect(url_for('customers.customer_profile', customer_id=customer_id))

        penalty = {
            "amount": amount,
            "reason": reason,
            "date": datetime.utcnow().isoformat()
        }

        if new_end_date:
            penalty["new_end_date"] = new_end_date
            purchases = customer.get("purchases", [])
            if 0 <= purchase_index < len(purchases):
                purchases[purchase_index]["end_date"] = new_end_date
                customers_col.update_one(
                    {'_id': customer_object_id},
                    {'$set': {"purchases": purchases}}
                )

        customers_col.update_one(
            {'_id': customer_object_id},
            {'$push': {"penalties": penalty}}
        )

        flash("Penalty added successfully.", "success")
        return redirect(url_for('customers.customer_profile', customer_id=customer_id))

    except Exception as e:
        print("Error adding penalty:", str(e))
        flash("An error occurred while adding penalty.", "danger")
        return redirect(url_for('customers.customer_profile', customer_id=customer_id))

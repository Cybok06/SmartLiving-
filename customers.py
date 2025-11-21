from __future__ import annotations

from datetime import datetime, date
from typing import Dict, Any, List, Optional
import re

from flask import (
    Blueprint, render_template, session, redirect,
    url_for, request, flash
)
from bson.objectid import ObjectId

from db import db

customers_bp = Blueprint('customers', __name__)

# Collections
users_col = db.users
customers_col = db.customers
payments_col = db.payments
deleted_col = db.deleted


def _require_manager() -> Optional[ObjectId]:
    """
    Guard: ensure a manager is logged in.
    Returns manager ObjectId or None (and redirects via caller).
    """
    mid = session.get("manager_id")
    if not mid:
        return None
    try:
        return ObjectId(mid)
    except Exception:
        return None


def _slugify(text: str) -> str:
    """
    Simple slug for tag keys (lowercase, alnum + hyphen).
    """
    text = (text or "").strip().lower()
    text = re.sub(r"[^a-z0-9]+", "-", text)
    return text.strip("-") or "tag"


@customers_bp.route('/customers')
def customers_list():
    """
    Manager-facing customer list with:
      - status computation (Active / Overdue / Not Active)
      - Not Active: last payment > 14 days ago (or never paid)
      - search by name/phone
      - filter by agent
      - filter by status
      - filter by tag
      - agent name on each card
      - tag palette for quick filtering
    """
    manager_oid = _require_manager()
    if not manager_oid:
        return redirect(url_for('login.login'))

    # ---------- Pagination ----------
    page = max(int(request.args.get('page', 1) or 1), 1)
    # lighter page: only 10 customers per page
    per_page = 10

    # ---------- Filters ----------
    search_term = (request.args.get('search') or '').strip()
    agent_id_param = (request.args.get('agent_id') or '').strip()
    status_filter = (request.args.get('status') or '').strip()
    tag_filter = (request.args.get('tag') or '').strip()

    # Base filter – only this manager’s customers
    base_filter: Dict[str, Any] = {'manager_id': manager_oid}

    # Filter by agent (handle both string and ObjectId in DB)
    if agent_id_param and agent_id_param != 'all':
        agent_filter_values: List[Any] = [agent_id_param]
        try:
            agent_filter_values.append(ObjectId(agent_id_param))
        except Exception:
            # ignore if not a valid ObjectId, still match string
            pass
        base_filter['agent_id'] = {'$in': agent_filter_values}

    # Search by name / phone
    if search_term:
        base_filter['$or'] = [
            {'name': {'$regex': search_term, '$options': 'i'}},
            {'phone_number': {'$regex': search_term, '$options': 'i'}},
        ]

    # Filter by tag (tag key)
    if tag_filter:
        # tags stored as [{ key, label, color, note, ... }]
        base_filter['tags.key'] = tag_filter

    # ---------- Fetch customers (filtered by manager/agent/search/tag) ----------
    # Projection to reduce payload (faster load)
    projection = {
        'name': 1,
        'phone_number': 1,
        'image_url': 1,
        'purchases': 1,
        'agent_id': 1,
        'tags': 1,
        'manager_id': 1,
    }
    all_customers: List[Dict[str, Any]] = list(customers_col.find(base_filter, projection))

    # ---------- Agents list for filter dropdown ----------
    agents = list(users_col.find(
        {'manager_id': manager_oid, 'role': 'agent'},
        {'_id': 1, 'name': 1}
    ))

    if not all_customers:
        # still render page with empty stats + agent dropdown
        return render_template(
            'customers.html',
            customers=[],
            total_customers=0,
            total_active=0,
            total_overdue=0,
            total_not_active=0,
            agents=agents,
            selected_agent=agent_id_param,
            search_term=search_term,
            page=page,
            total_pages=1,
            selected_status=status_filter,
            available_tags=[],
            selected_tag=tag_filter
        )

    # ---------- Pre-load agent names for these customers ----------
    # Some customers may not have agent_id, and agent_id may be string or ObjectId
    agent_ids_raw = [c.get('agent_id') for c in all_customers if c.get('agent_id')]

    agent_oid_set = set()
    for aid in agent_ids_raw:
        try:
            if isinstance(aid, ObjectId):
                agent_oid_set.add(aid)
            else:
                agent_oid_set.add(ObjectId(aid))
        except Exception:
            # ignore invalid ids
            continue

    agent_map: Dict[str, str] = {}
    if agent_oid_set:
        for a in users_col.find({'_id': {'$in': list(agent_oid_set)}}, {'_id': 1, 'name': 1}):
            agent_map[str(a['_id'])] = a.get('name', 'Agent')

    # ---------- Payments for status calc ----------
    customer_ids = [c['_id'] for c in all_customers]
    payments = list(payments_col.find({'customer_id': {'$in': customer_ids}}))

    from collections import defaultdict
    payment_map: Dict[ObjectId, List[Dict[str, Any]]] = defaultdict(list)
    for p in payments:
        cid = p.get('customer_id')
        if cid:
            payment_map[cid].append(p)

    # ---------- Build customers + compute statuses + tags summary ----------
    total_active = 0
    total_overdue = 0
    total_not_active = 0
    customers_with_status: List[Dict[str, Any]] = []

    # tag palette for top filter buttons
    tags_summary: Dict[str, Dict[str, Any]] = {}

    today = date.today()

    for c in all_customers:
        cid: ObjectId = c['_id']
        cust_payments = payment_map.get(cid, [])

        # ----- Compute last payment date (for Not Active logic) -----
        last_payment_date: Optional[date] = None
        for p in cust_payments:
            dt_candidate: Optional[date] = None

            ts = p.get('timestamp')
            if isinstance(ts, datetime):
                dt_candidate = ts.date()
            else:
                date_str = p.get('date')
                if isinstance(date_str, str):
                    try:
                        dt_candidate = datetime.strptime(date_str, "%Y-%m-%d").date()
                    except Exception:
                        pass

            if dt_candidate:
                if not last_payment_date or dt_candidate > last_payment_date:
                    last_payment_date = dt_candidate

        days_since_last_payment: Optional[int] = None
        if last_payment_date:
            days_since_last_payment = (today - last_payment_date).days

        # ----- Debt & Overdue logic stays same -----
        total_debt = sum(
            int(purch.get('product', {}).get('total', 0))
            for purch in c.get('purchases', [])
        )
        paid = sum(
            p.get('amount', 0)
            for p in cust_payments
            if p.get('payment_type') != 'WITHDRAWAL'
        )
        withdrawn = sum(
            p.get('amount', 0)
            for p in cust_payments
            if p.get('payment_type') == 'WITHDRAWAL'
        )
        net_paid = paid - withdrawn

        overdue = False
        for purch in c.get('purchases', []):
            end_str = purch.get("end_date")
            if not end_str:
                continue
            try:
                end_date = datetime.strptime(end_str, "%Y-%m-%d").date()
            except ValueError:
                try:
                    end_date = datetime.strptime(end_str, "%y-%m-%d").date()
                except ValueError:
                    continue

            if end_date < today and net_paid < total_debt:
                overdue = True
                break

        # ----- Status definition -----
        # 1. Overdue: business rule based on end_date & net_paid
        # 2. Active: has payment in last 14 days and not overdue
        # 3. Not Active: no payment in last 14 days (including never paid)
        if overdue:
            status = "Overdue"
            total_overdue += 1
        else:
            if last_payment_date and days_since_last_payment is not None and days_since_last_payment <= 14:
                status = "Active"
                total_active += 1
            else:
                status = "Not Active"
                total_not_active += 1

        # ----- Agent name for card -----
        raw_agent_id = c.get('agent_id')
        agent_name = "Unassigned"
        if raw_agent_id:
            try:
                agent_name = agent_map.get(str(raw_agent_id), "Unassigned")
            except Exception:
                agent_name = "Unassigned"

        # ----- Avatar initials (e.g., Ama -> A) -----
        raw_name = (c.get('name') or '').strip() or "Unknown"
        first_letter = raw_name[0].upper() if raw_name else "?"

        # ----- Tags on this customer -----
        customer_tags = c.get('tags', []) or []
        normalized_tags: List[Dict[str, Any]] = []
        for t in customer_tags:
            t_key = t.get('key') or _slugify(t.get('label', 'tag'))
            t_label = t.get('label') or t_key.title()
            t_color = t.get('color') or "#6366f1"  # default indigo-like

            normalized_tags.append({
                "key": t_key,
                "label": t_label,
                "color": t_color,
            })

            # accumulate summary (count per tag)
            if t_key not in tags_summary:
                tags_summary[t_key] = {
                    "key": t_key,
                    "label": t_label,
                    "color": t_color,
                    "count": 0,
                }
            tags_summary[t_key]["count"] += 1

        customers_with_status.append({
            'id': str(cid),
            'name': raw_name,
            'phone': c.get('phone_number', 'N/A'),
            'image_url': c.get('image_url', ''),  # template will handle fallback
            'status': status,
            'agent_name': agent_name,
            'initials': first_letter,
            'tags': normalized_tags,
        })

    total_customers = len(customers_with_status)

    # ---------- Apply status filter (Active / Not Active / Overdue) ----------
    if status_filter:
        status_filter_lower = status_filter.lower()
        customers_with_status = [
            c for c in customers_with_status
            if c['status'].lower() == status_filter_lower
        ]

    # ---------- Pagination AFTER filtering ----------
    filtered_count = len(customers_with_status)
    total_pages = max((filtered_count + per_page - 1) // per_page, 1)
    if page > total_pages:
        page = total_pages

    start = (page - 1) * per_page
    end = start + per_page
    paginated_customers = customers_with_status[start:end]

    # ---------- Tag palette for top of page ----------
    available_tags = sorted(
        tags_summary.values(),
        key=lambda t: t["label"].lower()
    )

    return render_template(
        'customers.html',
        customers=paginated_customers,
        total_customers=total_customers,
        total_active=total_active,
        total_overdue=total_overdue,
        total_not_active=total_not_active,
        agents=agents,
        selected_agent=agent_id_param,
        search_term=search_term,
        page=page,
        total_pages=total_pages,
        selected_status=status_filter,
        available_tags=available_tags,
        selected_tag=tag_filter,
    )


# ---------- Tagging: add / update a tag on a customer ----------
@customers_bp.route('/customer/<customer_id>/tags', methods=['POST'])
def add_customer_tag(customer_id):
    """
    Add or update a tag on a customer.
    Expected form fields:
      - label  (e.g. "Payment follow-up")
      - color  (e.g. "#f97316" or any CSS color)
      - note   (optional description / reminder text)
    """
    manager_oid = _require_manager()
    if not manager_oid:
        return redirect(url_for('login.login'))

    label = (request.form.get('label') or '').strip()
    color = (request.form.get('color') or '').strip() or "#6366f1"
    note = (request.form.get('note') or '').strip()

    if not label:
        flash("Tag label is required.", "warning")
        return redirect(url_for('customers.customer_profile', customer_id=customer_id))

    tag_key = _slugify(label)

    try:
        cust_oid = ObjectId(customer_id)
    except Exception:
        flash("Invalid customer ID format.", "danger")
        return redirect(url_for('customers.customers_list'))

    # We store tags as a list; if same key exists, we update it; else we push new.
    existing = customers_col.find_one(
        {"_id": cust_oid, "tags.key": tag_key},
        {"tags.$": 1}
    )

    now = datetime.utcnow()

    if existing and existing.get("tags"):
        # Update existing tag in-place (color/label/note)
        customers_col.update_one(
            {"_id": cust_oid, "tags.key": tag_key},
            {"$set": {
                "tags.$.label": label,
                "tags.$.color": color,
                "tags.$.note": note,
                "tags.$.updated_at": now,
                "tags.$.updated_by": manager_oid,
            }}
        )
    else:
        # Add a new tag
        tag_doc = {
            "key": tag_key,
            "label": label,
            "color": color,
            "note": note,
            "created_at": now,
            "created_by": manager_oid,
        }
        customers_col.update_one(
            {"_id": cust_oid},
            {"$push": {"tags": tag_doc}}
        )

    flash("Tag saved successfully.", "success")
    return redirect(url_for('customers.customer_profile', customer_id=customer_id))


# ================== Existing routes below (unchanged) ==================


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
        total_penalty=total_penalty
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
            except Exception:
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

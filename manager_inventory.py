from flask import Blueprint, render_template, session, redirect, url_for, request
from bson.objectid import ObjectId

from db import db

manager_inventory_bp = Blueprint('manager_inventory', __name__)

# Collections
inventory_col = db.inventory


@manager_inventory_bp.route('/manager/inventory')
def view_manager_inventory():
    """
    Manager inventory view:
      - Only products for the logged-in manager
      - Optional category filter via ?category=<name>
      - Results sorted by newest first
    """
    if 'manager_id' not in session:
        return redirect(url_for('login.login'))

    try:
        manager_id = ObjectId(session['manager_id'])
    except Exception:
        # If session is corrupted, force login again
        return redirect(url_for('login.login'))

    selected_category = request.args.get('category', 'all')

    query_filter = {"manager_id": manager_id}
    if selected_category and selected_category != "all":
        query_filter["category"] = selected_category

    # Fetch inventory for this manager (filtered by category if provided)
    inventory_items = list(
        inventory_col.find(query_filter).sort("created_at", -1)
    )

    # Build category list for filter dropdown (distinct categories for this manager)
    raw_categories = inventory_col.distinct("category", {"manager_id": manager_id})
    categories = sorted([c for c in raw_categories if c])

    return render_template(
        'manager_inventory.html',
        inventory_items=inventory_items,
        categories=categories,
        selected_category=selected_category
    )

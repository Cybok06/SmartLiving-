from flask import Blueprint, render_template, request, redirect, url_for, flash, send_from_directory
from bson.objectid import ObjectId
from werkzeug.utils import secure_filename
import os

from db import db

product_profile_bp = Blueprint('product_profile', __name__)
products_col = db.products
inventory_col = db.inventory
users_col = db.users

# === File upload config ===
UPLOAD_FOLDER = os.path.join(os.getcwd(), 'uploads')
ALLOWED_EXTENSIONS = {'png', 'jpg', 'jpeg', 'gif'}
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


@product_profile_bp.route('/product/<product_id>')
def view_product(product_id):
    product = products_col.find_one({'_id': ObjectId(product_id)})
    if not product:
        return "Product not found", 404

    manager = users_col.find_one({'_id': product['manager_id']})

    # Load components from inventory
    components = []
    for comp in product.get('components', []):
        comp_id = comp['_id']
        if isinstance(comp_id, dict) and '$oid' in comp_id:
            comp_id = ObjectId(comp_id['$oid'])
        elif isinstance(comp_id, str):
            comp_id = ObjectId(comp_id)
        elif not isinstance(comp_id, ObjectId):
            continue

        inv_item = inventory_col.find_one({'_id': comp_id})
        if inv_item:
            components.append({
                'id': str(comp_id),
                'name': inv_item['name'],
                'image_url': inv_item['image_url'],
                'description': inv_item.get('description', ''),
                'price': inv_item.get('price', ''),
                'qty': comp.get('quantity', 1)
            })

    # Raw inventory for selection
    raw_inventory = list(inventory_col.find({'manager_id': product['manager_id']}))
    inventory_by_id = {str(item['_id']): item for item in raw_inventory}

    # Get all managers (for optional deletion)
    all_managers = list(users_col.find({'role': 'manager'}))

    return render_template("product_profile.html",
                           product=product,
                           manager=manager,
                           components=components,
                           inventory=inventory_by_id,
                           all_managers=all_managers)


@product_profile_bp.route('/product/<product_id>/edit', methods=['POST'])
def edit_product(product_id):
    data = request.form
    update_fields = {
        "name": data.get("name"),
        "product_type": data.get("product_type"),
        "price": float(data.get("price", 0)),
        "cash_price": float(data.get("cash_price", 0)),
        "category": data.get("category"),
        "package_name": data.get("package_name"),
        "description": data.get("description")
    }
    update_fields = {k: v for k, v in update_fields.items() if v not in [None, ""]}

    products_col.update_one({'_id': ObjectId(product_id)}, {'$set': update_fields})
    flash("✅ Product updated successfully.", "success")
    return redirect(url_for('product_profile.view_product', product_id=product_id))


@product_profile_bp.route('/product/<product_id>/delete/<comp_id>', methods=['POST'])
def delete_component(product_id, comp_id):
    products_col.update_one(
        {'_id': ObjectId(product_id)},
        {'$pull': {'components': {'_id': ObjectId(comp_id)}}}
    )
    flash("✅ Component removed successfully.", "success")
    return redirect(url_for('product_profile.view_product', product_id=product_id))


@product_profile_bp.route('/product/<product_id>/delete', methods=['POST'])
def delete_product(product_id):
    product = products_col.find_one({'_id': ObjectId(product_id)})
    if not product:
        flash("❌ Product not found.", "danger")
        return redirect(url_for('added_products.view_added_products'))

    products_col.delete_one({'_id': ObjectId(product_id)})

    delete_for_ids = request.form.getlist('delete_for[]')
    if delete_for_ids:
        deleted = products_col.delete_many({
            'name': product['name'],
            'manager_id': {'$in': [ObjectId(mid) for mid in delete_for_ids]},
            '_id': {'$ne': ObjectId(product_id)}
        }).deleted_count
        flash(f"✅ Product also deleted for {deleted} other manager(s).", "success")
    else:
        flash("✅ Product deleted successfully.", "success")

    return redirect(url_for('added_products.view_added_products'))


@product_profile_bp.route('/product/<product_id>/add-components', methods=['POST'])
def add_components(product_id):
    product = products_col.find_one({'_id': ObjectId(product_id)})
    if not product:
        flash("❌ Product not found.", "danger")
        return redirect(url_for('product_profile.view_product', product_id=product_id))

    component_ids = request.form.getlist('components')
    existing_ids = {
        str(comp['_id']['$oid']) if isinstance(comp['_id'], dict) and '$oid' in comp['_id']
        else str(comp['_id'])
        for comp in product.get('components', [])
    }

    new_components = []

    for comp_id in component_ids:
        qty = int(request.form.get(f'qty_{comp_id}', 1))
        if comp_id in existing_ids:
            continue
        try:
            inv_item = inventory_col.find_one({'_id': ObjectId(comp_id)})
            if inv_item:
                new_components.append({
                    '_id': ObjectId(comp_id),
                    'quantity': qty
                })
        except Exception as e:
            print("Component add error:", e)

    if new_components:
        products_col.update_one(
            {'_id': ObjectId(product_id)},
            {'$push': {'components': {'$each': new_components}}}
        )
        flash(f"✅ {len(new_components)} component(s) added.", "success")
    else:
        flash("❌ No valid components added or already exist.", "warning")

    return redirect(url_for('product_profile.view_product', product_id=product_id))


# === ✅ Change Product Image (for all products with same name + price) ===
@product_profile_bp.route('/product/<product_id>/change_image', methods=['POST'])
def change_image(product_id):
    product = products_col.find_one({'_id': ObjectId(product_id)})
    if not product:
        flash("❌ Product not found.", "danger")
        return redirect(url_for('product_profile.view_product', product_id=product_id))

    file = request.files.get('image')
    if not file or file.filename == '':
        flash("❌ No image selected.", "danger")
        return redirect(url_for('product_profile.view_product', product_id=product_id))

    if not allowed_file(file.filename):
        flash("❌ Invalid file type. Please upload an image.", "danger")
        return redirect(url_for('product_profile.view_product', product_id=product_id))

    filename = secure_filename(file.filename)
    path = os.path.join(UPLOAD_FOLDER, filename)

    # Prevent overwrite
    base, ext = os.path.splitext(filename)
    counter = 1
    while os.path.exists(path):
        filename = f"{base}_{counter}{ext}"
        path = os.path.join(UPLOAD_FOLDER, filename)
        counter += 1

    file.save(path)
    image_url = f"/uploads/{filename}"

    # Update all products with same name and price
    products_col.update_many(
        {'name': product['name'], 'price': product['price']},
        {'$set': {'image_url': image_url}}
    )

    flash("✅ Image updated for all matching products.", "success")
    return redirect(url_for('product_profile.view_product', product_id=product_id))


# === Serve uploaded images ===
@product_profile_bp.route('/uploads/<filename>')
def serve_uploaded_file(filename):
    return send_from_directory(UPLOAD_FOLDER, filename)

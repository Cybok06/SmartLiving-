from bson import ObjectId
from flask import Blueprint, render_template, request, jsonify
from flask_login import login_required, current_user
from werkzeug.utils import secure_filename
from db import db
from datetime import datetime
import traceback
import requests

customer_bp = Blueprint('customer', __name__)
customers_collection = db["customers"]
users_collection = db["users"]
images_col = db["images"]

# ===== Cloudflare (hardcoded as requested) =====
CF_ACCOUNT_ID   = "63e6f91eec9591f77699c4b434ab44c6"
CF_IMAGES_TOKEN = "Brz0BEfl_GqEUjEghS2UEmLZhK39EUmMbZgu_hIo"
CF_HASH         = "h9fmMoa1o2c2P55TcWJGOg"
DEFAULT_VARIANT = "public"

ALLOWED_EXTENSIONS = {'png', 'jpg', 'jpeg', 'gif'}


def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


# Show registration page
@customer_bp.route('/register', methods=['GET'])
@login_required
def register_customer():
    today_str = datetime.utcnow().strftime("%Y-%m-%d")
    return render_template(
        'register_customer.html',
        agent_id=current_user.id,
        today_str=today_str
    )


# =============== Upload directly to Cloudflare ===============
@customer_bp.route('/upload_image', methods=['POST'])
def upload_customer_image():
    try:
        if 'image' not in request.files:
            return jsonify({'success': False, 'error': 'No file part in request'}), 400

        image = request.files['image']
        if image.filename == '':
            return jsonify({'success': False, 'error': 'No selected file'}), 400

        if not (image and allowed_file(image.filename)):
            return jsonify({'success': False, 'error': 'File type not allowed'}), 400

        direct_url = f"https://api.cloudflare.com/client/v4/accounts/{CF_ACCOUNT_ID}/images/v2/direct_upload"
        headers = {"Authorization": f"Bearer {CF_IMAGES_TOKEN}"}
        data = {}

        res = requests.post(direct_url, headers=headers, data=data, timeout=20)
        try:
            j = res.json()
        except Exception:
            return jsonify({'success': False, 'error': 'Cloudflare (direct_upload) returned non-JSON'}), 502

        if not j.get('success'):
            return jsonify({'success': False, 'error': 'Cloudflare direct_upload failed', 'details': j}), 400

        upload_url = j['result']['uploadURL']
        image_id = j['result']['id']

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

        variant = request.args.get('variant', DEFAULT_VARIANT)
        image_url = f"https://imagedelivery.net/{CF_HASH}/{image_id}/{variant}"

        images_col.insert_one({
            'provider': 'cloudflare_images',
            'image_id': image_id,
            'variant': variant,
            'url': image_url,
            'original_filename': secure_filename(image.filename),
            'mimetype': image.mimetype,
            'size_bytes': request.content_length,
            'created_at': datetime.utcnow(),
            'module': 'customer_register'
        })

        return jsonify({'success': True, 'image_url': image_url, 'image_id': image_id, 'variant': variant})

    except Exception as e:
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 500


# Add a new customer
@customer_bp.route('/add', methods=['POST'])
@login_required
def add_customer():
    try:
        name = request.form.get('name')
        location = request.form.get('location')
        occupation = request.form.get('occupation')
        phone_number = request.form.get('phone_number')
        comment = request.form.get('comment')
        agent_id = request.form.get('agent_id') or str(current_user.id)
        latitude = request.form.get('latitude')
        longitude = request.form.get('longitude')
        image_url = (request.form.get('image_url') or '').strip()
        image_id = (request.form.get('image_id') or '').strip()
        date_registered_str = request.form.get('date_registered')

        if not agent_id:
            return jsonify({'error': 'Agent ID is required'}), 400

        # Fetch agent to get manager_id
        agent = users_collection.find_one({"_id": ObjectId(agent_id), "role": "agent"})
        if not agent or "manager_id" not in agent:
            return jsonify({'error': 'Manager ID not found for this agent'}), 400

        manager_id = agent["manager_id"]

        if not image_url:
            return jsonify({'error': 'Customer image upload is required'}), 400

        # Parse / fallback for date_registered
        try:
            if date_registered_str:
                date_registered = datetime.strptime(date_registered_str, "%Y-%m-%d")
            else:
                date_registered = datetime.utcnow()
        except ValueError:
            date_registered = datetime.utcnow()

        # Build customer object
        customer = {
            'name': name,
            'image_url': image_url,
            'cf_image_id': image_id or None,
            'location': location,
            'occupation': occupation,
            'phone_number': phone_number,
            'comment': comment,
            'agent_id': agent_id,
            'manager_id': manager_id,
            'date_registered': date_registered,  # ✅ NEW FIELD
        }

        # Coordinates
        if latitude and longitude:
            try:
                customer['coordinates'] = {
                    'latitude': float(latitude),
                    'longitude': float(longitude)
                }
            except ValueError:
                print("Invalid latitude or longitude format")

        # Insert into DB
        inserted_id = customers_collection.insert_one(customer).inserted_id
        return jsonify({
            'ok': True,
            'message': 'Customer registered successfully!',
            'customer_id': str(inserted_id),
            'customer_name': name
        }), 200

    except Exception as e:
        print("Error:", str(e))
        return jsonify({'error': 'Failed to register customer'}), 500

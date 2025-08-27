from flask import Blueprint, render_template, request, redirect, url_for, flash, session
from flask_bcrypt import Bcrypt
from flask_login import login_user, logout_user
from bson.objectid import ObjectId
from user_model import User  # used for agent login via Flask-Login
from datetime import datetime
import requests
from db import db

login_bp = Blueprint('login', __name__)
bcrypt = Bcrypt()

# MongoDB collections
users_col = db.users
logins_col = db.login_logs  # Login logs

# ---------------------------
# Utilities
# ---------------------------
def get_location(ip: str):
    try:
        resp = requests.get(f"http://ip-api.com/json/{ip}", timeout=5).json()
        return {
            "country": resp.get("country"),
            "region": resp.get("regionName"),
            "city": resp.get("city"),
            "isp": resp.get("isp")
        }
    except Exception:
        return {}

@login_bp.record_once
def on_load(state):
    bcrypt.init_app(state.app)

@login_bp.route('/')
def home():
    return redirect(url_for('login.login'))

# ---------------------------
# Role helpers (easy to extend)
# ---------------------------
def _set_role_session(role: str, user_data: dict) -> tuple[str, str]:
    """
    Sets the appropriate session keys for a role and returns (session_key, redirect_endpoint).
    Add new roles here as needed.
    """
    user_id_str = str(user_data['_id'])
    username = user_data.get('username') or user_data.get('name', '')

    # Map roles to their session mutations and redirect endpoints
    ROLE_CONFIG = {
        'admin': {
            'set': lambda: (session.__setitem__('admin_id', user_id_str),
                            session.__setitem__('username', username)),
            'endpoint': 'login.admin_dashboard'  # local in this file
        },
        'manager': {
            'set': lambda: (session.__setitem__('manager_id', user_id_str),
                            session.__setitem__('manager_name', user_data.get('name', username))),
            'endpoint': 'manager_dashboard.manager_dashboard_view'  # existing manager blueprint
        },
        'agent': {
            'set': lambda: (login_user(User(user_data)),
                            session.__setitem__('agent_id', user_id_str)),
            'endpoint': 'dashboard_agent.agent_dashboard'  # existing agent blueprint
        },
        'executive': {
            'set': lambda: (session.__setitem__('executive_id', user_id_str),
                            session.__setitem__('executive_name', user_data.get('name', username))),
            'endpoint': 'executive_dashboard.executive_dashboard'  # existing executive blueprint
        },
        # NEW: inventory role
        'inventory': {
            # Keep keys consistent with other roles for clarity
            'set': lambda: (session.__setitem__('inventory_id', user_id_str),
                            session.__setitem__('inventory_name', user_data.get('name', username))),
            # Point this to your inventory dashboard blueprint endpoint
            # Create a blueprint: inventory_dashboard with a view function inventory_dashboard_view
            'endpoint': 'inventory_dashboard.inventory_dashboard_view'
        }
    }

    role_lc = (role or '').lower().strip()
    cfg = ROLE_CONFIG.get(role_lc)
    if not cfg:
        raise ValueError(f"Unsupported role: {role}")

    # perform session mutations
    cfg['set']()
    # return the canonical session key for logging + redirect endpoint
    session_key = f"{role_lc}_id"
    return session_key, cfg['endpoint']

# ---------------------------
# Login
# ---------------------------
@login_bp.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = (request.form.get('username') or '').strip()
        password = request.form.get('password') or ''

        # Find by username OR email to be flexible (keeps username flow intact)
        user_data = users_col.find_one({
            '$or': [{'username': username}, {'email': username}]
        })

        if user_data:
            stored_hash = user_data.get('password')
            role = (user_data.get('role') or '').lower()

            # Optional: respect soft-disable flags
            if user_data.get('status') and str(user_data['status']).lower() in ('not active', 'disabled', 'inactive'):
                flash("Your account is not active. Contact an administrator.", "error")
                return redirect(url_for('login.login'))

            if stored_hash and bcrypt.check_password_hash(stored_hash, password):
                try:
                    session_key, endpoint = _set_role_session(role, user_data)
                except ValueError:
                    flash("Your role is not supported. Contact an administrator.", "error")
                    return redirect(url_for('login.login'))

                # Log login activity
                ip = request.headers.get('X-Forwarded-For', '').split(',')[0].strip() or request.remote_addr
                user_agent = request.headers.get('User-Agent')
                location_data = get_location(ip)

                logins_col.insert_one({
                    session_key: str(user_data['_id']),
                    "username": user_data.get('username'),
                    "role": role,
                    "ip": ip,
                    "user_agent": user_agent,
                    "location": location_data,
                    "timestamp": datetime.utcnow()
                })

                # Redirect
                return redirect(url_for(endpoint))

        flash("Invalid username or password.", "error")
        return redirect(url_for('login.login'))

    return render_template('login.html')

# ---------------------------
# Admin dashboard (local route)
# ---------------------------
@login_bp.route('/admin/dashboard')
def admin_dashboard():
    if 'admin_id' not in session:
        return redirect(url_for('login.login'))
    return render_template('admin_dashboard.html')

# ---------------------------
# Manager: Agent management (unchanged)
# ---------------------------
@login_bp.route('/agents')
def agent_list():
    if 'manager_id' not in session:
        return redirect(url_for('login.login'))

    try:
        manager_id = ObjectId(session['manager_id'])
    except Exception:
        flash("Invalid manager session ID.", "error")
        return redirect(url_for('login.logout'))

    search = (request.args.get('search') or '').strip()
    query = {'manager_id': manager_id, 'role': 'agent'}
    if search:
        query['$or'] = [
            {'name': {'$regex': search, '$options': 'i'}},
            {'phone': {'$regex': search, '$options': 'i'}}
        ]

    agents = list(users_col.find(query))
    return render_template('agent_list.html', agents=agents, search=search)

@login_bp.route('/agent/<agent_id>')
def view_agent(agent_id):
    if 'manager_id' not in session:
        return redirect(url_for('login.login'))

    try:
        oid = ObjectId(agent_id)
        manager_oid = ObjectId(session['manager_id'])
    except Exception:
        return "Invalid agent ID."

    agent = users_col.find_one({'_id': oid, 'manager_id': manager_oid, 'role': 'agent'})
    if not agent:
        return "Agent not found or access denied."

    return render_template('profile_agent.html', agent=agent)

@login_bp.route('/agent/<agent_id>/toggle_status', methods=['POST'])
def toggle_agent_status(agent_id):
    if 'manager_id' not in session:
        return redirect(url_for('login.login'))

    try:
        oid = ObjectId(agent_id)
        manager_oid = ObjectId(session['manager_id'])
    except Exception:
        flash("Invalid agent ID.", "error")
        return redirect(url_for('login.agent_list'))

    agent = users_col.find_one({'_id': oid, 'manager_id': manager_oid})
    if not agent:
        flash("Agent not found or unauthorized.", "error")
        return redirect(url_for('login.agent_list'))

    new_status = 'Not Active' if agent.get('status') == 'Active' else 'Active'
    users_col.update_one({'_id': oid}, {'$set': {'status': new_status}})
    flash(f"Agent status changed to {new_status}.", "success")
    return redirect(url_for('login.view_agent', agent_id=agent_id))

@login_bp.route('/agent/<agent_id>/edit', methods=['GET', 'POST'])
def edit_agent(agent_id):
    if 'manager_id' not in session:
        return redirect(url_for('login.login'))

    try:
        oid = ObjectId(agent_id)
        manager_oid = ObjectId(session['manager_id'])
    except Exception:
        flash("Invalid agent ID.", "error")
        return redirect(url_for('login.agent_list'))

    agent = users_col.find_one({'_id': oid, 'manager_id': manager_oid})
    if not agent:
        flash("Agent not found or unauthorized.", "error")
        return redirect(url_for('login.agent_list'))

    if request.method == 'POST':
        updated = {
            'name': request.form.get('name'),
            'phone': request.form.get('phone'),
            'email': request.form.get('email'),
            'gender': request.form.get('gender'),
            'branch': request.form.get('branch'),
            'position': request.form.get('position'),
            'location': request.form.get('location'),
            'start_date': request.form.get('start_date'),
            'image_url': request.form.get('image_url'),
            'assets': [x.strip() for x in (request.form.get('assets') or '').split(',') if x.strip()],
        }
        if request.form.get('password'):
            updated['password'] = bcrypt.generate_password_hash(request.form['password']).decode('utf-8')

        users_col.update_one({'_id': oid}, {'$set': updated})
        flash('Agent details updated successfully.', 'success')
        return redirect(url_for('login.view_agent', agent_id=agent_id))

    return render_template('edit_agent.html', agent=agent)

# ---------------------------
# Logout (all roles)
# ---------------------------
@login_bp.route('/logout')
def logout():
    session.clear()
    logout_user()
    return redirect(url_for('login.login'))

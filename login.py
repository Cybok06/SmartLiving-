from flask import Blueprint, render_template, request, redirect, url_for, flash, session
from flask_bcrypt import Bcrypt
from flask_login import login_user, logout_user
from bson.objectid import ObjectId
from user_model import User  # used for agent login via Flask-Login
from datetime import datetime, timedelta
import requests
from db import db

login_bp = Blueprint('login', __name__)
bcrypt = Bcrypt()

# MongoDB collections
users_col   = db.users
logins_col  = db.login_logs  # Login logs
complaints_col = db.complaints  # ✅ Complaints collection for dashboard/badges

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

def _date_to_str(d):
    """Safe date->YYYY-MM-DD (or empty)."""
    if isinstance(d, datetime):
        return d.strftime("%Y-%m-%d")
    return d or ""

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
            'set': lambda: (session.__setitem__('inventory_id', user_id_str),
                            session.__setitem__('inventory_name', user_data.get('name', username))),
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
# Admin dashboard (local route) ✅ UPDATED to pass admin + complaint summaries
# ---------------------------
@login_bp.route('/admin/dashboard')
def admin_dashboard():
    if 'admin_id' not in session:
        return redirect(url_for('login.login'))

    # Admin doc for hero section/profile chip
    try:
        admin = users_col.find_one({'_id': ObjectId(session['admin_id'])}) or {}
    except Exception:
        admin = {}

    # Complaint summary tiles
    now = datetime.utcnow()
    start_today = datetime(now.year, now.month, now.day)
    end_today = start_today + timedelta(days=1)

    q_unresolved = {"status": {"$nin": ["Resolved", "Closed"]}}
    q_breaching  = {"status": {"$nin": ["Resolved", "Closed"]}, "sla_due": {"$lte": now}}
    q_resolved_30 = {
        "status": {"$in": ["Resolved", "Closed"]},
        "date_closed": {"$gte": now - timedelta(days=30)}
    }

    stats = {
        "open": complaints_col.count_documents(q_unresolved),
        "breaching": complaints_col.count_documents(q_breaching),
        "resolved_30": complaints_col.count_documents(q_resolved_30),
        "opened_today": complaints_col.count_documents({"created_at": {"$gte": start_today, "$lt": end_today}}),
        "closed_today": complaints_col.count_documents({
            "status": {"$in": ["Resolved", "Closed"]},
            "date_closed": {"$gte": start_today, "$lt": end_today}
        }),
    }

    # Top issue types (small chart/list)
    pipeline = [
        {"$group": {"_id": "$issue_type", "n": {"$sum": 1}}},
        {"$sort": {"n": -1}},
        {"$limit": 6},
    ]
    issue_tops_raw = list(complaints_col.aggregate(pipeline))
    issue_tops = [{"issue": (x.get("_id") or "Uncategorized"), "count": x.get("n", 0)} for x in issue_tops_raw]

    # Recent complaints (compact list)
    recent = list(complaints_col.find({}).sort([("created_at", -1)]).limit(8))
    for r in recent:
        r["_id"] = str(r["_id"])
        r["date_reported"] = _date_to_str(r.get("date_reported"))
        r["date_closed"]   = _date_to_str(r.get("date_closed"))
        r["sla_due"]       = _date_to_str(r.get("sla_due"))
        r["created_at"]    = _date_to_str(r.get("created_at"))
        r["updated_at"]    = _date_to_str(r.get("updated_at"))

    # Render with rich context (admin_dashboard.html expects these now)
    return render_template(
        'admin_dashboard.html',
        admin=admin,
        stats=stats,
        issue_tops=issue_tops,
        recent=recent
    )

# ---------------------------
# Admin: lightweight counts for sidebar badges ✅ NEW
# ---------------------------
@login_bp.route('/admin/complaints_open_count')
def admin_complaints_open_count():
    if 'admin_id' not in session:
        return {"ok": False, "message": "Unauthorized"}, 401

    now = datetime.utcnow()
    open_count = complaints_col.count_documents({"status": {"$nin": ["Resolved", "Closed"]}})
    breaching  = complaints_col.count_documents({"status": {"$nin": ["Resolved", "Closed"]}, "sla_due": {"$lte": now}})
    return {"ok": True, "open": int(open_count), "breaching": int(breaching)}

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

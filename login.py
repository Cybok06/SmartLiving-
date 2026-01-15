from flask import Blueprint, render_template, request, redirect, url_for, flash, session
from flask_bcrypt import Bcrypt
from flask_login import login_user, logout_user, current_user
from bson.objectid import ObjectId
from user_model import User  # used for agent login via Flask-Login
from datetime import datetime, timedelta
from urllib.parse import urlparse
import requests
import bcrypt as bcrypt_lib
from db import db
from services.login_audit import ensure_login_log_indexes, ensure_user_indexes

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


def _parse_device(user_agent: str | None) -> dict:
    ua = user_agent or ""
    try:
        from user_agents import parse as ua_parse
        parsed = ua_parse(ua)
        return {
            "browser": f"{parsed.browser.family} {parsed.browser.version_string}".strip(),
            "os": f"{parsed.os.family} {parsed.os.version_string}".strip(),
            "is_mobile": bool(parsed.is_mobile),
            "raw": ua,
        }
    except Exception:
        ua_lc = ua.lower()
        browser = "Unknown"
        if "chrome" in ua_lc and "safari" in ua_lc and "edge" not in ua_lc:
            browser = "Chrome"
        elif "safari" in ua_lc and "chrome" not in ua_lc:
            browser = "Safari"
        elif "firefox" in ua_lc:
            browser = "Firefox"
        elif "edge" in ua_lc:
            browser = "Edge"
        elif "msie" in ua_lc or "trident" in ua_lc:
            browser = "IE"

        os_name = "Unknown"
        if "windows" in ua_lc:
            os_name = "Windows"
        elif "mac os" in ua_lc or "macintosh" in ua_lc:
            os_name = "macOS"
        elif "android" in ua_lc:
            os_name = "Android"
        elif "iphone" in ua_lc or "ipad" in ua_lc:
            os_name = "iOS"
        elif "linux" in ua_lc:
            os_name = "Linux"

        is_mobile = "mobi" in ua_lc or "android" in ua_lc or "iphone" in ua_lc
        return {"browser": browser, "os": os_name, "is_mobile": bool(is_mobile), "raw": ua}

def _date_to_str(d):
    """Safe date->YYYY-MM-DD (or empty)."""
    if isinstance(d, datetime):
        return d.strftime("%Y-%m-%d")
    return d or ""


def _dashboard_endpoint_for_role(role: str) -> str:
    role_lc = (role or "").lower().strip()
    mapping = {
        "admin": "login.admin_dashboard",
        "manager": "manager_dashboard.manager_dashboard_view",
        "agent": "dashboard_agent.agent_dashboard",
        "executive": "executive_dashboard.executive_dashboard",
        "inventory": "inventory_dashboard.inventory_dashboard_view",
        "hr": "hr.dashboard",
        "accounting": "acc_dashboard.accounting_dashboard",
    }
    return mapping.get(role_lc, "login.login")


def _safe_next_url(next_url: str | None) -> str | None:
    if not next_url:
        return None
    parsed = urlparse(next_url)
    if parsed.scheme or parsed.netloc:
        return None
    if not parsed.path.startswith("/"):
        return None
    return next_url


def get_current_identity() -> dict:
    if getattr(current_user, "is_authenticated", False):
        role = (getattr(current_user, "role", "") or "").lower()
        user_id = str(getattr(current_user, "id", "") or "")
        return {
            "is_authenticated": True,
            "role": role,
            "user_id": user_id,
            "name": getattr(current_user, "name", "") or getattr(current_user, "username", "") or "User",
            "dashboard_endpoint": _dashboard_endpoint_for_role(role),
        }

    role = (session.get("role") or "").lower().strip()
    user_id = str(session.get("user_id") or "")

    if not role or not user_id:
        role_map = [
            ("executive_id", "executive"),
            ("manager_id", "manager"),
            ("admin_id", "admin"),
            ("inventory_id", "inventory"),
            ("agent_id", "agent"),
            ("hr_id", "hr"),
            ("accounting_id", "accounting"),
        ]
        for key, r in role_map:
            if session.get(key):
                role = r
                user_id = str(session.get(key))
                break

    if not role or not user_id:
        return {"is_authenticated": False}

    user_doc = users_col.find_one({"_id": ObjectId(user_id)}) if ObjectId.is_valid(user_id) else users_col.find_one({"_id": user_id})
    name = (user_doc or {}).get("name") or (user_doc or {}).get("username") or "User"

    return {
        "is_authenticated": True,
        "role": role,
        "user_id": user_id,
        "name": name,
        "dashboard_endpoint": _dashboard_endpoint_for_role(role),
    }


def role_required(*roles):
    def decorator(fn):
        def wrapper(*args, **kwargs):
            ident = get_current_identity()
            if not ident.get("is_authenticated"):
                return redirect(url_for("login.login", next=request.path))
            if roles and ident.get("role") not in roles:
                return "Forbidden", 403
            return fn(*args, **kwargs)
        wrapper.__name__ = fn.__name__
        return wrapper
    return decorator

@login_bp.record_once
def on_load(state):
    bcrypt.init_app(state.app)
    ensure_login_log_indexes()
    ensure_user_indexes()

@login_bp.route('/')
def home():
    ident = get_current_identity()
    if ident.get("is_authenticated"):
        return redirect(url_for(ident["dashboard_endpoint"]))
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
            'set': lambda: (login_user(User(user_data), remember=True),
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
        },
        'hr': {
            'set': lambda: (session.__setitem__('hr_id', user_id_str),
                            session.__setitem__('hr_name', user_data.get('name', username))),
            'endpoint': 'hr.dashboard'
        },
        'accounting': {
            'set': lambda: (session.__setitem__('accounting_id', user_id_str),
                            session.__setitem__('accounting_name', user_data.get('name', username))),
            'endpoint': 'acc_dashboard.accounting_dashboard'
        },
    }

    role_lc = (role or '').lower().strip()
    cfg = ROLE_CONFIG.get(role_lc)
    if not cfg:
        raise ValueError(f"Unsupported role: {role}")

    session['user_id'] = user_id_str
    session['role'] = role_lc

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
        geo_lat = request.form.get('geo_lat')
        geo_lng = request.form.get('geo_lng')
        geo_accuracy = request.form.get('geo_accuracy')
        geo_ts = request.form.get('geo_ts')
        geo_available_raw = (request.form.get('geo_available') or '').strip().lower()
        geo_reason = (request.form.get('geo_reason') or '').strip()

        try:
            geo_lat_f = float(geo_lat)
            geo_lng_f = float(geo_lng)
            geo_accuracy_f = float(geo_accuracy) if geo_accuracy is not None else None
        except Exception:
            geo_lat_f = None
            geo_lng_f = None
            geo_accuracy_f = None
        geo_available = geo_available_raw == "true"
        if geo_lat_f is not None and geo_lng_f is not None:
            if not (-90 <= geo_lat_f <= 90 and -180 <= geo_lng_f <= 180):
                geo_lat_f = None
                geo_lng_f = None
                geo_available = False

        # Find by username (schema: users.username)
        user_data = users_col.find_one({"username": username})

        if user_data:
            stored_hash = user_data.get('password')
            role = (user_data.get('role') or '').lower()

            # Optional: respect soft-disable flags
            status_val = str(user_data.get('status') or '').strip().lower()
            if status_val in ('not active', 'disabled', 'inactive'):
                flash("Your account is not active. Contact an administrator.", "error")
                return redirect(url_for('login.login'))

            if user_data.get("account_locked") is True or user_data.get("is_active") is False:
                flash("Your account is not active. Contact an administrator.", "error")
                return redirect(url_for('login.login'))

            ok = False
            if stored_hash and str(stored_hash).startswith("$2"):
                try:
                    ok = bcrypt_lib.checkpw(password.encode("utf-8"), str(stored_hash).encode("utf-8"))
                except Exception:
                    ok = False
            else:
                ok = (password == (stored_hash or ""))
                if ok:
                    try:
                        new_hash = bcrypt_lib.hashpw(password.encode("utf-8"), bcrypt_lib.gensalt(rounds=12)).decode("utf-8")
                        users_col.update_one({"_id": user_data["_id"]}, {"$set": {"password": new_hash, "updated_at": datetime.utcnow()}})
                        stored_hash = new_hash
                    except Exception:
                        pass

            if ok:
                session.permanent = True
                try:
                    session_key, endpoint = _set_role_session(role, user_data)
                except ValueError:
                    flash("Your role is not supported. Contact an administrator.", "error")
                    return redirect(url_for('login.login'))

                # Log login activity
                ip = request.headers.get('X-Forwarded-For', '').split(',')[0].strip() or request.remote_addr
                user_agent = request.headers.get('User-Agent')
                location_data = get_location(ip)
                device = _parse_device(user_agent)
                user_id = str(user_data['_id'])

                logins_col.insert_one({
                    session_key: user_id,
                    "user_id": user_id,
                    "username": user_data.get('username'),
                    "role": role,
                    "ip": ip,
                    "user_agent": user_agent,
                    "device": device,
                    "geo": {
                        "lat": geo_lat_f,
                        "lng": geo_lng_f,
                        "accuracy_m": geo_accuracy_f,
                        "source": "browser",
                        "browser_ts": geo_ts,
                    },
                    "location_available": bool(geo_available and geo_lat_f is not None and geo_lng_f is not None),
                    "location_reason": geo_reason or None,
                    "ip_location": location_data,
                    "timestamp": datetime.utcnow()
                })

                # Redirect
                next_url = _safe_next_url(request.args.get("next") or request.form.get("next") or session.pop("next", None))
                if next_url:
                    return redirect(next_url)
                return redirect(url_for(endpoint))

        flash("Invalid username or password.", "error")
        return redirect(url_for('login.login'))

    next_qs = _safe_next_url(request.args.get("next"))
    if next_qs:
        session["next"] = next_qs
    ident = get_current_identity()
    if ident.get("is_authenticated"):
        return redirect(url_for(ident["dashboard_endpoint"]))
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
    status = (request.args.get('status') or 'all').strip().lower()
    per_page = 10
    try:
        page = max(1, int(request.args.get('page', 1)))
    except Exception:
        page = 1

    query = {'manager_id': manager_id, 'role': 'agent'}
    if search:
        query['$or'] = [
            {'name': {'$regex': search, '$options': 'i'}},
            {'phone': {'$regex': search, '$options': 'i'}}
        ]

    if status == 'active':
        query['status'] = {'$in': ['Active', 'active']}
    elif status == 'not_active':
        query['status'] = {'$nin': ['Active', 'active']}

    total_agents = users_col.count_documents({'manager_id': manager_id, 'role': 'agent'})
    total_active = users_col.count_documents({
        'manager_id': manager_id,
        'role': 'agent',
        'status': {'$in': ['Active', 'active']}
    })
    total_not_active = max(total_agents - total_active, 0)

    total_agents_filtered = users_col.count_documents(query)
    total_pages = max(1, (total_agents_filtered + per_page - 1) // per_page)
    if page > total_pages:
        page = total_pages

    skip = (page - 1) * per_page
    agents = list(users_col.find(query).sort([('name', 1)]).skip(skip).limit(per_page))

    return render_template(
        'agent_list.html',
        agents=agents,
        search=search,
        status=status,
        page=page,
        total_pages=total_pages,
        total_agents=total_agents,
        total_active=total_active,
        total_not_active=total_not_active
    )

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
    flash("Logged out.", "success")
    return redirect(url_for('login.login'))

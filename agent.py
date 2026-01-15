from flask import Blueprint, render_template, request, flash, redirect, url_for, session
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from flask_bcrypt import Bcrypt
from bson.objectid import ObjectId
from db import db

# Setup Blueprint
agent_bp = Blueprint('agent', __name__)
bcrypt = Bcrypt()


users_col = db.users

@agent_bp.route('/agents')
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
        total_active=total_active,
        total_not_active=total_not_active,
        total_agents=total_agents
    )


@agent_bp.route('/agent/<agent_id>')
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


@agent_bp.route('/agent/<agent_id>/toggle_status', methods=['POST'])
def toggle_agent_status(agent_id):
    if 'manager_id' not in session:
        return redirect(url_for('login.login'))

    try:
        oid = ObjectId(agent_id)
        manager_oid = ObjectId(session['manager_id'])
    except Exception:
        flash("Invalid agent ID.", "error")
        return redirect(url_for('agent.agent_list'))

    agent = users_col.find_one({'_id': oid, 'manager_id': manager_oid})
    if not agent:
        flash("Agent not found or unauthorized.", "error")
        return redirect(url_for('agent.agent_list'))

    new_status = 'Not Active' if agent.get('status') == 'Active' else 'Active'
    users_col.update_one({'_id': oid}, {'$set': {'status': new_status}})
    flash(f"Agent status changed to {new_status}.", "success")
    return redirect(url_for('agent.view_agent', agent_id=agent_id))


@agent_bp.route('/agent/<agent_id>/edit', methods=['GET', 'POST'])
def edit_agent(agent_id):
    if 'manager_id' not in session:
        return redirect(url_for('login.login'))

    try:
        oid = ObjectId(agent_id)
        manager_oid = ObjectId(session['manager_id'])
    except Exception:
        flash("Invalid agent ID.", "error")
        return redirect(url_for('agent.agent_list'))

    agent = users_col.find_one({'_id': oid, 'manager_id': manager_oid})
    if not agent:
        flash("Agent not found or unauthorized.", "error")
        return redirect(url_for('agent.agent_list'))

    if request.method == 'POST':
        updated_data = {
            'name': request.form['name'],
            'phone': request.form['phone'],
            'email': request.form['email'],
            'gender': request.form['gender'],
            'branch': request.form['branch'],
            'position': request.form['position'],
            'location': request.form['location'],
            'start_date': request.form['start_date'],
            'image_url': request.form['image_url'],
            'assets': [item.strip() for item in request.form.get('assets', '').split(',')],
        }

        if request.form.get('password'):
            updated_data['password'] = bcrypt.generate_password_hash(request.form['password']).decode('utf-8')

        users_col.update_one({'_id': oid}, {'$set': updated_data})
        flash('Agent details updated successfully.', 'success')
        return redirect(url_for('agent.view_agent', agent_id=agent_id))

    return render_template('edit_agent.html', agent=agent)

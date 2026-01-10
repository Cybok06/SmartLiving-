from flask import Blueprint, render_template, session, redirect, url_for, flash
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from bson.objectid import ObjectId
from datetime import datetime
from db import db

agent_lead_bp = Blueprint('agent_lead', __name__)


users_col = db.users
leads_col = db.leads  # assuming leads stored in 'leads' collection

@agent_lead_bp.route('/agent-lead')
def agent_leads():
    if 'manager_id' not in session:
        return redirect(url_for('login.login'))

    try:
        manager_id = ObjectId(session['manager_id'])
    except Exception:
        flash("Invalid session, please log in again.", "error")
        return redirect(url_for('login.logout'))

    # Get all agents for this manager
    agents = list(users_col.find({'manager_id': manager_id, 'role': 'agent'}))
    agent_ids_str = [str(agent['_id']) for agent in agents]

    # Fetch leads for these agents
    leads = list(leads_col.find({'agent_id': {'$in': agent_ids_str}}))

    # Format created_at to readable date for each lead
    for lead in leads:
        created_at = lead.get('created_at')
        if created_at:
            # The created_at stored like {"$date": {"$numberLong": "timestamp"}}
            try:
                if isinstance(created_at, dict):
                    timestamp_ms = int(created_at.get('$date', {}).get('$numberLong', 0))
                    lead['created_at_str'] = datetime.utcfromtimestamp(timestamp_ms / 1000).strftime('%Y-%m-%d %H:%M:%S')
                elif isinstance(created_at, datetime):
                    lead['created_at_str'] = created_at.strftime('%Y-%m-%d %H:%M:%S')
                else:
                    lead['created_at_str'] = str(created_at)
            except Exception:
                lead['created_at_str'] = "N/A"
        else:
            lead['created_at_str'] = "N/A"

    return render_template('agent_lead.html', leads=leads)

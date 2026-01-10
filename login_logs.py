from flask import Blueprint, render_template, request, session, redirect, url_for
from pymongo import MongoClient
from pymongo.server_api import ServerApi
from datetime import datetime
from user_agents import parse  # <-- required to parse browser/platform
from db import db
login_logs_bp = Blueprint('login_logs', __name__)

# MongoDB connection

login_logs_col = db.login_logs

@login_logs_bp.route('/login_logs', methods=['GET'])
def login_logs():
    if not any(role in session for role in ['admin_id', 'manager_id', 'agent_id']):
        return redirect(url_for('login.login'))

    user_id = str(session.get('admin_id') or session.get('manager_id') or session.get('agent_id'))
    username = session.get('username') or session.get('manager_name', 'Agent')

    selected_date = request.args.get('date')
    query = {'$or': [
        {'admin_id': user_id},
        {'manager_id': user_id},
        {'agent_id': user_id}
    ]}

    if selected_date:
        try:
            date_obj = datetime.strptime(selected_date, "%Y-%m-%d")
            start = datetime(date_obj.year, date_obj.month, date_obj.day)
            end = datetime(date_obj.year, date_obj.month, date_obj.day, 23, 59, 59)
            query['timestamp'] = {"$gte": start, "$lte": end}
        except:
            pass

    logs_raw = login_logs_col.find(query).sort("timestamp", -1)
    logs = []

    for log in logs_raw:
        timestamp = log.get("timestamp")
        user_agent_str = log.get("user_agent", "")
        ua = parse(user_agent_str)

        logs.append({
            "timestamp": timestamp.strftime("%Y-%m-%d %H:%M:%S") if timestamp else "Unknown",
            "ip": log.get("ip", "N/A"),
            "browser": ua.browser.family,
            "platform": ua.os.family,
            "location": {k: v for k, v in log.get("location", {}).items() if v}
        })

    return render_template('login_logs.html', logs=logs, username=username, selected_date=selected_date)

from flask import Blueprint, render_template
from flask_login import login_required, current_user
from user_model import get_agent_by_id  # Ensure this function is available in user_model.py
from services.login_audit import get_login_logs_for_user, get_login_stats_for_user, annotate_login_logs

# Create a blueprint for agent profile
agent_profile_bp = Blueprint('agent_profile', __name__)

# Route to display the agent profile page
@agent_profile_bp.route('/agent/profile')
@login_required
def profile():
    # Check if current_user.id is available
    if not current_user.id:
        return "User ID not found", 400  # Bad Request if no user ID is present

    # Log current user ID for debugging purposes
    print(f"Fetching profile for agent with user ID: {current_user.id}")
    
    # Fetch agent data using the logged-in user's ID
    agent = get_agent_by_id(current_user.id)  # This will call your function in user_model.py

    def _agent_id_value(doc):
        if not doc:
            return None
        if hasattr(doc, "get"):
            return doc.get("_id")
        return getattr(doc, "_id", None)
    
    # Check if the agent was found
    if agent:
        agent_id = _agent_id_value(agent) or current_user.id
        logs = annotate_login_logs(get_login_logs_for_user(str(agent_id), limit=5))
        stats = get_login_stats_for_user(str(agent_id), days=30)
        return render_template('agent_profile.html', agent=agent, login_logs=logs, login_stats=stats)
    else:
        # Log when agent is not found
        print(f"Agent with user ID {current_user.id} not found.")
        return "Agent not found", 404  # Return a 404 error page if agent is not found

from flask import Blueprint, render_template
from flask_login import login_required, current_user
from user_model import get_agent_by_id  # Ensure this function is available in user_model.py

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
    
    # Check if the agent was found
    if agent:
        return render_template('agent_profile.html', agent=agent)
    else:
        # Log when agent is not found
        print(f"Agent with user ID {current_user.id} not found.")
        return "Agent not found", 404  # Return a 404 error page if agent is not found

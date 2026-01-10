from flask import Blueprint, render_template
from flask_login import login_required, current_user
from bson.objectid import ObjectId
from db import db

agent_account_bp = Blueprint('agent_account', __name__)

# Database collection
commissions_collection = db["commissions"]

@agent_account_bp.route('/agent/account_balance')
@login_required
def account_balance():
    # Get the logged-in agent's ID
    agent_id = str(current_user.id)

    # Fetch all commission entries for the agent
    commissions = list(commissions_collection.find({'agent_id': agent_id}))

    # Calculate total confirmed commissions only
    total_commission = sum(
        c.get('commission_amount', 0) for c in commissions if c.get('status') == 'approved'
    )

    return render_template(
        'account_balance.html',
        commissions=commissions,
        total_commission=total_commission
    )

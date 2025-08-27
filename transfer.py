from flask import Blueprint, render_template, request, flash, redirect, url_for, session
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from bson.objectid import ObjectId
from db import db
# MongoDB connection setup

# Reference database and collections

users_col = db.users
customers_col = db.customers

# Blueprint
transfer_bp = Blueprint('transfer', __name__)

@transfer_bp.route('/transfer', methods=['GET', 'POST'])
def transfer_view():
    if 'manager_id' not in session:
        return redirect(url_for('login.login'))

    try:
        current_manager_id = ObjectId(session['manager_id'])
    except Exception:
        flash("Invalid session manager ID.", "error")
        return redirect(url_for('login.logout'))

    # Get all agents under current manager
    agents = list(users_col.find({'manager_id': current_manager_id, 'role': 'agent'}))
    # Get all managers except current manager (for transfer target)
    managers = list(users_col.find({'role': 'manager', '_id': {'$ne': current_manager_id}}))

    if request.method == 'POST':
        transfer_type = request.form.get('transfer_type')

        if transfer_type == 'agent':
            agent_id = request.form['agent_id']
            new_manager_id = request.form['new_manager_id']

            try:
                agent = users_col.find_one({'_id': ObjectId(agent_id), 'manager_id': current_manager_id})
                new_manager = users_col.find_one({'_id': ObjectId(new_manager_id), 'role': 'manager'})

                if not agent:
                    flash("Agent not found or unauthorized.", "error")
                elif not new_manager:
                    flash("Target manager not found.", "error")
                else:
                    users_col.update_one({'_id': ObjectId(agent_id)}, {'$set': {'manager_id': ObjectId(new_manager_id)}})
                    flash(f"Agent successfully transferred to {new_manager['name']}.", "success")
                    # Redirect to some page showing agent list; make sure this endpoint exists
                    return redirect(url_for('login.agent_list'))
            except Exception as e:
                flash(f"Error transferring agent: {str(e)}", "error")

        elif transfer_type == 'customer':
            source_agent_id = request.form['source_agent_id']
            target_agent_id = request.form['target_agent_id']

            try:
                # Update agent_id for all customers belonging to source agent
                customers_col.update_many({'agent_id': source_agent_id}, {'$set': {'agent_id': target_agent_id}})

                # Also update purchases inside customers
                customers = customers_col.find({'agent_id': target_agent_id})
                for customer in customers:
                    updated = False
                    purchases = customer.get('purchases', [])
                    for purchase in purchases:
                        if purchase.get('agent_id') == source_agent_id:
                            purchase['agent_id'] = target_agent_id
                            updated = True
                    if updated:
                        customers_col.update_one({'_id': customer['_id']}, {'$set': {'purchases': purchases}})

                flash("Customers successfully transferred to the new agent.", "success")
            except Exception as e:
                flash(f"Error transferring customers: {str(e)}", "error")

    return render_template('transfer.html', agents=agents, managers=managers)

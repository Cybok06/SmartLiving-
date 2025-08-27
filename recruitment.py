from flask import Blueprint, render_template, request, redirect, url_for
from pymongo import MongoClient
from pymongo.server_api import ServerApi
from bson.objectid import ObjectId
from db import db
recruitment_bp = Blueprint('recruitment', __name__)



users_col = db.users
customers_col = db.customers
deleted_col = db.deleted

@recruitment_bp.route('/recruitment')
def recruitment():
    manager_filter = request.args.get('manager_id')

    # Fetch all managers
    managers = list(users_col.find({"role": "manager"}))

    # Optional filtering by manager
    if manager_filter:
        agents = list(users_col.find({"role": "agent", "manager_id": ObjectId(manager_filter)}))
        agent_ids = [agent["_id"] for agent in agents]
        customers = list(customers_col.find({"agent_id": {"$in": [str(aid) for aid in agent_ids]}}))
    else:
        agents = list(users_col.find({"role": "agent"}))
        customers = list(customers_col.find())

    # Calculate total agents and customers per manager
    total_per_branch = []
    for manager in managers:
        manager_id = manager["_id"]

        # Agents under this manager
        manager_agents = list(users_col.find({"role": "agent", "manager_id": manager_id}))
        agent_ids = [str(agent["_id"]) for agent in manager_agents]

        # Customers of those agents
        customer_count = customers_col.count_documents({"agent_id": {"$in": agent_ids}})

        total_per_branch.append({
            "manager": manager,
            "agent_count": len(manager_agents),
            "customer_count": customer_count
        })

    return render_template('recruitment.html',
                           managers=managers,
                           agents=agents,
                           customers=customers,
                           selected_manager=manager_filter,
                           total_per_branch=total_per_branch)

@recruitment_bp.route('/delete_person/<person_type>/<person_id>', methods=['POST'])
def delete_person(person_type, person_id):
    if person_type == 'agent':
        person = users_col.find_one_and_delete({"_id": ObjectId(person_id), "role": "agent"})
    elif person_type == 'customer':
        person = customers_col.find_one_and_delete({"_id": ObjectId(person_id)})
    else:
        return "Invalid type", 400

    if person:
        deleted_col.insert_one({**person, "original_type": person_type})

    return redirect(url_for('recruitment.recruitment'))

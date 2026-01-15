from flask import Blueprint, render_template, request, redirect, url_for
from bson.objectid import ObjectId
from db import db
recruitment_bp = Blueprint('recruitment', __name__)



users_col = db.users
deleted_col = db.deleted

try:
    users_col.create_index([("role", 1), ("manager_id", 1), ("name", 1)], background=True)
    users_col.create_index([("role", 1), ("manager_id", 1), ("phone", 1)], background=True)
    users_col.create_index([("role", 1), ("manager_id", 1), ("date_registered", -1)], background=True)
except Exception:
    pass

@recruitment_bp.route('/recruitment')
def recruitment():
    selected_manager = (request.args.get('manager_id') or '').strip()
    search = (request.args.get('search') or '').strip()
    per_page = 10
    try:
        page = max(1, int(request.args.get('page', 1)))
    except Exception:
        page = 1

    managers = list(
        users_col.find(
            {"role": "manager"},
            {"name": 1, "username": 1, "branch": 1}
        ).sort("branch", 1)
    )

    branch_cards = []
    for manager in managers:
        manager_id = manager["_id"]
        agent_count = users_col.count_documents({"role": "agent", "manager_id": manager_id})
        branch_cards.append({
            "manager": manager,
            "agent_count": agent_count
        })

    agent_query = {"role": "agent"}
    if selected_manager:
        try:
            agent_query["manager_id"] = ObjectId(selected_manager)
        except Exception:
            agent_query["manager_id"] = None

    if search:
        agent_query["$or"] = [
            {"name": {"$regex": search, "$options": "i"}},
            {"phone": {"$regex": search, "$options": "i"}}
        ]

    total_count = users_col.count_documents(agent_query)
    total_pages = max(1, (total_count + per_page - 1) // per_page)
    if page > total_pages:
        page = total_pages

    skip = (page - 1) * per_page
    projection = {
        "name": 1,
        "phone": 1,
        "email": 1,
        "image_url": 1,
        "status": 1,
        "branch": 1,
        "username": 1,
        "manager_id": 1,
        "date_registered": 1
    }
    agents = list(
        users_col.find(agent_query, projection)
        .sort("date_registered", -1)
        .skip(skip)
        .limit(per_page)
    )

    total_agents_in_view = total_count
    total_agents = users_col.count_documents({"role": "agent"})
    total_active = users_col.count_documents({"role": "agent", "status": {"$in": ["Active", "active"]}})
    total_not_active = max(total_agents - total_active, 0)

    return render_template(
        'recruitment.html',
        managers=managers,
        branch_cards=branch_cards,
        agents=agents,
        selected_manager_id=selected_manager,
        search=search,
        page=page,
        per_page=per_page,
        total_pages=total_pages,
        total_count=total_count,
        total_agents_in_view=total_agents_in_view,
        total_agents=total_agents,
        total_active=total_active,
        total_not_active=total_not_active,
        has_prev=page > 1,
        has_next=page < total_pages
    )

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

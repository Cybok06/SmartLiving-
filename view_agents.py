# agent.py or view.py
from flask import Blueprint, render_template
from pymongo import MongoClient

agent_bp = Blueprint('agent', __name__)
client = MongoClient("mongodb://localhost:27017/")
db = client.crm_system

@agent_bp.route('/view_agents')
def view_agents():
    agents = db.users.find({"role": "agent"})
    return render_template('view_agents.html', agents=agents)

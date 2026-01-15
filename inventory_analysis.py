from flask import Blueprint, render_template
from pymongo import MongoClient
from pymongo.server_api import ServerApi
from collections import defaultdict
import json
from db import db
inventory_analysis_bp = Blueprint('inventory_analysis', __name__)

# MongoDB connection

inventory_col = db.inventory
users_col = db.users

@inventory_analysis_bp.route('/inventory_analysis')
def inventory_analysis():
    inventory = list(inventory_col.aggregate([
        {
            "$lookup": {
                "from": "users",
                "localField": "manager_id",
                "foreignField": "_id",
                "as": "manager"
            }
        },
        {"$unwind": "$manager"}
    ]))

    overall = {"low": 0, "high": 0}
    per_branch = defaultdict(lambda: {"low": 0, "high": 0, "products": {"low": [], "high": []}})

    for item in inventory:
        qty = item.get("qty", 0)
        branch = item["manager"].get("branch", "Unknown")
        product = item.get("name", "Unknown")

        if qty <= 5:
            overall["low"] += 1
            per_branch[branch]["low"] += 1
            per_branch[branch]["products"]["low"].append(product)
        else:
            overall["high"] += 1
            per_branch[branch]["high"] += 1
            per_branch[branch]["products"]["high"].append(product)

    return render_template("inventory_analysis.html",
                           overall=json.dumps(overall),
                           per_branch=json.dumps(per_branch))

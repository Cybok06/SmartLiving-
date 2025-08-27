from flask import Blueprint, render_template, request, redirect, url_for, flash
from flask_login import login_required, current_user
from bson import ObjectId
from datetime import datetime
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from db import db
# MongoDB Atlas connection



# Database and collections

leads_collection = db['leads']
customers_collection = db['customers']

# Define Blueprint
lead_bp = Blueprint('lead_bp', __name__)

@lead_bp.route('/', methods=['GET'])
@login_required
def view_leads():
    leads = list(leads_collection.find({'agent_id': str(current_user.id)}))
    total_leads = len(leads)
    total_converted = leads_collection.count_documents({
        "converted": True,
        "agent_id": str(current_user.id)
    })
    
    # Calculate leads by status for the chart
    total_not_converted = total_leads - total_converted
    leads_by_status = {
        "Converted": total_converted,
        "Not Converted": total_not_converted
    }
    
    return render_template('lead.html', leads=leads, total_leads=total_leads,
                           total_converted=total_converted,
                           leads_by_status=leads_by_status)

@lead_bp.route('/add_lead', methods=['POST'])
@login_required
def add_lead():
    data = {
        "name": request.form.get('name', ''),
        "image_url": request.form.get('image_url', ''),
        "location": request.form.get('location', ''),
        "occupation": request.form.get('occupation', ''),
        "phone_number": request.form.get('phone_number', ''),
        "comments": request.form.get('comments', ''),
        "converted": False,
        "created_at": datetime.now(),
        "agent_id": str(current_user.id)  # associate lead with agent
    }
    leads_collection.insert_one(data)
    flash("✅ Lead added successfully.")
    return redirect(url_for('lead_bp.view_leads'))

@lead_bp.route('/convert/<lead_id>', methods=['POST'])
@login_required
def convert_lead(lead_id):
    lead = leads_collection.find_one({
        "_id": ObjectId(lead_id),
        "agent_id": str(current_user.id)  # Only allow converting leads belonging to the current agent
    })
    
    if lead:
        lead_data = {k: v for k, v in lead.items() if k != '_id'}
        lead_data["converted_at"] = datetime.now()

        # Ensure "name" is used
        if "full_name" in lead_data:
            lead_data["name"] = lead_data.pop("full_name")

        customers_collection.insert_one(lead_data)
        leads_collection.update_one({"_id": ObjectId(lead_id)}, {"$set": {"converted": True}})
        flash("✅ Lead converted to customer.")
    else:
        flash("❌ Lead not found or unauthorized.")
    
    return redirect(url_for('lead_bp.view_leads'))

from flask import Blueprint, render_template, request, session, jsonify, redirect, url_for
from pymongo import MongoClient
from bson.objectid import ObjectId
from datetime import datetime
from db import db

chat_bp = Blueprint('chat', __name__)


users_col = db.users
messages_col = db.chat_messages

def get_logged_in_user():
    """Returns current session user's ID and full user document."""
    user_id = session.get('admin_id') or session.get('manager_id') or session.get('agent_id')
    if user_id:
        user = users_col.find_one({"_id": ObjectId(user_id)})
        return user_id, user
    return None, None

@chat_bp.route('/chat')
def chat():
    user_id, current_user = get_logged_in_user()
    if not current_user:
        return redirect(url_for('login.login'))

    # Optional search filter
    search = request.args.get('search', '').strip()
    query = {"_id": {"$ne": ObjectId(user_id)}}
    if search:
        query["name"] = {"$regex": search, "$options": "i"}

    users = list(users_col.find(query))

    # Get unread message counts and sort users accordingly
    unread_map = get_unread_count_map(user_id)
    users.sort(key=lambda u: -unread_map.get(str(u['_id']), 0))

    return render_template('chat.html', users=users, current_user=current_user)

@chat_bp.route('/get_messages/<receiver_id>')
def get_messages(receiver_id):
    user_id, _ = get_logged_in_user()
    if not user_id:
        return jsonify([])

    # Fetch chats between current user and selected receiver
    chats = list(messages_col.find({
        "$or": [
            {"sender_id": user_id, "receiver_id": receiver_id},
            {"sender_id": receiver_id, "receiver_id": user_id}
        ]
    }).sort("timestamp", 1))

    # Mark messages from receiver as read
    messages_col.update_many(
        {"receiver_id": user_id, "sender_id": receiver_id, "is_read": False},
        {"$set": {"is_read": True}}
    )

    return jsonify([
        {
            "sender_id": msg['sender_id'],
            "receiver_id": msg['receiver_id'],
            "content": msg['content'],
            "timestamp": msg['timestamp'].strftime('%Y-%m-%d %H:%M')
        } for msg in chats
    ])

@chat_bp.route('/send_message', methods=['POST'])
def send_message():
    sender_id, _ = get_logged_in_user()
    receiver_id = request.form.get('receiver_id')
    content = request.form.get('content', '').strip()

    if not sender_id or not receiver_id or not content:
        return "Invalid", 400

    messages_col.insert_one({
        "sender_id": sender_id,
        "receiver_id": receiver_id,
        "content": content,
        "timestamp": datetime.utcnow(),
        "is_read": False
    })

    return "Sent", 200

@chat_bp.route('/unread_counts')
def unread_counts():
    user_id, _ = get_logged_in_user()
    if not user_id:
        return jsonify({})
    return jsonify(get_unread_count_map(user_id))

def get_unread_count_map(user_id):
    """Returns a dictionary mapping sender_id to unread message count."""
    pipeline = [
        {"$match": {"receiver_id": user_id, "is_read": False}},
        {"$group": {"_id": "$sender_id", "count": {"$sum": 1}}}
    ]
    return {
        str(row["_id"]): row["count"]
        for row in messages_col.aggregate(pipeline)
    }

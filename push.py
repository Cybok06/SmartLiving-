# seed_user.py
from datetime import datetime
from flask_bcrypt import Bcrypt
from db import db  # uses your app's Mongo client

bcrypt = Bcrypt()
users_col = db["users"]

user_data = {
    "username": "inventory",
    "password": bcrypt.generate_password_hash("1234").decode("utf-8"),
    "role": "inventory",  # change to "inventory" if needed
    "name": "Fred Asare Boahene",
    "phone": "0541560711",
    "email": "freddyfrixx@gmail.com",
    "gender": "Male",
    "branch": "HQ",
    "position": "Logistics Management",
    "location": "Club Corner",
    "start_date": "2024-09-23",
    "image_url": "https://res.cloudinary.com/drmbldyfx/image/upload/v1749231046/manager-images/hs8xkwxbmqdphumvz9xn.jpg",
    "status": "Active",
    "assets": ["phone"],
    "date_registered": "2025-06-06",
}

# Optional (run-safe): make username unique to avoid duplicates
try:
    users_col.create_index("username", unique=True)
except Exception:
    pass

# Upsert by username; _id will be auto-created on first insert
res = users_col.update_one(
    {"username": user_data["username"]},
    {
        "$set": {k: v for k, v in user_data.items() if k != "username"},
        "$setOnInsert": {"created_at": datetime.utcnow()},
    },
    upsert=True,
)

if res.upserted_id:
    print(f"✅ Inserted new user with _id={res.upserted_id}")
else:
    print(f"✅ Updated existing user: {user_data['username']}")

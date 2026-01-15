# seed_inventory_user.py
from bson import ObjectId
from flask_bcrypt import Bcrypt
from db import db  # <-- uses your existing app/db config

bcrypt = Bcrypt()
users_col = db["users"]

user_data = {
    "_id": ObjectId("684325d705a08a53aa506224"),  # keep fixed, or remove to auto-generate
    "username": "fred21",
    "password": bcrypt.generate_password_hash("password123").decode("utf-8"),  # change as needed
    "role": "inventory",
    "name": "Fred Asare Boahene",
    "phone": "0541560711",
    "email": "freddyfrixx@gmail.com",
    "gender": "Male",
    "branch": "HQ",
    "position": "Admin",
    "location": "Club Corner",
    "start_date": "2024-09-23",
    "image_url": "https://res.cloudinary.com/drmbldyfx/image/upload/v1749231046/manager-images/hs8xkwxbmqdphumvz9xn.jpg",
    "status": "Active",
    "assets": ["phone"],
    "date_registered": "2025-06-06"
}

# Upsert by _id; if you prefer by username, replace the filter with {"username": user_data["username"]}
users_col.update_one({"_id": user_data["_id"]}, {"$set": user_data}, upsert=True)

print("✅ Inventory user inserted/updated successfully.")

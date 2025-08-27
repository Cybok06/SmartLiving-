from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

# MongoDB Atlas connection URI
uri = "mongodb+srv://cybok:0500868021Yaw@cluster0.mpkoedf.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"

# Connect using Server API version 1
client = MongoClient(uri, server_api=ServerApi('1'))

# Test the connection
try:
    client.admin.command('ping')
    print("Connected to MongoDB!")
except Exception as e:
    print("MongoDB connection failed:", e)

# Select the database
db = client['crm_system']

# Collections
users_collection = db['users']
tasks_collection = db['tasks']  # ✅ ADD THIS LINE

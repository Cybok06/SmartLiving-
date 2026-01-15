from flask_login import UserMixin
from db import users_collection
import bson
from datetime import datetime

class User(UserMixin):
    def __init__(self, user_data):
        # Basic user info
        self.id = str(user_data['_id'])
        self.username = user_data.get('username')
        self.role = user_data.get('role', '').lower()
        self.location = user_data.get('location')
        self.name = user_data.get('name')
        self.email = user_data.get('email')
        self.phone = user_data.get('phone')
        self.image_url = user_data.get('image_url')

        # Common timestamps (converted to datetime objects if possible)
        self.date_registered = self._convert_to_datetime(user_data.get('date_registered'))
        self.start_date = self._convert_to_datetime(user_data.get('start_date'))

        # Agent-specific fields
        self.position = user_data.get('position')
        self.branch = user_data.get('branch')
        self.status = user_data.get('status')
        self.assets = user_data.get('assets', [])

    def _convert_to_datetime(self, value):
        """
        Attempts to convert a value to a datetime object.
        If it's already a datetime or cannot be converted, it returns the original value.
        """
        if isinstance(value, datetime):
            return value
        if isinstance(value, str):
            try:
                return datetime.fromisoformat(value)
            except ValueError:
                try:
                    # Fallback to parsing common date string formats
                    return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
                except:
                    pass
        return value  # Leave as-is if conversion fails

    def __repr__(self):
        return f"<User {self.username}, {self.role}>"

# Generic function to get a user by ID
def get_user_by_id(user_id):
    try:
        user_data = users_collection.find_one({'_id': bson.ObjectId(user_id)})
        if user_data:
            return User(user_data)
        else:
            print(f"User with ID {user_id} not found.")
    except Exception as e:
        print(f"Error fetching user by ID: {e}")
    return None

# Function to specifically get an agent (based on lowercase role match)
def get_agent_by_id(user_id):
    try:
        user_data = users_collection.find_one({'_id': bson.ObjectId(user_id)})
        if user_data:
            role = user_data.get('role', '').lower()
            if role == 'agent':
                return User(user_data)
            else:
                print(f"User with ID {user_id} is not an agent (Role: {role})")
        else:
            print(f"Agent with ID {user_id} not found.")
    except Exception as e:
        print(f"Error fetching agent: {e}")
    return None

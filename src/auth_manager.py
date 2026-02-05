import json
import os
import hashlib

USERS_FILE = "users.json"

class AuthManager:
    def __init__(self):
        self.users = self._load_users()

    def _load_users(self):
        if os.path.exists(USERS_FILE):
            try:
                with open(USERS_FILE, "r") as f:
                    return json.load(f)
            except:
                pass
        
        # Default admin user if file doesn't exist or is corrupt
        default_admin = {
            "admin": {
                "password": self._hash_password("admin123"),
                "role": "Admin",
                "metrics": [] # Empty means all for Admin
            }
        }
        self._save_users(default_admin)
        return default_admin

    def _save_users(self, users):
        try:
            with open(USERS_FILE, "w") as f:
                json.dump(users, f, indent=2)
        except:
            pass

    def _hash_password(self, password):
        return hashlib.sha256(password.encode()).hexdigest()

    def authenticate(self, username, password):
        if username in self.users:
            stored_hash = self.users[username]["password"]
            if stored_hash == self._hash_password(password):
                return self.users[username]
        return None

    def add_user(self, username, password, role, metrics=None):
        if username in self.users:
            return False, "User already exists"
        
        self.users[username] = {
            "password": self._hash_password(password),
            "role": role,
            "metrics": metrics or []
        }
        self._save_users(self.users)
        return True, "User added successfully"

    def reset_password(self, username, new_password):
        if username not in self.users:
            return False, "User not found"
        
        self.users[username]["password"] = self._hash_password(new_password)
        self._save_users(self.users)
        return True, "Password updated successfully"

    def delete_user(self, username):
        if username == "admin":
            return False, "Cannot delete default admin"
        if username in self.users:
            del self.users[username]
            self._save_users(self.users)
            return True, "User deleted"
        return False, "User not found"

    def get_all_users(self):
        return self.users

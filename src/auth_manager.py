import json
import os
import hashlib

ORG_BASE_DIR = "orgs"
USERS_FILE = "users.json"

class AuthManager:
    def __init__(self):
        self.users = self._load_users()

    def _load_users(self):
        data = {}
        if os.path.isdir(ORG_BASE_DIR):
            try:
                for org_code in os.listdir(ORG_BASE_DIR):
                    org_path = os.path.join(ORG_BASE_DIR, org_code)
                    if not os.path.isdir(org_path):
                        continue
                    users_path = os.path.join(org_path, USERS_FILE)
                    if os.path.exists(users_path):
                        with open(users_path, "r") as f:
                            data[org_code] = json.load(f)
            except Exception:
                pass
        if data:
            return data

        # Migration from legacy single users.json
        if os.path.exists(USERS_FILE):
            try:
                with open(USERS_FILE, "r") as f:
                    legacy = json.load(f)
                    # Migration: If old format (no org nesting), move to 'default' org
                    first_val = next(iter(legacy.values())) if legacy else None
                    if first_val and "password" in first_val:
                        legacy = {"default": legacy}
                    self._save_users(legacy)
                    try:
                        os.remove(USERS_FILE)
                    except Exception:
                        pass
                    return legacy
            except Exception:
                pass
        
        # Default setup
        default_data = {
            "default": {
                "admin": {
                    "password": self._hash_password("admin123"),
                    "role": "Admin",
                    "metrics": []
                }
            }
        }
        self._save_users(default_data)
        return default_data

    def _save_users(self, users):
        try:
            os.makedirs(ORG_BASE_DIR, exist_ok=True)
            for org_code, org_users in users.items():
                org_path = os.path.join(ORG_BASE_DIR, org_code)
                os.makedirs(org_path, exist_ok=True)
                users_path = os.path.join(org_path, USERS_FILE)
                with open(users_path, "w") as f:
                    json.dump(org_users, f, indent=2)
        except Exception:
            pass

    def _hash_password(self, password):
        return hashlib.sha256(password.encode()).hexdigest()

    def authenticate(self, org_code, username, password):
        org_users = self.users.get(org_code, {})
        if username in org_users:
            stored_hash = org_users[username]["password"]
            if stored_hash == self._hash_password(password):
                user_data = org_users[username].copy()
                user_data["org_code"] = org_code
                return user_data
        return None

    def add_user(self, org_code, username, password, role, metrics=None):
        if org_code not in self.users:
            self.users[org_code] = {}
            
        if username in self.users[org_code]:
            return False, "User already exists in this organization"
        
        self.users[org_code][username] = {
            "password": self._hash_password(password),
            "role": role,
            "metrics": metrics or []
        }
        self._save_users(self.users)
        return True, "User added successfully"

    def add_organization(self, org_code, admin_username, admin_password):
        if org_code in self.users:
            return False, "Organization already exists"
        self.users[org_code] = {}
        self.users[org_code][admin_username] = {
            "password": self._hash_password(admin_password),
            "role": "Admin",
            "metrics": []
        }
        self._save_users(self.users)
        return True, "Organization created"

    def delete_organization(self, org_code):
        if org_code == "default":
            return False, "Cannot delete default organization"
        if org_code in self.users:
            del self.users[org_code]
            self._save_users(self.users)
            return True, "Organization deleted"
        return False, "Organization not found"

    def reset_password(self, org_code, username, new_password):
        if org_code not in self.users or username not in self.users[org_code]:
            return False, "User not found"
        
        self.users[org_code][username]["password"] = self._hash_password(new_password)
        self._save_users(self.users)
        return True, "Password updated successfully"

    def delete_user(self, org_code, username):
        if username == "admin" and org_code == "default":
            return False, "Cannot delete default system admin"
            
        if org_code in self.users and username in self.users[org_code]:
            del self.users[org_code][username]
            if not self.users[org_code]: # Optional: keep empty orgs
                pass 
            self._save_users(self.users)
            return True, "User deleted"
        return False, "User not found"

    def get_all_users(self, org_code):
        return self.users.get(org_code, {})
    
    def get_organizations(self):
        return list(self.users.keys())

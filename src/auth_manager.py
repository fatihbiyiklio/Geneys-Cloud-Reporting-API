import json
import os
import base64
import hmac
import hashlib
import sys
import shutil
import re
import secrets
import string

def _resolve_org_base_dir():
    env_dir = os.environ.get("GENESYS_STATE_DIR")
    if env_dir:
        return os.path.abspath(env_dir)
    if getattr(sys, "frozen", False):
        appdata = os.environ.get("APPDATA")
        if appdata:
            return os.path.join(appdata, "GenesysCloudReporting", "orgs")
        return os.path.join(os.path.expanduser("~"), ".genesys_cloud_reporting", "orgs")
    return os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "orgs")

ORG_BASE_DIR = _resolve_org_base_dir()
USERS_FILE = "users.json"
PBKDF2_HASH_PREFIX = "pbkdf2_sha256"
PBKDF2_ITERATIONS = 260000
ORG_CODE_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{2,49}$")
DEFAULT_ADMIN_USERNAME = "admin"
DEFAULT_ADMIN_PASSWORD_ENV = "GENESYS_BOOTSTRAP_ADMIN_PASSWORD"
BOOTSTRAP_PASSWORD_FILENAME = "_bootstrap_admin_password.txt"
MIN_PASSWORD_LENGTH = 8


def _normalize_org_code(org_code, allow_default=True):
    raw = str(org_code or "").strip()
    if not raw:
        if allow_default:
            return "default"
        raise ValueError("Organization code is required")
    if not ORG_CODE_PATTERN.fullmatch(raw):
        raise ValueError("Organization code must match ^[A-Za-z0-9][A-Za-z0-9_-]{2,49}$")
    return raw


def _generate_secure_password(length=20):
    alphabet = string.ascii_letters + string.digits + "!@#$%^&*()-_=+"
    return "".join(secrets.choice(alphabet) for _ in range(length))

class AuthManager:
    def __init__(self):
        self._migrate_legacy_state_dir()
        self.users = self._load_users()

    def _safe_org_path(self, org_code):
        normalized = _normalize_org_code(org_code)
        base = os.path.abspath(ORG_BASE_DIR)
        path = os.path.abspath(os.path.join(base, normalized))
        if os.path.commonpath([base, path]) != base:
            raise ValueError(f"Invalid organization path for '{org_code}'")
        return path

    def _safe_users_path(self, org_code):
        return os.path.join(self._safe_org_path(org_code), USERS_FILE)

    def _bootstrap_password_file_path(self):
        return os.path.join(self._safe_org_path("default"), BOOTSTRAP_PASSWORD_FILENAME)

    def _migrate_legacy_state_dir(self):
        try:
            os.makedirs(ORG_BASE_DIR, exist_ok=True)
            if os.listdir(ORG_BASE_DIR):
                return
        except Exception:
            return
        candidates = []
        try:
            candidates.append(os.path.join(os.getcwd(), "orgs"))
        except Exception:
            pass
        try:
            if getattr(sys, "frozen", False):
                candidates.append(os.path.join(os.path.dirname(os.path.abspath(sys.executable)), "orgs"))
        except Exception:
            pass
        for source in candidates:
            try:
                if not source or os.path.abspath(source) == os.path.abspath(ORG_BASE_DIR):
                    continue
                if os.path.isdir(source) and os.listdir(source):
                    shutil.copytree(source, ORG_BASE_DIR, dirs_exist_ok=True)
                    return
            except Exception:
                continue

    def _load_users(self):
        data = {}
        if os.path.isdir(ORG_BASE_DIR):
            for org_code in os.listdir(ORG_BASE_DIR):
                org_path = os.path.join(ORG_BASE_DIR, org_code)
                if not os.path.isdir(org_path):
                    continue
                try:
                    safe_org = _normalize_org_code(org_code)
                except ValueError:
                    # Ignore non-org utility folders such as _tmp/_monitor.
                    continue
                users_path = os.path.join(org_path, USERS_FILE)
                if os.path.exists(users_path):
                    try:
                        with open(users_path, "r", encoding="utf-8") as f:
                            data[safe_org] = json.load(f)
                    except Exception as exc:
                        raise RuntimeError(f"Failed to read users file: {users_path}") from exc
        if data:
            return data

        # Migration from legacy single users.json
        if os.path.exists(USERS_FILE):
            try:
                with open(USERS_FILE, "r", encoding="utf-8") as f:
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
            except Exception as exc:
                raise RuntimeError("Failed to migrate legacy users.json") from exc
        
        # Default setup
        bootstrap_password = os.environ.get(DEFAULT_ADMIN_PASSWORD_ENV, "").strip()
        generated_password = not bool(bootstrap_password)
        if generated_password:
            bootstrap_password = _generate_secure_password(20)
        default_data = {
            "default": {
                DEFAULT_ADMIN_USERNAME: {
                    "password": self._hash_password(bootstrap_password),
                    "role": "Admin",
                    "metrics": [],
                    "must_change_password": generated_password,
                }
            }
        }
        self._save_users(default_data)
        if generated_password:
            bootstrap_path = self._bootstrap_password_file_path()
            os.makedirs(os.path.dirname(bootstrap_path), exist_ok=True)
            with open(bootstrap_path, "w", encoding="utf-8") as f:
                f.write(
                    "Initial admin credentials (generated once):\n"
                    f"username={DEFAULT_ADMIN_USERNAME}\n"
                    f"password={bootstrap_password}\n"
                )
            try:
                os.chmod(bootstrap_path, 0o600)
            except Exception:
                pass
            print(
                f"[SECURITY] Generated one-time admin password. Read: {bootstrap_path}",
                file=sys.stderr,
            )
        return default_data

    def _save_users(self, users):
        if not isinstance(users, dict):
            raise ValueError("users must be a dict")
        os.makedirs(ORG_BASE_DIR, exist_ok=True)
        for org_code, org_users in users.items():
            safe_org = _normalize_org_code(org_code)
            if not isinstance(org_users, dict):
                raise ValueError(f"Invalid users payload for org '{safe_org}'")
            users_path = self._safe_users_path(safe_org)
            os.makedirs(os.path.dirname(users_path), exist_ok=True)
            with open(users_path, "w", encoding="utf-8") as f:
                json.dump(org_users, f, indent=2)
            try:
                os.chmod(users_path, 0o600)
            except Exception:
                pass

    def _hash_password(self, password):
        salt = os.urandom(16)
        derived = hashlib.pbkdf2_hmac(
            "sha256",
            password.encode("utf-8"),
            salt,
            PBKDF2_ITERATIONS,
        )
        salt_b64 = base64.b64encode(salt).decode("ascii")
        hash_b64 = base64.b64encode(derived).decode("ascii")
        return f"{PBKDF2_HASH_PREFIX}${PBKDF2_ITERATIONS}${salt_b64}${hash_b64}"

    def _verify_password(self, password, stored_hash):
        if not stored_hash:
            return False
        if stored_hash.startswith(f"{PBKDF2_HASH_PREFIX}$"):
            try:
                _, iter_s, salt_b64, hash_b64 = stored_hash.split("$", 3)
                iterations = int(iter_s)
                salt = base64.b64decode(salt_b64.encode("ascii"))
                expected = base64.b64decode(hash_b64.encode("ascii"))
                actual = hashlib.pbkdf2_hmac(
                    "sha256",
                    password.encode("utf-8"),
                    salt,
                    iterations,
                )
                return hmac.compare_digest(actual, expected)
            except Exception:
                return False
        # Legacy format fallback: unsalted SHA-256 hex
        legacy = hashlib.sha256(password.encode("utf-8")).hexdigest()
        return hmac.compare_digest(legacy, stored_hash)

    def authenticate(self, org_code, username, password):
        try:
            org_code = _normalize_org_code(org_code)
        except ValueError:
            return None
        org_users = self.users.get(org_code, {})
        if username in org_users:
            stored_hash = org_users[username]["password"]
            if self._verify_password(password, stored_hash):
                if not stored_hash.startswith(f"{PBKDF2_HASH_PREFIX}$"):
                    self.users[org_code][username]["password"] = self._hash_password(password)
                    try:
                        self._save_users(self.users)
                    except Exception:
                        return None
                user_data = org_users[username].copy()
                user_data["org_code"] = org_code
                return user_data
        return None

    def add_user(self, org_code, username, password, role, metrics=None):
        try:
            org_code = _normalize_org_code(org_code)
        except ValueError as exc:
            return False, str(exc)
        username = str(username or "").strip()
        if not username:
            return False, "Username is required"
        if len(str(password or "")) < MIN_PASSWORD_LENGTH:
            return False, f"Password must be at least {MIN_PASSWORD_LENGTH} characters"
        if org_code not in self.users:
            self.users[org_code] = {}
            
        if username in self.users[org_code]:
            return False, "User already exists in this organization"
        
        self.users[org_code][username] = {
            "password": self._hash_password(password),
            "role": role,
            "metrics": metrics or []
        }
        try:
            self._save_users(self.users)
        except Exception as exc:
            return False, f"Failed to persist user data: {exc}"
        return True, "User added successfully"

    def add_organization(self, org_code, admin_username, admin_password):
        try:
            org_code = _normalize_org_code(org_code, allow_default=False)
        except ValueError as exc:
            return False, str(exc)
        admin_username = str(admin_username or "").strip()
        if not admin_username:
            return False, "Admin username is required"
        if len(str(admin_password or "")) < MIN_PASSWORD_LENGTH:
            return False, f"Admin password must be at least {MIN_PASSWORD_LENGTH} characters"
        if org_code in self.users:
            return False, "Organization already exists"
        self.users[org_code] = {}
        self.users[org_code][admin_username] = {
            "password": self._hash_password(admin_password),
            "role": "Admin",
            "metrics": []
        }
        try:
            self._save_users(self.users)
        except Exception as exc:
            return False, f"Failed to persist organization data: {exc}"
        return True, "Organization created"

    def delete_organization(self, org_code):
        try:
            org_code = _normalize_org_code(org_code, allow_default=False)
        except ValueError as exc:
            return False, str(exc)
        if org_code == "default":
            return False, "Cannot delete default organization"
        if org_code in self.users:
            del self.users[org_code]
            try:
                self._save_users(self.users)
            except Exception as exc:
                return False, f"Failed to persist organization deletion: {exc}"
            return True, "Organization deleted"
        return False, "Organization not found"

    def reset_password(self, org_code, username, new_password):
        try:
            org_code = _normalize_org_code(org_code)
        except ValueError as exc:
            return False, str(exc)
        username = str(username or "").strip()
        if len(str(new_password or "")) < MIN_PASSWORD_LENGTH:
            return False, f"Password must be at least {MIN_PASSWORD_LENGTH} characters"
        if org_code not in self.users or username not in self.users[org_code]:
            return False, "User not found"
        
        self.users[org_code][username]["password"] = self._hash_password(new_password)
        self.users[org_code][username]["must_change_password"] = False
        try:
            self._save_users(self.users)
        except Exception as exc:
            return False, f"Failed to persist password update: {exc}"
        if org_code == "default" and username == DEFAULT_ADMIN_USERNAME:
            try:
                bootstrap_path = self._bootstrap_password_file_path()
                if os.path.exists(bootstrap_path):
                    os.remove(bootstrap_path)
            except Exception:
                pass
        return True, "Password updated successfully"

    def delete_user(self, org_code, username):
        try:
            org_code = _normalize_org_code(org_code)
        except ValueError as exc:
            return False, str(exc)
        username = str(username or "").strip()
        if username == "admin" and org_code == "default":
            return False, "Cannot delete default system admin"
            
        if org_code in self.users and username in self.users[org_code]:
            target_user = self.users[org_code][username]
            if target_user.get("role") == "Admin":
                admin_count = sum(1 for u in self.users[org_code].values() if (u or {}).get("role") == "Admin")
                if admin_count <= 1:
                    return False, "Cannot delete the last admin in organization"
            del self.users[org_code][username]
            if not self.users[org_code]: # Optional: keep empty orgs
                pass 
            try:
                self._save_users(self.users)
            except Exception as exc:
                return False, f"Failed to persist user deletion: {exc}"
            return True, "User deleted"
        return False, "User not found"

    def get_all_users(self, org_code):
        try:
            org_code = _normalize_org_code(org_code)
        except ValueError:
            return {}
        return self.users.get(org_code, {})
    
    def get_organizations(self):
        return sorted([org for org in self.users.keys() if ORG_CODE_PATTERN.fullmatch(org)])

import json
import os
import threading
import time
import requests
import sys
import re
from cryptography.fernet import Fernet

_cache_lock = threading.Lock()
_mem_cache = {}
_MAX_MEM_CACHE_ENTRIES = 50
ORG_CODE_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{2,49}$")
SECRET_KEY_FILENAME = ".secret.key"

def _cache_key(client_id, region):
    return f"{client_id}:{region}"

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


def _safe_org_code(org_code):
    raw = str(org_code or "").strip()
    if not raw:
        return None
    if not ORG_CODE_PATTERN.fullmatch(raw):
        raise ValueError("Organization code must match ^[A-Za-z0-9][A-Za-z0-9_-]{2,49}$")
    return raw


def _safe_org_dir(org_code, create=False):
    safe_org = _safe_org_code(org_code)
    if not safe_org:
        return None
    base = os.path.abspath(ORG_BASE_DIR)
    path = os.path.abspath(os.path.join(base, safe_org))
    if os.path.commonpath([base, path]) != base:
        raise ValueError(f"Invalid organization code: {org_code}")
    if create:
        os.makedirs(path, exist_ok=True)
    return path


def _get_secret_key_path():
    base_dir = os.environ.get("GENESYS_STATE_DIR") or ORG_BASE_DIR
    os.makedirs(base_dir, exist_ok=True)
    return os.path.join(base_dir, SECRET_KEY_FILENAME)


def _get_or_create_key():
    key_path = _get_secret_key_path()
    if os.path.exists(key_path):
        with open(key_path, "rb") as f:
            return f.read()
    key = Fernet.generate_key()
    with open(key_path, "wb") as f:
        f.write(key)
    try:
        os.chmod(key_path, 0o600)
    except Exception:
        pass
    return key


def _get_cipher():
    return Fernet(_get_or_create_key())


def _decrypt_cache_payload(raw_bytes):
    if not raw_bytes:
        return None
    # Legacy plaintext cache compatibility.
    try:
        text = raw_bytes.decode("utf-8")
        if text.strip().startswith("{"):
            return json.loads(text)
    except Exception:
        pass
    try:
        cipher = _get_cipher()
        decrypted = cipher.decrypt(raw_bytes).decode("utf-8")
        return json.loads(decrypted)
    except Exception:
        return None


def _encrypt_cache_payload(entry):
    cipher = _get_cipher()
    payload = json.dumps(entry, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    return cipher.encrypt(payload)

def _org_token_cache_path(org_code):
    base = _safe_org_dir(org_code, create=True)
    if not base:
        return None
    return os.path.join(base, ".token_cache.json")

def _load_cached_token(client_id, region, org_code=None):
    key = _cache_key(client_id, region)
    now = time.time()
    with _cache_lock:
        # Prune expired entries opportunistically.
        expired = [k for k, v in _mem_cache.items() if v.get("expires_at", 0) <= (now + 60)]
        for k in expired:
            _mem_cache.pop(k, None)
        entry = _mem_cache.get(key)
        if entry and entry.get("expires_at", 0) > (now + 60):
            return entry
    path = _org_token_cache_path(org_code)
    if not path or not os.path.exists(path):
        return None
    try:
        with open(path, "rb") as f:
            raw = f.read()
        legacy_plaintext = raw.lstrip().startswith(b"{")
        entry = _decrypt_cache_payload(raw)
        if not isinstance(entry, dict):
            return None
        if entry.get("expires_at", 0) > (now + 60):
            with _cache_lock:
                _mem_cache[key] = entry
            if legacy_plaintext:
                # Opportunistic migration to encrypted on successful read.
                _store_cached_token(client_id, region, entry, org_code=org_code)
            return entry
    except Exception:
        return None
    return None

def _store_cached_token(client_id, region, entry, org_code=None):
    key = _cache_key(client_id, region)
    with _cache_lock:
        _mem_cache[key] = entry
        if len(_mem_cache) > _MAX_MEM_CACHE_ENTRIES:
            # Keep most recent-ish entries by expiry.
            oldest = sorted(_mem_cache.items(), key=lambda kv: kv[1].get("expires_at", 0))[:len(_mem_cache) - _MAX_MEM_CACHE_ENTRIES]
            for k, _ in oldest:
                _mem_cache.pop(k, None)
    path = _org_token_cache_path(org_code)
    if not path:
        return
    try:
        enc = _encrypt_cache_payload(entry)
        with open(path, "wb") as f:
            f.write(enc)
        try:
            os.chmod(path, 0o600)
        except Exception:
            pass
    except Exception:
        pass

def authenticate(client_id, client_secret, region='mypurecloud.ie', org_code=None):
    """
    Authenticates with Genesys Cloud using Client Credentials via direct HTTP request.
    Returns: (access_token, error_message)
    """
    if not client_id or not client_secret:
        return None, "Missing credentials"

    try:
        safe_org = _safe_org_code(org_code)
    except ValueError as exc:
        return None, str(exc)

    cached = _load_cached_token(client_id, region, org_code=safe_org)
    if cached:
        return {
            "access_token": cached["access_token"],
            "region": cached["region"],
            "api_host": cached["api_host"]
        }, None

    # Set login host based on region
    login_host = f"https://login.{region}"
    token_url = f"{login_host}/oauth/token"
    
    try:
        response = requests.post(
            token_url,
            data={"grant_type": "client_credentials"},
            auth=(client_id, client_secret),
            timeout=10
        )
        
        if response.status_code == 200:
            token_data = response.json()
            # We return a dict that simulates an api_client with token and region info
            token_entry = {
                "access_token": token_data['access_token'],
                "region": region,
                "api_host": f"https://api.{region}",
                "expires_at": time.time() + int(token_data.get("expires_in", 3600))
            }
            _store_cached_token(client_id, region, token_entry, org_code=safe_org)
            return {
                "access_token": token_entry["access_token"],
                "region": token_entry["region"],
                "api_host": token_entry["api_host"]
            }, None
        else:
            return None, f"Auth failed ({response.status_code}): {response.text}"
            
    except Exception as e:
        return None, f"Connection error: {str(e)}"

def check_connection():
    # Simple check to see if token is valid if needed
    pass

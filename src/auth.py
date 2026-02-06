import json
import os
import threading
import time
import requests
import streamlit as st

_cache_lock = threading.Lock()
_mem_cache = {}

def _cache_key(client_id, region):
    return f"{client_id}:{region}"

def _org_token_cache_path(org_code):
    if not org_code:
        return None
    base = os.path.join("orgs", org_code)
    os.makedirs(base, exist_ok=True)
    return os.path.join(base, ".token_cache.json")

def _load_cached_token(client_id, region, org_code=None):
    key = _cache_key(client_id, region)
    now = time.time()
    with _cache_lock:
        entry = _mem_cache.get(key)
        if entry and entry.get("expires_at", 0) > (now + 60):
            return entry
    path = _org_token_cache_path(org_code)
    if not path or not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            entry = json.load(f)
        if entry.get("expires_at", 0) > (now + 60):
            with _cache_lock:
                _mem_cache[key] = entry
            return entry
    except Exception:
        return None
    return None

def _store_cached_token(client_id, region, entry, org_code=None):
    key = _cache_key(client_id, region)
    with _cache_lock:
        _mem_cache[key] = entry
    path = _org_token_cache_path(org_code)
    if not path:
        return
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(entry, f, ensure_ascii=False)
    except Exception:
        pass

def authenticate(client_id, client_secret, region='mypurecloud.ie', org_code=None):
    """
    Authenticates with Genesys Cloud using Client Credentials via direct HTTP request.
    Returns: (access_token, error_message)
    """
    if not client_id or not client_secret:
        return None, "Missing credentials"

    cached = _load_cached_token(client_id, region, org_code=org_code)
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
            _store_cached_token(client_id, region, token_entry, org_code=org_code)
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

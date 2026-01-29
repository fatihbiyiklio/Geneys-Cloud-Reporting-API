import requests
import streamlit as st

def authenticate(client_id, client_secret, region='mypurecloud.ie'):
    """
    Authenticates with Genesys Cloud using Client Credentials via direct HTTP request.
    Returns: (access_token, error_message)
    """
    if not client_id or not client_secret:
        return None, "Missing credentials"

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
            return {
                "access_token": token_data['access_token'],
                "region": region,
                "api_host": f"https://api.{region}"
            }, None
        else:
            return None, f"Auth failed ({response.status_code}): {response.text}"
            
    except Exception as e:
        return None, f"Connection error: {str(e)}"

def check_connection():
    # Simple check to see if token is valid if needed
    pass

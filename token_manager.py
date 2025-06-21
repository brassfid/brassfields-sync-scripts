import requests
import json
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

CLIENT_ID = os.getenv("LIGHTSPEED_CLIENT_ID")
CLIENT_SECRET = os.getenv("LIGHTSPEED_CLIENT_SECRET")
TOKEN_URL = os.getenv("LIGHTSPEED_TOKEN_URL")
TOKEN_FILE = os.path.join(os.path.dirname(__file__), "token_data.json")

def load_token_data():
    if os.path.exists(TOKEN_FILE):
        with open(TOKEN_FILE, "r") as f:
            return json.load(f)
    raise FileNotFoundError(f"❌ {TOKEN_FILE} not found. Run exchange_token.py first.")

def save_token_data(token_data):
    with open(TOKEN_FILE, "w") as f:
        json.dump(token_data, f, indent=2)
    print(f"💾 Saved refreshed tokens to {TOKEN_FILE}")

def refresh_token_if_needed(token_data):
    # If token_data doesn't have expires_at, add it now
    if "expires_at" not in token_data and "expires_in" in token_data:
        token_data["expires_at"] = (
            datetime.utcnow() + timedelta(seconds=token_data["expires_in"])
        ).strftime("%Y-%m-%dT%H:%M:%S")
        save_token_data(token_data)

    expires_at_str = token_data.get("expires_at")
    if not expires_at_str:
        raise Exception("❌ No expires_at in token_data.json. Please refresh manually.")

    expires_at = datetime.strptime(expires_at_str, "%Y-%m-%dT%H:%M:%S")
    if datetime.utcnow() < expires_at:
        return token_data["access_token"]

    print("🔄 Token expired. Refreshing...")

    payload = {
        "grant_type": "refresh_token",
        "refresh_token": token_data["refresh_token"],
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET
    }

    headers = {
        "Content-Type": "application/x-www-form-urlencoded"
    }

    response = requests.post(TOKEN_URL, data=payload, headers=headers)
    print("🌐 Token refresh response:", response.status_code)

    if response.status_code != 200 or "application/json" not in response.headers.get("Content-Type", ""):
        raise Exception(f"❌ Token refresh failed: {response.text[:500]}")

    new_data = response.json()
    if "access_token" not in new_data:
        raise Exception("❌ No access_token in response.")

    new_data["refresh_token"] = new_data.get("refresh_token", token_data["refresh_token"])
    new_data["expires_at"] = (
        datetime.utcnow() + timedelta(seconds=new_data["expires_in"])
    ).strftime("%Y-%m-%dT%H:%M:%S")

    save_token_data(new_data)
    print("✅ Token refreshed successfully.")
    return new_data["access_token"]

def get_access_token():
    token_data = load_token_data()
    return refresh_token_if_needed(token_data)

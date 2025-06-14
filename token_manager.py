import requests
import json
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Load .env variables
load_dotenv()

# === Configuration (securely loaded from environment) ===
CLIENT_ID = os.getenv("LIGHTSPEED_CLIENT_ID")
CLIENT_SECRET = os.getenv("LIGHTSPEED_CLIENT_SECRET")
TOKEN_URL = os.getenv("LIGHTSPEED_TOKEN_URL")
TOKEN_FILE = os.path.join(os.path.dirname(__file__), "token_data.json")


def save_token_data(token_data):
    with open(TOKEN_FILE, "w") as f:
        json.dump(token_data, f, indent=2)
    print(f"💾 Saved refreshed tokens to {TOKEN_FILE}")


def load_token_data():
    if os.path.exists(TOKEN_FILE):
        with open(TOKEN_FILE, "r") as f:
            return json.load(f)
    raise FileNotFoundError(f"❌ {TOKEN_FILE} not found. Run exchange_token.py first.")


def refresh_token():
    token_data = load_token_data()
    refresh_token = token_data.get("refresh_token")

    if not refresh_token:
        raise Exception("❌ No refresh_token found in token_data.json. Please run exchange_token.py.")

    print("\n🔄 Refreshing access token using refresh_token:", refresh_token[:8] + "..." if refresh_token else "[None]")

    headers = {
        "Content-Type": "application/x-www-form-urlencoded"
    }

    payload = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET
    }

    response = requests.post(TOKEN_URL, data=payload, headers=headers, allow_redirects=False)
    print("🌐 POST", TOKEN_URL)
    print("📥 Status Code:", response.status_code)
    print("📋 Content-Type:", response.headers.get("Content-Type", ""))

    if "application/json" not in response.headers.get("Content-Type", ""):
        print("⚠️ Unexpected response (not JSON):")
        print(response.text[:500])
        raise Exception("❌ Token refresh failed. Non-JSON response.")

    token_data = response.json()

    if "access_token" not in token_data:
        print("⚠️ Lightspeed did not return an access_token.")
        print("🔎 Full response:", token_data)
        raise Exception("❌ Invalid response: no access_token.")

    # Calculate new expiration time
    expires_in = token_data.get("expires_in", 3600)
    token_data["expires_at"] = (
        datetime.utcnow() + timedelta(seconds=expires_in)
    ).strftime("%Y-%m-%dT%H:%M:%S")

    save_token_data(token_data)
    print("✅ Token refreshed successfully.")
    return token_data["access_token"]


def get_access_token():
    """
    Simple getter that returns the current access_token from file without refreshing.
    """
    token_data = load_token_data()
    access_token = token_data.get("access_token")

    if not access_token:
        raise Exception("❌ No access_token found in token_data.json. Run refresh_token.py first.")

    return access_token

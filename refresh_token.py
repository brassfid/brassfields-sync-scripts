from token_manager import refresh_token
import os
import traceback

if __name__ == "__main__":
    print("👋 Token refresh script started")

    try:
        token = refresh_token()
        print("✅ Refreshed token:", token)
    except Exception as e:
        print("❌ Exception occurred while refreshing token:")
        traceback.print_exc()

    token_path = os.path.join(os.path.dirname(__file__), "token_data.json")
    print("📂 Checked token file at:", token_path)

    if os.path.exists(token_path):
        with open(token_path, "r") as f:
            print("📄 Token file contents:")
            print(f.read())
    else:
        print("⚠️ Token file does not exist at expected location.")

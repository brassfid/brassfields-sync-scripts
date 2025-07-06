import requests
import mysql.connector
from token_manager import get_access_token
from db_config import DB_CONFIG

# Fetch token and set headers
access_token = get_access_token()
headers = {
    "Authorization": f"Bearer {access_token}",
    "Accept": "application/json"
}

# Fetch tags from Lightspeed
print("🔄 Fetching tags from Lightspeed...")
resp = requests.get("https://brassfields.retail.lightspeed.app/api/2.0/tags", headers=headers)
if resp.status_code != 200:
    print(f"❌ Error fetching tags: {resp.text}")
    exit()

tags = resp.json().get("data", [])
print(f"Found {len(tags)} tags.")

# Insert into database
conn = mysql.connector.connect(**DB_CONFIG)
cursor = conn.cursor()

for tag in tags:
    tag_id = tag["id"]
    tag_name = tag["name"]
    cursor.execute("""
        INSERT INTO tags (id, name)
        VALUES (%s, %s)
        ON DUPLICATE KEY UPDATE name = VALUES(name)
    """, (tag_id, tag_name))

conn.commit()
cursor.close()
conn.close()
print("✅ Tags table updated.")

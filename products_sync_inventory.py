import requests
import mysql.connector
from db_config import DB_CONFIG
from token_manager import get_access_token

# === Auth ===
print("🔐 Getting access token...")
access_token = get_access_token()

headers = {
    "Authorization": f"Bearer {access_token}",
    "Accept": "application/json"
}

# === Connect to DB ===
conn = mysql.connector.connect(**DB_CONFIG)
cursor = conn.cursor()

# === Step: Pull ALL inventory records from Lightspeed ===
print("📦 Fetching full inventory from Lightspeed...")
offset = 0
inserted = 0

while True:
    url = f"https://brassfields.retail.lightspeed.app/api/2.0/inventory?limit=1000&offset={offset}"
    response = requests.get(url, headers=headers, timeout=15)

    if response.status_code != 200:
        print(f"❌ Request failed at offset {offset}: {response.status_code}")
        break

    data = response.json().get("data", [])
    if not data:
        print("✅ Done fetching inventory records.")
        break

    for i, item in enumerate(data):
        product_id = item.get("product_id")
        current_amount = item.get("current_amount")

        if product_id is None or current_amount is None:
            continue

        try:
            cursor.execute("""
                INSERT INTO inventory_cache (product_id, current_amount)
                VALUES (%s, %s)
                ON DUPLICATE KEY UPDATE current_amount = VALUES(current_amount)
            """, (product_id, current_amount))
            inserted += 1

            if offset == 0 and i < 5:
                print(f"🧪 {i}: {product_id}, current_amount = {current_amount}")
        except Exception as e:
            print(f"⚠️ DB error for {product_id}: {e}")

    conn.commit()
    offset += 1000
    print(f"🔁 Offset {offset} processed. Total inserted/updated: {inserted}")

# === Done ===
print(f"✅ Inventory cache completed. Total products processed: {inserted}")
cursor.close()
conn.close()

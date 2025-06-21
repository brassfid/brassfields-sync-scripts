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

# === Sync Inventory to inventory_cache ===
print("📦 Syncing inventory to inventory_cache table...")
offset = 0
page_size = 1000
inserted = 0

while True:
    url = f"https://brassfields.retail.lightspeed.app/api/2.0/inventory?limit={page_size}&offset={offset}"
    response = requests.get(url, headers=headers, timeout=15)

    if response.status_code != 200:
        print(f"❌ Request failed: {response.status_code} {response.text}")
        break

    data = response.json().get("data", [])
    if not data:
        print("🚫 No more inventory data.")
        break

    print(f"🔎 Retrieved {len(data)} records at offset {offset}")

    for i, item in enumerate(data):
        product_id = item.get("product_id")
        outlet_id = item.get("outlet_id")
        current_amount = item.get("current_amount")

        if product_id is None or current_amount is None:
            continue

        if i < 5 and offset == 0:
            print(f"🧪 {i}: product_id={product_id}, outlet_id={outlet_id}, current_amount={current_amount}")

        try:
            cursor.execute("""
                INSERT INTO inventory_cache (product_id, outlet_id, current_amount)
                VALUES (%s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    current_amount = VALUES(current_amount),
                    outlet_id = VALUES(outlet_id),
                    last_updated = CURRENT_TIMESTAMP
            """, (product_id, outlet_id, current_amount))
            inserted += 1
        except Exception as e:
            print(f"⚠️ Error inserting {product_id}: {e}")

    conn.commit()
    offset += page_size

print(f"\n✅ Inventory caching complete. Total inserted or updated: {inserted}")
cursor.close()
conn.close()

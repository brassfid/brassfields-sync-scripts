import requests
import mysql.connector
import json
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
def sync_inventory_to_cache():
    print("📦 Syncing inventory to inventory_cache table...")
    inventory_url = "https://brassfields.retail.lightspeed.app/api/2.0/inventory"
    offset = 0
    total_inserted = 0

    while True:
        paged_url = f"{inventory_url}?limit=1000&offset={offset}"
        response = requests.get(paged_url, headers=headers, timeout=15)

        if response.status_code != 200:
            print(f"❌ Request failed: {response.status_code} {response.text}")
            break

        data = response.json().get("data", [])
        if not data:
            print("✅ No more inventory data.")
            break

        for i, item in enumerate(data):
            product_id = item.get("product_id")
            count = item.get("current_amount")

            if i < 5:
                print(f"🧪 Inventory record {i}: product_id={product_id}, current_amount={count}")
                print(json.dumps(item, indent=2))

            if product_id is None or count is None:
                continue

            try:
                cursor.execute("""
                    INSERT INTO inventory_cache (product_id, current_amount)
                    VALUES (%s, %s)
                    ON DUPLICATE KEY UPDATE
                        current_amount = VALUES(current_amount),
                        last_updated = CURRENT_TIMESTAMP
                """, (product_id, count))
                total_inserted += 1
            except Exception as e:
                print(f"⚠️ Failed to insert product_id {product_id}: {e}")

        conn.commit()
        offset += 1000
        print(f"🔁 Offset {offset} processed, total inserted/updated: {total_inserted}")

    print(f"✅ Inventory caching complete. Total inserted/updated: {total_inserted}")

# === Run ===
sync_inventory_to_cache()

cursor.close()
conn.close()

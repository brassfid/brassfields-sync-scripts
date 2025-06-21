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

# === Sync Inventory to products ===
def sync_inventory_to_products():
    print("📦 Syncing inventory to products table...")
    inventory_url = "https://brassfields.retail.lightspeed.app/api/2.0/inventory"
    offset = 0
    total_updated = 0

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

        updates = []

        for i, item in enumerate(data):
            product_id = item.get("product_id")
            count = item.get("current_amount")

            if i < 5:
                print(f"🧪 Inventory record {i}: product_id={product_id}, current_amount={count}")
                print(json.dumps(item, indent=2))

            if product_id is not None and count is not None:
                updates.append((count, product_id))

        if updates:
            try:
                cursor.executemany(
                    "UPDATE products SET inventory_count = %s WHERE product_id = %s",
                    updates
                )
                conn.commit()
                total_updated += cursor.rowcount
            except Exception as e:
                print(f"⚠️ Failed batch update: {e}")

        offset += 1000
        print(f"🔁 Offset {offset} processed, total updated: {total_updated}")

    print(f"✅ Inventory sync complete. Total updated: {total_updated}")

# === Run ===
sync_inventory_to_products()

cursor.close()
conn.close()

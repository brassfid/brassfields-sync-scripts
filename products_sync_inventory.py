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

# === Connect to Database ===
conn = mysql.connector.connect(**DB_CONFIG)
cursor = conn.cursor()

def sync_inventory():
    print("📦 Syncing inventory to product_lines table...")
    url = "https://brassfields.retail.lightspeed.app/api/2.0/inventory"
    offset = 0
    total_updated = 0

    while True:
        paged_url = f"{url}?limit=1000&offset={offset}"
        response = requests.get(paged_url, headers=headers, timeout=15)

        if response.status_code != 200:
            print(f"❌ Error {response.status_code}: {response.text}")
            break

        inventory_data = response.json().get("data", [])
        if not inventory_data:
            print("✅ All inventory records processed.")
            break

        for item in inventory_data:
            product_id = item.get("product_id")
            count = item.get("count")

            if product_id is None or count is None:
                continue

            try:
                cursor.execute("""
                    UPDATE product_lines
                    SET inventory_count = %s
                    WHERE product_id = %s
                """, (count, product_id))
                total_updated += cursor.rowcount
            except Exception as e:
                print(f"⚠️ Error updating product_id {product_id}: {e}")

        conn.commit()
        offset += 1000
        print(f"🔁 Offset {offset} processed, total updated: {total_updated}")

    print(f"✅ Inventory sync finished. Total rows updated: {total_updated}")

# === Run ===
sync_inventory()
cursor.close()
conn.close()

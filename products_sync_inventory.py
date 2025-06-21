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

# === Get all product IDs in our database ===
cursor.execute("SELECT id FROM products")
existing_product_ids = set(row[0] for row in cursor.fetchall())

# === Sync Inventory ===
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

        for item in data:
            product_id = item.get("product_id")
            count = item.get("current_amount")

            if product_id in existing_product_ids and count is not None and count != 0:
                try:
                    cursor.execute(
                        "UPDATE products SET inventory_count = %s WHERE id = %s",
                        (count, product_id)
                    )
                    total_updated += cursor.rowcount
                except Exception as e:
                    print(f"⚠️ Failed to update product_id {product_id}: {e}")

        conn.commit()
        offset += 1000
        print(f"🔁 Offset {offset} processed, total updated: {total_updated}")

    print(f"✅ Inventory sync complete. Total updated: {total_updated}")

# === Run ===
sync_inventory_to_products()
cursor.close()
conn.close()

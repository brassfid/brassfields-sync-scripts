import requests
import mysql.connector
from db_config import DB_CONFIG
from token_manager import get_access_token

# === Auth ===
print("🔐 Fetching access token...")
access_token = get_access_token()
print(f"✅ Using token: {access_token[:10]}...")

headers = {
    "Authorization": f"Bearer {access_token}",
    "Accept": "application/json"
}

# === Connect to DB ===
conn = mysql.connector.connect(**DB_CONFIG)
cursor = conn.cursor()

# === Fetch Inventory Data ===
def fetch_inventory():
    print("📦 Fetching inventory data...")
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
            if offset == 0 and total_updated < 1:
            print("🧪 Sample inventory record:", item)    
            count = item.get("count")


            if product_id is None or count is None:
                continue

            try:
                cursor.execute(
                    "UPDATE products SET inventory_count = %s WHERE id = %s",
                    (count, product_id)
                )
                total_updated += 1
            except Exception as e:
                print(f"⚠️ Failed to update product {product_id}: {e}")

        conn.commit()
        offset += 1000
        print(f"🔁 Processed offset {offset}, total updated so far: {total_updated}")

    print(f"✅ Inventory update complete. Total records updated: {total_updated}")

# === Run ===
fetch_inventory()

cursor.close()
conn.close()

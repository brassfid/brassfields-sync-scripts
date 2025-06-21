import requests
import mysql.connector
from db_config import DB_CONFIG
from token_manager import get_access_token
import time

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

# === Fetch product IDs ===
cursor.execute("SELECT id FROM products")
product_ids = [row[0] for row in cursor.fetchall()]

print(f"📦 Syncing inventory for {len(product_ids)} products...")

updated = 0

for i, product_id in enumerate(product_ids):
    url = f"https://brassfields.retail.lightspeed.app/api/2.0/inventory?filter=product_id=={product_id}"
    try:
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code != 200:
            print(f"⚠️ {i}: Failed to fetch inventory for {product_id} - {response.status_code}")
            continue

        data = response.json().get("data", [])
        if not data:
            continue

        # Sum current_amounts across all outlets (if any)
        total_amount = sum(int(record.get("current_amount", 0)) for record in data)

        cursor.execute(
            "UPDATE products SET inventory_count = %s WHERE id = %s",
            (total_amount, product_id)
        )
        updated += cursor.rowcount

        if i < 5:
            print(f"🧪 {i}: product_id={product_id}, inventory={total_amount}")

        # Be polite to API
        time.sleep(0.1)

    except Exception as e:
        print(f"❌ {i}: Error for product_id {product_id} - {e}")

conn.commit()
cursor.close()
conn.close()

print(f"✅ Inventory sync complete. Total products updated: {updated}")

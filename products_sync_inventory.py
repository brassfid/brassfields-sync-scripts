import requests
import mysql.connector
from db_config import DB_CONFIG
from token_manager import get_access_token

print("🔐 Getting access token...")
access_token = get_access_token()

headers = {
    "Authorization": f"Bearer {access_token}",
    "Accept": "application/json"
}

# === Connect to DB ===
conn = mysql.connector.connect(**DB_CONFIG)
cursor = conn.cursor()

# === Get all product_ids from your table ===
cursor.execute("SELECT id FROM products")
product_ids = [row[0] for row in cursor.fetchall()]

print(f"📦 Syncing inventory for {len(product_ids)} products...\n")
updated = 0

for i, product_id in enumerate(product_ids):
    url = f"https://brassfields.retail.lightspeed.app/api/2.0/inventory?product_id={product_id}"
    response = requests.get(url, headers=headers, timeout=10)

    if response.status_code != 200:
        print(f"⚠️ Error for {product_id}: {response.status_code}")
        continue

    data = response.json().get("data", [])
    if not data:
        continue

    # Get current_amount for this product (assuming one outlet for now)
    current_amount = data[0].get("current_amount")

    if current_amount is not None:
        try:
            cursor.execute(
                "UPDATE products SET inventory_count = %s WHERE id = %s",
                (current_amount, product_id)
            )
            updated += cursor.rowcount
            if i < 5:
                print(f"🧪 {i}: product_id={product_id}, inventory={current_amount}")
        except Exception as e:
            print(f"❌ Failed to update product_id {product_id}: {e}")

conn.commit()
cursor.close()
conn.close()

print(f"\n✅ Done. Total updated: {updated}")

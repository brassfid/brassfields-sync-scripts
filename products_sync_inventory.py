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

# === Get product IDs from products table ===
cursor.execute("SELECT id FROM products")
product_ids = [row[0] for row in cursor.fetchall()]
print(f"📦 Checking inventory for {len(product_ids)} products...\n")

updated = 0

for i, product_id in enumerate(product_ids):
    url = f"https://brassfields.retail.lightspeed.app/api/2.0/products/{product_id}/inventory"
    try:
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code != 200:
            print(f"⚠️ Error for {product_id}: {response.status_code}")
            continue

        data = response.json().get("data", [])
        if not data:
            continue

        # Loop through outlets (should usually be 1 for your case)
        for entry in data:
            current_amount = entry.get("current_amount")
            outlet_id = entry.get("outlet_id")

            if current_amount is not None:
                cursor.execute("""
                    UPDATE products
                    SET inventory_count = %s
                    WHERE id = %s
                """, (current_amount, product_id))
                updated += 1
                print(f"🧪 {i}: Updated {product_id} (Outlet: {outlet_id}) → inventory_count = {current_amount}")

        time.sleep(0.25)  # Prevent rate limiting

    except Exception as e:
        print(f"❌ Failed for {product_id}: {e}")

conn.commit()
cursor.close()
conn.close()

print(f"\n✅ Done. Updated inventory_count for {updated} products.")

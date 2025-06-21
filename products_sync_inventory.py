import requests
import mysql.connector
from db_config import DB_CONFIG
from token_manager import get_access_token

# === Step 1: Get access token ===
print("🔐 Getting access token...")
access_token = get_access_token()

headers = {
    "Authorization": f"Bearer {access_token}",
    "Accept": "application/json"
}

# === Step 2: Connect to DB and fetch product IDs ===
conn = mysql.connector.connect(**DB_CONFIG)
cursor = conn.cursor()
cursor.execute("SELECT id FROM products")
product_ids = [row[0] for row in cursor.fetchall()]
print(f"📦 Checking inventory for {len(product_ids)} products...\n")

# === Step 3: Loop and fetch inventory per product ===
inserted = 0

for i, product_id in enumerate(product_ids):
    url = f"https://brassfields.retail.lightspeed.app/api/2.0/products/{product_id}/inventory"

    try:
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code != 200:
            print(f"⚠️ Failed for {product_id}: HTTP {response.status_code}")
            continue

        data = response.json().get("data", [])
        if not data:
            continue

        # Loop through all outlet-specific inventory entries
        for item in data:
            outlet_id = item.get("outlet_id")
            current_amount = item.get("current_amount")

            if product_id and outlet_id and current_amount is not None:
                cursor.execute("""
                    INSERT INTO inventory_cache (product_id, outlet_id, current_amount)
                    VALUES (%s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        current_amount = VALUES(current_amount),
                        last_updated = CURRENT_TIMESTAMP
                """, (product_id, outlet_id, current_amount))
                inserted += 1

        if i < 5:
            print(f"🧪 {i}: product_id={product_id}, outlets={len(data)}")

    except Exception as e:
        print(f"❌ Error for {product_id}: {e}")

# === Finalize ===
conn.commit()
cursor.close()
conn.close()
print(f"\n✅ Inventory cache updated for {inserted} records.")

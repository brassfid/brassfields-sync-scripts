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

# === Ensure inventory_cache table exists ===
cursor.execute("""
    CREATE TABLE IF NOT EXISTS inventory_cache (
        product_id VARCHAR(255) PRIMARY KEY,
        current_amount INT,
        last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    )
""")

# === Get product IDs from products table ===
cursor.execute("SELECT id FROM products")
product_ids = [row[0] for row in cursor.fetchall()]
print(f"📦 Fetching inventory for {len(product_ids)} products...\n")

# === Step 1: Cache inventory in inventory_cache ===
inserted = 0
for i, product_id in enumerate(product_ids):
    url = f"https://brassfields.retail.lightspeed.app/api/2.0/inventory?product_id={product_id}"
    try:
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code != 200:
            print(f"⚠️ Error for {product_id}: {response.status_code}")
            continue

        data = response.json().get("data", [])
        if not data:
            continue

        current_amount = data[0].get("current_amount")

        if current_amount is not None:
            cursor.execute("""
                INSERT INTO inventory_cache (product_id, current_amount)
                VALUES (%s, %s)
                ON DUPLICATE KEY UPDATE current_amount = VALUES(current_amount), last_updated = CURRENT_TIMESTAMP
            """, (product_id, current_amount))
            inserted += 1
            if i < 5:
                print(f"🧪 {i}: {product_id}, current_amount = {current_amount}")

    except Exception as e:
        print(f"❌ Failed for {product_id}: {e}")

conn.commit()
print(f"\n✅ Inventory cached for {inserted} products.")

# === Step 2: Sync inventory from inventory_cache to products ===
print("🔁 Updating products table from inventory_cache...")

cursor.execute("""
    UPDATE products p
    JOIN inventory_cache ic ON p.id = ic.product_id
    SET p.inventory_count = ic.current_amount
""")

conn.commit()
print("✅ Products table updated.")

# === Clean up ===
cursor.close()
conn.close()

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

# === Get all product IDs from the products table ===
cursor.execute("SELECT id FROM products")
product_ids = [row[0] for row in cursor.fetchall()]
print(f"📦 Checking inventory for {len(product_ids)} products...\n")

inserted = 0

for i, product_id in enumerate(product_ids, start=1):
    url = f"https://brassfields.retail.lightspeed.app/api/2.0/products/{product_id}/inventory"
    try:
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code != 200:
            print(f"⚠️ {i}: Failed for {product_id} — Status Code {response.status_code}")
            continue

        data = response.json().get("data", [])
        if not data:
            print(f"⚠️ {i}: No inventory data for {product_id}")
            continue

        for item in data:
            outlet_id = item.get("outlet_id")
            current_amount = item.get("current_amount")

            if outlet_id and current_amount is not None:
                cursor.execute("""
                    INSERT INTO inventory_cache (product_id, outlet_id, current_amount)
                    VALUES (%s, %s, %s)
                    ON DUPLICATE KEY UPDATE current_amount = VALUES(current_amount), last_updated = CURRENT_TIMESTAMP
                """, (product_id, outlet_id, current_amount))

                inserted += 1
                print(f"✅ {i}: product_id={product_id}, outlet_id={outlet_id}, current_amount={current_amount}")
            else:
                print(f"⚠️ {i}: Incomplete data for product {product_id}")

    except Exception as e:
        print(f"❌ {i}: Error for {product_id}: {e}")

conn.commit()
cursor.close()
conn.close()

print(f"\n✅ Finished syncing inventory. Rows inserted or updated: {inserted}")

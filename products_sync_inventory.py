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

# === Sync inventory from offset 1000 ===
print("📦 Syncing inventory from offset 1000 to inventory_cache table...")

inserted = 0
seen = set()

url = "https://brassfields.retail.lightspeed.app/api/2.0/inventory?limit=1000&offset=1000"
response = requests.get(url, headers=headers, timeout=15)

if response.status_code != 200:
    print(f"❌ Request failed: {response.status_code} {response.text}")
else:
    data = response.json().get("data", [])
    print(f"🔎 Retrieved {len(data)} records from offset 1000")

    for i, item in enumerate(data):
        product_id = item.get("product_id")
        outlet_id = item.get("outlet_id")
        current_amount = item.get("current_amount")

        if product_id and outlet_id and current_amount is not None:
            key = (product_id, outlet_id)
            if key in seen:
                continue
            seen.add(key)

            if i < 5:
                print(f"🧪 {i}: product_id={product_id}, outlet_id={outlet_id}, current_amount={current_amount}")

            try:
                cursor.execute("""
                    INSERT INTO inventory_cache (product_id, outlet_id, current_amount)
                    VALUES (%s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        current_amount = VALUES(current_amount),
                        last_updated = CURRENT_TIMESTAMP
                """, (product_id, outlet_id, current_amount))
                inserted += 1
            except Exception as e:
                print(f"⚠️ Error inserting {product_id}: {e}")

    conn.commit()

print(f"\n✅ Done. Total inserted or updated from offset 1000: {inserted}")
cursor.close()
conn.close()

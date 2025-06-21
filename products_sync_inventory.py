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

print("📦 Syncing inventory to inventory_cache table...\n")

offset = 0
limit = 1000
seen_keys = set()
total_updated = 0

while True:
    url = f"https://brassfields.retail.lightspeed.app/api/2.0/inventory?limit={limit}&offset={offset}"
    response = requests.get(url, headers=headers, timeout=15)

    if response.status_code != 200:
        print(f"❌ Request failed at offset {offset}: {response.status_code} {response.text}")
        break

    data = response.json().get("data", [])
    print(f"🔎 Retrieved {len(data)} records at offset {offset}")

    if not data:
        break

    new_records = 0

    for i, item in enumerate(data):
        product_id = item.get("product_id")
        outlet_id = item.get("outlet_id")
        current_amount = item.get("current_amount")

        key = (product_id, outlet_id)
        if key in seen_keys:
            continue
        seen_keys.add(key)

        if product_id and outlet_id is not None:
            try:
                cursor.execute("""
                    INSERT INTO inventory_cache (product_id, outlet_id, current_amount)
                    VALUES (%s, %s, %s)
                    ON DUPLICATE KEY UPDATE current_amount = VALUES(current_amount)
                """, (product_id, outlet_id, current_amount))
                total_updated += cursor.rowcount
                new_records += 1
                if i < 5 and offset == 0:
                    print(f"🧪 {i}: product_id={product_id}, outlet_id={outlet_id}, current_amount={current_amount}")
            except Exception as e:
                print(f"⚠️ Failed to insert {product_id} @ {outlet_id}: {e}")

    conn.commit()
    offset += limit

    if new_records == 0:
        print("🚫 No new unique products found — ending pagination.\n")
        break

print(f"✅ Inventory caching complete. Total inserted or updated: {total_updated}\n")

cursor.close()
conn.close()

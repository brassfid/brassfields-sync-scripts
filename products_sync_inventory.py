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

# === Log start ===
print("📦 Syncing inventory to inventory_cache table...")

# === Fetch and insert inventory data ===
inventory_url = "https://brassfields.retail.lightspeed.app/api/2.0/inventory"
offset = 0
limit = 1000
total_updated = 0

while True:
    paged_url = f"{inventory_url}?limit={limit}&offset={offset}"
    response = requests.get(paged_url, headers=headers, timeout=20)

    if response.status_code != 200:
        print(f"❌ API error at offset {offset}: {response.status_code} {response.text[:100]}")
        break

    data = response.json().get("data", [])
    print(f"🔎 Retrieved {len(data)} records at offset {offset}")
    
    if not data:
        print("✅ No more data. Ending pagination.")
        break

    for i, item in enumerate(data):
        product_id = item.get("product_id")
        outlet_id = item.get("outlet_id")
        current_amount = item.get("current_amount")

        if not product_id or current_amount is None:
            continue

        if i < 5:
            print(f"🧪 {i}: product_id={product_id}, outlet_id={outlet_id}, current_amount={current_amount}")

        try:
            cursor.execute("""
                INSERT INTO inventory_cache (product_id, outlet_id, current_amount)
                VALUES (%s, %s, %s)
                ON DUPLICATE KEY UPDATE current_amount = VALUES(current_amount), last_updated = CURRENT_TIMESTAMP
            """, (product_id, outlet_id, current_amount))
            total_updated += cursor.rowcount
        except Exception as e:
            print(f"⚠️ Failed to insert/update {product_id}: {e}")

    conn.commit()
    offset += limit

print(f"✅ Inventory caching complete. Total inserted or updated: {total_updated}")
cursor.close()
conn.close()

import requests
import mysql.connector
from db_config import DB_CONFIG
from token_manager import get_access_token

# === Get access token ===
print("🔐 Getting access token...")
access_token = get_access_token()

# === API setup ===
url = "https://brassfields.retail.lightspeed.app/api/2.0/inventory?after=2001"
headers = {
    "Authorization": f"Bearer {access_token}",
    "Accept": "application/json"
}

# === DB connection ===
conn = mysql.connector.connect(**DB_CONFIG)
cursor = conn.cursor()

print("📦 Fetching inventory (after=1001) and updating inventory_cache...\n")

response = requests.get(url, headers=headers, timeout=15)

inserted = 0

if response.status_code != 200:
    print(f"❌ API Error {response.status_code}: {response.text}")
else:
    data = response.json().get("data", [])
    print(f"🔎 Retrieved {len(data)} records")

    for i, record in enumerate(data):
        product_id = record.get("product_id")
        outlet_id = record.get("outlet_id")
        current_amount = record.get("current_amount")

        if product_id and outlet_id and current_amount is not None:
            try:
                cursor.execute("""
                    INSERT INTO inventory_cache (product_id, outlet_id, current_amount)
                    VALUES (%s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        current_amount = VALUES(current_amount),
                        last_updated = CURRENT_TIMESTAMP
                """, (product_id, outlet_id, current_amount))
                inserted += 1
                if i < 5:
                    print(f"✅ {i}: product_id={product_id}, outlet_id={outlet_id}, current_amount={current_amount}")
            except Exception as e:
                print(f"⚠️ SQL error inserting record {i}: {e}")
        else:
            print(f"⚠️ Skipping incomplete record {i}: {record}")

    conn.commit()

print(f"\n✅ Done. Total inserted or updated: {inserted}")
cursor.close()
conn.close()

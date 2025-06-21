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

# === Sync inventory to inventory_cache table ===
print("📦 Syncing inventory to inventory_cache table...")

offset = 0
limit = 1000
inserted = 0

while True:
    url = f"https://brassfields.retail.lightspeed.app/api/2.0/inventory?limit={limit}&offset={offset}"
    response = requests.get(url, headers=headers, timeout=15)

    if response.status_code != 200:
        print(f"❌ Failed at offset {offset}: {response.status_code} - {response.text}")
        break

    records = response.json().get("data", [])
    print(f"🔎 Retrieved {len(records)} records at offset {offset}")

    if not records:
        print("🚫 No more records found — ending pagination.\n")
        break

    for i, record in enumerate(records):
        product_id = record.get("product_id")
        outlet_id = record.get("outlet_id")
        current_amount = record.get("current_amount")

        if product_id and outlet_id is not None:
            try:
                cursor.execute(
                    """
                    INSERT INTO inventory_cache (product_id, outlet_id, current_amount)
                    VALUES (%s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        current_amount = VALUES(current_amount),
                        outlet_id = VALUES(outlet_id),
                        last_updated = CURRENT_TIMESTAMP
                    """,
                    (product_id, outlet_id, current_amount)
                )
                inserted += 1
                if i < 5 and offset == 0:
                    print(f"🧪 {i}: product_id={product_id}, outlet_id={outlet_id}, current_amount={current_amount}")
            except Exception as e:
                print(f"⚠️ DB error for product_id={product_id}: {e}")

    conn.commit()
    offset += limit

print(f"\n✅ Inventory sync complete. Total inserted or updated: {inserted}")
cursor.close()
conn.close()

import requests
import mysql.connector
from db_config import DB_CONFIG

# === Direct Access Token ===
access_token = "lsxs_at_sOp7JJlmSfluWAt8UtotRtULjN097RUK"

headers = {
    "accept": "application/json",
    "authorization": f"Bearer {access_token}"
}

# === Connect to DB ===
conn = mysql.connector.connect(**DB_CONFIG)
cursor = conn.cursor()

# === Begin Pagination ===
url = "https://brassfields.retail.lightspeed.app/api/2.0/inventory"
offset = 0
limit = 1000
total_updated = 0

print("📦 Fetching inventory and updating inventory_cache...\n")

while True:
    paged_url = f"{url}?limit={limit}&offset={offset}"
    response = requests.get(paged_url, headers=headers, timeout=20)

    if response.status_code != 200:
        print(f"❌ Error: {response.status_code} - {response.text}")
        break

    data = response.json().get("data", [])
    if not data:
        print("✅ Done: No more inventory data.")
        break

    for i, item in enumerate(data):
        product_id = item.get("product_id")
        current_amount = item.get("current_amount")

        if product_id is None or current_amount is None:
            continue

        try:
            cursor.execute("""
                INSERT INTO inventory_cache (product_id, current_amount)
                VALUES (%s, %s)
                ON DUPLICATE KEY UPDATE current_amount = VALUES(current_amount)
            """, (product_id, current_amount))
            total_updated += 1

            if i < 5 and offset == 0:
                print(f"🧪 {i}: product_id={product_id}, current_amount={current_amount}")

        except Exception as e:
            print(f"⚠️ Failed to update {product_id}: {e}")

    conn.commit()
    offset += limit
    print(f"🔁 Offset {offset} done, total so far: {total_updated}")

print(f"\n✅ inventory_cache updated: {total_updated} total records.")
cursor.close()
conn.close()

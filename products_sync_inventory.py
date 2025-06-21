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

def sync_inventory_to_cache():
    print("📦 Syncing inventory to inventory_cache table...")
    inventory_url = "https://brassfields.retail.lightspeed.app/api/2.0/inventory"
    offset = 0
    total_inserted = 0
    seen_product_ids = set()

    while True:
        paged_url = f"{inventory_url}?limit=1000&offset={offset}"
        response = requests.get(paged_url, headers=headers, timeout=15)

        if response.status_code != 200:
            print(f"❌ Request failed: {response.status_code} {response.text}")
            break

        data = response.json().get("data", [])
        print(f"🔎 Retrieved {len(data)} records at offset {offset}")

        if not data:
            print("✅ No more inventory data.")
            break

        new_product_found = False
        for i, item in enumerate(data):
            product_id = item.get("product_id")
            count = item.get("current_amount")

            if not product_id or product_id in seen_product_ids:
                continue

            seen_product_ids.add(product_id)

            if count is None or count == 0:
                continue

            new_product_found = True

            if i < 10:
                print(f"🧪 {i}: product_id={product_id}, current_amount={count}")

            try:
                cursor.execute("""
                    INSERT INTO inventory_cache (product_id, current_amount)
                    VALUES (%s, %s)
                    ON DUPLICATE KEY UPDATE current_amount = VALUES(current_amount), last_updated = CURRENT_TIMESTAMP
                """, (product_id, count))
                total_inserted += cursor.rowcount
            except Exception as e:
                print(f"❌ DB insert failed for product_id={product_id}: {e}")

        conn.commit()
        offset += 1000

        if not new_product_found:
            print("🚫 No new unique products found — ending pagination.")
            break

    print(f"\n✅ Inventory caching complete. Total inserted or updated: {total_inserted}")

sync_inventory_to_cache()
cursor.close()
conn.close()

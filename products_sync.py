fimport requests
import mysql.connector
from datetime import datetime, timedelta
import time
from token_manager import get_access_token
from db_config import DB_CONFIG

# === Auth ===
print("\U0001f510 Fetching access token...")
access_token = get_access_token()
print(f"✅ Using token: {access_token[:10]}...")

headers = {
    "Authorization": f"Bearer {access_token}",
    "Accept": "application/json"
}

# === Setup ===
yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%dT%H:%M:%SZ')
SEARCH_URL = (
    "https://brassfields.retail.lightspeed.app/api/2.0/search?"
    f"type=products&expand=inventory_levels&filter=updated_at>{yesterday}&order_direction=asc&page_size=1000&offset={{offset}}"
)

conn = mysql.connector.connect(**DB_CONFIG)
cursor = conn.cursor()

# === Backups ===
backup_table_name = f"products_sales_{datetime.now().strftime('%Y%m%d')}"
cursor.execute(f"CREATE TABLE IF NOT EXISTS {backup_table_name} AS SELECT * FROM products")

cutoff_date = datetime.now() - timedelta(days=7)
old_backup_table = f"products_sales_{cutoff_date.strftime('%Y%m%d')}"
cursor.execute(f"DROP TABLE IF EXISTS {old_backup_table}")

# === Helpers ===
def get_product_code(product):
    codes = product.get("product_codes", [])
    for code in codes:
        if code.get("type") == "UPC":
            return code.get("code"), code.get("type")
    return (codes[0].get("code"), codes[0].get("type")) if codes else (None, None)

def get_most_recent_sale(product_id):
    cursor.execute("""
        SELECT sale_date FROM sales_lines
        WHERE product_id = %s
        ORDER BY sale_date DESC
        LIMIT 1
    """, (product_id,))
    result = cursor.fetchone()
    return result[0] if result else None

# === Sync Loop ===
offset = 0
inserted = 0
updated = 0

while True:
    print(f"\U0001f501 Fetching products updated since yesterday (offset={offset})...")
    url = SEARCH_URL.format(offset=offset)
    response = requests.get(url, headers=headers)

    if response.status_code != 200:
        print(f"❌ Error {response.status_code}: {response.text}")
        break

    data = response.json().get("data", [])
    if not data:
        print("✅ No more updated products.")
        break

    for product in data:
        try:
            prod_id = product.get("id")
            name = product.get("name")
            handle = product.get("handle")
            description = product.get("description")
            supply_price = str(product.get("supply_price", ""))
            retail_price = str(product.get("price_including_tax", ""))

            brand = product.get("brand") or {}
            brand_name = brand.get("name") if brand else None

            supplier = product.get("supplier") or {}
            supplier_name = supplier.get("name") if supplier else None

            category = product.get("product_category") or {}
            product_category = category.get("name") if category else None

            tag_ids = product.get("tag_ids", [])
            tags = ",".join(tag_ids)[:250]

            outlet_taxes = product.get("outlet_taxes") or []
            outlet_tax = str(outlet_taxes[0].get("rate")) if outlet_taxes else None

            sku = product.get("sku")
            active_online = str(product.get("ecwid_enabled_webstore", False))
            created_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            last_synced_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            product_code, product_code_type = get_product_code(product)
            most_recent_sale = get_most_recent_sale(prod_id)

            inventory_count = None
            for level in product.get("inventory_levels", []):
                if level.get("outlet_id") == "06e94082-ed4f-11ee-f619-85357b2ae2f0":
                    inventory_count = level.get("count")
                    break

            # Check if product exists
            cursor.execute("SELECT * FROM products WHERE id = %s", (prod_id,))
            existing = cursor.fetchone()
            while cursor.nextset(): pass

            if existing:
                update_query = """
                    UPDATE products SET
                        name=%s, handle=%s, description=%s, supply_price=%s, retail_price=%s,
                        brand_name=%s, supplier_name=%s, product_category=%s, tags=%s,
                        outlet_tax_341_Douglas=%s, sku=%s, active_online=%s,
                        last_synced_at=%s, product_code=%s, product_code_type=%s,
                        most_recent_sale=%s, inventory_count=%s
                    WHERE id=%s
                """
                values = (
                    name, handle, description, supply_price, retail_price, brand_name,
                    supplier_name, product_category, tags, outlet_tax, sku, active_online,
                    last_synced_at, product_code, product_code_type, most_recent_sale,
                    inventory_count, prod_id
                )
                cursor.execute(update_query, values)
                updated += 1
            else:
                insert_query = """
                    INSERT INTO products (
                        id, name, handle, description, supply_price, retail_price, brand_name,
                        supplier_name, product_category, tags, outlet_tax_341_Douglas, sku,
                        active_online, created_at, last_synced_at, product_code, product_code_type,
                        most_recent_sale, inventory_count
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                values = (
                    prod_id, name, handle, description, supply_price, retail_price, brand_name,
                    supplier_name, product_category, tags, outlet_tax, sku, active_online,
                    created_at, last_synced_at, product_code, product_code_type, most_recent_sale,
                    inventory_count
                )
                cursor.execute(insert_query, values)
                inserted += 1
        except Exception as e:
            print(f"⚠️ Skipping product {product.get('id')} due to error: {e}")

    conn.commit()
    offset += 1000
    time.sleep(0.3)

print(f"✅ Sync complete! Inserted: {inserted}, Updated: {updated}")
cursor.close()
conn.close()

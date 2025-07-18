import requests
import mysql.connector
from datetime import datetime, timedelta
import time
from token_manager import get_access_token
from db_config import DB_CONFIG

# Auth and headers
print("🔐 Fetching access token...")
access_token = get_access_token()
print(f"✅ Using token: {access_token[:10]}...")

headers = {
    "Authorization": f"Bearer {access_token}",
    "Accept": "application/json"
}

# Calculate yesterday's timestamp
yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%dT%H:%M:%SZ')
SEARCH_URL = f"https://brassfields.retail.lightspeed.app/api/2.0/search?type=products&filter=updated_at>{yesterday}&order_direction=asc&page_size=1000&offset={{offset}}"

# Database connection
conn = mysql.connector.connect(**DB_CONFIG)
cursor = conn.cursor()

# Back up current products table
backup_table_name = f"products_sales_{datetime.now().strftime('%Y%m%d')}"
cursor.execute(f"CREATE TABLE IF NOT EXISTS {backup_table_name} AS SELECT * FROM products")

# Delete backup from 7 days ago
cutoff_date = datetime.now() - timedelta(days=7)
old_backup_table = f"products_sales_{cutoff_date.strftime('%Y%m%d')}"
cursor.execute(f"DROP TABLE IF EXISTS {old_backup_table}")

# Helper: get UPC or fallback code
def get_product_code(product):
    codes = product.get("product_codes", [])
    for code in codes:
        if code.get("type") == "UPC":
            return code.get("code"), code.get("type")
    return (codes[0].get("code"), codes[0].get("type")) if codes else (None, None)

# Helper: get most recent sale date
def get_most_recent_sale(product_id):
    cursor.execute("""
        SELECT sale_date FROM sales_lines
        WHERE product_id = %s
        ORDER BY sale_date DESC
        LIMIT 1
    """, (product_id,))
    result = cursor.fetchone()
    return result[0] if result else None

# Loop through paginated results
offset = 0
inserted = 0
updated = 0

while True:
    print(f"🔄 Fetching products updated since yesterday (offset={offset})...")
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
        prod_id = product.get("id")
        name = product.get("name")
        handle = product.get("handle")
        description = product.get("description")
        supply_price = str(product.get("supply_price", ""))
        retail_price = str(product.get("price_including_tax", ""))
        brand = product.get("brand")
        brand_name = brand.get("name") if brand else None
        supplier = product.get("supplier")
        supplier_name = supplier.get("name") if supplier else None
        category = product.get("product_category")
        product_category = category.get("name") if category else None
        tags = ",".join(product.get("tag_ids", []))
        outlet_tax = str(product.get("outlet_taxes", [{}])[0].get("rate")) if product.get("outlet_taxes") else None
        sku = product.get("sku")
        active_online = str(product.get("ecwid_enabled_webstore", False))
        created_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        last_synced_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        product_code, product_code_type = get_product_code(product)
        most_recent_sale = get_most_recent_sale(prod_id)

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
                    most_recent_sale=%s
                WHERE id=%s
            """
            values = (
                name, handle, description, supply_price, retail_price, brand_name,
                supplier_name, product_category, tags, outlet_tax, sku, active_online,
                last_synced_at, product_code, product_code_type, most_recent_sale, prod_id
            )
            cursor.execute(update_query, values)
            updated += 1
        else:
            insert_query = """
                INSERT INTO products (
                    id, name, handle, description, supply_price, retail_price, brand_name,
                    supplier_name, product_category, tags, outlet_tax_341_Douglas, sku,
                    active_online, created_at, last_synced_at, product_code, product_code_type,
                    most_recent_sale
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            values = (
                prod_id, name, handle, description, supply_price, retail_price, brand_name,
                supplier_name, product_category, tags, outlet_tax, sku, active_online,
                created_at, last_synced_at, product_code, product_code_type, most_recent_sale
            )
            cursor.execute(insert_query, values)
            inserted += 1
            
        # ✅ Tag sync: clear old links and add new ones
        cursor.execute("DELETE FROM product_tags WHERE product_id = %s", (prod_id,))
        for tag_id in product.get("tag_ids", []):
            cursor.execute("""
                INSERT IGNORE INTO product_tags (product_id, tag_id)
                VALUES (%s, %s)
            """, (prod_id, tag_id))
    conn.commit()
    offset += 1000
    time.sleep(0.3)

print(f"✅ Done! Inserted: {inserted}, Updated: {updated}")
cursor.close()
conn.close()

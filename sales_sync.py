import requests
import mysql.connector
from datetime import datetime, timedelta
import pytz
from zoneinfo import ZoneInfo
from token_manager import get_access_token
from db_config import DB_CONFIG

print("🔐 Fetching access token...")
access_token = get_access_token()
print(f"✅ Using token: {access_token[:10]}...")

headers = {
    "Authorization": f"Bearer {access_token}",
    "Accept": "application/json"
}

# Set Central Time Zone
central = ZoneInfo("America/Chicago")

# Get yesterday in Central Time
yesterday = (datetime.now(central) - timedelta(days=3)).date()

# Build datetime range in Central Time
start_ct = datetime(yesterday.year, yesterday.month, yesterday.day, 0, 0, 0, tzinfo=central)
end_ct = datetime(yesterday.year, yesterday.month, yesterday.day, 23, 59, 59, tzinfo=central)

# Convert to UTC ISO format
DATE_FROM = start_ct.astimezone(ZoneInfo("UTC")).isoformat().replace('+00:00', 'Z')
DATE_TO = end_ct.astimezone(ZoneInfo("UTC")).isoformat().replace('+00:00', 'Z')

print("DATE_FROM:", DATE_FROM)
print("DATE_TO:", DATE_TO)

# Convert UTC time to Central
tz_central = pytz.timezone("America/Chicago")
def to_central_time(utc_str):
    utc_time = datetime.strptime(utc_str, "%Y-%m-%dT%H:%M:%S%z")
    return utc_time.astimezone(tz_central).strftime("%Y-%m-%d %H:%M:%S")

# ====== Fetch Sales from URL-based Date Range ======
def fetch_sales_by_date():
    url = f"https://brassfields.retail.lightspeed.app/api/2.0/search?type=sales&date_from={DATE_FROM}&date_to={DATE_TO}"
    print(f"\n🔕 Fetching sales from {DATE_FROM} to {DATE_TO}...")
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        print(f"❌ Error {response.status_code}: {response.text}")
        return []
    return response.json().get("data", [])

# ====== Load product info from DB ======
def load_product_lookup():
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor(dictionary=True)
    cursor.execute("""
        SELECT p.id, p.name, p.sku, p.brand_name, p.product_category, ps.supplier_name
        FROM products p
        LEFT JOIN product_suppliers ps ON p.id = ps.product_id
    """)
    products = cursor.fetchall()
    cursor.close()
    conn.close()
    return {p["id"]: p for p in products}

# ====== Insert into MySQL ======
def insert_sales(sales, product_lookup):
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()
    inserted = 0

    # ✅ Backup and cleanup
    backup_table_name = f"sales_lines_{datetime.now().strftime('%Y%m%d')}"
    cursor.execute(f"CREATE TABLE IF NOT EXISTS {backup_table_name} AS SELECT * FROM sales_lines")
    cutoff_date = datetime.now() - timedelta(days=7)
    old_backup_table = f"sales_lines_{cutoff_date.strftime('%Y%m%d')}"
    cursor.execute(f"DROP TABLE IF EXISTS {old_backup_table}")

    for sale in sales:
        sale_id = sale.get("id")
        sale_date = to_central_time(sale.get("sale_date"))
        updated_at = to_central_time(sale.get("updated_at")) if sale.get("updated_at") else None
        invoice_number = sale.get("invoice_number")
        customer_id = sale.get("customer_id")
        staff_id = sale.get("user_id")
        register_id = sale.get("register_id")

        payment_list = sale.get("payments")
        payments = payment_list[0] if isinstance(payment_list, list) and payment_list else {}

        payments_id = payments.get("id")
        payment_type_id = payments.get("payment_type_id")
        payment_date = to_central_time(payments.get("payment_date")) if payments.get("payment_date") else None
        payment_source = payments.get("source")
        sales_total = float(sale.get("total", 0))
        sales_and_service = float(sale.get("total_with_service_charge", 0))

        for item in sale.get("line_items", []):
            product_id = item.get("product_id")
            product_info = product_lookup.get(product_id, {})
            product_name = product_info.get("name") or item.get("name") or "Unknown Product"
            product_category = product_info.get("product_category")
            brand_name = product_info.get("brand_name")
            quantity = int(item.get("quantity", 0))
            unit_price = float(item.get("price", 0))
            total_price = float(item.get("total_price", 0))
            cost_price = float(item.get("total_cost", 0))
            discount_line_item = float(item.get("discount_total", 0))
            tax_amount = float(item.get("tax_total", 0))
            tax_code = item.get("tax_id")
            service_fee = 0.00
            promotion = ",".join([p.get("name") for p in item.get("promotions", [])]) if item.get("promotions") else None
            is_refund = False

            sql = """
                INSERT INTO sales_lines (
                    invoice_number, sale_id, sale_date, product_id, product_name,
                    quantity, unit_price, cost_price, discount_line_item, tax_amount,
                    total_price, tax_code, service_fee, customer_id, staff_id,
                    register_id, product_category, brand_name, payments_id,
                    promotion, is_refund
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            values = (
                invoice_number, sale_id, sale_date, product_id, product_name,
                quantity, unit_price, cost_price, discount_line_item, tax_amount,
                total_price, tax_code, service_fee, customer_id, staff_id,
                register_id, product_category, brand_name, payments_id,
                promotion, is_refund
            )
            cursor.execute(sql, values)
            inserted += 1
            print(f"🗓 Inserted sale line {sale_id} → {product_name}")

        for adj in sale.get("adjustments", []):
            if adj.get("adjustment_type") == "NON_CASH_FEE":
                product_name = adj.get("name", "Adjustment")
                total_price = float(adj.get("total", 0))
                if total_price > 0:
                    sql = sql
                    values = (
                        invoice_number, sale_id, sale_date, None, product_name,
                        1, 0, 0, 0, 0, 0, None, total_price, customer_id, staff_id, register_id, None, None, payments_id,
                        "Adjustment", False
                    )
                    cursor.execute(sql, values)
                    inserted += 1
                    print(f"💳 Inserted adjustment line → {product_name} for sale {sale_id}")

    conn.commit()
    cursor.close()
    conn.close()
    print(f"✅ Inserted {inserted} sales_lines records including adjustments.")

# ====== MAIN ======
if __name__ == "__main__":
    sales_data = fetch_sales_by_date()
    if sales_data:
        product_lookup = load_product_lookup()
        insert_sales(sales_data, product_lookup)
    else:
        print("⚠️ No sales to insert for this period.")

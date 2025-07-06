import subprocess
import time
from datetime import datetime

print("⏰ Job runner started at", datetime.now().isoformat())

# ✅ Step 1: Refresh token
print("\n🔁 Running refresh_token.py...")
subprocess.run(["python", "refresh_token.py"], check=True)

# ✅ Optional: give token_manager a moment to write token
time.sleep(10)

# ✅ Step 2: Sync Tags
print("\n🔁 Running sync_tags.py...")
subprocess.run(["python", "sync_tags.py"], check=True)

# time.sleep(60)

# ✅ Step 3: Sync Products
print("\n🔁 Running products_sync.py...")
subprocess.run(["python", "products_sync.py"], check=True)

# time.sleep(60)

# ✅ Step 4: Sync Inventory
print("\n🔁 Running products_sync_inventory.py...")
subprocess.run(["python", "products_sync_inventory.py"], check=True)

time.sleep(60)

# ✅ Step 5: Sync Sales
print("\n🔁 Running sales_sync.py...")
subprocess.run(["python", "sales_sync.py"], check=True)

print("\n✅ All jobs completed successfully.")

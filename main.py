import subprocess
import time
from datetime import datetime

print("⏰ Job runner started at", datetime.now().isoformat())

# Step 1: Refresh the token
print("\n🔁 Running refresh_token.py...")
subprocess.run(["python", "refresh_token.py"], check=True)

# Step 2: Wait a few seconds for the token file to update
time.sleep(10)

# Step 3: Run product sync
print("\n🔁 Running products_sync.py...")
subprocess.run(["python", "products_sync.py"], check=True)

# Optional short pause between jobs
time.sleep(5)

# Step 4: Run sales sync
print("\n🔁 Running sales_sync.py...")
subprocess.run(["python", "sales_sync.py"], check=True)

print("\n✅ All jobs completed successfully.")

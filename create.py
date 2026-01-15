from bson import ObjectId
from db import db

# Collections
customers_col = db["customers"]
payments_col = db["payments"]

# Manager ID (optional filter for final count)
manager_id = ObjectId("68433eda05a08a53aa506250")

# Step 1: Find all customer_ids who made SUSU payments
susu_customer_ids = payments_col.distinct("customer_id", {"payment_type": "SUSU"})

# Step 2: Update any customer missing the "customer_type" field
updated = 0
for customer_id in susu_customer_ids:
    result = customers_col.update_one(
        {
            "_id": ObjectId(customer_id),
            "customer_type": {"$ne": "SUSU"}  # Only update if not already set
        },
        {
            "$set": {"customer_type": "SUSU"}
        }
    )
    if result.modified_count > 0:
        updated += 1

# Step 3: Count SUSU customers under a specific manager
susu_customers = customers_col.count_documents({
    "customer_type": "SUSU",
    "manager_id": manager_id
})

print(f"✅ Tagged {updated} new customers as 'SUSU'")
print(f"✅ Total SUSU customers for manager {manager_id}: {susu_customers}")

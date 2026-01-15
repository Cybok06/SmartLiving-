# clear.py
from bson import ObjectId
from datetime import datetime
from db import db

# =========================
# SET THIS CUSTOMER ID HERE
# =========================
CUSTOMER_ID_STR = "PUT_CUSTOMER_MONGO_ID_HERE"  # e.g. "66b8f2c9a3f1f2f8d2c1a999"

# If you want a "dry-run" (show what would be deleted but don't delete), set True
DRY_RUN = False

# Common places customer refs appear in your project
COMMON_CUSTOMER_FIELDS = [
    "_id",                       # customers collection
    "customer_id",               # most collections
    "customerId",                # alt naming
    "customer._id",              # embedded document
    "customer.id",               # embedded alt
    "customer_oid",              # alt naming
    "customer_object_id",        # alt naming
    "customer_id_str",           # some apps store string versions
]

# Collections you specifically showed or likely have customer records in
PRIORITY_COLLECTIONS = [
    "customers",
    "payments",
    "packages",
    "undelivered_items",
    "inventory_products_outflow",
    "inventory_products_outflow_col",  # (sometimes name duplicates, harmless if not found)
    "instant_sales",                   # may not have customer_id, but some projects do
]

def _as_objectid(s: str) -> ObjectId:
    try:
        return ObjectId(str(s).strip())
    except Exception:
        raise ValueError("Invalid CUSTOMER_ID_STR. Paste a valid MongoDB ObjectId string.")

def _build_or_query(oid: ObjectId, oid_str: str):
    """
    Builds a query that matches common customer reference patterns:
      - ObjectId stored in customer_id
      - string stored in customer_id
      - embedded customer._id
      - _id itself (for customers collection)
    """
    ors = []
    for f in COMMON_CUSTOMER_FIELDS:
        # match ObjectId
        ors.append({f: oid})
        # match string
        ors.append({f: oid_str})
    return {"$or": ors}

def _safe_find_count(col, query):
    try:
        return col.count_documents(query)
    except Exception:
        return None

def _safe_delete_many(col, query):
    try:
        return col.delete_many(query).deleted_count
    except Exception as e:
        return f"ERROR: {e}"

def _safe_delete_one(col, query):
    try:
        res = col.delete_one(query)
        return res.deleted_count
    except Exception as e:
        return f"ERROR: {e}"

def main():
    oid = _as_objectid(CUSTOMER_ID_STR)
    oid_str = str(oid)

    print("=" * 72)
    print("CLEAR CUSTOMER SCRIPT")
    print(f"Customer ID: {oid_str}")
    print(f"DRY_RUN: {DRY_RUN}")
    print(f"Time: {datetime.utcnow().isoformat()}Z")
    print("=" * 72)

    # 1) Remove from favorites lists in users (important!)
    users_col = db["users"]
    fav_query = {"favorites_customer_ids": {"$in": [oid, oid_str]}}
    fav_count = _safe_find_count(users_col, fav_query)
    print(f"\n[users] favorites references found: {fav_count}")

    if not DRY_RUN:
        try:
            # Pull both objectId and string version
            users_col.update_many({}, {"$pull": {"favorites_customer_ids": oid}})
            users_col.update_many({}, {"$pull": {"favorites_customer_ids": oid_str}})
            print("[users] favorites_customer_ids: removed references ✅")
        except Exception as e:
            print(f"[users] ERROR removing favorites refs: {e}")

    # 2) Delete from priority collections first (clear + predictable)
    print("\n--- Priority collections cleanup ---")
    for name in PRIORITY_COLLECTIONS:
        if name not in db.list_collection_names():
            print(f"[skip] {name} (collection not found)")
            continue

        col = db[name]

        # Customers collection should be deleted by _id specifically
        if name == "customers":
            q = {"_id": oid}
            found = _safe_find_count(col, q)
            print(f"[customers] matches: {found}")
            if DRY_RUN:
                continue
            deleted = _safe_delete_one(col, q)
            print(f"[customers] deleted: {deleted}")
            continue

        # Others: best-effort OR query
        q = _build_or_query(oid, oid_str)
        found = _safe_find_count(col, q)
        print(f"[{name}] matches: {found}")
        if DRY_RUN:
            continue
        deleted = _safe_delete_many(col, q)
        print(f"[{name}] deleted: {deleted}")

    # 3) Best-effort scan ALL collections for customer references
    #    (this helps catch new collections you added later)
    print("\n--- Full DB scan (best-effort) ---")
    for name in db.list_collection_names():
        # skip system collections if any
        if name.startswith("system."):
            continue
        # avoid double-work (already handled above)
        if name in PRIORITY_COLLECTIONS or name == "users":
            continue

        col = db[name]
        q = _build_or_query(oid, oid_str)
        found = _safe_find_count(col, q)

        # Only act if something was found
        if found and found > 0:
            print(f"[{name}] matches: {found}")
            if DRY_RUN:
                continue
            deleted = _safe_delete_many(col, q)
            print(f"[{name}] deleted: {deleted}")

    print("\nDONE ✅")
    print("Tip: set DRY_RUN=True first to preview what will be deleted safely.")
    print("=" * 72)

if __name__ == "__main__":
    main()

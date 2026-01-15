# sync_product_components_by_inventory_name.py
# ✅ What it does (safe + automatic):
# 1) Takes a TEMPLATE manager's products (with full components)
# 2) For each product name, finds same-name products under other managers
# 3) Resolves each component by INVENTORY ITEM NAME:
#      template component inventory_id -> template inventory name -> target manager inventory _id with same name
# 4) Writes target product.components using the target manager's inventory _ids
#
# ✅ Notes:
# - This does NOT deduct stock. It only fixes/matches components arrays across managers correctly.
# - Quantity checks are NOT enforced (even if inventory qty is low or negative, it still maps).
#
# Run:
#   python sync_product_components_by_inventory_name.py
#
# Options:
#   python sync_product_components_by_inventory_name.py --dry-run
#   python sync_product_components_by_inventory_name.py --name "Cookery Ghc 25"
#   python sync_product_components_by_inventory_name.py --strict-fields
#   python sync_product_components_by_inventory_name.py --fallback-template-id   (if target inventory item missing, keep template id)

from __future__ import annotations
import argparse
import re
from datetime import datetime
from bson import ObjectId
from db import db

products_col = db["products"]
inventory_col = db["inventory"]

TEMPLATE_MANAGER_ID = "68433eda05a08a53aa506250"  # manager whose products have correct components

def norm(s: str) -> str:
    s = "" if s is None else str(s)
    s = s.strip().lower()
    s = re.sub(r"\s+", " ", s)
    return s

def as_oid(v):
    try:
        return ObjectId(str(v))
    except Exception:
        return None

def same_name_regex(name_norm: str):
    # exact match, case-insensitive
    return {"$regex": f"^{re.escape(name_norm)}$", "$options": "i"}

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--name", type=str, default=None, help="Only sync this exact product name")
    ap.add_argument("--strict-fields", action="store_true",
                    help="Match products by name + category + package_name (recommended if you have duplicates)")
    ap.add_argument("--fallback-template-id", action="store_true",
                    help="If target manager inventory item is missing, keep the template component _id (NOT recommended)")
    args = ap.parse_args()

    template_oid = as_oid(TEMPLATE_MANAGER_ID)
    if not template_oid:
        raise SystemExit("Invalid TEMPLATE_MANAGER_ID")

    # 1) Fetch template products with components
    q_template = {"manager_id": template_oid}
    if args.name:
        q_template["name"] = args.name

    template_products = list(products_col.find(q_template, {
        "_id": 1, "name": 1, "category": 1, "package_name": 1, "components": 1
    }))

    template_products = [
        p for p in template_products
        if isinstance(p.get("components"), list) and len(p["components"]) > 0
    ]

    if not template_products:
        raise SystemExit("No template products found with components for the given filters.")

    # 2) Preload ALL template inventory items referenced by template components
    template_component_ids = set()
    for p in template_products:
        for c in p.get("components", []):
            cid = c.get("_id")
            oid = as_oid(cid) if cid is not None else None
            if oid:
                template_component_ids.add(oid)

    template_inventory_docs = list(inventory_col.find(
        {"_id": {"$in": list(template_component_ids)}},
        {"_id": 1, "name": 1, "description": 1}
    ))
    template_inv_by_id = {d["_id"]: d for d in template_inventory_docs}

    # Build template component resolver:
    # template component inventory_id -> normalized inventory name
    template_comp_name_by_id = {}
    missing_template_inv = 0
    for oid in template_component_ids:
        inv = template_inv_by_id.get(oid)
        if not inv:
            missing_template_inv += 1
            continue
        template_comp_name_by_id[oid] = norm(inv.get("name"))

    # 3) Build template map: product-key -> list of (component_name_norm, qty, template_component_id)
    template_map = {}  # key -> list[dict]
    for p in template_products:
        name = p.get("name", "")
        cat = p.get("category", "")
        pkg = p.get("package_name", "")

        if args.strict_fields:
            key = (norm(name), norm(cat), norm(pkg))
        else:
            key = (norm(name),)

        comp_list = []
        for c in p.get("components", []):
            cid = as_oid(c.get("_id"))
            qty = int(c.get("quantity", 1) or 1)
            comp_name = template_comp_name_by_id.get(cid) if cid else None
            comp_list.append({
                "template_component_id": cid,
                "component_name_norm": comp_name,  # may be None if template inv missing
                "quantity": qty
            })

        template_map[key] = comp_list

    # Helper: for each target manager, cache inventory name->id map
    inv_cache = {}  # manager_oid -> {name_norm: inventory_id}

    def get_inv_map_for_manager(manager_oid: ObjectId):
        if manager_oid in inv_cache:
            return inv_cache[manager_oid]

        docs = list(inventory_col.find(
            {"manager_id": manager_oid},
            {"_id": 1, "name": 1}
        ))
        m = {}
        for d in docs:
            nm = norm(d.get("name"))
            if nm and nm not in m:
                m[nm] = d["_id"]
        inv_cache[manager_oid] = m
        return m

    # 4) Apply mapping to all target products
    stats = {
        "template_manager": TEMPLATE_MANAGER_ID,
        "template_products_used": len(template_map),
        "missing_template_inventory_docs": missing_template_inv,
        "targets_matched": 0,
        "products_updated": 0,
        "components_mapped": 0,
        "components_missing_in_target_inventory": 0,
        "dry_run": args.dry_run,
        "strict_fields": args.strict_fields,
        "fallback_template_id": args.fallback_template_id
    }

    for key, template_components in template_map.items():
        if args.strict_fields:
            k_name, k_cat, k_pkg = key
            q_targets = {
                "manager_id": {"$ne": template_oid},
                "name": same_name_regex(k_name),
                "category": same_name_regex(k_cat),
                "package_name": same_name_regex(k_pkg),
            }
        else:
            (k_name,) = key
            q_targets = {
                "manager_id": {"$ne": template_oid},
                "name": same_name_regex(k_name),
            }

        targets = list(products_col.find(q_targets, {
            "_id": 1, "manager_id": 1, "name": 1, "category": 1, "package_name": 1, "components": 1
        }))
        stats["targets_matched"] += len(targets)

        for t in targets:
            manager_oid = as_oid(t.get("manager_id"))
            if not manager_oid:
                continue

            inv_map = get_inv_map_for_manager(manager_oid)

            new_components = []
            for tc in template_components:
                t_cid = tc["template_component_id"]
                comp_name = tc["component_name_norm"]
                qty = tc["quantity"]

                # If we can't resolve template inv name, we can't safely map
                if not comp_name:
                    # fallback only if requested
                    if args.fallback_template_id and t_cid:
                        new_components.append({"_id": t_cid, "quantity": qty})
                    continue

                target_inv_id = inv_map.get(comp_name)

                if target_inv_id:
                    new_components.append({"_id": target_inv_id, "quantity": qty})
                    stats["components_mapped"] += 1
                else:
                    stats["components_missing_in_target_inventory"] += 1
                    if args.fallback_template_id and t_cid:
                        new_components.append({"_id": t_cid, "quantity": qty})

            if not new_components:
                # nothing to write for this product
                continue

            if args.dry_run:
                stats["products_updated"] += 1
                continue

            products_col.update_one(
                {"_id": t["_id"]},
                {"$set": {
                    "components": new_components,
                    "components_synced_from_manager": template_oid,  # optional audit
                    "components_synced_at": datetime.utcnow()        # optional audit
                }}
            )
            stats["products_updated"] += 1

    # Terminal-only summary (no UI / no flash)
    print(stats)

if __name__ == "__main__":
    main()

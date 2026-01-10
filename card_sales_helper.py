from bson import ObjectId


def norm_name(val: str) -> str:
    return " ".join((val or "").strip().lower().split())


def _id_variants(val):
    if val is None:
        return []
    if isinstance(val, ObjectId):
        return [val, str(val)]
    sval = str(val).strip()
    if ObjectId.is_valid(sval):
        return [ObjectId(sval), sval]
    return [sval]


def _max_dt(a, b):
    if not a:
        return b
    if not b:
        return a
    return max(a, b)


def sold_counts_by_name(
    customers_col,
    instant_sales_col,
    agent_id=None,
    manager_id=None,
    product_name=None,
    start_dt=None,
    end_dt=None,
    group_by_agent=False,
):
    product_norm = norm_name(product_name)
    agent_id_str = str(agent_id).strip() if agent_id else None
    manager_variants = _id_variants(manager_id) if manager_id else []

    date_match = {}
    if start_dt:
        date_match["$gte"] = start_dt
    if end_dt:
        date_match["$lt"] = end_dt

    customer_match = {}
    if manager_variants:
        customer_match["manager_id"] = {"$in": manager_variants}

    installment_pipeline = []
    if customer_match:
        installment_pipeline.append({"$match": customer_match})
    installment_pipeline.extend(
        [
            {"$unwind": "$purchases"},
            {
                "$addFields": {
                    "sold_agent_id": {"$ifNull": ["$purchases.agent_id", "$agent_id"]},
                    "sold_product_name": {"$ifNull": ["$purchases.product.name", "$purchases.product_name"]},
                    "sold_date_str": "$purchases.purchase_date",
                }
            },
            {
                "$addFields": {
                    "sold_date": {
                        "$dateFromString": {
                            "dateString": "$sold_date_str",
                            "format": "%Y-%m-%d",
                            "onError": None,
                            "onNull": None,
                        }
                    }
                }
            },
            {"$match": {"sold_date": {"$ne": None}}},
        ]
    )
    if agent_id_str:
        installment_pipeline.append({"$match": {"sold_agent_id": agent_id_str}})
    if date_match:
        installment_pipeline.append({"$match": {"sold_date": date_match}})

    group_id = {"product_name": "$sold_product_name"}
    if group_by_agent:
        group_id["agent_id"] = "$sold_agent_id"
    installment_pipeline.append(
        {"$group": {"_id": group_id, "total": {"$sum": 1}, "last_at": {"$max": "$sold_date"}}}
    )

    instant_match = {}
    if agent_id_str:
        instant_match["agent_id"] = agent_id_str
    if manager_variants:
        instant_match["manager_id"] = {"$in": manager_variants}

    instant_pipeline = []
    if instant_match:
        instant_pipeline.append({"$match": instant_match})
    instant_pipeline.extend(
        [
            {
                "$addFields": {
                    "sold_product_name": {"$ifNull": ["$product.name", "$product_name"]},
                    "sold_date_str": "$purchase_date",
                }
            },
            {
                "$addFields": {
                    "sold_date": {
                        "$dateFromString": {
                            "dateString": "$sold_date_str",
                            "format": "%Y-%m-%d",
                            "onError": None,
                            "onNull": None,
                        }
                    }
                }
            },
            {"$match": {"sold_date": {"$ne": None}}},
        ]
    )
    if date_match:
        instant_pipeline.append({"$match": {"sold_date": date_match}})

    group_id = {"product_name": "$sold_product_name"}
    if group_by_agent:
        group_id["agent_id"] = "$agent_id"
    instant_pipeline.append(
        {"$group": {"_id": group_id, "total": {"$sum": 1}, "last_at": {"$max": "$sold_date"}}}
    )

    installment_rows = list(customers_col.aggregate(installment_pipeline))
    instant_rows = list(instant_sales_col.aggregate(instant_pipeline))

    def _merge(rows):
        out = {}
        for row in rows:
            raw_name = (row.get("_id", {}).get("product_name") or "").strip()
            pkey = norm_name(raw_name)
            if not pkey:
                continue
            if product_norm and pkey != product_norm:
                continue
            agent_key = None
            if group_by_agent:
                agent_key = str(row.get("_id", {}).get("agent_id") or "")
            key = (agent_key, pkey) if group_by_agent else pkey
            entry = out.setdefault(key, {"count": 0, "last_at": None, "name": raw_name, "agent_id": agent_key})
            entry["count"] += int(row.get("total", 0) or 0)
            entry["last_at"] = _max_dt(entry.get("last_at"), row.get("last_at"))
            if not entry.get("name") and raw_name:
                entry["name"] = raw_name
        return out

    installment = _merge(installment_rows)
    instant = _merge(instant_rows)

    total = {}
    for key in set(installment) | set(instant):
        inst = installment.get(key, {})
        ins = instant.get(key, {})
        total[key] = {
            "count": inst.get("count", 0) + ins.get("count", 0),
            "last_at": _max_dt(inst.get("last_at"), ins.get("last_at")),
            "name": inst.get("name") or ins.get("name") or "",
            "agent_id": inst.get("agent_id") or ins.get("agent_id") or "",
        }

    return {"installment": installment, "instant": instant, "total": total}

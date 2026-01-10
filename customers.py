from __future__ import annotations

from datetime import datetime, date, timedelta
from typing import Dict, Any, List, Optional
import re

from flask import (
    Blueprint, render_template, session, redirect,
    url_for, request, flash, jsonify
)
from bson.objectid import ObjectId

from db import db

customers_bp = Blueprint("customers", __name__)

# Collections
users_col             = db.users
customers_col         = db.customers
payments_col          = db.payments
deleted_col           = db.deleted
packages_col          = db.packages           # ✅ submitted / completed items
stopped_customers_col = db.stopped_customers  # ✅ closed cards archive


def _require_manager() -> Optional[ObjectId]:
    """Guard: ensure a manager is logged in. Returns manager ObjectId or None."""
    mid = session.get("manager_id")
    if not mid:
        return None
    try:
        return ObjectId(mid)
    except Exception:
        return None


def _slugify(text: str) -> str:
    """Simple slug for tag keys (lowercase, alnum + hyphen)."""
    text = (text or "").strip().lower()
    text = re.sub(r"[^a-z0-9]+", "-", text)
    return text.strip("-") or "tag"


def _safe_int(v: Any, default: int = 0) -> int:
    try:
        return int(v)
    except Exception:
        return default


def _build_base_match(
    manager_oid: Optional[ObjectId],
    search_term: str,
    agent_id_param: str,
    tag_filter: str,
    branch_filter: str = ""
) -> Dict[str, Any]:
    """Base filter – manager scope plus optional agent/search/tag/branch filters."""
    base_match: Dict[str, Any] = {}
    if manager_oid:
        base_match["manager_id"] = manager_oid

    # Branch filter (via agent branch)
    if branch_filter:
        agent_query: Dict[str, Any] = {"role": "agent", "branch": branch_filter}
        if manager_oid:
            agent_query["manager_id"] = manager_oid
        agent_ids = list(users_col.find(agent_query, {"_id": 1}))
        ids: List[Any] = []
        for a in agent_ids:
            oid = a.get("_id")
            if oid:
                ids.append(oid)
                ids.append(str(oid))
        base_match["agent_id"] = {"$in": ids}

    # Agent filter (support both string and ObjectId stored in DB)
    if agent_id_param and agent_id_param != "all":
        vals: List[Any] = [agent_id_param]
        try:
            vals.append(ObjectId(agent_id_param))
        except Exception:
            pass
        if "agent_id" in base_match:
            allowed = {str(v) for v in base_match["agent_id"].get("$in", [])}
            if str(agent_id_param) not in allowed:
                base_match["agent_id"] = {"$in": []}
            else:
                base_match["agent_id"] = {"$in": vals}
        else:
            base_match["agent_id"] = {"$in": vals}

    # Search by name / phone
    if search_term:
        base_match["$or"] = [
            {"name": {"$regex": search_term, "$options": "i"}},
            {"phone_number": {"$regex": search_term, "$options": "i"}},
        ]

    # Filter by tag key
    if tag_filter:
        base_match["tags.key"] = tag_filter

    return base_match


# ============================================================
# FAST SHELL ROUTE (renders within ~1–2 seconds)
# ============================================================
@customers_bp.route("/customers")
def customers_list():
    """
    FAST render: return the UI shell immediately.
    Customers + metrics are fetched asynchronously via /customers/data.
    """
    manager_oid = _require_manager()
    if not manager_oid:
        return redirect(url_for("login.login"))

    # keep existing query params (UI will send same to /customers/data)
    page = max(_safe_int(request.args.get("page", 1), 1), 1)
    per_page = min(max(_safe_int(request.args.get("per_page", 12), 12), 6), 48)

    search_term    = (request.args.get("search") or "").strip()
    agent_id_param = (request.args.get("agent_id") or "").strip()
    status_filter  = (request.args.get("status") or "").strip()
    tag_filter     = (request.args.get("tag") or "").strip()
    branch_filter  = (request.args.get("branch") or "").strip()

    # fast agents list (small query)
    agents = list(
        users_col.find(
            {"manager_id": manager_oid, "role": "agent"},
            {"_id": 1, "name": 1}
        ).sort("name", 1)
    )

    branches = sorted([b for b in users_col.distinct("branch", {"role": "agent", "manager_id": manager_oid}) if b])

    # fast counters (light)
    agent_ids_str = [str(a["_id"]) for a in agents]
    total_packages = packages_col.count_documents({"agent_id": {"$in": agent_ids_str}}) if agent_ids_str else 0

    total_closed_cards = stopped_customers_col.count_documents({
        "$or": [
            {"customer_snapshot.manager_id": manager_oid},
            {"customer_snapshot.manager_id": str(manager_oid)}
        ]
    })

    # IMPORTANT: placeholders; UI will populate via /customers/data
    return render_template(
        "customers.html",

        customers=[],
        customers_for_print=[],  # keep for print section; UI can fetch full list later if needed

        total_customers=0,
        total_active=0,
        total_inactive=0,
        total_dormant=0,
        total_churned=0,
        total_closed=0,
        total_overdue=0,
        total_not_active=0,
        total_no_payment=0,
        total_completed=0,

        total_packages=total_packages,
        total_closed_cards=total_closed_cards,

        agents=agents,
        selected_agent=agent_id_param,
        search_term=search_term,
        page=page,
        total_pages=1,
        selected_status=status_filter,
        available_tags=[],
        selected_tag=tag_filter,
        selected_branch=branch_filter,
        branches=branches,
        role="manager",
        customers_route="customers.customers_list",
        customers_data_route="customers.customers_data",
        customer_profile_route="customers.customer_profile",

        # flags for your template JS (we will wire this in HTML next)
        async_mode=True,
        per_page=per_page
    )


# ============================================================
# ASYNC DATA ENDPOINT (fast filtering + pagination)
# ============================================================
@customers_bp.route("/customers/data")
def customers_data():
    """
    Returns JSON:
      - customers (paginated)
      - metrics (counts)
      - available_tags (tag palette)
      - total_pages / total_customers

    Uses Mongo aggregation (much faster than Python loops over all payments).
    """
    manager_oid = _require_manager()
    if not manager_oid:
        return jsonify({"ok": False, "error": "unauthorized"}), 401

    page = max(_safe_int(request.args.get("page", 1), 1), 1)
    per_page = min(max(_safe_int(request.args.get("per_page", 12), 12), 6), 48)

    search_term    = (request.args.get("search") or "").strip()
    agent_id_param = (request.args.get("agent_id") or "").strip()
    status_filter  = (request.args.get("status") or "").strip().lower()
    tag_filter     = (request.args.get("tag") or "").strip()
    branch_filter  = (request.args.get("branch") or "").strip()

    base_match = _build_base_match(manager_oid, search_term, agent_id_param, tag_filter, branch_filter)

    now_dt = datetime.utcnow()

    # -------- shared pipeline core (match -> compute) --------
    # Note: end_date parsing supports "%Y-%m-%d". If you still store "%y-%m-%d" for some records,
    # those specific overdue checks may not be detected in Mongo (we can add a fallback later if needed).
    core_pipeline: List[Dict[str, Any]] = [
        {"$match": base_match},
        {"$project": {
            "name": 1,
            "phone_number": 1,
            "image_url": 1,
            "purchases": 1,
            "agent_id": 1,
            "tags": 1,
            "lifecycle_status": 1,
        }},
        {"$addFields": {
            "total_debt": {
                "$sum": {
                    "$map": {
                        "input": {"$ifNull": ["$purchases", []]},
                        "as": "p",
                        "in": {"$toDouble": {"$ifNull": ["$$p.product.total", 0]}}
                    }
                }
            }
        }},
        {"$lookup": {
            "from": "payments",
            "let": {"cid": "$_id"},
            "pipeline": [
                {"$match": {"$expr": {"$eq": ["$customer_id", "$$cid"]}}},
                {"$project": {"payment_type": 1, "amount": 1, "timestamp": 1, "date": 1}},
                {"$addFields": {
                    "pay_date": {
                        "$ifNull": [
                            "$timestamp",
                            {"$dateFromString": {
                                "dateString": "$date",
                                "format": "%Y-%m-%d",
                                "onError": None,
                                "onNull": None
                            }}
                        ]
                    }
                }},
                {"$group": {
                    "_id": None,
                    "last_non_withdraw": {"$max": {
                        "$cond": [
                            {"$ne": ["$payment_type", "WITHDRAWAL"]},
                            "$pay_date",
                            None
                        ]
                    }},
                    "sum_paid": {"$sum": {
                        "$cond": [
                            {"$ne": ["$payment_type", "WITHDRAWAL"]},
                            {"$toDouble": {"$ifNull": ["$amount", 0]}},
                            0
                        ]
                    }},
                    "sum_withdraw": {"$sum": {
                        "$cond": [
                            {"$eq": ["$payment_type", "WITHDRAWAL"]},
                            {"$toDouble": {"$ifNull": ["$amount", 0]}},
                            0
                        ]
                    }},
                    "count_non_withdraw": {"$sum": {
                        "$cond": [
                            {"$ne": ["$payment_type", "WITHDRAWAL"]},
                            1,
                            0
                        ]
                    }},
                }}
            ],
            "as": "pay_agg"
        }},
        {"$addFields": {
            "pay_agg": {"$ifNull": [{"$arrayElemAt": ["$pay_agg", 0]}, {}]},
        }},
        {"$addFields": {
            "last_payment_dt": "$pay_agg.last_non_withdraw",
            "sum_paid": {"$ifNull": ["$pay_agg.sum_paid", 0]},
            "sum_withdraw": {"$ifNull": ["$pay_agg.sum_withdraw", 0]},
            "count_non_withdraw": {"$ifNull": ["$pay_agg.count_non_withdraw", 0]},
            "net_paid": {"$subtract": [
                {"$ifNull": ["$pay_agg.sum_paid", 0]},
                {"$ifNull": ["$pay_agg.sum_withdraw", 0]}
            ]},
        }},
        {"$addFields": {
            "days_since_last_payment": {
                "$cond": [
                    {"$ifNull": ["$last_payment_dt", False]},
                    {"$dateDiff": {"startDate": "$last_payment_dt", "endDate": now_dt, "unit": "day"}},
                    None
                ]
            }
        }},
        {"$addFields": {
            "purchase_end_dates": {
                "$map": {
                    "input": {"$ifNull": ["$purchases", []]},
                    "as": "pp",
                    "in": {
                        "$dateFromString": {
                            "dateString": "$$pp.end_date",
                            "format": "%Y-%m-%d",
                            "onError": None,
                            "onNull": None
                        }
                    }
                }
            }
        }},
        {"$addFields": {
            "is_overdue_time": {
                "$anyElementTrue": {
                    "$map": {
                        "input": {"$ifNull": ["$purchase_end_dates", []]},
                        "as": "ed",
                        "in": {"$and": [
                            {"$ne": ["$$ed", None]},
                            {"$lt": ["$$ed", now_dt]}
                        ]}
                    }
                }
            }
        }},
        {"$addFields": {
            "stored_lifecycle": {"$toLower": {"$ifNull": ["$lifecycle_status", ""]}}
        }},
        {"$addFields": {
            "computed_status": {
                "$switch": {
                    "branches": [
                        {"case": {"$and": [
                            {"$gt": ["$total_debt", 0]},
                            {"$gte": ["$net_paid", "$total_debt"]}
                        ]}, "then": "Completed"},

                        {"case": {"$eq": ["$stored_lifecycle", "churned"]}, "then": "Churned"},
                        {"case": {"$eq": ["$stored_lifecycle", "closed"]}, "then": "Closed"},

                        {"case": {"$and": [
                            {"$eq": ["$is_overdue_time", True]},
                            {"$lt": ["$net_paid", "$total_debt"]}
                        ]}, "then": "Closed"},

                        {"case": {"$eq": ["$count_non_withdraw", 0]}, "then": "Dormant"},
                        {"case": {"$lte": ["$days_since_last_payment", 14]}, "then": "Active"},
                        {"case": {"$and": [
                            {"$gte": ["$days_since_last_payment", 15]},
                            {"$lte": ["$days_since_last_payment", 30]},
                        ]}, "then": "Inactive"},
                    ],
                    "default": "Dormant"
                }
            }
        }},
    ]

    # total count (fast, indexed if manager_id is indexed)
    total_customers = customers_col.count_documents(base_match)
    total_pages = max((total_customers + per_page - 1) // per_page, 1)
    if page > total_pages:
        page = total_pages

    # page docs
    page_pipeline = core_pipeline + [
        {"$sort": {"name": 1}},
        {"$skip": (page - 1) * per_page},
        {"$limit": per_page},
    ]
    page_docs = list(customers_col.aggregate(page_pipeline, allowDiskUse=True))

    # metrics counts (group by computed_status)
    counts_pipeline = core_pipeline + [
        {"$group": {"_id": "$computed_status", "count": {"$sum": 1}}}
    ]
    counts_docs = list(customers_col.aggregate(counts_pipeline, allowDiskUse=True))
    counts_map = {d["_id"]: int(d.get("count", 0)) for d in counts_docs if d.get("_id")}

    total_active    = counts_map.get("Active", 0)
    total_inactive  = counts_map.get("Inactive", 0)
    total_dormant   = counts_map.get("Dormant", 0)
    total_churned   = counts_map.get("Churned", 0)
    total_closed    = counts_map.get("Closed", 0)
    total_completed = counts_map.get("Completed", 0)

    # legacy metrics
    total_not_active = total_inactive + total_dormant + total_churned + total_closed
    total_overdue = total_closed

    # status filter (applied to current page results; UI should also pass status and re-fetch)
    if status_filter:
        page_docs = [d for d in page_docs if (d.get("computed_status") or "").lower() == status_filter]

    # Agent names map for current page only (fast)
    agent_oids: List[ObjectId] = []
    for d in page_docs:
        aid = d.get("agent_id")
        if not aid:
            continue
        try:
            agent_oids.append(aid if isinstance(aid, ObjectId) else ObjectId(aid))
        except Exception:
            continue

    agent_map: Dict[str, Dict[str, Any]] = {}
    if agent_oids:
        for a in users_col.find({"_id": {"$in": list(set(agent_oids))}}, {"_id": 1, "name": 1, "branch": 1}):
            agent_map[str(a["_id"])] = {
                "name": a.get("name", "Agent"),
                "branch": a.get("branch")
            }

    # Normalize tags and shape customers for UI
    tags_summary: Dict[str, Dict[str, Any]] = {}
    customers_out: List[Dict[str, Any]] = []

    for d in page_docs:
        cid = d["_id"]
        name = (d.get("name") or "").strip() or "Unknown"
        phone = d.get("phone_number") or "N/A"

        raw_agent_id = d.get("agent_id")
        agent_name = "Unassigned"
        agent_branch = None
        if raw_agent_id:
            agent_meta = agent_map.get(str(raw_agent_id), {})
            agent_name = agent_meta.get("name", "Unassigned")
            agent_branch = agent_meta.get("branch")

        initials = name[0].upper() if name else "?"

        normalized_tags: List[Dict[str, Any]] = []
        for t in (d.get("tags") or []):
            t_key = t.get("key") or _slugify(t.get("label", "tag"))
            t_label = t.get("label") or t_key.title()
            t_color = t.get("color") or "#6366f1"

            normalized_tags.append({"key": t_key, "label": t_label, "color": t_color})

            if t_key not in tags_summary:
                tags_summary[t_key] = {"key": t_key, "label": t_label, "color": t_color, "count": 0}
            tags_summary[t_key]["count"] += 1

        last_dt = d.get("last_payment_dt")
        last_payment_date = last_dt.strftime("%Y-%m-%d") if isinstance(last_dt, datetime) else None

        customers_out.append({
            "id": str(cid),
            "name": name,
            "phone": phone,
            "image_url": d.get("image_url", ""),
            "status": d.get("computed_status", "Dormant"),
            "agent_name": agent_name,
            "agent_branch": agent_branch,
            "initials": initials,
            "tags": normalized_tags,
            "last_payment_date": last_payment_date,
            "days_since_last_payment": d.get("days_since_last_payment", None),
        })

    available_tags = sorted(tags_summary.values(), key=lambda x: (x.get("label") or "").lower())

    return jsonify({
        "ok": True,
        "page": page,
        "per_page": per_page,
        "total_pages": total_pages,
        "total_customers": total_customers,
        "metrics": {
            "total_active": total_active,
            "total_inactive": total_inactive,
            "total_dormant": total_dormant,
            "total_churned": total_churned,
            "total_closed": total_closed,
            "total_overdue": total_overdue,
            "total_not_active": total_not_active,
            "total_no_payment": 0,  # computed in UI/extra endpoint if you really need exact
            "total_completed": total_completed,
        },
        "customers": customers_out,
        "available_tags": available_tags
    })


# ============================================================
# ADMIN: FULL CUSTOMER ACCESS (same UI, no manager scope)
# ============================================================
@customers_bp.route("/admin/customers")
def admin_customers_list():
    if "admin_id" not in session:
        return redirect(url_for("login.login"))

    page = max(_safe_int(request.args.get("page", 1), 1), 1)
    per_page = min(max(_safe_int(request.args.get("per_page", 12), 12), 6), 48)

    search_term    = (request.args.get("search") or "").strip()
    agent_id_param = (request.args.get("agent_id") or "").strip()
    status_filter  = (request.args.get("status") or "").strip()
    tag_filter     = (request.args.get("tag") or "").strip()
    branch_filter  = (request.args.get("branch") or "").strip()
    branch_filter  = (request.args.get("branch") or "").strip()

    agents = list(
        users_col.find(
            {"role": "agent"},
            {"_id": 1, "name": 1}
        ).sort("name", 1)
    )
    branches = sorted([b for b in users_col.distinct("branch", {"role": "agent"}) if b])

    total_packages = packages_col.count_documents({})
    total_closed_cards = stopped_customers_col.count_documents({})

    return render_template(
        "customers.html",
        customers=[],
        customers_for_print=[],
        total_customers=0,
        total_active=0,
        total_inactive=0,
        total_dormant=0,
        total_churned=0,
        total_closed=0,
        total_overdue=0,
        total_not_active=0,
        total_no_payment=0,
        total_completed=0,
        total_packages=total_packages,
        total_closed_cards=total_closed_cards,
        agents=agents,
        selected_agent=agent_id_param,
        search_term=search_term,
        page=page,
        total_pages=1,
        selected_status=status_filter,
        available_tags=[],
        selected_tag=tag_filter,
        selected_branch=branch_filter,
        branches=branches,
        role="admin",
        customers_route="customers.admin_customers_list",
        customers_data_route="customers.admin_customers_data",
        customer_profile_route="customers.customer_profile",
        async_mode=True,
        per_page=per_page
    )


@customers_bp.route("/admin/customers/data")
def admin_customers_data():
    if "admin_id" not in session:
        return jsonify({"ok": False, "error": "unauthorized"}), 401

    page = max(_safe_int(request.args.get("page", 1), 1), 1)
    per_page = min(max(_safe_int(request.args.get("per_page", 12), 12), 6), 48)

    search_term    = (request.args.get("search") or "").strip()
    agent_id_param = (request.args.get("agent_id") or "").strip()
    status_filter  = (request.args.get("status") or "").strip().lower()
    tag_filter     = (request.args.get("tag") or "").strip()
    branch_filter  = (request.args.get("branch") or "").strip()

    base_match = _build_base_match(None, search_term, agent_id_param, tag_filter, branch_filter)

    now_dt = datetime.utcnow()

    core_pipeline: List[Dict[str, Any]] = [
        {"$match": base_match},
        {"$project": {
            "name": 1,
            "phone_number": 1,
            "image_url": 1,
            "purchases": 1,
            "agent_id": 1,
            "tags": 1,
            "lifecycle_status": 1,
        }},
        {"$addFields": {
            "total_debt": {
                "$sum": {
                    "$map": {
                        "input": {"$ifNull": ["$purchases", []]},
                        "as": "p",
                        "in": {"$toDouble": {"$ifNull": ["$$p.product.total", 0]}}
                    }
                }
            }
        }},
        {"$lookup": {
            "from": "payments",
            "let": {"cid": "$_id"},
            "pipeline": [
                {"$match": {"$expr": {"$eq": ["$customer_id", "$$cid"]}}},
                {"$project": {"payment_type": 1, "amount": 1, "timestamp": 1, "date": 1}},
                {"$addFields": {
                    "pay_date": {
                        "$ifNull": [
                            "$timestamp",
                            {"$dateFromString": {
                                "dateString": "$date",
                                "format": "%Y-%m-%d",
                                "onError": None,
                                "onNull": None
                            }}
                        ]
                    }
                }},
                {"$group": {
                    "_id": None,
                    "last_non_withdraw": {"$max": {
                        "$cond": [
                            {"$ne": ["$payment_type", "WITHDRAWAL"]},
                            "$pay_date",
                            None
                        ]
                    }},
                    "sum_paid": {"$sum": {
                        "$cond": [
                            {"$ne": ["$payment_type", "WITHDRAWAL"]},
                            {"$toDouble": {"$ifNull": ["$amount", 0]}},
                            0
                        ]
                    }},
                    "sum_withdraw": {"$sum": {
                        "$cond": [
                            {"$eq": ["$payment_type", "WITHDRAWAL"]},
                            {"$toDouble": {"$ifNull": ["$amount", 0]}},
                            0
                        ]
                    }},
                    "count_non_withdraw": {"$sum": {
                        "$cond": [
                            {"$ne": ["$payment_type", "WITHDRAWAL"]},
                            1,
                            0
                        ]
                    }},
                }}
            ],
            "as": "pay_agg"
        }},
        {"$addFields": {
            "pay_agg": {"$ifNull": [{"$arrayElemAt": ["$pay_agg", 0]}, {}]},
        }},
        {"$addFields": {
            "last_payment_dt": "$pay_agg.last_non_withdraw",
            "sum_paid": {"$ifNull": ["$pay_agg.sum_paid", 0]},
            "sum_withdraw": {"$ifNull": ["$pay_agg.sum_withdraw", 0]},
            "count_non_withdraw": {"$ifNull": ["$pay_agg.count_non_withdraw", 0]},
            "net_paid": {"$subtract": [
                {"$ifNull": ["$pay_agg.sum_paid", 0]},
                {"$ifNull": ["$pay_agg.sum_withdraw", 0]}
            ]},
        }},
        {"$addFields": {
            "days_since_last_payment": {
                "$cond": [
                    {"$ifNull": ["$last_payment_dt", False]},
                    {"$dateDiff": {"startDate": "$last_payment_dt", "endDate": now_dt, "unit": "day"}},
                    None
                ]
            }
        }},
        {"$addFields": {
            "purchase_end_dates": {
                "$map": {
                    "input": {"$ifNull": ["$purchases", []]},
                    "as": "pp",
                    "in": {
                        "$dateFromString": {
                            "dateString": "$$pp.end_date",
                            "format": "%Y-%m-%d",
                            "onError": None,
                            "onNull": None
                        }
                    }
                }
            }
        }},
        {"$addFields": {
            "is_overdue_time": {
                "$anyElementTrue": {
                    "$map": {
                        "input": {"$ifNull": ["$purchase_end_dates", []]},
                        "as": "ed",
                        "in": {"$and": [
                            {"$ne": ["$$ed", None]},
                            {"$lt": ["$$ed", now_dt]}
                        ]}
                    }
                }
            }
        }},
        {"$addFields": {
            "stored_lifecycle": {"$toLower": {"$ifNull": ["$lifecycle_status", ""]}}
        }},
        {"$addFields": {
            "computed_status": {
                "$switch": {
                    "branches": [
                        {"case": {"$and": [
                            {"$gt": ["$total_debt", 0]},
                            {"$gte": ["$net_paid", "$total_debt"]}
                        ]}, "then": "Completed"},

                        {"case": {"$eq": ["$stored_lifecycle", "churned"]}, "then": "Churned"},
                        {"case": {"$eq": ["$stored_lifecycle", "closed"]}, "then": "Closed"},

                        {"case": {"$and": [
                            {"$eq": ["$is_overdue_time", True]},
                            {"$lt": ["$net_paid", "$total_debt"]}
                        ]}, "then": "Closed"},

                        {"case": {"$eq": ["$count_non_withdraw", 0]}, "then": "Dormant"},
                        {"case": {"$lte": ["$days_since_last_payment", 14]}, "then": "Active"},
                        {"case": {"$and": [
                            {"$gte": ["$days_since_last_payment", 15]},
                            {"$lte": ["$days_since_last_payment", 30]},
                        ]}, "then": "Inactive"},
                    ],
                    "default": "Dormant"
                }
            }
        }},
    ]

    total_customers = customers_col.count_documents(base_match)
    total_pages = max((total_customers + per_page - 1) // per_page, 1)
    if page > total_pages:
        page = total_pages

    page_pipeline = core_pipeline + [
        {"$sort": {"name": 1}},
        {"$skip": (page - 1) * per_page},
        {"$limit": per_page},
    ]
    page_docs = list(customers_col.aggregate(page_pipeline, allowDiskUse=True))

    counts_pipeline = core_pipeline + [
        {"$group": {"_id": "$computed_status", "count": {"$sum": 1}}}
    ]
    counts_docs = list(customers_col.aggregate(counts_pipeline, allowDiskUse=True))
    counts_map = {d["_id"]: int(d.get("count", 0)) for d in counts_docs if d.get("_id")}

    total_active    = counts_map.get("Active", 0)
    total_inactive  = counts_map.get("Inactive", 0)
    total_dormant   = counts_map.get("Dormant", 0)
    total_churned   = counts_map.get("Churned", 0)
    total_closed    = counts_map.get("Closed", 0)
    total_completed = counts_map.get("Completed", 0)

    total_not_active = total_inactive + total_dormant + total_churned + total_closed
    total_overdue = total_closed

    if status_filter:
        page_docs = [d for d in page_docs if (d.get("computed_status") or "").lower() == status_filter]

    agent_oids: List[ObjectId] = []
    for d in page_docs:
        aid = d.get("agent_id")
        if not aid:
            continue
        try:
            agent_oids.append(aid if isinstance(aid, ObjectId) else ObjectId(aid))
        except Exception:
            continue

    agent_map: Dict[str, Dict[str, Any]] = {}
    if agent_oids:
        for a in users_col.find({"_id": {"$in": list(set(agent_oids))}}, {"_id": 1, "name": 1, "branch": 1}):
            agent_map[str(a["_id"])] = {
                "name": a.get("name", "Agent"),
                "branch": a.get("branch")
            }

    tags_summary: Dict[str, Dict[str, Any]] = {}
    customers_out: List[Dict[str, Any]] = []

    for d in page_docs:
        cid = d["_id"]
        name = (d.get("name") or "").strip() or "Unknown"
        phone = d.get("phone_number") or "N/A"

        raw_agent_id = d.get("agent_id")
        agent_name = "Unassigned"
        agent_branch = None
        if raw_agent_id:
            agent_meta = agent_map.get(str(raw_agent_id), {})
            agent_name = agent_meta.get("name", "Unassigned")
            agent_branch = agent_meta.get("branch")

        initials = name[0].upper() if name else "?"

        normalized_tags: List[Dict[str, Any]] = []
        for t in (d.get("tags") or []):
            t_key = t.get("key") or _slugify(t.get("label", "tag"))
            t_label = t.get("label") or t_key.title()
            t_color = t.get("color") or "#6366f1"

            normalized_tags.append({"key": t_key, "label": t_label, "color": t_color})

            if t_key not in tags_summary:
                tags_summary[t_key] = {"key": t_key, "label": t_label, "color": t_color, "count": 0}
            tags_summary[t_key]["count"] += 1

        last_dt = d.get("last_payment_dt")
        last_payment_date = last_dt.strftime("%Y-%m-%d") if isinstance(last_dt, datetime) else None

        customers_out.append({
            "id": str(cid),
            "name": name,
            "phone": phone,
            "image_url": d.get("image_url", ""),
            "status": d.get("computed_status", "Dormant"),
            "agent_name": agent_name,
            "agent_branch": agent_branch,
            "initials": initials,
            "tags": normalized_tags,
            "last_payment_date": last_payment_date,
            "days_since_last_payment": d.get("days_since_last_payment", None),
        })

    available_tags = sorted(tags_summary.values(), key=lambda x: (x.get("label") or "").lower())

    return jsonify({
        "ok": True,
        "page": page,
        "per_page": per_page,
        "total_pages": total_pages,
        "total_customers": total_customers,
        "metrics": {
            "total_active": total_active,
            "total_inactive": total_inactive,
            "total_dormant": total_dormant,
            "total_churned": total_churned,
            "total_closed": total_closed,
            "total_overdue": total_overdue,
            "total_not_active": total_not_active,
            "total_no_payment": 0,
            "total_completed": total_completed,
        },
        "customers": customers_out,
        "available_tags": available_tags
    })


# ---------- Tagging: add / update a tag on a customer ----------
@customers_bp.route("/customer/<customer_id>/tags", methods=["POST"])
def add_customer_tag(customer_id):
    """
    Add or update a tag on a customer.
    Expected form fields:
      - label
      - color
      - note (optional)
    """
    manager_oid = _require_manager()
    if not manager_oid:
        return redirect(url_for("login.login"))

    label = (request.form.get("label") or "").strip()
    color = (request.form.get("color") or "").strip() or "#6366f1"
    note = (request.form.get("note") or "").strip()

    if not label:
        flash("Tag label is required.", "warning")
        return redirect(url_for("customers.customer_profile", customer_id=customer_id))

    tag_key = _slugify(label)

    try:
        cust_oid = ObjectId(customer_id)
    except Exception:
        flash("Invalid customer ID format.", "danger")
        return redirect(url_for("customers.customers_list"))

    existing = customers_col.find_one(
        {"_id": cust_oid, "tags.key": tag_key},
        {"tags.$": 1}
    )

    now = datetime.utcnow()

    if existing and existing.get("tags"):
        customers_col.update_one(
            {"_id": cust_oid, "tags.key": tag_key},
            {"$set": {
                "tags.$.label": label,
                "tags.$.color": color,
                "tags.$.note": note,
                "tags.$.updated_at": now,
                "tags.$.updated_by": manager_oid,
            }}
        )
    else:
        tag_doc = {
            "key": tag_key,
            "label": label,
            "color": color,
            "note": note,
            "created_at": now,
            "created_by": manager_oid,
        }
        customers_col.update_one({"_id": cust_oid}, {"$push": {"tags": tag_doc}})

    flash("Tag saved successfully.", "success")
    return redirect(url_for("customers.customer_profile", customer_id=customer_id))


# ================== Existing routes below (unchanged) ==================
@customers_bp.route("/customer/<customer_id>")
def customer_profile(customer_id):
    if "manager_id" not in session and "admin_id" not in session:
        return redirect(url_for("login.login"))

    try:
        customer_object_id = ObjectId(customer_id)
    except Exception:
        flash("Invalid customer ID format.", "danger")
        return redirect(url_for("customers.customers_list"))

    customer = customers_col.find_one({"_id": customer_object_id})
    if not customer:
        return "Customer not found", 404

    agent_doc = None
    agent_id_raw = customer.get("agent_id")
    if agent_id_raw:
        try:
            agent_oid = agent_id_raw if isinstance(agent_id_raw, ObjectId) else ObjectId(agent_id_raw)
            agent_doc = users_col.find_one({"_id": agent_oid}, {"name": 1, "branch": 1})
        except Exception:
            agent_doc = None
    customer["agent_name"] = (agent_doc or {}).get("name")
    customer["agent_branch"] = (agent_doc or {}).get("branch")

    purchases = customer.get("purchases", [])
    total_debt = sum(int(p.get("product", {}).get("total", 0)) for p in purchases)

    all_payments = list(payments_col.find({"customer_id": customer_object_id}))

    def _classify_susu_withdraw(p):
        if p.get("payment_type") != "WITHDRAWAL":
            return None
        method_raw = (p.get("method") or "").strip().lower()
        note_raw = (p.get("note") or "").strip().lower()
        is_cash = method_raw in ("susu withdrawal", "manual", "cash", "withdrawal", "susu cash")
        is_profit = method_raw in ("susu profit", "deduction", "susu deduction")
        if "susu" in note_raw:
            if "profit" in note_raw or "deduction" in note_raw:
                is_profit = True
            if "withdraw" in note_raw or "cash" in note_raw or "payout" in note_raw:
                is_cash = True
        if is_cash:
            return "cash"
        if is_profit:
            return "profit"
        return None

    product_deposits = [p for p in all_payments if p.get("payment_type") not in ("WITHDRAWAL", "SUSU")]
    withdrawals = [p for p in all_payments if p.get("payment_type") == "WITHDRAWAL"]
    product_withdrawals = [p for p in withdrawals if _classify_susu_withdraw(p) is None]

    susu_payments = [p for p in all_payments if p.get("payment_type") == "SUSU"]
    susu_withdrawals = [p for p in withdrawals if _classify_susu_withdraw(p) is not None]
    susu_withdraw_cash = sum(p.get("amount", 0) for p in susu_withdrawals if _classify_susu_withdraw(p) == "cash")
    susu_profit = sum(p.get("amount", 0) for p in susu_withdrawals if _classify_susu_withdraw(p) == "profit")

    deposits = sum(p.get("amount", 0) for p in product_deposits)
    withdrawn_amount = sum(p.get("amount", 0) for p in product_withdrawals)

    total_paid = round(deposits - withdrawn_amount, 2)
    withdrawn_amount = round(withdrawn_amount, 2)
    amount_left = round(total_debt - total_paid, 2)

    current_status = customer.get("status", "payment_ongoing")

    if current_status == "payment_ongoing" and amount_left <= 0:
        customers_col.update_one(
            {"_id": customer_object_id},
            {"$set": {"status": "completed", "status_updated_at": datetime.utcnow()}}
        )
        customer["status"] = "completed"
    else:
        customer["status"] = current_status

    def calculate_progress(purchase_date_str):
        try:
            purchase_date = datetime.strptime(purchase_date_str, "%Y-%m-%d")
        except (ValueError, TypeError):
            return {"progress": 0, "end_date": "N/A"}

        end_date = purchase_date + timedelta(days=180)
        today = datetime.now()
        total_days = (end_date - purchase_date).days
        elapsed_days = (today - purchase_date).days
        progress = max(0, min(100, round((elapsed_days / total_days) * 100))) if total_days > 0 else 0
        return {"progress": progress, "end_date": end_date.strftime("%Y-%m-%d")}

    penalties = customer.get("penalties", [])
    total_penalty = round(sum(p.get("amount", 0) for p in penalties), 2)

    # Decorate purchases with per-product paid/left & time progress (match agent view)
    for index, purchase in enumerate(purchases):
        purchase_date = purchase.get("purchase_date")
        tracking = calculate_progress(purchase_date)
        purchase["progress"] = tracking["progress"]
        purchase["end_date"] = tracking["end_date"]

        product_total = float(purchase.get("product", {}).get("total", 0))
        product_payments = [p for p in product_deposits if p.get("product_index") == index]
        product_paid = sum(p.get("amount", 0) for p in product_payments)
        product_left = max(0, round(product_total - product_paid, 2))

        purchase["amount_paid"] = product_paid
        purchase["amount_left"] = product_left
        purchase["can_submit"] = (product_left == 0)
        if purchase.get("product", {}).get("status") == "closed":
            purchase["can_submit"] = False

    customer["status"] = current_status
    customer["_id"] = str(customer["_id"])

    susu_total = round(sum(p.get("amount", 0) for p in susu_payments), 2)
    susu_withdrawn = round(susu_withdraw_cash + susu_profit, 2)
    susu_left = round(susu_total - susu_withdrawn, 2)

    return render_template(
        "customer_profile.html",
        customer=customer,
        total_debt=total_debt,
        total_paid=total_paid,
        withdrawn_amount=withdrawn_amount,
        amount_left=amount_left,
        payments=product_deposits,
        withdrawals=withdrawals,
        penalties=penalties,
        susu_total=susu_total,
        susu_withdrawn=susu_withdrawn,
        susu_left=susu_left,
        susu_payments=susu_payments,
        susu_withdrawals=susu_withdrawals
    )


@customers_bp.route("/customer/<customer_id>/approve_status", methods=["POST"])
def approve_customer_status(customer_id):
    if "manager_id" not in session:
        return redirect(url_for("login.login"))

    try:
        customer_object_id = ObjectId(customer_id)
        manager_id = ObjectId(session["manager_id"])
    except Exception:
        flash("Invalid customer ID format.", "danger")
        return redirect(url_for("customers.customers_list"))

    customer = customers_col.find_one({"_id": customer_object_id})
    if not customer:
        flash("Customer not found.", "danger")
        return redirect(url_for("customers.customers_list"))

    current_status = customer.get("status", "payment_ongoing")
    if current_status != "completed":
        flash("Customer must be in 'completed' status to approve.", "warning")
        return redirect(url_for("customers.customer_profile", customer_id=customer_id))

    purchases = customer.get("purchases", [])

    for p in purchases:
        product = p.get("product", {})
        end_date_str = p.get("end_date")
        if end_date_str:
            try:
                end_date = datetime.strptime(end_date_str, "%Y-%m-%d").date()
                purchase_date = datetime.strptime(p.get("purchase_date", "")[:10], "%Y-%m-%d").date()
                today = date.today()
                total_days = (end_date - purchase_date).days
                elapsed_days = (today - purchase_date).days
                progress = int((elapsed_days / total_days) * 100) if total_days > 0 else 100
                product["progress"] = max(0, min(progress, 100))
            except Exception:
                product["progress"] = None
        else:
            product["progress"] = None

    insufficient_items = []

    for purchase in purchases:
        product = purchase.get("product")
        quantity = int(product.get("quantity", 1))
        components = product.get("components", [])

        for component in components:
            comp_id = ObjectId(component["_id"]) if isinstance(component["_id"], str) else component["_id"]
            required_qty = int(component["quantity"]) * quantity

            inventory_item = db.inventory.find_one({
                "_id": comp_id,
                "manager_id": manager_id
            })

            if not inventory_item or inventory_item.get("qty", 0) < required_qty:
                insufficient_items.append({
                    "component_id": str(comp_id),
                    "needed": required_qty,
                    "available": inventory_item.get("qty", 0) if inventory_item else 0
                })

    if insufficient_items:
        details = "; ".join(
            [f"ID: {item['component_id']} (Need: {item['needed']}, Have: {item['available']})"
             for item in insufficient_items]
        )
        flash(f"Cannot approve. Not enough inventory for components: {details}", "danger")
        return redirect(url_for("customers.customer_profile", customer_id=customer_id))

    # Deduct inventory
    for purchase in purchases:
        product = purchase.get("product")
        quantity = int(product.get("quantity", 1))
        components = product.get("components", [])

        for component in components:
            comp_id = ObjectId(component["_id"]) if isinstance(component["_id"], str) else component["_id"]
            used_qty = int(component["quantity"]) * quantity

            db.inventory.update_one(
                {"_id": comp_id, "manager_id": manager_id},
                {"$inc": {"qty": -used_qty}}
            )

    customers_col.update_one(
        {"_id": customer_object_id},
        {"$set": {"status": "approved", "status_updated_at": datetime.utcnow()}}
    )

    flash("Customer status updated to 'approved' and inventory adjusted.", "success")
    return redirect(url_for("customers.customer_profile", customer_id=customer_id))


@customers_bp.route("/customer/<customer_id>/edit", methods=["POST"])
def edit_customer(customer_id):
    if "manager_id" not in session:
        return redirect(url_for("login.login"))

    form_data = {
        "name": request.form.get("name"),
        "phone_number": request.form.get("phone_number"),
        "location": request.form.get("location"),
        "occupation": request.form.get("occupation"),
        "comment": request.form.get("comment"),
    }

    result = customers_col.update_one({"_id": ObjectId(customer_id)}, {"$set": form_data})
    flash("Customer updated successfully!" if result.modified_count else "No changes made.", "success")
    return redirect(url_for("customers.customer_profile", customer_id=customer_id))


@customers_bp.route("/customer/<customer_id>/delete", methods=["POST"])
def delete_customer(customer_id):
    if "manager_id" not in session:
        return redirect(url_for("login.login"))

    customer = customers_col.find_one({"_id": ObjectId(customer_id)})
    if not customer:
        flash("Customer not found", "danger")
        return redirect(url_for("customers.customers_list"))

    deleted_col.insert_one(customer)
    customers_col.delete_one({"_id": ObjectId(customer_id)})
    flash("Customer deleted and archived.", "info")
    return redirect(url_for("customers.customers_list"))


@customers_bp.route("/customer/<customer_id>/withdraw", methods=["POST"])
def withdraw_from_customer(customer_id):
    if "manager_id" not in session:
        return redirect(url_for("login.login"))

    customer = customers_col.find_one({"_id": ObjectId(customer_id)})
    if not customer:
        flash("Customer not found.", "danger")
        return redirect(url_for("customers.customers_list"))

    agent_id = customer.get("agent_id")
    manager_id = ObjectId(session["manager_id"])

    # Combined mode (new modal with both fields)
    if request.form.get("combined_mode"):
        try:
            withdraw_amount = float(request.form.get("withdraw_amount", 0))
        except (TypeError, ValueError):
            withdraw_amount = 0

        try:
            deduction_amount = float(request.form.get("deduction_amount", 0))
        except (TypeError, ValueError):
            deduction_amount = 0

        note = request.form.get("note", "").strip()

        if withdraw_amount <= 0 and deduction_amount <= 0:
            flash("Please enter at least one valid amount.", "warning")
            return redirect(url_for("customers.customer_profile", customer_id=customer_id))

        now_str = datetime.utcnow().strftime("%Y-%m-%d")
        now_ts = datetime.utcnow()

        if withdraw_amount > 0:
            payments_col.insert_one({
                "manager_id": manager_id,
                "agent_id": agent_id,
                "customer_id": ObjectId(customer_id),
                "amount": withdraw_amount,
                "payment_type": "WITHDRAWAL",
                "method": "Manual",
                "note": note,
                "date": now_str,
                "timestamp": now_ts
            })

        if deduction_amount > 0:
            payments_col.insert_one({
                "manager_id": manager_id,
                "agent_id": agent_id,
                "customer_id": ObjectId(customer_id),
                "amount": deduction_amount,
                "payment_type": "WITHDRAWAL",
                "method": "Deduction",
                "note": "SUSU deduction" if not note else note,
                "date": now_str,
                "timestamp": now_ts
            })

        flash("✅ Withdrawal and/or Deduction recorded successfully.", "success")
        return redirect(url_for("customers.customer_profile", customer_id=customer_id))

    # Fallback: Old single withdrawal or deduction (for backward compatibility)
    try:
        amount = float(request.form.get("amount"))
    except (TypeError, ValueError):
        flash("Invalid amount entered.", "danger")
        return redirect(url_for("customers.customer_profile", customer_id=customer_id))

    if amount <= 0:
        flash("Enter a positive amount.", "danger")
        return redirect(url_for("customers.customer_profile", customer_id=customer_id))

    is_deduction = request.form.get("deduction_only") == "true"

    payment_record = {
        "manager_id": manager_id,
        "agent_id": agent_id,
        "customer_id": ObjectId(customer_id),
        "amount": amount,
        "payment_type": "WITHDRAWAL",
        "method": "Deduction" if is_deduction else "Manual",
        "note": "SUSU deduction" if is_deduction else "",
        "date": datetime.utcnow().strftime("%Y-%m-%d"),
        "timestamp": datetime.utcnow()
    }

    payments_col.insert_one(payment_record)

    flash("✅ Deduction recorded successfully." if is_deduction else "✅ Withdrawal recorded successfully.", "success")
    return redirect(url_for("customers.customer_profile", customer_id=customer_id))


@customers_bp.route("/customer/<customer_id>/add_penalty/<int:purchase_index>", methods=["POST"])
def add_penalty(customer_id, purchase_index):
    if "manager_id" not in session:
        return redirect(url_for("login.login"))

    try:
        customer_object_id = ObjectId(customer_id)
        customer = customers_col.find_one({"_id": customer_object_id})
        if not customer:
            flash("Customer not found.", "danger")
            return redirect(url_for("customers.customer_profile", customer_id=customer_id))

        amount = float(request.form.get("amount", 0))
        reason = request.form.get("reason")
        new_end_date = request.form.get("new_end_date")

        if amount <= 0 or not reason:
            flash("Amount and reason are required. Amount must be greater than 0.", "danger")
            return redirect(url_for("customers.customer_profile", customer_id=customer_id))

        penalty = {
            "amount": amount,
            "reason": reason,
            "date": datetime.utcnow().isoformat()
        }

        if new_end_date:
            penalty["new_end_date"] = new_end_date
            purchases = customer.get("purchases", [])
            if 0 <= purchase_index < len(purchases):
                purchases[purchase_index]["end_date"] = new_end_date
                customers_col.update_one(
                    {"_id": customer_object_id},
                    {"$set": {"purchases": purchases}}
                )

        customers_col.update_one(
            {"_id": customer_object_id},
            {"$push": {"penalties": penalty}}
        )

        flash("Penalty added successfully.", "success")
        return redirect(url_for("customers.customer_profile", customer_id=customer_id))

    except Exception as e:
        print("Error adding penalty:", str(e))
        flash("An error occurred while adding penalty.", "danger")
        return redirect(url_for("customers.customer_profile", customer_id=customer_id))

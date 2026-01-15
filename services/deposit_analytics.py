from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from db import db

manager_deposits_col = db["manager_deposits"]


def _sum_amount(match: Dict[str, Any]) -> float:
    pipeline = [
        {"$match": match},
        {"$group": {"_id": None, "total": {"$sum": {"$toDouble": {"$ifNull": ["$amount", 0]}}}}},
    ]
    row = next(manager_deposits_col.aggregate(pipeline), None)
    return float(row.get("total", 0.0) if row else 0.0)


def _group_sum(match: Dict[str, Any], field: str) -> List[Dict[str, Any]]:
    pipeline = [
        {"$match": match},
        {"$group": {"_id": f"${field}", "total": {"$sum": {"$toDouble": {"$ifNull": ["$amount", 0]}}}}},
        {"$sort": {"total": -1}},
    ]
    rows = list(manager_deposits_col.aggregate(pipeline))
    out = []
    for r in rows:
        key = r.get("_id") or "Unassigned"
        out.append({"key": key, "total": float(r.get("total", 0.0) or 0.0)})
    return out


def _daily_trend(match: Dict[str, Any], start_dt: datetime, end_dt: datetime) -> List[Dict[str, Any]]:
    pipeline = [
        {"$match": {**match, "created_at": {"$gte": start_dt, "$lte": end_dt}}},
        {"$group": {
            "_id": {"$dateToString": {"format": "%Y-%m-%d", "date": "$created_at"}},
            "total": {"$sum": {"$toDouble": {"$ifNull": ["$amount", 0]}}},
        }},
        {"$sort": {"_id": 1}},
    ]
    rows = list(manager_deposits_col.aggregate(pipeline))
    by_date = {r["_id"]: float(r.get("total", 0.0) or 0.0) for r in rows}

    days = []
    current = start_dt
    while current.date() <= end_dt.date():
        key = current.strftime("%Y-%m-%d")
        days.append({"date": key, "total": float(by_date.get(key, 0.0))})
        current += timedelta(days=1)
    return days


def _parse_date(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        d = datetime.strptime(value, "%Y-%m-%d").date()
        return datetime(d.year, d.month, d.day, tzinfo=timezone.utc)
    except Exception:
        return None


def _validate_range(start_dt: datetime, end_dt: datetime) -> Tuple[bool, str]:
    if end_dt < start_dt:
        return False, "Start date must be before end date."
    if (end_dt - start_dt).days > 365:
        return False, "Custom range cannot exceed 365 days."
    return True, ""


def compute_deposit_analytics(
    branch_name: Optional[str] = None,
    custom_start: Optional[str] = None,
    custom_end: Optional[str] = None,
) -> Dict[str, Any]:
    now = datetime.now(timezone.utc)
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    week_start = (now - timedelta(days=now.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
    month_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    trend_start = (now - timedelta(days=13)).replace(hour=0, minute=0, second=0, microsecond=0)

    base_match: Dict[str, Any] = {"status": "approved"}
    if branch_name:
        base_match["branch_name"] = branch_name

    today_total = _sum_amount({**base_match, "created_at": {"$gte": today_start, "$lte": now}})
    week_total = _sum_amount({**base_match, "created_at": {"$gte": week_start, "$lte": now}})
    month_total = _sum_amount({**base_match, "created_at": {"$gte": month_start, "$lte": now}})

    by_branch_raw = _group_sum({**base_match, "created_at": {"$gte": month_start, "$lte": now}}, "branch_name")
    by_branch = [{"branch": r["key"], "total": r["total"]} for r in by_branch_raw]

    by_method_raw = _group_sum({**base_match, "created_at": {"$gte": month_start, "$lte": now}}, "method_type")
    by_method = [{"method": r["key"], "total": r["total"]} for r in by_method_raw]

    daily_trend = _daily_trend(base_match, trend_start, now)

    custom_total = 0.0
    custom_count = 0
    custom_error = ""
    custom_start_dt = _parse_date(custom_start)
    custom_end_dt = _parse_date(custom_end)
    if custom_start_dt and custom_end_dt:
        custom_end_dt = custom_end_dt.replace(hour=23, minute=59, second=59, microsecond=999999)
        ok, msg = _validate_range(custom_start_dt, custom_end_dt)
        if ok:
            custom_match = {
                **base_match,
                "created_at": {"$gte": custom_start_dt, "$lte": custom_end_dt},
            }
            custom_total = _sum_amount(custom_match)
            custom_count = manager_deposits_col.count_documents(custom_match)
        else:
            custom_error = msg

    return {
        "today_total": today_total,
        "week_total": week_total,
        "month_total": month_total,
        "by_branch": by_branch,
        "daily_trend": daily_trend,
        "by_method": by_method,
        "custom_total": custom_total,
        "custom_count": custom_count,
        "custom_error": custom_error,
        "custom_start": custom_start or "",
        "custom_end": custom_end or "",
    }

# executive_expense.py
from flask import Blueprint, render_template, request, jsonify, session, redirect, url_for
from bson.objectid import ObjectId
from datetime import datetime, timedelta
from db import db

executive_expense_bp = Blueprint("executive_expense", __name__, url_prefix="/executive-expense")

users_col    = db["users"]
expenses_col = db["manager_expenses"]

# ---------- helpers ----------
def _today_str():
    return datetime.utcnow().strftime("%Y-%m-%d")

def _current_exec_session():
    if session.get("executive_id"):
        return "executive_id", session["executive_id"]
    if session.get("admin_id"):   # allow admin to act as exec if needed
        return "admin_id", session["admin_id"]
    return None, None

def _ensure_exec_or_redirect():
    _, uid = _current_exec_session()
    if not uid: return redirect(url_for("login.login"))
    try:
        user = users_col.find_one({"_id": ObjectId(uid)})
    except Exception:
        user = users_col.find_one({"_id": uid})
    if not user: return redirect(url_for("login.login"))
    role = (user.get("role") or "").lower()
    if role not in ("executive", "admin"):
        return redirect(url_for("login.login"))
    return str(user["_id"]), user

def _range_dates(key: str):
    now = datetime.utcnow()
    if key == "today":
        start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        return start, start + timedelta(days=1)
    if key == "week":
        start = (now - timedelta(days=now.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
        return start, now
    if key == "month":
        start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        end = start.replace(year=start.year+1, month=1) if start.month == 12 else start.replace(month=start.month+1)
        return start, end
    if key == "last7":  return now - timedelta(days=7),  now
    if key == "last30": return now - timedelta(days=30), now
    return now - timedelta(days=30), now

# ---------- page ----------
@executive_expense_bp.route("/", methods=["GET"])
def exec_expense_page():
    scope = _ensure_exec_or_redirect()
    if not isinstance(scope, tuple): return scope
    exec_id, exec_doc = scope

    # managers for filter dropdown
    managers = list(users_col.find({"role": "manager"}, {"_id": 1, "name": 1, "branch": 1}).sort("name", 1))
    m_opts = [{"_id": str(m["_id"]), "name": m.get("name","Manager"), "branch": m.get("branch","")} for m in managers]

    # latest 30 days table initial (all statuses, all managers)
    start, end = _range_dates("last30")
    cursor = expenses_col.find(
        {"created_at": {"$gte": start, "$lte": end}},
        {"_id":1,"manager_id":1,"date":1,"time":1,"category":1,"description":1,"amount":1,"status":1}
    ).sort([("status", 1), ("created_at", -1)]).limit(100)

    rows = []
    # prefetch manager names/branches map
    mids = list({c.get("manager_id") for c in cursor})
    # need to re-run cursor (it was consumed), so fetch again
    cursor = expenses_col.find(
        {"created_at": {"$gte": start, "$lte": end}},
        {"_id":1,"manager_id":1,"date":1,"time":1,"category":1,"description":1,"amount":1,"status":1}
    ).sort([("status", 1), ("created_at", -1)]).limit(100)

    m_map = {}
    if mids:
        # convert to ObjectId where possible in one pass
        obj_ids, str_ids = [], []
        for mid in mids:
            try:
                obj_ids.append(ObjectId(mid))
            except Exception:
                str_ids.append(mid)
        if obj_ids:
            for u in users_col.find({"_id": {"$in": obj_ids}}, {"_id":1,"name":1,"branch":1}):
                m_map[str(u["_id"])] = {"name": u.get("name","Manager"), "branch": u.get("branch","")}
        # if any str_ids match as strings (unlikely), attempt direct match
        if str_ids:
            for u in users_col.find({"_id": {"$in": str_ids}}, {"_id":1,"name":1,"branch":1}):
                m_map[str(u["_id"])] = {"name": u.get("name","Manager"), "branch": u.get("branch","")}

    for d in cursor:
        amt = float(d.get("amount", 0) or 0)
        mid = d.get("manager_id","")
        info = m_map.get(mid, {"name":"Manager","branch":""})
        rows.append({
            "_id": str(d["_id"]),
            "manager_name": info["name"],
            "branch": info["branch"],
            "date": d.get("date",""),
            "time": d.get("time",""),
            "category": d.get("category",""),
            "description": d.get("description",""),
            "status": d.get("status","Unapproved"),
            "amount": f"{amt:,.2f}"
        })

    return render_template(
        "executive_expense.html",
        executive_name=exec_doc.get("name","Executive"),
        managers=m_opts,
        today=_today_str(),
        rows=rows
    )

# ---------- stats ----------
@executive_expense_bp.route("/stats", methods=["GET"])
def exec_stats():
    """
    Query:
      range=today|week|month|last7|last30
      status=All|Approved|Unapproved
      manager_id=<id or empty for all>
      group=category|branch  (default: category)
    """
    scope = _ensure_exec_or_redirect()
    if not isinstance(scope, tuple): return jsonify(ok=False, message="Please log in."), 401

    rng      = request.args.get("range", "last30")
    status   = (request.args.get("status") or "All").title()
    manager  = request.args.get("manager_id") or ""
    group_by = (request.args.get("group") or "category").lower()
    if status not in ("All","Approved","Unapproved"): status = "All"
    if group_by not in ("category","branch"): group_by = "category"

    start, end = _range_dates(rng)

    match = {"created_at": {"$gte": start, "$lte": end}}
    if status != "All": match["status"] = status
    if manager:
        match["manager_id"] = manager

    pipeline = [{"$match": match}]

    if group_by == "branch":
        # join with users to get branch from manager_id (string→ObjectId)
        pipeline += [
            {"$lookup":{
                "from":"users",
                "let":{"mid":"$manager_id"},
                "pipeline":[
                    {"$match":{"$expr":{"$eq":["$_id", {"$toObjectId":"$$mid"}]}}},
                    {"$project":{"branch":1}}
                ],
                "as":"mgr"
            }},
            {"$unwind":{"path":"$mgr", "preserveNullAndEmptyArrays": True}},
            {"$group":{
                "_id": {"$ifNull":["$mgr.branch","Unknown"]},
                "sum_amount":{"$sum":{"$toDouble":{"$ifNull":["$amount",0]}}}
            }},
            {"$sort":{"sum_amount":-1}}
        ]
    else:
        pipeline += [
            {"$group":{
                "_id":"$category",
                "sum_amount":{"$sum":{"$toDouble":{"$ifNull":["$amount",0]}}}
            }},
            {"$sort":{"sum_amount":-1}}
        ]

    agg = list(expenses_col.aggregate(pipeline))
    by_group = [{"name": a["_id"], "total": float(a["sum_amount"])} for a in agg]
    total = sum(x["total"] for x in by_group)
    return jsonify(ok=True, total=round(total,2), items=by_group[:20])

# ---------- list (for table refresh with filters) ----------
@executive_expense_bp.route("/list", methods=["GET"])
def exec_list():
    scope = _ensure_exec_or_redirect()
    if not isinstance(scope, tuple): return jsonify(ok=False, message="Please log in."), 401

    rng     = request.args.get("range","last30")
    status  = (request.args.get("status") or "All").title()
    manager = request.args.get("manager_id") or ""
    start, end = _range_dates(rng)

    q = {"created_at":{"$gte":start,"$lte":end}}
    if status != "All": q["status"] = status
    if manager: q["manager_id"] = manager

    cursor = expenses_col.find(
        q, {"_id":1,"manager_id":1,"date":1,"time":1,"category":1,
            "description":1,"amount":1,"status":1}
    ).sort([("status",1),("created_at",-1)]).limit(300)

    # map managers
    mids = list({doc.get("manager_id") for doc in cursor})
    cursor = expenses_col.find(q, {"_id":1,"manager_id":1,"date":1,"time":1,"category":1,
                                   "description":1,"amount":1,"status":1}
             ).sort([("status",1),("created_at",-1)]).limit(300)
    m_map = {}
    if mids:
        obj_ids=[]
        for mid in mids:
            try: obj_ids.append(ObjectId(mid))
            except: pass
        if obj_ids:
            for u in users_col.find({"_id":{"$in":obj_ids}}, {"_id":1,"name":1,"branch":1}):
                m_map[str(u["_id"])] = {"name":u.get("name","Manager"), "branch":u.get("branch","")}

    rows=[]
    for d in cursor:
        info = m_map.get(d.get("manager_id",""), {"name":"Manager","branch":""})
        rows.append({
            "_id": str(d["_id"]),
            "manager_name": info["name"],
            "branch": info["branch"],
            "date": d.get("date",""),
            "time": d.get("time",""),
            "category": d.get("category",""),
            "description": d.get("description",""),
            "status": d.get("status","Unapproved"),
            "amount": f"{float(d.get('amount',0) or 0):,.2f}"
        })
    return jsonify(ok=True, rows=rows)

# ---------- approve / delete ----------
@executive_expense_bp.route("/approve", methods=["POST"])
def approve_expense():
    scope = _ensure_exec_or_redirect()
    if not isinstance(scope, tuple): return jsonify(ok=False, message="Please log in."), 401
    exec_id, exec_doc = scope

    eid = request.form.get("id") or (request.json.get("id") if request.is_json else "")
    if not eid: return jsonify(ok=False, message="Missing expense id."), 400

    try:
        res = expenses_col.update_one(
            {"_id": ObjectId(eid), "status": {"$ne":"Approved"}},
            {"$set":{
                "status":"Approved",
                "approved_by": exec_id,
                "approved_at": datetime.utcnow(),
                "updated_at": datetime.utcnow()
            }}
        )
        if res.modified_count == 1:
            return jsonify(ok=True, message="Expense approved.")
        return jsonify(ok=False, message="Already approved or not found.")
    except Exception:
        return jsonify(ok=False, message="Invalid expense id."), 400

@executive_expense_bp.route("/delete", methods=["POST"])
def delete_expense():
    scope = _ensure_exec_or_redirect()
    if not isinstance(scope, tuple): return jsonify(ok=False, message="Please log in."), 401

    eid = request.form.get("id") or (request.json.get("id") if request.is_json else "")
    if not eid: return jsonify(ok=False, message="Missing expense id."), 400
    try:
        res = expenses_col.delete_one({"_id": ObjectId(eid)})
        if res.deleted_count == 1:
            return jsonify(ok=True, message="Expense deleted.")
        return jsonify(ok=False, message="Expense not found.")
    except Exception:
        return jsonify(ok=False, message="Invalid expense id."), 400
   
# hr_backend/hr_files.py

from datetime import datetime
from typing import Any, Dict, List

from flask import jsonify, request, redirect
from bson import ObjectId
from werkzeug.utils import secure_filename
import requests
import traceback

from db import db
from hr_backend.hr_dashboard import hr_bp, _hr_access_guard

users_col = db["users"]

# -------------------------------
# Cloudflare (same as other modules)
# -------------------------------
CF_ACCOUNT_ID   = "63e6f91eec9591f77699c4b434ab44c6"
CF_IMAGES_TOKEN = "Brz0BEfl_GqEUjEghS2UEmLZhK39EUmMbZgu_hIo"
CF_HASH         = "h9fmMoa1o2c2P55TcWJGOg"
DEFAULT_VARIANT = "public"

HR_FILES_ALLOWED_EXT = {"png", "jpg", "jpeg", "gif"}  # CV scans, ID cards, etc.


def _today_str() -> str:
    return datetime.utcnow().strftime("%Y-%m-%d")


def _serialize_hr_file(raw: Any) -> Dict[str, Any]:
    """
    Normalize HR file record so frontend always gets consistent keys.
    """
    if isinstance(raw, dict):
        doc = raw
    else:
        doc = {"name": str(raw) if raw is not None else ""}

    created = doc.get("created_at") or ""
    if isinstance(created, datetime):
        created_str = created.strftime("%Y-%m-%d %H:%M")
    else:
        created_str = str(created) if created else ""

    return {
        "id": str(doc.get("_id")) if doc.get("_id") else None,
        "name": doc.get("name", ""),
        "description": doc.get("description", ""),
        "url": doc.get("url", ""),
        "original_filename": doc.get("original_filename", ""),
        "mimetype": doc.get("mimetype", ""),
        "created_at": created_str,
    }


def _allowed_hr_file(filename: str) -> bool:
    return (
        "." in filename
        and filename.rsplit(".", 1)[1].lower() in HR_FILES_ALLOWED_EXT
    )


# -------------------------------
# GET: HR files list
# -------------------------------
@hr_bp.route(
    "/employee/<employee_id>/hr_files",
    methods=["GET"],
    endpoint="get_hr_files",
)
def get_hr_files(employee_id):
    if not _hr_access_guard():
        return jsonify(ok=False, message="Unauthorized"), 401

    try:
        oid = ObjectId(employee_id)
    except Exception:
        return jsonify(ok=False, message="Invalid employee ID"), 400

    emp = users_col.find_one({"_id": oid}, {"hr_files": 1})
    if not emp:
        return jsonify(ok=False, message="Employee not found"), 404

    raw_list = emp.get("hr_files") or []
    if not isinstance(raw_list, list):
        raw_list = [raw_list]

    files = [_serialize_hr_file(f) for f in raw_list]

    return jsonify(ok=True, files=files)


# -------------------------------
# POST: Upload HR file (Cloudflare)
# -------------------------------
@hr_bp.route(
    "/employee/<employee_id>/hr_files/upload",
    methods=["POST"],
    endpoint="upload_hr_file",
)
def upload_hr_file(employee_id):
    """
    Upload CV / HR image scans to Cloudflare Images and store reference on user.
    """
    if not _hr_access_guard():
        return jsonify(ok=False, message="Unauthorized"), 401

    try:
        oid = ObjectId(employee_id)
    except Exception:
        return jsonify(ok=False, message="Invalid employee ID"), 400

    # Simple guard: employee must exist
    emp = users_col.find_one({"_id": oid}, {"_id": 1})
    if not emp:
        return jsonify(ok=False, message="Employee not found"), 404

    name = (request.form.get("name") or "").strip()
    description = (request.form.get("description") or "").strip()

    if "file" not in request.files:
        return jsonify(ok=False, message="No file part in request."), 400

    f = request.files["file"]
    if f.filename == "":
        return jsonify(ok=False, message="No file selected."), 400

    if not _allowed_hr_file(f.filename):
        return jsonify(
            ok=False,
            message="Only image files are allowed (png, jpg, jpeg, gif).",
        ), 400

    if not name:
        # Use original filename as fallback name
        name = f.filename

    try:
        # Step 1: Cloudflare direct upload URL
        direct_url = (
            f"https://api.cloudflare.com/client/v4/accounts/"
            f"{CF_ACCOUNT_ID}/images/v2/direct_upload"
        )
        headers = {"Authorization": f"Bearer {CF_IMAGES_TOKEN}"}
        data = {}  # could add metadata later if needed

        res = requests.post(direct_url, headers=headers, data=data, timeout=20)
        j = res.json()
        if not j.get("success"):
            return (
                jsonify(
                    ok=False,
                    message="Cloudflare direct_upload failed",
                    details=j,
                ),
                502,
            )

        upload_url = j["result"]["uploadURL"]
        image_id = j["result"]["id"]

        # Step 2: upload actual file to Cloudflare
        up = requests.post(
            upload_url,
            files={
                "file": (
                    secure_filename(f.filename),
                    f.stream,
                    f.mimetype or "application/octet-stream",
                )
            },
            timeout=60,
        )
        uj = up.json()
        if not uj.get("success"):
            return (
                jsonify(
                    ok=False,
                    message="Cloudflare upload failed",
                    details=uj,
                ),
                502,
            )

        variant = request.args.get("variant", DEFAULT_VARIANT)
        image_url = f"https://imagedelivery.net/{CF_HASH}/{image_id}/{variant}"

        now = datetime.utcnow()
        file_doc: Dict[str, Any] = {
            "_id": ObjectId(),
            "name": name,
            "description": description,
            "image_id": image_id,
            "variant": variant,
            "url": image_url,
            "original_filename": secure_filename(f.filename),
            "mimetype": f.mimetype,
            "created_at": now,
        }

        users_col.update_one(
            {"_id": oid},
            {
                "$push": {"hr_files": file_doc},
                "$set": {"updated_at": now},
            },
        )

        return jsonify(
            ok=True,
            message="HR file uploaded.",
            file=_serialize_hr_file(file_doc),
        )

    except Exception as e:
        traceback.print_exc()
        return jsonify(ok=False, message=str(e)), 500


# -------------------------------
# GET: Download / open HR file (redirect to Cloudflare)
# -------------------------------
@hr_bp.route(
    "/employee/<employee_id>/hr_files/<file_id>/download",
    methods=["GET"],
    endpoint="download_hr_file",
)
def download_hr_file(employee_id, file_id):
    """
    Redirects to Cloudflare URL. Browser can 'Save as' or 'Print to PDF'.
    """
    if not _hr_access_guard():
        return jsonify(ok=False, message="Unauthorized"), 401

    try:
        oid = ObjectId(employee_id)
        fid = ObjectId(file_id)
    except Exception:
        return jsonify(ok=False, message="Invalid IDs supplied"), 400

    emp = users_col.find_one(
        {"_id": oid, "hr_files._id": fid},
        {"hr_files.$": 1},
    )
    if not emp or "hr_files" not in emp or not emp["hr_files"]:
        return jsonify(ok=False, message="File not found"), 404

    file_doc = emp["hr_files"][0]
    url = file_doc.get("url")
    if not url:
        return jsonify(ok=False, message="File URL missing"), 404

    # Simple redirect – let browser handle download/print/PDF
    return redirect(url)

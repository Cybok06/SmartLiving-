from __future__ import annotations

from flask import Blueprint, render_template, request, redirect, url_for, flash, session, jsonify
from bson import ObjectId
from datetime import datetime, timedelta
import random
import time
import bcrypt as bcrypt_lib
import requests
from urllib.parse import quote

from db import db

auth_password_reset_bp = Blueprint("auth_password_reset", __name__, url_prefix="/auth")

users_col = db["users"]
resets_col = db["password_resets"]

# ✅ HARD-CODED (as requested)
ARKESEL_API_KEY = "b3dheEVqUWNyeVBuUGxDVWFxZ0E"

_LOOKUP_LIMIT = 10
_LOOKUP_WINDOW_SEC = 60
_LOOKUP_IP_BUCKET: dict[str, list[float]] = {}


def _normalize_phone(raw: str | None) -> str | None:
    if not raw:
        return None
    p = raw.strip().replace(" ", "").replace("-", "").replace("+", "")
    if p.startswith("0") and len(p) == 10:
        p = "233" + p[1:]
    if p.startswith("233") and len(p) == 12:
        return p
    return None


def _mask_phone(raw: str | None) -> str:
    if not raw:
        return ""
    digits = raw.strip().replace(" ", "").replace("-", "").replace("+", "")
    if len(digits) <= 4:
        return digits
    return digits[:4] + "*" * max(0, len(digits) - 7) + digits[-3:]


def _get_client_ip() -> str:
    return (request.headers.get("X-Forwarded-For", "").split(",")[0].strip() or request.remote_addr or "")


def _can_lookup(ip: str) -> bool:
    now = time.time()
    bucket = _LOOKUP_IP_BUCKET.get(ip, [])
    bucket = [ts for ts in bucket if now - ts < _LOOKUP_WINDOW_SEC]
    if len(bucket) >= _LOOKUP_LIMIT:
        _LOOKUP_IP_BUCKET[ip] = bucket
        return False
    bucket.append(now)
    _LOOKUP_IP_BUCKET[ip] = bucket
    return True


def _parse_arkesel_status(http_status: int | None, body: str) -> str:
    """
    Returns: sent / failed / error
    We still store raw body in DB for debugging.
    """
    if http_status is None:
        return "error"
    if http_status == 200 and body and '"code":"ok"' in body:
        return "sent"
    return "failed"


def _send_sms_arkesel(phone: str, message: str) -> tuple[str, int | None, str, str]:
    """
    ✅ Always returns:
    (status, http_status, response_text, sms_url)
    And prints log for server console.
    """
    sms_url = (
        "https://sms.arkesel.com/sms/api?action=send-sms"
        f"&api_key={ARKESEL_API_KEY}"
        f"&to={phone}"
        "&from=SMARTLIVING"
        f"&sms={quote(message)}"
    )

    try:
        resp = requests.get(sms_url, timeout=15)
        body = resp.text or ""
        status = _parse_arkesel_status(resp.status_code, body)

        # ✅ LOG FOR DEBUGGING (you said you’re not receiving SMS)
        print("[ARKESEL][RESET] url=", sms_url)
        print("[ARKESEL][RESET] status=", resp.status_code)
        print("[ARKESEL][RESET] body=", body)

        return status, resp.status_code, body, sms_url
    except Exception as exc:
        err = str(exc)
        print("[ARKESEL][RESET] url=", sms_url)
        print("[ARKESEL][RESET] error=", err)
        return "error", None, err, sms_url


def _find_user_by_phone(raw_phone: str) -> dict | None:
    raw_clean = raw_phone.replace(" ", "").replace("-", "").replace("+", "")
    normalized = _normalize_phone(raw_phone)
    candidates = [raw_phone, raw_clean]
    if normalized:
        candidates.append(normalized)

    # supports both phone and phone_number keys (your DB seems to use "phone")
    return (
        users_col.find_one({"phone": {"$in": candidates}})
        or users_col.find_one({"phone_number": {"$in": candidates}})
    )


def _create_reset_doc(user: dict, phone_raw: str, phone_normalized: str, code_hash: str) -> ObjectId:
    now = datetime.utcnow()
    reset_doc = {
        # ✅ store ObjectId safely (so password update won’t break)
        "user_id": user["_id"],
        "username": user.get("username"),
        "phone_raw": phone_raw,
        "phone_normalized": phone_normalized,
        "code_hash": code_hash,
        "created_at": now,
        "expires_at": now + timedelta(minutes=10),
        "attempts": 0,
        "verified": False,
        "consumed": False,
        "last_sent_at": now,
        # ✅ SMS log fields
        "last_sms_status": None,
        "last_sms_http_status": None,
        "last_sms_response": None,
        "last_sms_url": None,
    }
    res = resets_col.insert_one(reset_doc)
    return res.inserted_id


def _update_sms_log(reset_id: ObjectId, status: str, http_status: int | None, resp_text: str, sms_url: str) -> None:
    resets_col.update_one(
        {"_id": reset_id},
        {"$set": {
            "last_sms_status": status,
            "last_sms_http_status": http_status,
            "last_sms_response": resp_text,
            "last_sms_url": sms_url,
            "last_sent_at": datetime.utcnow(),
        }}
    )


def _best_phone_for_user(user: dict, fallback_phone_raw: str = "") -> str | None:
    """
    Pick the most reliable phone:
    - normalize typed phone first
    - else normalize user.phone
    - else normalize user.phone_number
    """
    p1 = _normalize_phone(fallback_phone_raw or "")
    if p1:
        return p1
    p2 = _normalize_phone(user.get("phone") or "")
    if p2:
        return p2
    p3 = _normalize_phone(user.get("phone_number") or "")
    if p3:
        return p3
    return None


@auth_password_reset_bp.route("/lookup-username", methods=["GET"])
def lookup_username():
    ip = _get_client_ip()
    if not _can_lookup(ip):
        return jsonify(ok=False, message="Too many requests. Please try again."), 429

    phone = (request.args.get("phone") or "").strip()
    if not phone:
        return jsonify(ok=False)

    user = _find_user_by_phone(phone)
    if not user:
        return jsonify(ok=False)

    username = user.get("username") or ""
    masked_phone = _mask_phone(user.get("phone") or user.get("phone_number") or phone)
    return jsonify(ok=True, username=username, masked_phone=masked_phone)


@auth_password_reset_bp.route("/forgot-password", methods=["GET", "POST"])
def forgot_password():
    if request.method == "POST":
        phone_raw = (request.form.get("phone") or "").strip()
        username = (request.form.get("username") or "").strip()

        user = None
        if phone_raw:
            user = _find_user_by_phone(phone_raw)
        if not user and username:
            user = users_col.find_one({"username": username})

        if not user:
            flash("User not found. Please check your details.", "error")
            return redirect(url_for("auth_password_reset.forgot_password"))

        # OTP + hash
        otp = f"{random.randint(0, 999999):06d}"
        code_hash = bcrypt_lib.hashpw(otp.encode("utf-8"), bcrypt_lib.gensalt(rounds=12)).decode("utf-8")

        phone_normalized = _normalize_phone(phone_raw) or ""
        reset_id = _create_reset_doc(user, phone_raw, phone_normalized, code_hash)

        phone_to_send = _best_phone_for_user(user, phone_raw)
        if not phone_to_send:
            _update_sms_log(reset_id, "invalid_phone", None, "Invalid phone number", "")
            flash("Phone number is invalid. Please contact support.", "error")
            return redirect(url_for("auth_password_reset.forgot_password"))

        message = f"SmartLiving Password Reset: Your code is {otp}. Expires in 10 minutes."
        status, http_status, resp_text, sms_url = _send_sms_arkesel(phone_to_send, message)
        _update_sms_log(reset_id, status, http_status, resp_text, sms_url)

        # ✅ Keep flow moving even if failed; user will see log on verify page
        session["reset_id"] = str(reset_id)

        if status == "sent":
            flash("We sent a verification code. Please check your phone.", "success")
        elif status == "failed":
            flash("SMS request sent but delivery failed. Check the SMS log on the next page.", "warning")
        else:
            flash("SMS sending error. Check the SMS log on the next page.", "warning")

        return redirect(url_for("auth_password_reset.verify_code"))

    return render_template("forgot_password.html")


@auth_password_reset_bp.route("/verify-code", methods=["GET", "POST"])
def verify_code():
    reset_id = session.get("reset_id")
    if not reset_id:
        flash("Session expired. Please request a new code.", "error")
        return redirect(url_for("auth_password_reset.forgot_password"))

    try:
        oid = ObjectId(reset_id)
    except Exception:
        flash("Session expired. Please request a new code.", "error")
        return redirect(url_for("auth_password_reset.forgot_password"))

    doc = resets_col.find_one({"_id": oid})
    now = datetime.utcnow()
    if not doc or doc.get("consumed") or (doc.get("expires_at") and doc["expires_at"] < now):
        flash("Code expired. Please request a new one.", "error")
        return redirect(url_for("auth_password_reset.forgot_password"))

    if request.method == "POST":
        code = (request.form.get("code") or "").strip()
        attempts = int(doc.get("attempts") or 0)
        if attempts >= 5:
            flash("Too many attempts. Please request a new code.", "error")
            return redirect(url_for("auth_password_reset.forgot_password"))

        ok = False
        try:
            ok = bcrypt_lib.checkpw(code.encode("utf-8"), str(doc.get("code_hash") or "").encode("utf-8"))
        except Exception:
            ok = False

        if not ok:
            resets_col.update_one({"_id": oid}, {"$set": {"attempts": attempts + 1}})
            flash("Invalid code. Please try again.", "error")
            return redirect(url_for("auth_password_reset.verify_code"))

        resets_col.update_one({"_id": oid}, {"$set": {"verified": True}})
        flash("Code verified. You can reset your password now.", "success")
        return redirect(url_for("auth_password_reset.reset_password"))

    # ✅ show masked phone beside the sentence in UI
    masked_phone = _mask_phone(doc.get("phone_raw") or doc.get("phone_normalized") or "")
    return render_template("verify_code.html", reset=doc, masked_phone=masked_phone)


@auth_password_reset_bp.route("/resend-code", methods=["POST"])
def resend_code():
    reset_id = session.get("reset_id")
    if not reset_id:
        flash("Session expired. Please request a new code.", "error")
        return redirect(url_for("auth_password_reset.forgot_password"))

    try:
        oid = ObjectId(reset_id)
    except Exception:
        flash("Session expired. Please request a new code.", "error")
        return redirect(url_for("auth_password_reset.forgot_password"))

    doc = resets_col.find_one({"_id": oid})
    if not doc or doc.get("consumed"):
        flash("Please request a new code.", "error")
        return redirect(url_for("auth_password_reset.forgot_password"))

    last_sent = doc.get("last_sent_at")
    if last_sent and isinstance(last_sent, datetime) and datetime.utcnow() - last_sent < timedelta(seconds=60):
        flash("Please wait a moment before resending.", "warning")
        return redirect(url_for("auth_password_reset.verify_code"))

    # new OTP
    otp = f"{random.randint(0, 999999):06d}"
    code_hash = bcrypt_lib.hashpw(otp.encode("utf-8"), bcrypt_lib.gensalt(rounds=12)).decode("utf-8")
    resets_col.update_one(
        {"_id": oid},
        {"$set": {
            "code_hash": code_hash,
            "attempts": 0,
            "expires_at": datetime.utcnow() + timedelta(minutes=10),
        }}
    )

    # send to stored normalized phone if possible
    phone_to_send = doc.get("phone_normalized") or _normalize_phone(doc.get("phone_raw") or "")
    if not phone_to_send:
        _update_sms_log(oid, "invalid_phone", None, "Invalid phone number", "")
        flash("Phone number is invalid. Please contact support.", "error")
        return redirect(url_for("auth_password_reset.verify_code"))

    message = f"SmartLiving Password Reset: Your code is {otp}. Expires in 10 minutes."
    status, http_status, resp_text, sms_url = _send_sms_arkesel(phone_to_send, message)
    _update_sms_log(oid, status, http_status, resp_text, sms_url)

    if status == "sent":
        flash("A new code has been sent.", "success")
    elif status == "failed":
        flash("Resend attempted but delivery failed. Check the SMS log.", "warning")
    else:
        flash("Resend error. Check the SMS log.", "warning")

    return redirect(url_for("auth_password_reset.verify_code"))


@auth_password_reset_bp.route("/reset-password", methods=["GET", "POST"])
def reset_password():
    reset_id = session.get("reset_id")
    if not reset_id:
        flash("Session expired. Please request a new code.", "error")
        return redirect(url_for("auth_password_reset.forgot_password"))

    try:
        oid = ObjectId(reset_id)
    except Exception:
        flash("Session expired. Please request a new code.", "error")
        return redirect(url_for("auth_password_reset.forgot_password"))

    doc = resets_col.find_one({"_id": oid})
    now = datetime.utcnow()
    if not doc or not doc.get("verified") or doc.get("consumed") or (doc.get("expires_at") and doc["expires_at"] < now):
        flash("Reset session expired. Please request a new code.", "error")
        return redirect(url_for("auth_password_reset.forgot_password"))

    if request.method == "POST":
        new_password = (request.form.get("new_password") or "").strip()
        confirm_password = (request.form.get("confirm_password") or "").strip()

        if not new_password or len(new_password) < 8:
            flash("Password must be at least 8 characters.", "error")
            return redirect(url_for("auth_password_reset.reset_password"))
        if new_password != confirm_password:
            flash("Passwords do not match.", "error")
            return redirect(url_for("auth_password_reset.reset_password"))

        new_hash = bcrypt_lib.hashpw(new_password.encode("utf-8"), bcrypt_lib.gensalt(rounds=12)).decode("utf-8")

        # ✅ only update password field (don’t break user doc)
        user_oid = doc.get("user_id")
        if isinstance(user_oid, str):
            user_oid = ObjectId(user_oid)

        users_col.update_one({"_id": user_oid}, {"$set": {"password": new_hash}})
        resets_col.update_one({"_id": oid}, {"$set": {"consumed": True, "consumed_at": now}})

        session.pop("reset_id", None)
        flash("Password reset successfully. Please log in.", "success")
        return redirect(url_for("login.login"))

    return render_template("reset_password.html")

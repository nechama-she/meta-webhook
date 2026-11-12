"""Website chat messages from BoxBot (the gorillahaulers.com chat window)."""

import hashlib
import hmac
import os

from db import save_conversation_message

PLATFORM = "website_gorillah_bot"
ROLES = {"user", "assistant"}


def _signed(headers: dict, raw_bytes: bytes) -> bool:
    """X-BoxBot-Signature-256 is sha256=<HMAC-SHA256 of the raw body with WEBSITE_CHAT_SECRET>,
    as Meta signs its events, so the secret itself never travels or reaches the logs."""
    secret = os.environ.get("WEBSITE_CHAT_SECRET", "")
    if not secret:
        print("Website chat signature check failed: WEBSITE_CHAT_SECRET is not configured")
        return False
    sig = str({k.lower(): v for k, v in (headers or {}).items()}.get("x-boxbot-signature-256", ""))
    if not sig.startswith("sha256="):
        print("Website chat signature check failed: X-BoxBot-Signature-256 is missing or invalid")
        return False
    expected = hmac.new(secret.encode("utf-8"), raw_bytes, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, sig.split("=", 1)[1])


def handle_website_chat_message(body: dict, headers: dict, raw_bytes: bytes) -> dict:
    """Save one website chat message like a Messenger message, with platform website_gorillah_bot."""
    if not _signed(headers, raw_bytes):
        print("Website chat message rejected: bad signature")
        return {"statusCode": 403, "body": "Forbidden"}

    user_id = str(body.get("user_id") or "").strip()
    message_id = str(body.get("message_id") or "").strip()
    role = str(body.get("role") or "").strip().lower()
    text = str(body.get("text") or "").strip()
    try:
        timestamp = int(body.get("timestamp"))
    except (TypeError, ValueError):
        timestamp = 0
    if not user_id.startswith("web-") or not message_id or role not in ROLES or not text or timestamp <= 0:
        print(f"Website chat message rejected: invalid payload user_id={user_id!r} role={role!r}")
        return {"statusCode": 400, "body": "Bad Request"}

    save_conversation_message(
        user_id=user_id,
        message_id=message_id,
        text=text,
        platform=PLATFORM,
        page_id=str(body.get("page_id") or "").strip(),
        timestamp=timestamp,
        role=role,
        lead_id=str(body.get("lead_id") or "").strip() or None,
    )
    return {"statusCode": 200, "body": "OK"}

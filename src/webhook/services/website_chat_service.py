"""Chat messages from BoxBot: the gorillahaulers.com chat window and the CRM customer page assistant."""

import hashlib
import hmac
import os

from db import save_conversation_message

PLATFORM = "website_gorillah_bot"
CUSTOMER_PAGE_PLATFORM = "customer_page_bot"
# event-type -> (platform saved, user id prefix): "web-<visitor>" on the website,
# "cp-<access id>" (customer) or "cp-rep-<access id>" (rep) on the customer page.
SOURCES = {
    "website-chat-message": (PLATFORM, "web-"),
    "customer-page-chat-message": (CUSTOMER_PAGE_PLATFORM, "cp-"),
}
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


def handle_website_chat_message(body: dict, headers: dict, raw_bytes: bytes,
                                event_type: str = "website-chat-message") -> dict:
    """Save one BoxBot chat message like a Messenger message, with the platform of its event type."""
    platform, prefix = SOURCES[event_type]
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
    if not user_id.startswith(prefix) or not message_id or role not in ROLES or not text or timestamp <= 0:
        print(f"Website chat message rejected: invalid payload user_id={user_id!r} role={role!r}")
        return {"statusCode": 400, "body": "Bad Request"}

    save_conversation_message(
        user_id=user_id,
        message_id=message_id,
        text=text,
        platform=platform,
        page_id=str(body.get("page_id") or "").strip(),
        timestamp=timestamp,
        role=role,
        lead_id=str(body.get("lead_id") or "").strip() or None,
    )
    return {"statusCode": 200, "body": "OK"}

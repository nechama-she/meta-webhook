"""AWS Lambda handler - routing only, no business logic."""

import base64
import hashlib
import hmac
import json
import os

from services.comment_service import process_comment
from services.lead_service import process_leadgen
from services.messenger_service import handle_echo, handle_user_message
from services.aircall_service import handle_aircall_message
from services.smartmoving_service import handle_followup_created, handle_followup_deleted, handle_opportunity_changed

VERIFY_TOKEN = os.environ["VERIFY_TOKEN"]
APP_SECRET = os.environ.get("APP_SECRET", "")


def _verify_meta_signature(event: dict, raw_bytes: bytes) -> bool:
    """Verify Meta's X-Hub-Signature-256 (HMAC-SHA256 of the raw body with APP_SECRET)."""
    if not APP_SECRET:
        print("Meta signature check failed: APP_SECRET is not configured")
        return False
    headers = {k.lower(): v for k, v in (event.get("headers") or {}).items()}
    sig = headers.get("x-hub-signature-256", "")
    if not sig:
        print("Meta signature check failed: X-Hub-Signature-256 header is missing")
        return False
    if not sig.startswith("sha256="):
        print("Meta signature check failed: X-Hub-Signature-256 header has an invalid format")
        return False
    expected = hmac.new(APP_SECRET.encode("utf-8"), raw_bytes, hashlib.sha256).hexdigest()
    if not hmac.compare_digest(expected, sig.split("=", 1)[1]):
        print(f"Meta signature check failed: HMAC mismatch (body_bytes={len(raw_bytes)})")
        return False
    return True


def lambda_handler(event, context):
    method = (
        (event.get("requestContext", {}).get("http", {}) or {})
        .get("method", "GET")
    )

    # ── GET: webhook verification ─────────────────────────────────────
    if method == "GET":
        q = event.get("queryStringParameters") or {}
        if q.get("hub.verify_token") == VERIFY_TOKEN:
            return {"statusCode": 200, "body": q.get("hub.challenge", "")}
        return {"statusCode": 403, "body": "Forbidden"}

    # ── POST: webhook events ────────────────────────────────────────────
    if method == "POST":
        try:
            raw = event.get("body") or ""
            raw_bytes = base64.b64decode(raw) if event.get("isBase64Encoded") else raw.encode("utf-8")
            body = json.loads(raw_bytes or b"{}")

            # Aircall events have "resource", Meta events have "object"
            if body.get("resource") == "message":
                handle_aircall_message(body)
                return {"statusCode": 200, "body": "OK"}

            # SmartMoving events have "event-type"
            event_type = body.get("event-type")
            if event_type in ("follow-up-created", "follow-up-changed", "follow-up-completed"):
                handle_followup_created(body)
                return {"statusCode": 200, "body": "OK"}
            if event_type == "follow-up-deleted":
                handle_followup_deleted(body)
                return {"statusCode": 200, "body": "OK"}
            if event_type == "opportunity-changed":
                handle_opportunity_changed(body)
                return {"statusCode": 200, "body": "OK"}

            # Meta (Facebook/Instagram) events must carry a valid app-signed signature.
            if not _verify_meta_signature(event, raw_bytes):
                headers = {k.lower(): v for k, v in (event.get("headers") or {}).items()}
                http_context = (event.get("requestContext") or {}).get("http") or {}
                print(
                    "Meta signature verification failed - continuing: "
                    f"object={body.get('object')!r} "
                    f"body_keys={sorted(body.keys())} "
                    f"source_ip={http_context.get('sourceIp')!r} "
                    f"user_agent={headers.get('user-agent')!r} "
                    f"content_type={headers.get('content-type')!r} "
                    f"base64_encoded={bool(event.get('isBase64Encoded'))}"
                )

            entries = body.get("entry", [])
            print(f"Processing {len(entries)} entries")

            object_type = body.get("object", "page")
            platform = "instagram" if object_type == "instagram" else "messenger"

            for entry in entries:
                entry_id = entry.get("id", "?")
                print(f"── Entry {entry_id} ({platform}) ──")
                _handle_changes(entry)
                _handle_messaging(entry, platform)

        except Exception as exc:
            print("Handler error:", repr(exc))

        return {"statusCode": 200, "body": "OK"}

    return {"statusCode": 405, "body": "Method Not Allowed"}


# ── Internal dispatch helpers ─────────────────────────────────────────

def _handle_changes(entry: dict) -> None:
    """Dispatch ``changes`` items to the right service."""
    for change in entry.get("changes", []):
        field = change.get("field")
        value = change.get("value") or {}
        print(f"Change: field={field}")

        if field == "feed" and value.get("item") == "comment":
            print(f"Dispatching comment: comment_id={value.get('comment_id')}")
            process_comment(entry, value)

        elif field == "leadgen":
            print(f"Dispatching leadgen: leadgen_id={value.get('leadgen_id')}")
            process_leadgen(entry, value)


def _handle_messaging(entry: dict, platform: str = "messenger") -> None:
    """Dispatch ``messaging`` items to the messenger service."""
    for messaging in entry.get("messaging", []):
        message_data = messaging.get("message") or {}
        text = (message_data.get("text") or "").strip()
        mid = message_data.get("mid")
        if not text or not mid:
            continue

        is_echo = message_data.get("is_echo", False)
        sender = messaging.get("sender", {}).get("id", "?")
        recipient = messaging.get("recipient", {}).get("id", "?")
        print(f"Messaging: mid={mid}, echo={is_echo}, sender={sender}, recipient={recipient}, text={text!r}")

        if is_echo:
            handle_echo(messaging, entry, platform)
        else:
            handle_user_message(messaging, entry, platform)

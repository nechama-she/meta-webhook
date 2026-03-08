"""AWS Lambda handler - routing only, no business logic."""

import json

from meta_webhook.config import VERIFY_TOKEN
from meta_webhook.services.comment_service import process_comment
from meta_webhook.services.lead_service import process_leadgen
from meta_webhook.services.messenger_service import handle_echo, handle_user_message


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

    # ── POST: Meta webhook events ─────────────────────────────────────
    if method == "POST":
        try:
            body = json.loads(event.get("body") or "{}")
            print("Event:", json.dumps(body))

            entries = body.get("entry", [])
            print(f"Processing {len(entries)} entries")

            for entry in entries:
                entry_id = entry.get("id", "?")
                print(f"── Entry {entry_id} ──")
                _handle_changes(entry)
                _handle_messaging(entry)

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


def _handle_messaging(entry: dict) -> None:
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
            handle_echo(messaging, entry)
        else:
            handle_user_message(messaging, entry)

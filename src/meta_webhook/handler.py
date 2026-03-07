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
            print("Event:", body)

            for entry in body.get("entry", []):
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

        if field == "feed" and value.get("item") == "comment":
            process_comment(entry, value)

        elif field == "leadgen":
            process_leadgen(entry, value)


def _handle_messaging(entry: dict) -> None:
    """Dispatch ``messaging`` items to the messenger service."""
    for messaging in entry.get("messaging", []):
        message_data = messaging.get("message") or {}
        if not (message_data.get("text") or "").strip() or not message_data.get("mid"):
            continue

        if message_data.get("is_echo", False):
            handle_echo(messaging, entry)
        else:
            handle_user_message(messaging, entry)

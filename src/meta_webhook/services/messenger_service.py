"""Messenger inbound-message handling and auto-reply logic."""

import re
import time
import uuid

from meta_webhook.config import ENABLE_OPENAI_ANSWER
from meta_webhook.clients.openai_client import generate_reply
from meta_webhook.clients.facebook_client import send_messenger_message
from meta_webhook.services.conversation_service import (
    save_message,
    fetch_conversation,
    log_conversation,
    summarize,
)


# ── Pattern-based auto-replies ────────────────────────────────────────

_STATE_RE = re.compile(
    r"are you moving within the state or out of state\?[:\s]*([a-zA-Z ]+)",
    re.IGNORECASE,
)

_REPLIES = {
    "out of state": (
        "Thank you for reaching out to Gorilla Haulers.\n"
        "For out-of-state moves, pricing is based on the total size of your shipment. "
        "To give you an accurate quote, we need a list of items that will not go into "
        "boxes, such as furniture or appliances, and about how many boxes you expect. "
        "You can list the items here in the chat, send pictures, or we can schedule a "
        "call to create the inventory together. You can also call us anytime at Gorilla "
        "Haulers for a quick estimate at (202) 937-2625."
    ),
    "within the state": (
        "Thank you for reaching out to Gorilla Haulers.\n"
        "For local moves, pricing is based on the number of hours the move takes. "
        "To give you an accurate estimate, we need a list of items that will not go "
        "into boxes, such as furniture or appliances, and about how many boxes you "
        "expect. You can list the items here in the chat, send pictures, or we can "
        "schedule a call to create the inventory together. You can also call us anytime "
        "at Gorilla Haulers for a quick estimate at (202) 937-2625."
    ),
}


def _pattern_reply(text: str) -> str | None:
    """Return a canned reply if *text* matches a known pattern, else ``None``."""
    if "move size: storage" in text.lower():
        return "What size is the storage unit, and approximately what percentage of it is full?"

    match = _STATE_RE.search(text)
    if match:
        answer = match.group(1).strip().lower()
        return _REPLIES.get(answer)

    return None


# ── Core handler ──────────────────────────────────────────────────────

def handle_echo(messaging: dict, entry: dict) -> None:
    """Process a page-echo (admin/bot outbound) message."""
    message_data = messaging.get("message") or {}
    text = (message_data.get("text") or "").strip()
    mid = message_data.get("mid")
    if not text or not mid:
        return

    save_message(
        user_id=messaging["recipient"]["id"],
        message_id=mid,
        text=text,
        platform="messenger",
        page_id=entry.get("id"),
        timestamp=messaging.get("timestamp", 0),
        role="sales",
    )


def handle_user_message(messaging: dict, entry: dict) -> None:
    """Process an inbound user message - save, classify, reply."""
    sender_id = messaging["sender"]["id"]
    message_data = messaging.get("message") or {}
    text = (message_data.get("text") or "").strip()
    mid = message_data.get("mid")

    if not text or not mid:
        return

    page_id = entry.get("id")

    # 1. Persist the user message
    save_message(
        user_id=sender_id,
        message_id=mid,
        text=text,
        platform="messenger",
        page_id=page_id,
        timestamp=messaging.get("timestamp", 0),
        role="user",
    )

    # 2. Load & log full conversation
    conversation = fetch_conversation(sender_id)
    log_conversation(conversation)

    # 3. Summarise if threshold reached, and get messages ready for API
    messages_for_api = summarize(conversation, sender_id, page_id)

    # 4. Determine reply
    reply_text: str | None = None

    if ENABLE_OPENAI_ANSWER:
        answer = generate_reply(messages_for_api)
        if answer:
            save_message(
                user_id=sender_id,
                message_id=str(uuid.uuid4()),
                text=answer,
                platform="openai",
                page_id=page_id,
                timestamp=int(messaging.get("timestamp", 0)) + 1,
                role="assistant",
            )
            print(f"OpenAI answer for user {sender_id}: {answer}")
            reply_text = answer

    # Pattern-based override
    pattern_reply = _pattern_reply(text)
    if pattern_reply:
        reply_text = pattern_reply

    # 5. Send reply
    if reply_text:
        print(f"Sending reply to {sender_id}: {reply_text}")
        send_messenger_message(sender_id, reply_text, page_id)
        print("Reply sent.")

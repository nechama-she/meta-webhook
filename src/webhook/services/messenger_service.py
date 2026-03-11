"""Messenger inbound-message handling and auto-reply logic."""

import os
import re
import time
import uuid

from ai import generate_reply
from meta_api import send_messenger_message
from services.conversation_service import (
    save_message,
    fetch_conversation,
    log_conversation,
    summarize,
)

ENABLE_OPENAI_ANSWER = (
    os.environ.get("ENABLE_OPENAI_ANSWER", "true").lower() == "true"
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
        print("Echo skipped: empty text or missing mid")
        return

    recipient = messaging["recipient"]["id"]
    page_id = entry.get("id")
    print(f"Echo from page {page_id} to user {recipient}: {text!r}")

    save_message(
        user_id=recipient,
        message_id=mid,
        text=text,
        platform="messenger",
        page_id=page_id,
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
        print(f"User message skipped: empty text or missing mid (sender={sender_id})")
        return

    page_id = entry.get("id")
    print(f"\n══ User message from {sender_id} (page {page_id}) ══")
    print(f"Text: {text!r}")

    # 1. Persist the user message
    print("Step 1: Saving user message...")
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
    print("Step 2: Fetching conversation history...")
    conversation = fetch_conversation(sender_id)
    log_conversation(conversation)

    # 3. Summarise if threshold reached, and get messages ready for API
    print(f"Step 3: Summarize check ({len(conversation)} messages)...")
    messages_for_api = summarize(conversation, sender_id, page_id)
    print(f"Messages for API: {len(messages_for_api)} items")

    # 4. Determine reply
    reply_text: str | None = None

    if ENABLE_OPENAI_ANSWER:
        print("Step 4a: Generating OpenAI reply...")
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
            print(f"OpenAI answer: {answer!r}")
            reply_text = answer
        else:
            print("OpenAI returned no answer")
    else:
        print("Step 4a: OpenAI disabled (ENABLE_OPENAI_ANSWER=false)")

    # Pattern-based override
    pattern_reply = _pattern_reply(text)
    if pattern_reply:
        print(f"Step 4b: Pattern match override: {pattern_reply!r}")
        reply_text = pattern_reply
    else:
        print("Step 4b: No pattern match")

    # 5. Send reply
    if reply_text:
        print(f"Step 5: Sending reply to {sender_id} ({len(reply_text)} chars)")
        send_messenger_message(sender_id, reply_text, page_id)
        print("Reply sent successfully")
    else:
        print(f"Step 5: No reply to send for {sender_id}")

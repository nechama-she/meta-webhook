"""Messenger inbound-message handling and auto-reply logic."""

import re
import uuid

from ai import chat_reply
from crm.moving_crm import get_company
from db import try_claim_dedupe_key, save_sender_info
from meta_api import send_messenger_message
from pipeline import run_pipeline
from services.conversation_service import save_message

_PHONE_RE = re.compile(r"phone\s*(?:number)?\s*[:\-]\s*\+?([0-9\s\-().]+)", re.IGNORECASE)
_EMAIL_RE = re.compile(r"email\s*[:\-]\s*([\w.\-+]+@[\w.\-]+\.\w+)", re.IGNORECASE)
_NAME_RE = re.compile(r"full\s*name\s*[:\-]\s*(.+)", re.IGNORECASE)
# ── Pattern-based auto-replies ────────────────────────────────────────

_STATE_RE = re.compile(
    r"are you moving within the state or out of state\?[:\s]*([a-zA-Z ]+)",
    re.IGNORECASE,
)

def _pattern_reply(text: str, page_id: str | None) -> str | None:
    """Return a canned reply if *text* matches a known pattern, else ``None``."""
    if "move size: storage" in text.lower():
        return "What size is the storage unit, and approximately what percentage of it is full?"

    company = get_company(page_id or "")
    if not company or not company.get("name") or not company.get("phone"):
        return None
    company_name = company["name"]
    company_phone = company["phone"]

    match = _STATE_RE.search(text)
    if match:
        answer = match.group(1).strip().lower()
        if answer == "out of state":
            return (
                f"Thank you for reaching out to {company_name}.\n"
                "For out-of-state moves, pricing is based on the total size of your shipment. "
                "To give you an accurate quote, we need a list of items that will not go into "
                "boxes, such as furniture or appliances, and about how many boxes you expect. "
                "You can list the items here in the chat, send pictures, or we can schedule a "
                f"call to create the inventory together. You can also call us anytime at {company_name} for a "
                f"quick estimate at {company_phone}."
            )
        if answer == "within the state":
            return (
                f"Thank you for reaching out to {company_name}.\n"
                "For local moves, pricing is based on the number of hours the move takes. "
                "To give you an accurate estimate, we need a list of items that will not go "
                "into boxes, such as furniture or appliances, and about how many boxes you "
                "expect. You can list the items here in the chat, send pictures, or we can "
                f"schedule a call to create the inventory together. You can also call us anytime at {company_name} "
                f"for a quick estimate at {company_phone}."
            )
        return None

    return None


# ── Sender-info cache ────────────────────────────────────────────────


def _cache_sender_info(sender_id: str, text: str) -> None:
    """Parse phone/email/name from message text and cache keyed by sender_id."""
    phone_match = _PHONE_RE.search(text)
    email_match = _EMAIL_RE.search(text)
    name_match = _NAME_RE.search(text)

    if not phone_match and not email_match and not name_match:
        return

    phone = re.sub(r"\D", "", phone_match.group(1)) if phone_match else ""
    email = email_match.group(1).strip().lower() if email_match else ""
    name = name_match.group(1).strip() if name_match else ""

    save_sender_info(sender_id, phone=phone, email=email, name=name)


def _is_duplicate_event(cache_key: str) -> bool:
    if not try_claim_dedupe_key(cache_key):
        print(f"Messenger: duplicate event skipped ({cache_key})")
        return True
    return False


# ── Core handler ──────────────────────────────────────────────────────

def handle_echo(messaging: dict, entry: dict, platform: str = "messenger") -> None:
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
        platform=platform,
        page_id=page_id,
        timestamp=messaging.get("timestamp", 0),
        role="sales",
    )

    # Forward outbound message as a note to SmartMoving
    run_pipeline("messenger_message", {"sender_id": recipient, "text": text, "direction": "sales"})


def handle_user_message(messaging: dict, entry: dict, platform: str = "messenger") -> None:
    """Process an inbound user message - save, classify, reply."""
    sender_id = messaging["sender"]["id"]
    message_data = messaging.get("message") or {}
    text = (message_data.get("text") or "").strip()
    mid = message_data.get("mid")

    if not text or not mid:
        print(f"User message skipped: empty text or missing mid (sender={sender_id})")
        return

    dedupe_key = f"messenger:user:{platform}:{sender_id}:{mid}"
    if _is_duplicate_event(dedupe_key):
        return

    page_id = entry.get("id")
    print(f"\n══ User message from {sender_id} (page {page_id}) [{platform}] ══")
    print(f"Text: {text!r}")

    # 1. Persist the user message
    print("Step 1: Saving user message...")
    save_message(
        user_id=sender_id,
        message_id=mid,
        text=text,
        platform=platform,
        page_id=page_id,
        timestamp=messaging.get("timestamp", 0),
        role="user",
    )

    # 1b. Cache sender contact info for leadgen lookup
    _cache_sender_info(sender_id, text)

    # 1c. Run messenger_message pipeline (SmartMoving note, etc.)
    run_pipeline("messenger_message", {"sender_id": sender_id, "text": text, "direction": "user"})

    # 2. Pattern-based replies
    pattern_text = _pattern_reply(text, page_id)
    if pattern_text:
        print(f"Step 2: Pattern match – sending to {sender_id}")
        send_messenger_message(sender_id, pattern_text, page_id)
        print("Pattern reply sent")

    # 3. Call chat API (dry run – save reply but don't send to client)
    print("Step 3: Calling chat API...")
    answer = chat_reply(sender_id, text, "messenger")
    if answer:
        save_message(
            user_id=sender_id,
            message_id=str(uuid.uuid4()),
            text=answer,
            platform=platform,
            page_id=page_id,
            timestamp=int(messaging.get("timestamp", 0)) + 1,
            role="assistant",
        )
        print(f"Chat API answer (not sent): {answer!r}")
    else:
        print("Chat API returned no answer")

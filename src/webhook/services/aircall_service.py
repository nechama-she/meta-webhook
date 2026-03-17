"""Aircall SMS webhook handling."""

import os
import re
import uuid

from ai import generate_reply
from aircall import send_sms
from db import save_sms_message, get_sms_messages

ENABLE_OPENAI_ANSWER = (
    os.environ.get("ENABLE_OPENAI_ANSWER", "false").lower() == "true"
)

_GORILLA_NUMBER_ID = 645873
_TEST_PHONE = "+12403703417"


def _normalize_phone(raw: str) -> str:
    """Strip to E.164 format: +13015261984"""
    return re.sub(r"[^\d+]", "", raw)


def handle_aircall_message(body: dict) -> None:
    """Process an Aircall message.sent or message.received event."""
    event_type = body.get("event", "")
    data = body.get("data", {})

    text = (data.get("body") or "").strip()
    message_id = str(data.get("id", ""))
    if not text or not message_id:
        print("Aircall: skipped (empty body or missing id)")
        return

    phone_number = _normalize_phone(data.get("external_number", ""))
    timestamp = body.get("timestamp", 0)

    number_info = data.get("number") or {}
    company_number = _normalize_phone(number_info.get("e164_digits", ""))
    company_name = number_info.get("name", "")
    number_id = number_info.get("id")

    if number_id != _GORILLA_NUMBER_ID:
        print(f"Aircall: ignoring non-Gorilla number {number_id} ({company_name})")
        return

    direction = "sent" if event_type == "message.sent" else "received"

    user_info = data.get("user") or {}
    sales_name = user_info.get("name") if direction == "sent" else None

    print(f"Aircall SMS: {direction} | {phone_number} | {company_name} | {text!r}")

    # 1. Save the message
    save_sms_message(
        phone_number=phone_number,
        timestamp=timestamp,
        message_id=message_id,
        text=text,
        direction=direction,
        company_number=company_number,
        company_name=company_name,
        number_id=number_id,
        sales_name=sales_name,
    )

    # 2. Auto-reply only on received messages
    if direction != "received" or not number_id:
        return

    # Test auto-reply for a specific number
    if phone_number == _TEST_PHONE:
        reply = "Test response from Gorilla Haulers"
        print(f"Aircall: test auto-reply to {phone_number}")
        result = send_sms(number_id, phone_number, reply)
        if result:
            save_sms_message(
                phone_number=phone_number,
                timestamp=timestamp + 1,
                message_id=result,
                text=reply,
                direction="sent",
                company_number=company_number,
                company_name=company_name,
                number_id=number_id,
                sales_name="AI",
            )
        return

    if not ENABLE_OPENAI_ANSWER:
        print("Aircall: OpenAI reply disabled")
        return

    # 3. Build conversation history for OpenAI
    history = get_sms_messages(phone_number)
    messages_for_api = [
        {
            "role": "assistant" if m.get("direction") == "sent" else "user",
            "content": m.get("text", ""),
        }
        for m in history
    ]
    print(f"Aircall: {len(messages_for_api)} messages for OpenAI")

    # 4. Generate reply
    answer = generate_reply(messages_for_api)
    if not answer:
        print("Aircall: OpenAI returned no answer")
        return

    print(f"Aircall OpenAI answer: {answer!r}")

    # 5. Send SMS reply
    result = send_sms(number_id, phone_number, answer)
    if result:
        save_sms_message(
            phone_number=phone_number,
            timestamp=timestamp + 1,
            message_id=result,
            text=answer,
            direction="sent",
            company_number=company_number,
            company_name=company_name,
            number_id=number_id,
            sales_name="AI",
        )

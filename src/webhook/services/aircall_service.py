"""Aircall SMS webhook handling."""

import re

from db import save_sms_message


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

    direction = "sent" if event_type == "message.sent" else "received"

    user_info = data.get("user") or {}
    sales_name = user_info.get("name") if direction == "sent" else None

    print(f"Aircall SMS: {direction} | {phone_number} | {company_name} | {text!r}")

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

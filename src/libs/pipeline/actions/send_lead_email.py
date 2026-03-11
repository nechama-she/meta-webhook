"""Action: Gorilla lead-notification email to the sales team."""

import os

from mailer import send_email


# Each email action reads its own env vars, making it easy to add more
# email actions later (e.g. email_borat_notification, email_lead_resource).


def email_gorilla_notification(data: dict) -> dict:
    """Send a plain-text lead notification from the Gorilla brand."""
    from_addr = os.environ.get("HHG_NOTIFY_FROM", "")
    to_raw = os.environ.get("HHG_NOTIFY_TO", "")
    to = [e.strip() for e in to_raw.split(",") if e.strip()]
    if not from_addr or not to:
        print("email_gorilla_notification: HHG_NOTIFY_FROM / _TO not configured, skipping")
        return data

    name = data.get("full_name", "Unknown")
    email = data.get("email", "")
    phone = data.get("phone_number", "")
    created = data.get("created_time", "")
    ozip = data.get("pickup_zip", data.get("ozip", ""))
    dzip = data.get("delivery_zip", data.get("dzip", ""))
    move_date = data.get("move_date", "")

    subject = f"New Lead on Facebook From {name}, Gorilla"

    body_text = (
        f"Email: {email}\n"
        f"Full Name: {name}\n"
        f"Phone Number: {phone}\n"
        f"Created Date: {created}\n"
        f"Pickup Zip: {ozip}\n"
        f"Delivery Zip: {dzip}\n"
        f"Move Date: {move_date}"
    )

    send_email(from_addr, to, subject, body_text, body_text)
    return data

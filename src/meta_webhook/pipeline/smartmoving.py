"""Pipeline action: send a new lead to SmartMoving CRM."""

from meta_webhook.clients.smartmoving_client import create_lead

# ── Referral source mapping ──────────────────────────────────────────
_DEFAULT_REFERRAL = "Facebook-Gorilla-HHG-Local"
_CAMPAIGN_REFERRAL = {
    "Northeast-Midwest": "Facebook-Gorilla-HHG-Nationwide",
    "FL-GA-NC": "Facebook-Gorilla-HHG-FL-GA-NC",
}


def _clean_phone(phone: str) -> str:
    """Strip leading +1 / 1 country code and dashes."""
    phone = phone.replace("-", "").strip()
    if phone.startswith("+1"):
        phone = phone[2:]
    elif phone.startswith("1") and len(phone) == 11:
        phone = phone[1:]
    return phone


def send_to_smartmoving(lead: dict) -> str | None:
    """Transform a lead dict and POST it to SmartMoving.

    Returns the SmartMoving lead ID or None on failure.
    """
    phone = _clean_phone(lead.get("phone_number", ""))
    full_name = lead.get("full_name", "")
    email = lead.get("email", "")
    ozip = lead.get("pickup_zip", lead.get("ozip", ""))
    dzip = lead.get("delivery_zip", lead.get("dzip", ""))
    move_date = lead.get("move_date", lead.get("gptMoveDate", ""))
    move_size = lead.get("move_size", lead.get("moveSize", "Room or Less"))
    campaign = lead.get("campaign", "")

    message = (
        f"pickup {ozip}\n"
        f"delivery {dzip}\n"
        f"date {move_date}\n"
        f"size {move_size}\n"
        f"email {email}\n"
        f"name {full_name}\n"
        f"phone {phone}"
    )

    note = (
        f"email: {email}. "
        f"pickup:{ozip}. "
        f"delivery:{dzip}. "
        f"moveDate:{move_date}. "
        f"campaign:{campaign}. "
        f"adset:{lead.get('adset', '')}. "
        f"ad:{lead.get('ad', '')} "
        f"message: {message}"
    )

    referral_source = _CAMPAIGN_REFERRAL.get(campaign, _DEFAULT_REFERRAL)

    payload = {
        "fullName": full_name,
        "phoneNumber": phone,
        "email": email,
        "originZip": ozip,
        "destinationZip": dzip,
        "moveDate": move_date,
        "notes": note,
        "referralSource": referral_source,
        "leadno": lead.get("leadgen_id", ""),
        "serviceType": "Moving",
        "moveSize": move_size,
    }

    print(f"SmartMoving payload: {payload}")
    return create_lead(payload)

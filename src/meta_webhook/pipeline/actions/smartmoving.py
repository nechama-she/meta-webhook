"""Action: send a new lead to SmartMoving CRM."""

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


def send_to_smartmoving(data: dict) -> dict:
    """Transform a lead dict and POST it to SmartMoving.

    Returns the data dict (possibly enriched with smartmoving_lead_id).
    """
    phone = _clean_phone(data.get("phone_number", ""))
    full_name = data.get("full_name", "")
    email = data.get("email", "")
    ozip = data.get("pickup_zip", data.get("ozip", ""))
    dzip = data.get("delivery_zip", data.get("dzip", ""))
    move_date = data.get("move_date", "")
    move_size = data.get("move_size", data.get("moveSize", "Room or Less"))
    campaign = data.get("campaign", "")

    message = (
        f"pickup {ozip}\n"
        f"delivery {dzip}\n"
        f"date {move_date}\n"
        f"size {move_size}\n"
        f"email {email}\n"
        f"name {full_name}\n"
        f"phone {phone}"
    )

    move_date_raw = data.get("move_date_raw", move_date)

    note = (
        f"email: {email}. "
        f"pickup:{ozip}. "
        f"delivery:{dzip}. "
        f"moveDate:{move_date_raw}. "
        f"campaign:{campaign}. "
        f"adset:{data.get('adset', '')}. "
        f"ad:{data.get('ad', '')} "
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
        "leadno": data.get("leadgen_id", ""),
        "serviceType": "Moving",
        "moveSize": move_size,
    }

    print(f"SmartMoving payload: {payload}")
    result = create_lead(payload)
    if result:
        data["smartmoving_lead_id"] = result
    return data

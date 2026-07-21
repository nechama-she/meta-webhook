"""Action: send a new lead to SmartMoving CRM."""

import os

from crm.smartmoving import create_lead

# ── Referral source mapping ──────────────────────────────────────────
_DEFAULT_REFERRAL = "Facebook-Gorilla-HHG-Local"
_CAMPAIGN_REFERRAL = {
    "Northeast-Midwest": "Facebook-Gorilla-HHG-Nationwide",
    "FL-GA-NC": "Facebook-Gorilla-HHG-FL-GA-NC",
}
_PAGE_REFERRAL = {
    "340823849673554": "Facebook-Movers95",
    "1037282016129017": "Facebook-Simple Moving Campaign",
    "517722408094755": "Facebook-Wilson Bros-HHG",
    "1194752063710686" : "Facebook-TTVL",
}


def _clean_phone(phone: str) -> str:
    """Strip leading +1 / 1 country code and dashes."""
    phone = phone.replace("-", "").strip()
    if phone.startswith("+1"):
        phone = phone[2:]
    elif phone.startswith("1") and len(phone) == 11:
        phone = phone[1:]
    return phone


def _build_payload(data: dict) -> dict:
    """Build the SmartMoving payload from lead data."""
    phone = _clean_phone(data.get("phone_number", ""))
    full_name = data.get("full_name", "")
    email = data.get("email", "")
    ozip = data.get("pickup_zip", data.get("ozip", ""))
    dzip = data.get("delivery_zip", data.get("dzip", ""))
    move_date = data.get("move_date", "")
    move_size = data.get("move_size", data.get("moveSize", "Room or Less"))
    campaign = data.get("campaign", "")
    page_id = str(data.get("page_id") or "").strip()
    pushed_by_parts = [f"lambda:{os.environ.get('AWS_LAMBDA_FUNCTION_NAME', 'unknown')}"]
    trace_values = (
        ("arn", data.get("_lambda_invoked_arn")),
        ("requestId", data.get("_lambda_request_id")),
        ("logGroup", os.environ.get("AWS_LAMBDA_LOG_GROUP_NAME")),
        ("logStream", os.environ.get("AWS_LAMBDA_LOG_STREAM_NAME")),
    )
    pushed_by_parts.extend(f"{key}:{value}" for key, value in trace_values if value)
    pushed_by = " | ".join(pushed_by_parts)

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
        f"pushedBy:{pushed_by}. "
        f"email: {email}. "
        f"pickup:{ozip}. "
        f"delivery:{dzip}. "
        f"moveDate:{move_date_raw}. "
        f"campaign:{campaign}. "
        f"adset:{data.get('adset', '')}. "
        f"ad:{data.get('ad', '')} "
        f"message: {message}"
    )

    referral_source = _PAGE_REFERRAL.get(page_id) or _CAMPAIGN_REFERRAL.get(campaign, _DEFAULT_REFERRAL)

    return {
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


def send_to_smartmoving_by_branch(data: dict) -> dict:
    """Send lead to SmartMoving using branch ID from lead/company data."""
    branch_id = str(data.get("smartmoving_branch_id") or "").strip()
    company_name = data.get("company_name", "Unknown")

    if not branch_id:
        print(f"SmartMoving: no smartmoving_branch_id for company={company_name}, skipping")
        return data

    payload = _build_payload(data)
    data["referral_source"] = payload.get("referralSource", "")
    print(f"SmartMoving {company_name} payload: {payload}")
    result = create_lead(payload, branch_id=branch_id)

    if result:
        data["smartmoving_lead_id"] = result
    return data


def send_to_smartmoving(data: dict) -> dict:
    """Send lead to SmartMoving primary account (no branch)."""
    data["company_name"] = "Household Goods Moving And Storage"
    payload = _build_payload(data)
    data["referral_source"] = payload.get("referralSource", "")
    print("SmartMoving Primary payload: sending without branch_id")
    result = create_lead(payload)
    if result:
        data["smartmoving_lead_id"] = result
    return data


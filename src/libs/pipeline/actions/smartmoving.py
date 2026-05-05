"""Action: send a new lead to SmartMoving CRM."""

import os

from crm.smartmoving import create_lead

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
    pushed_by = f"lambda:{os.environ.get('AWS_LAMBDA_FUNCTION_NAME', 'unknown')}"

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

    referral_source = _CAMPAIGN_REFERRAL.get(campaign, _DEFAULT_REFERRAL)

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


def send_to_smartmoving_by_branch(data: dict, branch_env_var: str, company_name: str, field_name: str) -> dict:
    """Send lead to SmartMoving using specified branch ID from environment.

    Args:
        data: Lead data dict
        branch_env_var: Environment variable name for branch ID (e.g., "SMARTMOVING_WILSON_BRANCH_ID")
        company_name: Company name for logging (e.g., "Wilson")
        field_name: Field name to store result in data dict (e.g., "smartmoving_wilson_lead_id")

    Returns the data dict (possibly enriched with the result field).
    """
    branch_id = os.environ.get(branch_env_var, "") if branch_env_var else ""
    payload = _build_payload(data)
    print(f"SmartMoving {company_name} payload: {payload}")

    if branch_id:
        result = create_lead(payload, branch_id=branch_id)
    elif not branch_env_var:
        # Explicit primary-branch mode when no branch env var is provided.
        result = create_lead(payload)
    else:
        print(f"SmartMoving {company_name}: {branch_env_var} not set, skipping")
        return data

    if result:
        data[field_name] = result
    return data


def send_to_smartmoving(data: dict) -> dict:
    """Send lead to SmartMoving using the Gorilla branch.

    Returns the data dict (possibly enriched with smartmoving_lead_id).
    """
    return send_to_smartmoving_by_branch(
        data,
        branch_env_var="",
        company_name="Primary",
        field_name="smartmoving_HHG_lead_id",
    )


def send_to_smartmoving_wilson(data: dict) -> dict:
    """Send lead to SmartMoving using the Wilson Bros branch.

    Returns the data dict (possibly enriched with smartmoving_wilson_lead_id).
    """
    return send_to_smartmoving_by_branch(
        data,
        branch_env_var="SMARTMOVING_WILSON_BRANCH_ID",
        company_name="Wilson",
        field_name="smartmoving_wilson_lead_id",
    )

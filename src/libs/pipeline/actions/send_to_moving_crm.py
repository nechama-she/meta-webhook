"""Action: send a new lead to the Moving CRM API."""

import json
import os
import urllib.request
import urllib.error
from datetime import datetime, timezone

from pipeline.actions.smartmoving import _clean_phone, _CAMPAIGN_REFERRAL, _DEFAULT_REFERRAL

_CRM_URL = "https://m6efygcjve.execute-api.us-east-1.amazonaws.com/api/leads"
_CRM_API_SECRET = os.environ.get("MOVING_CRM_API_SECRET", "")


def send_to_moving_crm(data: dict) -> dict:
    """Send lead to Moving CRM after SmartMoving creation."""
    # Extract facebook_user_id from inbox_url
    inbox_url = data.get("inbox_url", "")
    print(f"Moving CRM: inbox_url={inbox_url}")
    try:
        facebook_user_id = inbox_url.split("/latest/")[-1].split("?")[0]
    except Exception:
        facebook_user_id = ""
    print(f"Moving CRM: facebook_user_id={facebook_user_id}")

    # Parse smartmoving_id from the SmartMoving response
    sm_raw = data.get("smartmoving_lead_id", "")
    try:
        sm_result = json.loads(sm_raw)
        smartmoving_id = sm_result.get("leadId", sm_raw) if isinstance(sm_result, dict) else sm_raw.strip('"')
    except (json.JSONDecodeError, AttributeError):
        smartmoving_id = sm_raw.strip('"') if sm_raw else ""

    phone = _clean_phone(data.get("phone_number", ""))
    campaign = data.get("campaign", "")
    referral_source = _CAMPAIGN_REFERRAL.get(campaign, _DEFAULT_REFERRAL)

    ozip = data.get("pickup_zip", data.get("ozip", ""))
    dzip = data.get("delivery_zip", data.get("dzip", ""))
    move_date = data.get("move_date", "")
    move_size = data.get("move_size", data.get("moveSize", "Room or Less"))

    note = (
        f"email: {data.get('email', '')}. "
        f"pickup:{ozip}. "
        f"delivery:{dzip}. "
        f"moveDate:{data.get('move_date_raw', move_date)}. "
        f"campaign:{campaign}. "
        f"adset:{data.get('adset', '')}. "
        f"ad:{data.get('ad', '')}"
    )

    crm_payload = {
        "full_name": data.get("full_name", ""),
        "phone_number": phone,
        "email": data.get("email", ""),
        "pickup_zip": ozip,
        "delivery_zip": dzip,
        "move_date": move_date,
        "move_size": move_size,
        "move_type": data.get("move_type", data.get("moveType", "")),
        "leadgen_id": data.get("leadgen_id", ""),
        "smartmoving_id": smartmoving_id,
        "referral_source": referral_source,
        "service_type": "Moving",
        "notes": note,
        "created_time": datetime.now(timezone.utc).isoformat(),
        "company_name": "Gorilla Haulers",
        "source": "Facebook",
        "facebook_user_id": facebook_user_id,
    }

    body = json.dumps(crm_payload).encode("utf-8")
    req = urllib.request.Request(
        _CRM_URL,
        data=body,
        headers={
            "Content-Type": "application/json",
            "X-Api-Secret": _CRM_API_SECRET,
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            result = resp.read().decode("utf-8")
            print(f"Moving CRM response: {result}")
            data["moving_crm_ok"] = True
    except urllib.error.HTTPError as exc:
        print(f"Moving CRM HTTP error: {exc.code} {exc.read().decode('utf-8', 'ignore')}")
        data["moving_crm_ok"] = False
    except Exception as exc:
        print(f"Moving CRM error: {repr(exc)}")
        data["moving_crm_ok"] = False

    return data

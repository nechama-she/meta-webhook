"""HTTP client for the SmartMoving API."""

import json
import urllib.request
import urllib.error

from meta_webhook.config import SMARTMOVING_PROVIDER_KEY, SMARTMOVING_BRANCH_ID

_BASE_URL = "https://api.smartmoving.com/api/leads/from-provider/v2"


def create_lead(payload: dict) -> str | None:
    """POST a lead to SmartMoving and return the lead ID (or None on error)."""
    url = (
        f"{_BASE_URL}"
        f"?providerKey={SMARTMOVING_PROVIDER_KEY}"
        f"&branchId={SMARTMOVING_BRANCH_ID}"
    )
    body = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            result = resp.read().decode("utf-8")
            print(f"SmartMoving response: {result}")
            return result
    except urllib.error.HTTPError as exc:
        print(f"SmartMoving HTTP error: {exc.code} {exc.read().decode('utf-8', 'ignore')}")
    except Exception as exc:
        print(f"SmartMoving error: {repr(exc)}")
    return None

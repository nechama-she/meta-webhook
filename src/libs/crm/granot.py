"""HTTP client for the Granot lead API."""

import os
import urllib.parse
import urllib.request
import urllib.error

_BASE_URL = "https://lead.hellomoving.com/LEADSGWHTTP.lidgw"


def send_lead(payload: dict) -> str | None:
    """POST a lead to Granot and return the response text (or None on error)."""
    api_id = os.environ.get("GRANOT_API_ID", "")
    mover_ref = os.environ.get("GRANOT_MOVER_REF", "")
    if not api_id or not mover_ref:
        print("Granot: API_ID or MOVER_REF not configured, skipping")
        return None

    params = urllib.parse.urlencode({"API_ID": api_id, "MOVERREF": mover_ref})
    url = f"{_BASE_URL}?{params}"
    body = urllib.parse.urlencode(payload).encode("latin-1", "ignore")
    req = urllib.request.Request(
        url,
        data=body,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            result = resp.read().decode("utf-8")
            print(f"Granot response: {result}")
            return result
    except urllib.error.HTTPError as exc:
        print(f"Granot HTTP error: {exc.code} {exc.read().decode('utf-8', 'ignore')}")
    except Exception as exc:
        print(f"Granot error: {repr(exc)}")
    return None

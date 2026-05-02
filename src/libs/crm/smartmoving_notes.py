"""SmartMoving Premium API – communication notes & followups."""

import json
import os
import urllib.request
import urllib.error

_BASE_URL = "https://api-public.smartmoving.com/v1/api/premium/opportunities"
_OPP_URL = "https://api-public.smartmoving.com/v1/api/opportunities"
_API_KEY = os.environ.get("SMARTMOVING_API_KEY", "")


def add_note(opportunity_id: str, note: str) -> str | None:
    """POST a communication note to a SmartMoving opportunity.

    Returns SmartMoving response text on success, None on error.
    """
    url = f"{_BASE_URL}/{opportunity_id}/communication/notes"
    body = json.dumps({"notes": note}).encode("utf-8")
    print(f"SmartMoving add_note REQUEST: POST {url} body={body.decode('utf-8')!r}")
    req = urllib.request.Request(
        url,
        data=body,
        headers={
            "Content-Type": "application/json-patch+json",
            "Cache-Control": "no-cache",
            "x-api-key": _API_KEY,
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            result = resp.read().decode("utf-8")
            print(f"SmartMoving add_note RESPONSE: {resp.status} {result!r}")
            return result
    except urllib.error.HTTPError as exc:
        body_err = exc.read().decode("utf-8", "ignore")
        print(f"SmartMoving add_note ERROR: {exc.code} {body_err!r}")
    except Exception as exc:
        print(f"SmartMoving add_note ERROR: {repr(exc)}")
    return None


def get_followups(opportunity_id: str) -> list | None:
    """GET followups for a SmartMoving opportunity.

    Returns list of followup dicts or None on error.
    """
    url = f"{_BASE_URL}/{opportunity_id}/followups"
    req = urllib.request.Request(
        url,
        headers={
            "Cache-Control": "no-cache",
            "x-api-key": _API_KEY,
        },
        method="GET",
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            print(f"SmartMoving followups for {opportunity_id}: {resp.status} ({len(data)} items)")
            return data
    except urllib.error.HTTPError as exc:
        print(f"SmartMoving followups HTTP error: {exc.code} {exc.read().decode('utf-8', 'ignore')}")
    except Exception as exc:
        print(f"SmartMoving followups error: {repr(exc)}")
    return None


def get_audit_activity(opportunity_id: str) -> list | None:
    """GET audit activity for a SmartMoving opportunity.

    Returns list of activity dicts or None on error.
    """
    url = f"{_OPP_URL}/{opportunity_id}/audit-activity"
    req = urllib.request.Request(
        url,
        headers={
            "Cache-Control": "no-cache",
            "x-api-key": _API_KEY,
        },
        method="GET",
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            print(f"SmartMoving audit activity for {opportunity_id}: {resp.status} ({len(data)} items)")
            return data
    except urllib.error.HTTPError as exc:
        print(f"SmartMoving audit activity HTTP error: {exc.code} {exc.read().decode('utf-8', 'ignore')}")
    except Exception as exc:
        print(f"SmartMoving audit activity error: {repr(exc)}")
    return None

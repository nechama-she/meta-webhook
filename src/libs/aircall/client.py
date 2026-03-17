"""Aircall API client – send SMS via REST API."""

import base64
import json
import os
import urllib.request
import urllib.error

_API_ID = os.environ.get("AIRCALL_API_ID", "")
_API_TOKEN = os.environ.get("AIRCALL_API_TOKEN", "")

_BASE_URL = "https://api.aircall.io/v1"


def _auth_header() -> str:
    creds = base64.b64encode(f"{_API_ID}:{_API_TOKEN}".encode()).decode()
    return f"Basic {creds}"


def send_sms(number_id: int, to: str, text: str) -> str | None:
    """Send an SMS via Aircall.

    Returns the message id on success, None on failure.
    """
    url = f"{_BASE_URL}/numbers/{number_id}/messages"
    body = json.dumps({"to": to, "body": text}).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=body,
        headers={
            "Authorization": _auth_header(),
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            msg_id = str(data.get("id", ""))
            print(f"Aircall SMS sent to {to} via number {number_id}: {msg_id}")
            return msg_id
    except urllib.error.HTTPError as exc:
        print(f"Aircall send_sms error: {exc.code} {exc.read().decode()}")
        return None
    except Exception as exc:
        print(f"Aircall send_sms error: {repr(exc)}")
        return None

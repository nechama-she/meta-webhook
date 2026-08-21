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


def transfer_call(
    call_id: int | str,
    *,
    user_id: int | str | None = None,
    team_id: int | str | None = None,
    number: str | None = None,
    dispatching_strategy: str | None = None,
) -> bool:
    """Cold-transfer an active Aircall call to one destination.

    Returns True when Aircall accepts the transfer, otherwise False.
    """
    destinations = [user_id is not None, team_id is not None, bool(number)]
    if sum(destinations) != 1:
        raise ValueError("Exactly one of user_id, team_id, or number is required")
    if dispatching_strategy and team_id is None:
        raise ValueError("dispatching_strategy is only valid for team transfers")
    if dispatching_strategy not in (None, "random", "simultaneous", "longest_idle"):
        raise ValueError("Invalid Aircall team dispatching strategy")

    url = f"{_BASE_URL}/calls/{call_id}/transfers"
    payload = {}
    if user_id is not None:
        payload["user_id"] = str(user_id)
    elif team_id is not None:
        payload["team_id"] = str(team_id)
        if dispatching_strategy:
            payload["dispatching_strategy"] = dispatching_strategy
    else:
        payload["number"] = number
    body = json.dumps(payload).encode("utf-8")
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
            resp.read()
            print(f"Aircall call {call_id} transfer accepted")
            return True
    except urllib.error.HTTPError as exc:
        print(f"Aircall transfer_call error: {exc.code} {exc.read().decode()}")
    except Exception as exc:
        print(f"Aircall transfer_call error: {repr(exc)}")
    return False


def send_sms(number_id: int, to: str, text: str) -> str | None:
    """Send an SMS via Aircall (agent conversation endpoint).

    Messages appear in the Aircall app and trigger webhooks.
    Returns the message id on success, None on failure.
    """
    url = f"{_BASE_URL}/numbers/{number_id}/messages/native/send"
    body = json.dumps({
        "to": to,
        "body": text,
    }).encode("utf-8")
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


def trigger_outbound_call(agent_id: str, contact_phone: str, idempotency_key: str, context: dict) -> None:
    """Trigger an outbound call via Aircall agent webhook."""
    url = f"{_BASE_URL}/outbound-calls/agents/{agent_id}"
    body = json.dumps({
        "contact_phone": contact_phone,
        "idempotency_key": idempotency_key,
        "context": context,
    }).encode("utf-8")
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
            raw = resp.read().decode("utf-8")
            print(f"Aircall outbound call triggered for {contact_phone}: {raw}")
    except urllib.error.HTTPError as exc:
        print(f"Aircall trigger_outbound_call error: {exc.code} {exc.read().decode()}")
    except Exception as exc:
        print(f"Aircall trigger_outbound_call error: {repr(exc)}")

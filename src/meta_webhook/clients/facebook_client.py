"""Wrapper around the Facebook / Meta Graph API."""

import json
import urllib.request
import urllib.error
import urllib.parse

from meta_webhook.config import (
    GRAPH_API_URL,
    ACCOUNTS_API_VERSION,
    COMMENTS_DETECTION_USER_TOKEN,
)


# ── Low-level helper ─────────────────────────────────────────────────

def graph_api_request(
    path: str,
    *,
    method: str = "GET",
    data: dict | None = None,
    access_token: str | None = None,
    timeout: int = 10,
) -> dict | None:
    """Make a Graph API call and return the parsed JSON (or ``None`` on error)."""
    url = f"{GRAPH_API_URL}/{path}"
    if access_token:
        separator = "&" if "?" in url else "?"
        url += f"{separator}access_token={access_token}"

    headers = {"Content-Type": "application/json"} if data else {}
    body = json.dumps(data).encode("utf-8") if data else None
    req = urllib.request.Request(url, data=body, headers=headers, method=method)

    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8")
            print(f"FB API [{method}] {path}: {raw}")
            return json.loads(raw) if raw else {}
    except urllib.error.HTTPError as exc:
        print(f"FB API error [{method}] {path}: {exc.code} {exc.read().decode('utf-8', 'ignore')}")
    except Exception as exc:
        print(f"FB API exception [{method}] {path}: {repr(exc)}")
    return None


# ── Page token ────────────────────────────────────────────────────────

def get_page_token(page_id: str) -> str:
    """Retrieve a page access token for *page_id* via the user token."""
    url = (
        f"https://graph.facebook.com/{ACCOUNTS_API_VERSION}"
        f"/me/accounts?access_token={COMMENTS_DETECTION_USER_TOKEN}"
    )
    with urllib.request.urlopen(url) as resp:
        data = json.loads(resp.read().decode("utf-8"))

    for page in data.get("data", []):
        if page.get("id") == page_id:
            return page["access_token"]

    raise ValueError(f"Page token not found for page_id={page_id}")


# ── Comments / moderation ────────────────────────────────────────────

def delete_comment(comment_id: str, page_id: str) -> None:
    print(f"Deleting comment {comment_id}")
    token = get_page_token(page_id)
    graph_api_request(comment_id, method="DELETE", access_token=token)


def block_user(user_id: str, page_id: str) -> None:
    print(f"Blocking user {user_id}")
    token = get_page_token(page_id)
    graph_api_request(
        "me/blocked", method="POST", data={"uid": user_id}, access_token=token
    )


# ── Messenger ─────────────────────────────────────────────────────────

def send_messenger_message(
    recipient_id: str,
    message_text: str,
    page_id: str,
) -> None:
    """Send a text message to a Messenger user."""
    token = get_page_token(page_id)
    url = (
        f"https://graph.facebook.com/v18.0/me/messages"
        f"?access_token={token}"
    )
    payload = {
        "recipient": {"id": recipient_id},
        "message": {"text": message_text},
    }
    req = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            print(f"Sent message to {recipient_id}: {resp.read().decode('utf-8')}")
    except Exception as exc:
        print(f"Error sending message to {recipient_id}: {repr(exc)}")


# ── Leads ─────────────────────────────────────────────────────────────

def fetch_lead_details(leadgen_id: str, page_id: str) -> dict | None:
    """Fetch lead form data from the Graph API."""
    token = get_page_token(page_id)
    url = (
        f"https://graph.facebook.com/v18.0/{leadgen_id}"
        f"?access_token={token}"
    )
    try:
        with urllib.request.urlopen(url) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            print(f"Lead details: {json.dumps(data)}")
            return data
    except Exception as exc:
        print(f"Error fetching lead details: {repr(exc)}")
    return None


def get_leadgen_forms(page_id: str) -> list[dict]:
    """Return all leadgen forms for *page_id*."""
    token = get_page_token(page_id)
    forms: list[dict] = []
    url = (
        f"https://graph.facebook.com/v18.0/{page_id}/leadgen_forms"
        f"?access_token={token}"
    )
    try:
        while url:
            with urllib.request.urlopen(url, timeout=15) as resp:
                data = json.loads(resp.read().decode("utf-8"))
            forms.extend(data.get("data", []))
            url = data.get("paging", {}).get("next")
    except Exception as exc:
        print(f"Error fetching leadgen forms for page {page_id}: {repr(exc)}")
    print(f"Found {len(forms)} leadgen forms for page {page_id}")
    return forms


def get_form_leads(form_id: str, page_id: str, since_timestamp: int) -> list[dict]:
    """Pull leads created after *since_timestamp* from a single form."""
    token = get_page_token(page_id)
    filtering = json.dumps(
        [{"field": "time_created", "operator": "GREATER_THAN", "value": since_timestamp}]
    )
    leads: list[dict] = []
    url = (
        f"https://graph.facebook.com/v18.0/{form_id}/leads"
        f"?filtering={urllib.parse.quote(filtering)}"
        f"&access_token={token}"
    )
    try:
        while url:
            with urllib.request.urlopen(url, timeout=15) as resp:
                data = json.loads(resp.read().decode("utf-8"))
            leads.extend(data.get("data", []))
            url = data.get("paging", {}).get("next")
    except Exception as exc:
        print(f"Error fetching leads from form {form_id}: {repr(exc)}")
    print(f"Pulled {len(leads)} leads from form {form_id} (since {since_timestamp})")
    return leads

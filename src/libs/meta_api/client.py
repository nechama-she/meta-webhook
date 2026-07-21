"""Wrapper around the Facebook / Meta Graph API."""

import json
import urllib.request
import urllib.error
import urllib.parse

from meta_api.config import (
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

from db import cache_get, cache_set

_CACHE_PREFIX = "page_token:"


def _fetch_page_token(page_id: str) -> str:
    """Call /me/accounts, cache in memory + DynamoDB."""
    url = (
        f"https://graph.facebook.com/{ACCOUNTS_API_VERSION}"
        f"/me/accounts?access_token={COMMENTS_DETECTION_USER_TOKEN}"
    )
    print(f"Fetching page token for page_id={page_id} from {ACCOUNTS_API_VERSION}/me/accounts")
    print(f"Using user token from ssm (length={len(COMMENTS_DETECTION_USER_TOKEN)}, starts={COMMENTS_DETECTION_USER_TOKEN[:10]})")
    print(f"Full request URL: {url}")
    try:
        with urllib.request.urlopen(url) as resp:
            raw = resp.read().decode("utf-8")
            print(f"Meta /me/accounts raw response: {raw}")
            data = json.loads(raw)
    except urllib.error.HTTPError as exc:
        error_body = exc.read().decode("utf-8", "ignore")
        print(f"Meta /me/accounts HTTP error {exc.code}: {error_body}")
        raise

    print(f"Meta /me/accounts returned {len(data.get('data', []))} pages")

    for page in data.get("data", []):
        print(f"  Page: id={page.get('id')}, name={page.get('name')}")
        if page.get("id") == page_id:
            token = page["access_token"]
            cache_set(f"{_CACHE_PREFIX}{page_id}", token)
            print(f"Page token fetched from Meta for {page_id}")
            return token

    raise ValueError(f"Page token not found for page_id={page_id}")


def get_page_token(page_id: str) -> str:
    """Return page token from DynamoDB cache or Meta."""
    cached = cache_get(f"{_CACHE_PREFIX}{page_id}")
    if cached:
        print(f"Page token: DynamoDB cache hit for {page_id}")
        return cached

    print(f"Page token: no cache found for {page_id}, fetching from Meta")
    return _fetch_page_token(page_id)


def _invalidate_page_token(page_id: str) -> None:
    """Remove cached token from DynamoDB."""
    cache_set(f"{_CACHE_PREFIX}{page_id}", "")


def _is_token_error(exc: urllib.error.HTTPError, error_body: str | None = None) -> bool:
    """Return True if the error indicates an invalid/expired token."""
    if exc.code in (400, 401, 403):
        try:
            body = error_body if error_body is not None else exc.read().decode("utf-8", "ignore")
            return "OAuthException" in body or "Invalid OAuth" in body or "access token" in body.lower()
        except Exception:
            return exc.code in (400, 401)
    return False


# ── Comments / moderation ────────────────────────────────────────────

def delete_comment(comment_id: str, page_id: str) -> None:
    print(f"Deleting comment {comment_id}")
    token = get_page_token(page_id)
    try:
        graph_api_request(comment_id, method="DELETE", access_token=token)
    except urllib.error.HTTPError as exc:
        if _is_token_error(exc):
            _invalidate_page_token(page_id)
            token = get_page_token(page_id)
            graph_api_request(comment_id, method="DELETE", access_token=token)
        else:
            raise


def block_user(user_id: str, page_id: str) -> None:
    print(f"Blocking user {user_id}")
    token = get_page_token(page_id)
    try:
        graph_api_request(
            "me/blocked", method="POST", data={"uid": user_id}, access_token=token
        )
    except urllib.error.HTTPError as exc:
        if _is_token_error(exc):
            _invalidate_page_token(page_id)
            token = get_page_token(page_id)
            graph_api_request(
                "me/blocked", method="POST", data={"uid": user_id}, access_token=token
            )
        else:
            raise


# ── Messenger ─────────────────────────────────────────────────────────

def _send_messenger_request(recipient_id: str, message_text: str, token: str) -> None:
    url = (
        f"https://graph.facebook.com/v18.0/me/messages"
        f"?access_token={token}"
    )
    payload = {
        "recipient": {"id": recipient_id},
        "message": {"text": message_text},
    }
    request_body = json.dumps(payload, ensure_ascii=False)
    print(
        "Meta Messenger request: "
        f"method=POST endpoint=/v18.0/me/messages body={request_body}"
    )
    req = urllib.request.Request(
        url,
        data=request_body.encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=10) as resp:
        response_body = resp.read().decode("utf-8", "ignore")
        print(
            "Meta Messenger response: "
            f"status={resp.status} body={response_body}"
        )


def send_messenger_message(
    recipient_id: str,
    message_text: str,
    page_id: str,
) -> bool:
    """Send a text message to a Messenger user."""
    token = get_page_token(page_id)
    try:
        _send_messenger_request(recipient_id, message_text, token)
        return True
    except urllib.error.HTTPError as exc:
        error_body = exc.read().decode("utf-8", "ignore")
        print(
            "Meta Messenger response: "
            f"status={exc.code} body={error_body} recipient_id={recipient_id}"
        )
        if _is_token_error(exc, error_body):
            print(f"Page token rejected for {page_id}; invalidating cache and retrying once")
            _invalidate_page_token(page_id)
            token = get_page_token(page_id)
            try:
                _send_messenger_request(recipient_id, message_text, token)
                return True
            except urllib.error.HTTPError as retry_exc:
                retry_body = retry_exc.read().decode("utf-8", "ignore")
                print(
                    "Meta Messenger retry response: "
                    f"status={retry_exc.code} body={retry_body} recipient_id={recipient_id}"
                )
            except Exception as retry_exc:
                print(f"Error sending message to {recipient_id} after token refresh: {repr(retry_exc)}")
    except Exception as exc:
        print(f"Error sending message to {recipient_id}: {repr(exc)}")
    return False


# ── Leads ─────────────────────────────────────────────────────────────

def _fetch_lead(leadgen_id: str, token: str) -> dict:
    url = (
        f"https://graph.facebook.com/v18.0/{leadgen_id}"
        f"?access_token={token}"
    )
    with urllib.request.urlopen(url) as resp:
        data = json.loads(resp.read().decode("utf-8"))
        print(f"Lead details: {json.dumps(data)}")
        return data


def fetch_lead_details(leadgen_id: str, page_id: str) -> dict | None:
    """Fetch lead form data from the Graph API."""
    token = get_page_token(page_id)
    try:
        return _fetch_lead(leadgen_id, token)
    except urllib.error.HTTPError as exc:
        if _is_token_error(exc):
            _invalidate_page_token(page_id)
            token = get_page_token(page_id)
            try:
                return _fetch_lead(leadgen_id, token)
            except Exception as exc2:
                print(f"Error fetching lead details (retry): {repr(exc2)}")
        else:
            print(f"Error fetching lead details: {repr(exc)}")
    except Exception as exc:
        print(f"Error fetching lead details: {repr(exc)}")
    return None


def _fetch_leadgen_forms(page_id: str, token: str) -> list[dict]:
    forms: list[dict] = []
    url = (
        f"https://graph.facebook.com/v18.0/{page_id}/leadgen_forms"
        f"?fields=id,name,status&access_token={token}"
    )
    while url:
        log_url = url.split("access_token=")[0] + "access_token=***"
        print(f"GET {log_url}")
        with urllib.request.urlopen(url, timeout=15) as resp:
            raw = resp.read().decode("utf-8")
        print(f"Response: {raw}")
        data = json.loads(raw)
        for form in data.get("data", []):
            if form.get("status") == "ACTIVE":
                forms.append(form)
        url = data.get("paging", {}).get("next")
    return forms


def get_leadgen_forms(page_id: str, page_name: str = "") -> list[dict]:
    """Return only **active** leadgen forms for *page_id*."""
    page_label = f"{page_id} ({page_name})" if page_name else page_id
    token = get_page_token(page_id)
    try:
        forms = _fetch_leadgen_forms(page_id, token)
    except urllib.error.HTTPError as exc:
        if _is_token_error(exc):
            _invalidate_page_token(page_id)
            token = get_page_token(page_id)
            try:
                forms = _fetch_leadgen_forms(page_id, token)
            except Exception as exc2:
                print(f"Error fetching leadgen forms for page {page_label}: {repr(exc2)}")
                return []
        else:
            body = exc.read().decode("utf-8", "ignore")
            print(f"Error fetching leadgen forms for page {page_label}: HTTP {exc.code} {body}")
            return []
    except Exception as exc:
        print(f"Error fetching leadgen forms for page {page_label}: {repr(exc)}")
        return []
    print(f"Found {len(forms)} active leadgen forms for page {page_label}")
    return forms


def _fetch_form_leads(form_id: str, filtering: str, token: str) -> list[dict]:
    leads: list[dict] = []
    url = (
        f"https://graph.facebook.com/v18.0/{form_id}/leads"
        f"?filtering={urllib.parse.quote(filtering)}"
        f"&access_token={token}"
    )
    while url:
        log_url = url.split("access_token=")[0] + "access_token=***"
        print(f"GET {log_url}")
        with urllib.request.urlopen(url, timeout=15) as resp:
            raw = resp.read().decode("utf-8")
        print(f"Response: {raw}")
        data = json.loads(raw)
        leads.extend(data.get("data", []))
        url = data.get("paging", {}).get("next")
    return leads


def get_form_leads(form_id: str, page_id: str, since_timestamp: int, until_timestamp: int) -> list[dict]:
    """Pull leads created after *since_timestamp* and before *until_timestamp* from a single form."""
    token = get_page_token(page_id)
    filters = [
        {"field": "time_created", "operator": "GREATER_THAN", "value": since_timestamp},
        {"field": "time_created", "operator": "LESS_THAN", "value": until_timestamp},
    ]
    filtering = json.dumps(filters)
    try:
        leads = _fetch_form_leads(form_id, filtering, token)
    except urllib.error.HTTPError as exc:
        if _is_token_error(exc):
            _invalidate_page_token(page_id)
            token = get_page_token(page_id)
            try:
                leads = _fetch_form_leads(form_id, filtering, token)
            except Exception as exc2:
                print(f"Error fetching leads from form {form_id}: {repr(exc2)}")
                return []
        else:
            body = exc.read().decode("utf-8", "ignore")
            print(f"Error fetching leads from form {form_id}: HTTP {exc.code} {body}")
            return []
    except Exception as exc:
        print(f"Error fetching leads from form {form_id}: {repr(exc)}")
        return []
    print(f"Pulled {len(leads)} leads from form {form_id} (since {since_timestamp})")
    return leads

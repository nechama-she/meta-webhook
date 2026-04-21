"""Moving CRM admin API client: authentication and company lookup."""

import json
import os
import urllib.request
import urllib.error
import urllib.parse
import boto3

_BASE_URL = os.environ.get("MOVING_CRM_API_BASE_URL", "")
_ADMIN_EMAIL = os.environ.get("MOVING_CRM_ADMIN_EMAIL", "admin@gorillamove.com")
_ADMIN_PASSWORD_PARAM = os.environ.get(
    "MOVING_CRM_ADMIN_PASSWORD_PARAM",
    "/meta-webhook/MOVINGCRM_ADMIN_PASSWORD",
)
_AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")

_admin_password_cache: str | None = None
_admin_token_cache: str | None = None
_company_name_cache: dict[str, dict] = {}


def _get_admin_password() -> str | None:
    global _admin_password_cache
    if _admin_password_cache:
        print("Moving CRM auth: using cached admin password")
        return _admin_password_cache

    try:
        print(f"Moving CRM auth: reading SSM param {_ADMIN_PASSWORD_PARAM} in region {_AWS_REGION}")
        ssm = boto3.client("ssm", region_name=_AWS_REGION)
        resp = ssm.get_parameter(Name=_ADMIN_PASSWORD_PARAM, WithDecryption=True)
        password = (resp.get("Parameter") or {}).get("Value")
        if password:
            _admin_password_cache = password
            print(f"Moving CRM auth: loaded admin password from SSM (length={len(password)})")
            return password
    except Exception as exc:
        print(f"Moving CRM auth: failed to read SSM param {_ADMIN_PASSWORD_PARAM}: {repr(exc)}")
    return None


def _extract_token(payload: dict | None) -> str | None:
    if not isinstance(payload, dict):
        return None
    direct = payload.get("token")
    if isinstance(direct, str) and direct:
        return direct
    data = payload.get("data")
    if isinstance(data, dict):
        nested = data.get("token")
        if isinstance(nested, str) and nested:
            return nested
    return None


def _login() -> str | None:
    global _admin_token_cache

    if not _BASE_URL:
        print("Moving CRM auth: MOVING_CRM_API_BASE_URL is not configured")
        return None

    print(
        "Moving CRM auth: attempting login "
        f"base_url={_BASE_URL} email={_ADMIN_EMAIL} password_param={_ADMIN_PASSWORD_PARAM}"
    )

    password = _get_admin_password()
    if not password:
        return None

    url = f"{_BASE_URL.rstrip('/')}/api/auth/login"
    body = json.dumps({"email": _ADMIN_EMAIL, "password": password}).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
            token = _extract_token(payload)
            if token:
                _admin_token_cache = token
                print("Moving CRM auth: login successful")
                return token
            print(f"Moving CRM auth: login response missing token: {payload}")
    except urllib.error.HTTPError as exc:
        print(f"Moving CRM auth HTTP error: {exc.code} {exc.read().decode('utf-8', 'ignore')}")
    except Exception as exc:
        print(f"Moving CRM auth error: {repr(exc)}")
    return None


def get_company(company_id: str) -> dict | None:
    """Return company dict (name, phone) by ID, with cache and token refresh on 401/403."""
    global _admin_token_cache

    if company_id in _company_name_cache:
        print(f"Moving CRM companies: cache hit for company_id={company_id}")
        return _company_name_cache[company_id]

    if not company_id or not _BASE_URL:
        print(f"Moving CRM companies: invalid input company_id={company_id!r} base_url={_BASE_URL!r}")
        return None

    def _request_by_facebook_page(token: str) -> dict | None:
        url = f"{_BASE_URL.rstrip('/')}/api/companies/by-facebook-page/{urllib.parse.quote(company_id)}"
        print(f"Moving CRM companies REQUEST: GET {url} facebook_page_id={company_id}")
        req = urllib.request.Request(
            url,
            headers={"Authorization": f"Bearer {token}"},
            method="GET",
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            raw = resp.read().decode("utf-8")
            print(f"Moving CRM companies RESPONSE: {resp.status} {raw}")
            return json.loads(raw)

    token = _admin_token_cache or _login()
    if not token:
        return None

    try:
        payload = _request_by_facebook_page(token)
    except urllib.error.HTTPError as exc:
        error_body = exc.read().decode("utf-8", "ignore")
        if exc.code in (401, 403):
            print(f"Moving CRM companies AUTH error: {exc.code} {error_body}")
            _admin_token_cache = None
            refreshed = _login()
            if not refreshed:
                return None
            try:
                payload = _request_by_facebook_page(refreshed)
                print(f"Moving CRM companies: retry success for company_id={company_id}")
            except Exception as retry_exc:
                print(f"Moving CRM companies error after token refresh: {repr(retry_exc)}")
                return None
        else:
            print(f"Moving CRM companies by-facebook-page HTTP error: {exc.code} {error_body}")
            return None
    except Exception as exc:
        print(f"Moving CRM companies error: {repr(exc)}")
        return None

    if not isinstance(payload, dict):
        return None

    data = payload.get("data") if "data" in payload else payload
    if not isinstance(data, dict):
        return None

    result = {
        "name": data.get("name") or data.get("company_name") or data.get("companyName") or "",
        "phone": data.get("phone") or "",
    }
    print(f"Moving CRM companies parsed: company_id={company_id} name={result['name']!r} phone={result['phone']!r}")
    _company_name_cache[company_id] = result
    return result

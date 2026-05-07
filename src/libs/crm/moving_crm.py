"""Moving CRM admin API client: authentication and company lookup."""

import json
import os
import boto3
from datetime import datetime, timedelta

from http_client import request

_BASE_URL = os.environ.get("MOVING_CRM_API_BASE_URL", "")
_ADMIN_EMAIL = os.environ.get("MOVING_CRM_ADMIN_EMAIL", "admin@gorillamove.com")
_ADMIN_PASSWORD_PARAM = os.environ.get(
    "MOVING_CRM_ADMIN_PASSWORD_PARAM",
    "/meta-webhook/MOVINGCRM_ADMIN_PASSWORD",
)
_AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
_CACHE_TABLE = os.environ.get("DYNAMODB_TABLE", "meta-webhook")

_admin_password_cache: str | None = None
_admin_token_cache: str | None = None


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


def _get_cached_companies() -> list[dict] | None:
    """Get cached companies from DynamoDB if not expired (< 24 hours old)."""
    try:
        dynamodb = boto3.resource("dynamodb", region_name=_AWS_REGION)
        table = dynamodb.Table(_CACHE_TABLE)
        resp = table.get_item(Key={"id": "moving_crm_companies_cache"})
        item = resp.get("Item")
        if not item:
            return None
        
        cached_time = item.get("cached_at")
        companies = item.get("companies")
        
        if not cached_time or not companies:
            return None
        
        # Check if cache is still valid (< 24 hours)
        try:
            cached_dt = datetime.fromisoformat(cached_time)
            if datetime.utcnow() - cached_dt < timedelta(days=1):
                print(f"Moving CRM companies: cache hit (age={datetime.utcnow() - cached_dt})")
                return companies
        except (ValueError, TypeError):
            pass
        
        return None
    except Exception as exc:
        print(f"Moving CRM companies cache get error: {repr(exc)}")
        return None


def _cache_companies(companies: list[dict]) -> None:
    """Cache companies list in DynamoDB."""
    try:
        dynamodb = boto3.resource("dynamodb", region_name=_AWS_REGION)
        table = dynamodb.Table(_CACHE_TABLE)
        table.put_item(
            Item={
                "id": "moving_crm_companies_cache",
                "companies": companies,
                "cached_at": datetime.utcnow().isoformat(),
            }
        )
        print(f"Moving CRM companies: cached {len(companies)} companies")
    except Exception as exc:
        print(f"Moving CRM companies cache set error: {repr(exc)}")


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
    
    payload = request(
        url,
        method="POST",
        body=body,
        headers={"Content-Type": "application/json"},
        timeout=10,
    )
    
    if not isinstance(payload, dict):
        print(f"Moving CRM auth: unexpected response type: {type(payload)}")
        return None
    
    token = _extract_token(payload)
    if token:
        _admin_token_cache = token
        print("Moving CRM auth: login successful")
        return token
    
    print(f"Moving CRM auth: login response missing token: {payload}")
    return None


def get_company(page_id: str) -> dict | None:
    """Return a single company by facebook_page_id, using the cached companies list."""
    if not page_id:
        return None
    companies = get_companies()
    for company in companies:
        if company.get("facebook_page_id") == str(page_id):
            return company
    print(f"Moving CRM companies: no company found for page_id={page_id!r}")
    return None


def get_companies() -> list[dict]:
    """Return all companies from the Moving CRM API (cached for 24 hours)."""
    global _admin_token_cache

    # Check cache first
    cached = _get_cached_companies()
    if cached is not None:
        return cached

    if not _BASE_URL:
        print("Moving CRM companies: MOVING_CRM_API_BASE_URL is not configured")
        return []

    token = _admin_token_cache or _login()
    if not token:
        return []

    url = f"{_BASE_URL.rstrip('/')}/api/companies"
    
    payload = request(
        url,
        method="GET",
        headers={"Authorization": f"Bearer {token}"},
        timeout=10,
    )
    
    if payload is None:
        # Try refreshing token once on error
        print("Moving CRM companies: request failed, refreshing token")
        _admin_token_cache = None
        refreshed = _login()
        if not refreshed:
            return []
        payload = request(
            url,
            method="GET",
            headers={"Authorization": f"Bearer {refreshed}"},
            timeout=10,
        )
    
    if not isinstance(payload, (dict, list)):
        print(f"Moving CRM companies: unexpected response type: {type(payload)}")
        return []
    
    # Handle API response that may have data wrapped
    data = payload.get("data") if isinstance(payload, dict) and "data" in payload else payload
    if not isinstance(data, list):
        print(f"Moving CRM companies: data is not list: {type(data)}")
        return []
    
    # Cache the result
    _cache_companies(data)
    
    return data

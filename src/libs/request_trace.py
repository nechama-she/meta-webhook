"""Helpers for attaching safe upstream HTTP traces to CRM updates."""

from __future__ import annotations

import json
from copy import deepcopy
from typing import Any


_SENSITIVE_KEYS = {
    "authorization",
    "password",
    "token",
    "accesstoken",
    "apikey",
    "xapikey",
    "xapisecret",
    "clientsecret",
}


def _redact(value: Any, key: str = "") -> Any:
    normalized_key = "".join(char for char in key.lower() if char.isalnum())
    if normalized_key in _SENSITIVE_KEYS or normalized_key.endswith("accesstoken"):
        return "[REDACTED]"
    if isinstance(value, dict):
        return {str(k): _redact(v, str(k)) for k, v in value.items()}
    if isinstance(value, list):
        return [_redact(item) for item in value]
    return value


def parse_body(value: Any) -> Any:
    """Return JSON-compatible request/response content."""
    if value is None or value == "":
        return {}
    if isinstance(value, bytes):
        value = value.decode("utf-8", "ignore")
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except (json.JSONDecodeError, ValueError):
            return value
    return _redact(deepcopy(value))


def append_request_log(
    logs: list[dict] | None,
    *,
    method: str,
    url: str,
    headers: dict | None,
    payload: Any,
    status_code: int | None,
    response_body: Any,
) -> None:
    if logs is None:
        return
    logs.append(
        {
            "request": {
                "method": method,
                "url": url,
                "headers": _redact(deepcopy(headers or {})),
                "payload": parse_body(payload),
            },
            "response": {
                "status_code": status_code or 0,
                "body": parse_body(response_body),
            },
        }
    )

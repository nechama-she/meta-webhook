"""Generic HTTP client using urllib."""

import json
import urllib.request
import urllib.error


def request(
    url: str,
    method: str = "GET",
    body: bytes | None = None,
    headers: dict | None = None,
    timeout: int = 10,
) -> dict | list | str | None:
    """
    Make an HTTP request and return parsed response.
    
    Args:
        url: Full URL to request
        method: HTTP method (GET, POST, etc.)
        body: Request body as bytes
        headers: Dict of headers
        timeout: Request timeout in seconds
        
    Returns:
        Parsed JSON (dict/list) or raw text string, or None on error.
    """
    req_headers = headers or {}
    
    print(f"HTTP REQUEST: {method} {url}")
    if body:
        print(f"HTTP REQUEST body: {body.decode('utf-8', 'ignore')[:200]}")
    
    req = urllib.request.Request(
        url,
        data=body,
        headers=req_headers,
        method=method,
    )
    
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8")
            print(f"HTTP RESPONSE: {resp.status} {raw[:200]}")
            
            # Try to parse as JSON
            try:
                return json.loads(raw)
            except (json.JSONDecodeError, ValueError):
                return raw
                
    except urllib.error.HTTPError as exc:
        error_body = exc.read().decode("utf-8", "ignore")
        print(f"HTTP ERROR: {exc.code} {error_body[:200]}")
        return None
    except Exception as exc:
        print(f"HTTP ERROR: {repr(exc)}")
        return None

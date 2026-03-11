"""Google Sheets client – append rows via the Sheets API v4."""

import json
import urllib.error
import urllib.parse
import urllib.request

_SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
_BASE = "https://sheets.googleapis.com/v4/spreadsheets"
_SSM_PARAM = "/meta-webhook/GOOGLE_SHEETS_CREDENTIALS"


def _get_credentials():
    """Fetch service-account JSON from SSM and build credentials."""
    import boto3
    from google.oauth2 import service_account
    from google.auth.transport.requests import Request as AuthRequest

    try:
        ssm = boto3.client("ssm")
        resp = ssm.get_parameter(Name=_SSM_PARAM, WithDecryption=True)
        raw = resp["Parameter"]["Value"]
    except Exception as exc:
        print(f"Google Sheets: failed to read SSM param {_SSM_PARAM}: {exc}")
        return None
    if not raw:
        return None
    info = json.loads(raw)
    creds = service_account.Credentials.from_service_account_info(info, scopes=_SCOPES)
    creds.refresh(AuthRequest())
    return creds


def append_row(spreadsheet_id: str, sheet_name: str, values: list) -> bool:
    """Append a single row to *sheet_name* in the given spreadsheet.

    Returns True on success, False on error.
    """
    creds = _get_credentials()
    if creds is None:
        print("Google Sheets: credentials not configured, skipping")
        return False

    range_a1 = urllib.parse.quote(f"{sheet_name}!A1", safe="!:")
    url = (
        f"{_BASE}/{spreadsheet_id}/values/{range_a1}:append"
        f"?valueInputOption=USER_ENTERED"
        f"&insertDataOption=INSERT_ROWS"
    )

    body = json.dumps({"values": [values]}).encode()
    req = urllib.request.Request(
        url,
        data=body,
        headers={
            "Authorization": f"Bearer {creds.token}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            print(f"Google Sheets append: {resp.status}")
            return True
    except urllib.error.HTTPError as exc:
        print(f"Google Sheets HTTP error: {exc.code} {exc.read().decode('utf-8', 'ignore')}")
    except Exception as exc:
        print(f"Google Sheets error: {repr(exc)}")
    return False

"""Custom chat API provider."""

import json
import urllib.request
import urllib.error

from ai.config import CHAT_API_URL


def generate_reply(user_id: str, message: str, channel: str = "messenger") -> str | None:
    """Send a message to the chat API and return the reply.

    Returns None on any error.
    """
    payload = {
        "user_id": user_id,
        "message": message,
        "channel": channel,
    }
    req = urllib.request.Request(
        CHAT_API_URL,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            reply = data.get("response") or data.get("message") or data.get("reply")
            print(f"Chat API reply for {user_id}: {reply!r}")
            return reply.strip() if reply else None
    except urllib.error.HTTPError as exc:
        print(f"Chat API HTTP error: {exc.code} {exc.read().decode('utf-8', 'ignore')}")
    except Exception as exc:
        print(f"Chat API error: {repr(exc)}")
    return None

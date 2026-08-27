"""Custom chat API provider."""

import json
import urllib.request
import urllib.error

from ai.config import CHAT_API_KEY, CHAT_API_URL


def generate_reply(user_id: str, message: str, channel: str = "messenger") -> str | None:
    """Return the chat reply, or a persistable ``error: ...`` description."""
    if not CHAT_API_URL:
        error = "error: CHAT_API_URL is not configured"
        print(f"Chat API: {error}")
        return error
    if not CHAT_API_KEY:
        error = "error: CHAT_API_KEY is not configured"
        print(f"Chat API: {error}")
        return error

    payload = {
        "user_id": user_id,
        "message": message,
        "channel": channel,
    }
    req = urllib.request.Request(
        CHAT_API_URL,
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "x-api-key": CHAT_API_KEY,
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            raw_response = resp.read().decode("utf-8", "replace")
            data = json.loads(raw_response)
            reply = data.get("response") or data.get("message") or data.get("reply")
            print(f"Chat API reply for {user_id}: {reply!r}")
            if reply:
                return reply.strip() if isinstance(reply, str) else json.dumps(reply)
            return f"error: Chat API response contained no reply: {raw_response}"
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", "replace").strip()
        error = f"error: Chat API HTTP {exc.code}: {body or exc.reason}"
        print(error)
        return error
    except Exception as exc:
        error = f"error: Chat API request failed: {exc!r}"
        print(error)
        return error

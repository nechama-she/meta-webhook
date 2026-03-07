"""Conversation persistence, retrieval, and periodic summarisation."""

import time
import uuid

from meta_webhook.clients.dynamodb_client import (
    get_conversation,
    save_conversation_message,
    replace_summary,
)
from meta_webhook.clients.openai_client import summarize_conversation


def save_message(
    *,
    user_id: str,
    message_id: str,
    text: str,
    platform: str,
    page_id: str,
    timestamp: int,
    role: str,
    sales_name: str | None = None,
) -> None:
    """Persist a single message (delegates to the DB client)."""
    save_conversation_message(
        user_id=user_id,
        message_id=message_id,
        text=text,
        platform=platform,
        page_id=page_id,
        timestamp=timestamp,
        role=role,
        sales_name=sales_name,
    )


def fetch_conversation(user_id: str) -> list[dict]:
    return get_conversation(user_id)


def log_conversation(conversation: list[dict]) -> None:
    """Pretty-print a conversation to stdout for debugging."""
    for msg in conversation:
        platform = msg.get("platform", "unknown")
        ts = msg.get("timestamp", 0)
        try:
            ts_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts))
        except Exception:
            ts_str = str(ts)
        role = msg.get("role", "unknown")
        print(f"[{ts_str}] [{platform}] {role}: {msg.get('text', '')}")


# ── Summarisation ─────────────────────────────────────────────────────

_SUMMARISE_EVERY_N = 10  # Summarise after every N new messages


def _find_summary(conversation: list[dict]) -> tuple[dict | None, int | None]:
    """Return (summary_item, index) or (None, None)."""
    for idx, m in enumerate(conversation):
        if m.get("role") == "summary":
            return m, idx
    return None, None


def summarize(conversation: list[dict], user_id: str, page_id: str) -> list[dict]:
    """Summarize if threshold reached and return messages ready for the API.

    Always returns the message list to send to OpenAI — either just the
    fresh summary, or the existing conversation (from summary onward).
    """
    summary_item, summary_idx = _find_summary(conversation)

    if summary_idx is not None:
        new_msgs = conversation[summary_idx + 1:]
    else:
        new_msgs = conversation

    # Build the API messages from existing conversation
    start = 0 if summary_idx is None else summary_idx
    messages_for_api = [
        {"role": m.get("role"), "content": m["text"]}
        for m in conversation[start:]
    ]

    if len(new_msgs) % _SUMMARISE_EVERY_N != 0:
        return messages_for_api

    # Build text to summarise
    parts: list[str] = []
    if summary_item:
        parts.append(f"Previous summary:\n{summary_item['text']}\n")
    parts.extend(f"{m.get('role', 'user')}: {m['text']}" for m in new_msgs)
    text_to_summarise = "\n".join(parts)

    summary_text = summarize_conversation(text_to_summarise)
    if not summary_text:
        return messages_for_api

    new_summary = {
        "user_id": user_id,
        "timestamp": int(time.time()),
        "message_id": str(uuid.uuid4()),
        "text": summary_text,
        "platform": "system",
        "page_id": page_id,
        "role": "summary",
    }
    replace_summary(user_id, new_summary, summary_item)

    # Fresh summary covers everything
    return [{"role": "system", "content": summary_text}]

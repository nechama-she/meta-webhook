"""Thin wrapper around the OpenAI Chat Completions API."""

import json
import urllib.request
import urllib.error

from meta_webhook.config import OPENAI_URL, OPENAI_HEADERS, OPENAI_MODEL


def chat_completion(
    messages: list[dict],
    *,
    max_tokens: int = 10,
    timeout: int = 15,
) -> str | None:
    """Send *messages* to OpenAI and return the assistant's reply text.

    Returns ``None`` on any network / API error so callers can decide
    how to handle failures.
    """
    payload = {
        "model": OPENAI_MODEL,
        "messages": messages,
        "max_tokens": max_tokens,
    }
    req = urllib.request.Request(
        OPENAI_URL,
        data=json.dumps(payload).encode("utf-8"),
        headers=OPENAI_HEADERS,
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            return (data["choices"][0]["message"]["content"] or "").strip()
    except urllib.error.HTTPError as exc:
        print("OpenAI HTTPError:", exc.code, exc.read().decode("utf-8", "ignore"))
    except Exception as exc:
        print("OpenAI error:", repr(exc))
    return None


def classify_sentiment(text: str) -> str:
    """Return ``'Bad'`` or ``'Good'`` for *text*.

    Falls back to ``'Good'`` when the API is unreachable.
    """
    system_prompt = (
        "Classify the following text as 'Bad' if it expresses any negative "
        "sentiment, complaint, or dissatisfaction. Classify as 'Good' if it "
        "is positive or neutral.\nReply with only one word: Good or Bad."
    )
    result = chat_completion(
        [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": text},
        ],
        max_tokens=10,
        timeout=10,
    )
    if result and result.lower().startswith("bad"):
        return "Bad"
    return "Good"


def summarize_conversation(text: str) -> str | None:
    """Ask OpenAI to summarise a conversation block.

    Returns the summary string or ``None`` on failure.
    """
    return chat_completion(
        [
            {
                "role": "system",
                "content": (
                    "Summarize the following conversation between a user, "
                    "assistant, and sales in a few sentences, preserving "
                    "important context and questions."
                ),
            },
            {"role": "user", "content": text},
        ],
        max_tokens=200,
        timeout=15,
    )


def generate_reply(messages: list[dict]) -> str | None:
    """Generate a conversational reply given prior *messages*."""
    return chat_completion(messages, max_tokens=200, timeout=15)

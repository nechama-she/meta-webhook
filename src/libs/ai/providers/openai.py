"""OpenAI provider – Chat Completions API."""

import json
import urllib.request
import urllib.error

from ai.config import OPENAI_URL, OPENAI_HEADERS, OPENAI_MODEL


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
    print(f"OpenAI request: model={OPENAI_MODEL}, max_tokens={max_tokens}, messages={len(messages)}")
    req = urllib.request.Request(
        OPENAI_URL,
        data=json.dumps(payload).encode("utf-8"),
        headers=OPENAI_HEADERS,
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            result = (data["choices"][0]["message"]["content"] or "").strip()
            usage = data.get("usage", {})
            print(f"OpenAI response: {result!r} (tokens: prompt={usage.get('prompt_tokens', '?')}, completion={usage.get('completion_tokens', '?')})")
            return result
    except urllib.error.HTTPError as exc:
        print("OpenAI HTTPError:", exc.code, exc.read().decode("utf-8", "ignore"))
    except Exception as exc:
        print("OpenAI error:", repr(exc))
    return None


def classify_sentiment(text: str) -> str:
    """Return ``'Bad'`` or ``'Good'`` for *text*."""
    print(f"Classifying sentiment for: {text!r}")
    system_prompt = (
        "You are a comment moderator for a moving company's social media page. "
        "Classify the following comment as 'Bad' ONLY if you are highly confident "
        "it is defamatory, attacks the company's reputation, or contains hate speech, "
        "threats, or abusive language directed at the company or its staff. "
        "Examples of 'Bad': 'horrible company', 'worst movers ever', 'they broke everything', 'scam company'. "
        "When in doubt, always classify as 'Good'. "
        "Classify as 'Good' for everything else: spam, ads, quote requests, "
        "questions, positive feedback, neutral comments, or any irrelevant content. "
        "Reply with only one word: Good or Bad."
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
    """Ask OpenAI to summarise a conversation block."""
    print(f"Summarizing conversation ({len(text)} chars)")
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
    print(f"Generating reply from {len(messages)} messages")
    return chat_completion(messages, max_tokens=200, timeout=15)


def parse_date(text: str, today_str: str) -> tuple[str | None, str | None]:
    """Ask OpenAI to extract a date from natural language.

    Returns ``(iso_date, explanation)`` or ``(None, None)`` on failure.
    """
    system_prompt = (
        "You extract a date from natural language text and produce two responses:\n"
        "Date: exactly one ISO-formatted date (YYYY-MM-DD).\n"
        "Explanation: a short description of how that date was determined.\n\n"
        "You perform no other actions and add no extra content beyond these two responses.\n\n"
        f"Today's date is {today_str}.\n\n"
        "When interpreting dates without a year, assume the closest upcoming occurrence: "
        "if the specified month/day has already passed in the current year, use the next year instead.\n"
        "If only a month is provided (no day or year), use the last day of that month in the "
        "closest upcoming year (if the month has already passed this year, use that month in the next year).\n"
        "Never return a past date. Always choose a date that is today or in the future.\n\n"
        "If the input cannot be interpreted as a valid date, return the date exactly 14 days after "
        f"today ({today_str}), and in the explanation, state that no valid date was found so "
        "the fallback (today + 14 days) was used.\n\n"
        "Always output both parts clearly labeled as:\n"
        "Date: YYYY-MM-DD\n"
        "Explanation: <reasoning>"
    )
    print(f"OpenAI parse_date: '{text}'")
    result = chat_completion(
        [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": text},
        ],
        max_tokens=100,
        timeout=10,
    )
    if not result:
        return None, None

    iso_date = None
    explanation = None
    for line in result.splitlines():
        line = line.strip()
        if line.lower().startswith("date:"):
            iso_date = line.split(":", 1)[1].strip()
        elif line.lower().startswith("explanation:"):
            explanation = line.split(":", 1)[1].strip()
    return iso_date, explanation

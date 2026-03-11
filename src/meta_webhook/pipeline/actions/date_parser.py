"""Action: parse a free-text move date into YYYY-MM-DD format."""

import re
from datetime import date, timedelta

from meta_webhook.clients.openai_client import parse_date

_MONTH_MAP = {
    "jan": 1, "january": 1,
    "feb": 2, "february": 2,
    "mar": 3, "march": 3,
    "apr": 4, "april": 4,
    "may": 5,
    "jun": 6, "june": 6,
    "jul": 7, "july": 7,
    "aug": 8, "august": 8,
    "sep": 9, "september": 9,
    "oct": 10, "october": 10,
    "nov": 11, "november": 11,
    "dec": 12, "december": 12,
}

_FALLBACK_DAYS = 14

# Pattern: "April 20", "april 20", "Apr 20", "april20", "4/20", "04/20"
_MONTH_DAY_RE = re.compile(
    r"([a-zA-Z]+)\s*(\d{1,2})", re.IGNORECASE
)
_SLASH_RE = re.compile(r"(\d{1,2})/(\d{1,2})(?:/(\d{2,4}))?")
_ISO_RE = re.compile(r"(\d{4})-(\d{1,2})-(\d{1,2})")


def _ensure_future(d: date, reference: date) -> date:
    """If the date is in the past, bump it to the next year."""
    if d < reference:
        return d.replace(year=d.year + 1)
    return d


def format_move_date(data: dict) -> dict:
    """Parse ``move_date`` field into YYYY-MM-DD. Modifies data in place."""
    raw = (data.get("move_date") or data.get("when_is_the_move") or "").strip()
    data["move_date_raw"] = raw
    if not raw:
        data["move_date"] = _fallback()
        print(f"Date parser: no input, using fallback {data['move_date']}")
        return data

    today = date.today()
    parsed = _try_parse(raw, today)

    if parsed:
        data["move_date"] = parsed.isoformat()
        print(f"Date parser: '{raw}' → {data['move_date']}")
    else:
        # Regex failed – ask OpenAI
        ai_date, ai_explanation = parse_date(raw, today.isoformat())
        if ai_date:
            data["move_date"] = ai_date
            data["move_date_explanation"] = ai_explanation or ""
            print(f"Date parser (AI): '{raw}' → {ai_date} ({ai_explanation})")
        else:
            data["move_date"] = _fallback()
            data["move_date_explanation"] = "AI unavailable, used fallback"
            print(f"Date parser: AI failed for '{raw}', using fallback {data['move_date']}")

    return data


def _fallback() -> str:
    return (date.today() + timedelta(days=_FALLBACK_DAYS)).isoformat()


def _try_parse(text: str, today: date) -> date | None:
    """Try to extract a date from text. Returns None if unparseable."""
    text = text.strip()

    # ISO format: 2026-04-20
    m = _ISO_RE.match(text)
    if m:
        try:
            d = date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
            return _ensure_future(d, today)
        except ValueError:
            pass

    # Slash format: 4/20, 04/20, 4/20/2026
    m = _SLASH_RE.match(text)
    if m:
        try:
            month, day = int(m.group(1)), int(m.group(2))
            year = int(m.group(3)) if m.group(3) else today.year
            if year < 100:
                year += 2000
            d = date(year, month, day)
            return _ensure_future(d, today)
        except ValueError:
            pass

    # Month name + day: "April 20", "Apr 20"
    m = _MONTH_DAY_RE.search(text)
    if m:
        month_str = m.group(1).lower()
        day = int(m.group(2))
        month = _MONTH_MAP.get(month_str)
        if month:
            try:
                d = date(today.year, month, day)
                return _ensure_future(d, today)
            except ValueError:
                pass

    # Month name only: "April", "march"
    lower = text.lower().strip()
    if lower in _MONTH_MAP:
        month = _MONTH_MAP[lower]
        # Use last day of that month
        if month == 12:
            last_day = date(today.year, 12, 31)
        else:
            last_day = date(today.year, month + 1, 1) - timedelta(days=1)
        return _ensure_future(last_day, today)

    return None

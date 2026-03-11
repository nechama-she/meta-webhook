"""AI provider configuration."""

import os

OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "")
OPENAI_URL = "https://api.openai.com/v1/chat/completions"
OPENAI_MODEL = "gpt-4o-mini"
ENABLE_OPENAI_ANSWER = (
    os.environ.get("ENABLE_OPENAI_ANSWER", "true").lower() == "true"
)

OPENAI_HEADERS = {
    "Content-Type": "application/json",
    "Authorization": f"Bearer {OPENAI_API_KEY}",
}

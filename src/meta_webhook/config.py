"""
Centralised configuration - every setting lives here.

Values are read from environment variables at import time so the rest of
the package never touches ``os.environ`` directly.
"""

import os

# ── Meta / Facebook ──────────────────────────────────────────────────
VERIFY_TOKEN = os.environ["VERIFY_TOKEN"]
APP_SECRET = os.environ["APP_SECRET"]
COMMENTS_DETECTION_USER_TOKEN = os.environ["COMMENTS_DETECTION_USER_TOKEN"]

GRAPH_API_VERSION = "v21.0"
GRAPH_API_URL = f"https://graph.facebook.com/{GRAPH_API_VERSION}"
ACCOUNTS_API_VERSION = "v24.0"

# ── OpenAI ───────────────────────────────────────────────────────────
OPENAI_API_KEY = os.environ["OPENAI_API_KEY"]
OPENAI_URL = "https://api.openai.com/v1/chat/completions"
OPENAI_MODEL = "gpt-4o-mini"
ENABLE_OPENAI_ANSWER = (
    os.environ.get("ENABLE_OPENAI_ANSWER", "true").lower() == "true"
)

OPENAI_HEADERS = {
    "Content-Type": "application/json",
    "Authorization": f"Bearer {OPENAI_API_KEY}",
}

# ── DynamoDB table names ─────────────────────────────────────────────
EVENTS_TABLE = "fb_events"
CONVERSATIONS_TABLE = "conversations"
LEADS_TABLE = "leads"

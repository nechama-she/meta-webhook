"""Facebook / Meta Graph API configuration."""

import os

APP_SECRET = os.environ.get("APP_SECRET", "")
COMMENTS_DETECTION_USER_TOKEN = os.environ.get("COMMENTS_DETECTION_USER_TOKEN", "")

GRAPH_API_VERSION = "v21.0"
GRAPH_API_URL = f"https://graph.facebook.com/{GRAPH_API_VERSION}"
ACCOUNTS_API_VERSION = "v24.0"

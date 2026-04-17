"""AI abstraction layer – routes to the configured provider."""

from ai.providers import openai as _openai
from ai.providers import chat_api as _chat_api

# Public API – importable as: from ai import classify_sentiment, parse_date, ...

classify_sentiment = _openai.classify_sentiment
summarize_conversation = _openai.summarize_conversation
generate_reply = _openai.generate_reply
chat_reply = _chat_api.generate_reply
parse_date = _openai.parse_date

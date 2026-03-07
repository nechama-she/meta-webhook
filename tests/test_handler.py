"""Pure unit tests – every external dependency is mocked."""

import json
import os
import sys
from unittest.mock import patch, MagicMock, call

import pytest

# ── Mock boto3 before any application import ──────────────────────────
# dynamodb_client.py calls boto3.resource("dynamodb") at import time
# and imports boto3.dynamodb.conditions.Key, so we must provide the
# full module tree before any app module is imported.

_mock_boto3 = MagicMock()
sys.modules["boto3"] = _mock_boto3
sys.modules["boto3.dynamodb"] = _mock_boto3.dynamodb
sys.modules["boto3.dynamodb.conditions"] = _mock_boto3.dynamodb.conditions

ENV_VARS = {
    "VERIFY_TOKEN": "test_verify_token",
    "OPENAI_API_KEY": "test-openai-key",
    "COMMENTS_DETECTION_USER_TOKEN": "test-user-token",
    "APP_SECRET": "test-app-secret",
    "ENABLE_OPENAI_ANSWER": "true",
}


# ═══════════════════════════════════════════════════════════════════════
#  Handler – routing tests
# ═══════════════════════════════════════════════════════════════════════

class TestLambdaHandler:
    """Tests for the thin routing layer in handler.py."""

    @pytest.fixture(autouse=True)
    def _env(self):
        with patch.dict(os.environ, ENV_VARS):
            from meta_webhook.handler import lambda_handler
            self.handler = lambda_handler
            yield

    def test_get_valid_token_returns_challenge(self):
        event = {
            "requestContext": {"http": {"method": "GET"}},
            "queryStringParameters": {
                "hub.verify_token": "test_verify_token",
                "hub.challenge": "challenge_abc",
            },
        }
        resp = self.handler(event, None)
        assert resp == {"statusCode": 200, "body": "challenge_abc"}

    def test_get_wrong_token_returns_403(self):
        event = {
            "requestContext": {"http": {"method": "GET"}},
            "queryStringParameters": {"hub.verify_token": "wrong"},
        }
        resp = self.handler(event, None)
        assert resp["statusCode"] == 403

    def test_put_returns_405(self):
        event = {"requestContext": {"http": {"method": "PUT"}}}
        assert self.handler(event, None)["statusCode"] == 405

    def test_post_empty_body_returns_200(self):
        event = {
            "requestContext": {"http": {"method": "POST"}},
            "body": "{}",
        }
        assert self.handler(event, None) == {"statusCode": 200, "body": "OK"}

    @patch("meta_webhook.handler.process_comment")
    def test_post_feed_comment_dispatches(self, mock_comment):
        entry = {"id": "p1", "changes": [{"field": "feed", "value": {"item": "comment", "comment_id": "c1"}}]}
        event = {
            "requestContext": {"http": {"method": "POST"}},
            "body": json.dumps({"entry": [entry]}),
        }
        self.handler(event, None)
        mock_comment.assert_called_once()

    @patch("meta_webhook.handler.process_leadgen")
    def test_post_leadgen_dispatches(self, mock_lead):
        entry = {"id": "p1", "changes": [{"field": "leadgen", "value": {"leadgen_id": "L1", "page_id": "p1"}}]}
        event = {
            "requestContext": {"http": {"method": "POST"}},
            "body": json.dumps({"entry": [entry]}),
        }
        self.handler(event, None)
        mock_lead.assert_called_once()

    @patch("meta_webhook.handler.handle_user_message")
    def test_post_messenger_message_dispatches(self, mock_msg):
        entry = {
            "id": "p1",
            "messaging": [{"sender": {"id": "u1"}, "message": {"text": "hi", "mid": "m1"}}],
        }
        event = {
            "requestContext": {"http": {"method": "POST"}},
            "body": json.dumps({"entry": [entry]}),
        }
        self.handler(event, None)
        mock_msg.assert_called_once()

    @patch("meta_webhook.handler.handle_echo")
    def test_post_echo_dispatches(self, mock_echo):
        entry = {
            "id": "p1",
            "messaging": [{"sender": {"id": "p1"}, "recipient": {"id": "u1"}, "message": {"text": "hi", "mid": "m1", "is_echo": True}}],
        }
        event = {
            "requestContext": {"http": {"method": "POST"}},
            "body": json.dumps({"entry": [entry]}),
        }
        self.handler(event, None)
        mock_echo.assert_called_once()


# ═══════════════════════════════════════════════════════════════════════
#  Comment service
# ═══════════════════════════════════════════════════════════════════════

class TestCommentService:

    @pytest.fixture(autouse=True)
    def _env(self):
        with patch.dict(os.environ, ENV_VARS):
            yield

    @patch("meta_webhook.services.comment_service.save_event")
    @patch("meta_webhook.services.comment_service.classify_sentiment", return_value="Good")
    def test_good_comment_not_saved(self, mock_classify, mock_save):
        from meta_webhook.services.comment_service import process_comment
        process_comment({"id": "p1"}, {"comment_id": "c1", "message": "Nice!", "from": {"id": "u1"}})
        mock_classify.assert_called_once_with("Nice!")
        mock_save.assert_not_called()

    @patch("meta_webhook.services.comment_service.save_event")
    @patch("meta_webhook.services.comment_service.block_user")
    @patch("meta_webhook.services.comment_service.delete_comment")
    @patch("meta_webhook.services.comment_service.classify_sentiment", return_value="Bad")
    def test_bad_comment_deletes_blocks_saves(self, mock_classify, mock_del, mock_block, mock_save):
        from meta_webhook.services.comment_service import process_comment
        process_comment({"id": "p1"}, {"comment_id": "c2", "message": "Terrible", "from": {"id": "u2"}})
        mock_del.assert_called_once_with("c2", "p1")
        mock_block.assert_called_once_with("u2", "p1")
        mock_save.assert_called_once()
        assert mock_save.call_args[0][0]["classifier"] == "Bad"

    @patch("meta_webhook.services.comment_service.classify_sentiment")
    def test_empty_comment_skipped(self, mock_classify):
        from meta_webhook.services.comment_service import process_comment
        process_comment({"id": "p1"}, {"comment_id": "c3", "message": ""})
        mock_classify.assert_not_called()

    @patch("meta_webhook.services.comment_service.save_event")
    @patch("meta_webhook.services.comment_service.delete_comment")
    @patch("meta_webhook.services.comment_service.classify_sentiment", return_value="Bad")
    def test_bad_comment_no_user_skips_block(self, mock_classify, mock_del, mock_save):
        from meta_webhook.services.comment_service import process_comment
        process_comment({"id": "p1"}, {"comment_id": "c4", "message": "Awful"})
        mock_del.assert_called_once()
        # block_user not imported/called since no user_id
        mock_save.assert_called_once()


# ═══════════════════════════════════════════════════════════════════════
#  Lead service
# ═══════════════════════════════════════════════════════════════════════

class TestLeadService:

    @pytest.fixture(autouse=True)
    def _env(self):
        with patch.dict(os.environ, ENV_VARS):
            yield

    @patch("meta_webhook.services.lead_service.save_event")
    @patch("meta_webhook.services.lead_service.fetch_lead_details", return_value={"id": "L1", "fields": []})
    def test_lead_fetched_and_saved(self, mock_fetch, mock_save):
        from meta_webhook.services.lead_service import process_leadgen
        process_leadgen({"id": "p1"}, {"leadgen_id": "L1", "page_id": "p1"})
        mock_fetch.assert_called_once_with("L1", "p1")
        mock_save.assert_called_once()
        assert mock_save.call_args[0][0]["leadgen_id"] == "L1"

    @patch("meta_webhook.services.lead_service.fetch_lead_details")
    def test_missing_leadgen_id_skipped(self, mock_fetch):
        from meta_webhook.services.lead_service import process_leadgen
        process_leadgen({"id": "p1"}, {"page_id": "p1"})
        mock_fetch.assert_not_called()

    @patch("meta_webhook.services.lead_service.save_event")
    @patch("meta_webhook.services.lead_service.fetch_lead_details", return_value=None)
    def test_fetch_failure_does_not_save(self, mock_fetch, mock_save):
        from meta_webhook.services.lead_service import process_leadgen
        process_leadgen({"id": "p1"}, {"leadgen_id": "L2", "page_id": "p1"})
        mock_save.assert_not_called()


# ═══════════════════════════════════════════════════════════════════════
#  Conversation service
# ═══════════════════════════════════════════════════════════════════════

class TestConversationService:

    @pytest.fixture(autouse=True)
    def _env(self):
        with patch.dict(os.environ, ENV_VARS):
            yield

    def test_summarize_returns_conversation_when_below_threshold(self):
        from meta_webhook.services.conversation_service import summarize
        msgs = [{"role": "user", "text": f"msg{i}"} for i in range(3)]
        with patch("meta_webhook.services.conversation_service.replace_summary"), \
             patch("meta_webhook.services.conversation_service.summarize_conversation"):
            result = summarize(msgs, "u1", "p1")
        # 3 messages < 10 threshold → returns raw conversation
        assert len(result) == 3
        assert result[0] == {"role": "user", "content": "msg0"}

    @patch("meta_webhook.services.conversation_service.replace_summary")
    @patch("meta_webhook.services.conversation_service.summarize_conversation", return_value="Summary text")
    def test_summarize_creates_summary_at_threshold(self, mock_summarize, mock_replace):
        from meta_webhook.services.conversation_service import summarize
        msgs = [{"role": "user", "text": f"msg{i}"} for i in range(10)]
        result = summarize(msgs, "u1", "p1")
        mock_summarize.assert_called_once()
        mock_replace.assert_called_once()
        assert result == [{"role": "system", "content": "Summary text"}]

    @patch("meta_webhook.services.conversation_service.replace_summary")
    @patch("meta_webhook.services.conversation_service.summarize_conversation", return_value=None)
    def test_summarize_falls_back_when_openai_fails(self, mock_summarize, mock_replace):
        from meta_webhook.services.conversation_service import summarize
        msgs = [{"role": "user", "text": f"msg{i}"} for i in range(10)]
        result = summarize(msgs, "u1", "p1")
        mock_replace.assert_not_called()
        assert len(result) == 10  # returns raw conversation

    def test_summarize_skips_messages_before_existing_summary(self):
        from meta_webhook.services.conversation_service import summarize
        conv = [
            {"role": "summary", "text": "old summary"},
            {"role": "user", "text": "msg1"},
            {"role": "user", "text": "msg2"},
        ]
        with patch("meta_webhook.services.conversation_service.replace_summary"), \
             patch("meta_webhook.services.conversation_service.summarize_conversation"):
            result = summarize(conv, "u1", "p1")
        # Starts from summary index (0) → 3 items
        assert len(result) == 3
        assert result[0]["content"] == "old summary"


# ═══════════════════════════════════════════════════════════════════════
#  Messenger service
# ═══════════════════════════════════════════════════════════════════════

class TestMessengerService:

    @pytest.fixture(autouse=True)
    def _env(self):
        with patch.dict(os.environ, ENV_VARS):
            yield

    def _make_messaging(self, text="hello", mid="m1", sender="u1", ts=1000):
        return {
            "sender": {"id": sender},
            "recipient": {"id": "p1"},
            "message": {"text": text, "mid": mid},
            "timestamp": ts,
        }

    @patch("meta_webhook.services.messenger_service.save_message")
    def test_handle_echo_saves_as_sales(self, mock_save):
        from meta_webhook.services.messenger_service import handle_echo
        messaging = {
            "sender": {"id": "p1"},
            "recipient": {"id": "u1"},
            "message": {"text": "We'll call you", "mid": "m1", "is_echo": True},
            "timestamp": 1000,
        }
        handle_echo(messaging, {"id": "p1"})
        mock_save.assert_called_once()
        assert mock_save.call_args[1]["role"] == "sales"
        assert mock_save.call_args[1]["user_id"] == "u1"

    @patch("meta_webhook.services.messenger_service.send_messenger_message")
    @patch("meta_webhook.services.messenger_service.generate_reply", return_value="AI reply")
    @patch("meta_webhook.services.messenger_service.summarize", return_value=[{"role": "user", "content": "hello"}])
    @patch("meta_webhook.services.messenger_service.log_conversation")
    @patch("meta_webhook.services.messenger_service.fetch_conversation", return_value=[])
    @patch("meta_webhook.services.messenger_service.save_message")
    def test_user_message_generates_reply_and_sends(self, mock_save, mock_fetch, mock_log, mock_summarize, mock_reply, mock_send):
        from meta_webhook.services.messenger_service import handle_user_message
        handle_user_message(self._make_messaging(), {"id": "p1"})
        # Saves user message + AI answer = 2 calls
        assert mock_save.call_count == 2
        mock_reply.assert_called_once()
        mock_send.assert_called_once_with("u1", "AI reply", "p1")

    @patch("meta_webhook.services.messenger_service.send_messenger_message")
    @patch("meta_webhook.services.messenger_service.generate_reply", return_value="AI reply")
    @patch("meta_webhook.services.messenger_service.summarize", return_value=[])
    @patch("meta_webhook.services.messenger_service.log_conversation")
    @patch("meta_webhook.services.messenger_service.fetch_conversation", return_value=[])
    @patch("meta_webhook.services.messenger_service.save_message")
    def test_pattern_reply_overrides_openai(self, mock_save, mock_fetch, mock_log, mock_summarize, mock_reply, mock_send):
        from meta_webhook.services.messenger_service import handle_user_message
        handle_user_message(self._make_messaging(text="move size: storage"), {"id": "p1"})
        # Pattern reply overrides — send_messenger_message called with storage question
        sent_text = mock_send.call_args[0][1]
        assert "storage unit" in sent_text

    @patch("meta_webhook.services.messenger_service.send_messenger_message")
    @patch("meta_webhook.services.messenger_service.generate_reply", return_value=None)
    @patch("meta_webhook.services.messenger_service.summarize", return_value=[])
    @patch("meta_webhook.services.messenger_service.log_conversation")
    @patch("meta_webhook.services.messenger_service.fetch_conversation", return_value=[])
    @patch("meta_webhook.services.messenger_service.save_message")
    def test_no_reply_when_openai_returns_none_and_no_pattern(self, mock_save, mock_fetch, mock_log, mock_summarize, mock_reply, mock_send):
        from meta_webhook.services.messenger_service import handle_user_message
        handle_user_message(self._make_messaging(text="just chatting"), {"id": "p1"})
        mock_send.assert_not_called()

    @patch("meta_webhook.services.messenger_service.save_message")
    def test_empty_message_skipped(self, mock_save):
        from meta_webhook.services.messenger_service import handle_user_message
        messaging = {"sender": {"id": "u1"}, "message": {"text": "", "mid": "m1"}}
        handle_user_message(messaging, {"id": "p1"})
        mock_save.assert_not_called()


# ═══════════════════════════════════════════════════════════════════════
#  OpenAI client
# ═══════════════════════════════════════════════════════════════════════

class TestOpenAIClient:

    @pytest.fixture(autouse=True)
    def _env(self):
        with patch.dict(os.environ, ENV_VARS):
            yield

    @patch("meta_webhook.clients.openai_client.chat_completion", return_value="Bad")
    def test_classify_sentiment_bad(self, mock_cc):
        from meta_webhook.clients.openai_client import classify_sentiment
        assert classify_sentiment("terrible service") == "Bad"

    @patch("meta_webhook.clients.openai_client.chat_completion", return_value="Good")
    def test_classify_sentiment_good(self, mock_cc):
        from meta_webhook.clients.openai_client import classify_sentiment
        assert classify_sentiment("great job") == "Good"

    @patch("meta_webhook.clients.openai_client.chat_completion", return_value=None)
    def test_classify_sentiment_fallback_on_error(self, mock_cc):
        from meta_webhook.clients.openai_client import classify_sentiment
        assert classify_sentiment("anything") == "Good"

    @patch("meta_webhook.clients.openai_client.chat_completion", return_value="Short summary")
    def test_summarize_conversation(self, mock_cc):
        from meta_webhook.clients.openai_client import summarize_conversation
        assert summarize_conversation("long text") == "Short summary"

    @patch("meta_webhook.clients.openai_client.chat_completion", return_value="Hello!")
    def test_generate_reply(self, mock_cc):
        from meta_webhook.clients.openai_client import generate_reply
        assert generate_reply([{"role": "user", "content": "hi"}]) == "Hello!"

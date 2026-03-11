"""Pure unit tests - every external dependency is mocked."""

import json
import os
import sys
from datetime import date
from unittest.mock import patch, MagicMock, call

import pytest

# ── Mock boto3 before any application import ──────────────────────────
# db/client.py calls boto3.resource("dynamodb") at import time
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
            from handler import lambda_handler
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

    @patch("handler.process_comment")
    def test_post_feed_comment_dispatches(self, mock_comment):
        entry = {"id": "p1", "changes": [{"field": "feed", "value": {"item": "comment", "comment_id": "c1"}}]}
        event = {
            "requestContext": {"http": {"method": "POST"}},
            "body": json.dumps({"entry": [entry]}),
        }
        self.handler(event, None)
        mock_comment.assert_called_once()

    @patch("handler.process_leadgen")
    def test_post_leadgen_dispatches(self, mock_lead):
        entry = {"id": "p1", "changes": [{"field": "leadgen", "value": {"leadgen_id": "L1", "page_id": "p1"}}]}
        event = {
            "requestContext": {"http": {"method": "POST"}},
            "body": json.dumps({"entry": [entry]}),
        }
        self.handler(event, None)
        mock_lead.assert_called_once()

    @patch("handler.handle_user_message")
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

    @patch("handler.handle_echo")
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

    @patch("services.comment_service.save_event")
    @patch("services.comment_service.classify_sentiment", return_value="Good")
    def test_good_comment_not_saved(self, mock_classify, mock_save):
        from services.comment_service import process_comment
        process_comment({"id": "p1"}, {"comment_id": "c1", "message": "Nice!", "from": {"id": "u1"}})
        mock_classify.assert_called_once_with("Nice!")
        mock_save.assert_not_called()

    @patch("services.comment_service.save_event")
    @patch("services.comment_service.block_user")
    @patch("services.comment_service.delete_comment")
    @patch("services.comment_service.classify_sentiment", return_value="Bad")
    def test_bad_comment_deletes_blocks_saves(self, mock_classify, mock_del, mock_block, mock_save):
        from services.comment_service import process_comment
        process_comment({"id": "p1"}, {"comment_id": "c2", "message": "Terrible", "from": {"id": "u2"}})
        mock_del.assert_called_once_with("c2", "p1")
        mock_block.assert_called_once_with("u2", "p1")
        mock_save.assert_called_once()
        assert mock_save.call_args[0][0]["classifier"] == "Bad"

    @patch("services.comment_service.classify_sentiment")
    def test_empty_comment_skipped(self, mock_classify):
        from services.comment_service import process_comment
        process_comment({"id": "p1"}, {"comment_id": "c3", "message": ""})
        mock_classify.assert_not_called()

    @patch("services.comment_service.save_event")
    @patch("services.comment_service.delete_comment")
    @patch("services.comment_service.classify_sentiment", return_value="Bad")
    def test_bad_comment_no_user_skips_block(self, mock_classify, mock_del, mock_save):
        from services.comment_service import process_comment
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

    @patch("services.lead_service.save_event")
    @patch("services.lead_service.fetch_lead_details", return_value={
        "id": "L1",
        "created_time": "2026-03-08T04:10:08+0000",
        "field_data": [
            {"name": "full_name", "values": ["John Doe"]},
            {"name": "phone_number", "values": ["+15551234567"]},
            {"name": "email", "values": ["john@example.com"]},
            {"name": "move_size", "values": ["2_bedrooms"]},
            {"name": "pickup_zip", "values": ["20001"]},
            {"name": "delivery_zip", "values": ["10001"]},
        ],
    })
    def test_lead_fetched_and_saved(self, mock_fetch, mock_save):
        from services.lead_service import process_leadgen
        process_leadgen(
            {"id": "p1"},
            {"leadgen_id": "L1", "page_id": "p1", "ad_id": "A1", "form_id": "F1"},
        )
        mock_fetch.assert_called_once_with("L1", "p1")
        mock_save.assert_called_once()


# ═══════════════════════════════════════════════════════════════════════
#  Lead poll service
# ═══════════════════════════════════════════════════════════════════════

class TestLeadPollService:

    @pytest.fixture(autouse=True)
    def _env(self):
        with patch.dict(os.environ, ENV_VARS), \
             patch("lead_poll_service.PAGE_IDS", ["p1", "p2"]):
            yield

    @patch("lead_poll_service.run_pipeline")
    @patch("lead_poll_service.save_lead_if_new", return_value=True)
    @patch("lead_poll_service.get_form_leads", return_value=[
        {
            "id": "L50",
            "created_time": "2026-03-08T10:00:00+0000",
            "field_data": [
                {"name": "full_name", "values": ["Jane Doe"]},
                {"name": "email", "values": ["jane@example.com"]},
            ],
        },
    ])
    @patch("lead_poll_service.get_leadgen_forms", return_value=[
        {"id": "F1", "name": "Test Form"},
    ])
    def test_poll_saves_new_leads(self, mock_forms, mock_leads, mock_save, mock_actions):
        from lead_poll_service import poll_leads
        count = poll_leads()
        assert mock_forms.call_count == 2
        assert mock_save.call_count == 2  # one lead per page x 2 pages
        assert mock_actions.call_count == 2
        assert count == 2
        saved_item = mock_save.call_args_list[0][0][0]
        assert saved_item["leadgen_id"] == "L50"
        assert saved_item["full_name"] == "Jane Doe"
        assert saved_item["source"] == "poll"

    @patch("lead_poll_service.save_lead_if_new", return_value=False)
    @patch("lead_poll_service.get_form_leads", return_value=[
        {"id": "L50", "field_data": []},
    ])
    @patch("lead_poll_service.get_leadgen_forms", return_value=[
        {"id": "F1"},
    ])
    def test_poll_duplicate_leads_not_counted(self, mock_forms, mock_leads, mock_save):
        from lead_poll_service import poll_leads
        count = poll_leads()
        assert count == 0
        mock_save.assert_called()

    @patch("lead_poll_service.save_lead_if_new")
    @patch("lead_poll_service.get_form_leads", return_value=[])
    @patch("lead_poll_service.get_leadgen_forms", return_value=[
        {"id": "F1"},
    ])
    def test_poll_no_leads_saves_nothing(self, mock_forms, mock_leads, mock_save):
        from lead_poll_service import poll_leads
        count = poll_leads()
        assert count == 0
        mock_save.assert_not_called()

    @patch("lead_poll_service.PAGE_IDS", [])
    def test_poll_no_pages_configured(self):
        from lead_poll_service import poll_leads
        count = poll_leads()
        assert count == 0

    @patch("lead_poll_service.save_lead_if_new")
    @patch("lead_poll_service.get_leadgen_forms", return_value=[])
    def test_poll_no_forms_saves_nothing(self, mock_forms, mock_save):
        from lead_poll_service import poll_leads
        count = poll_leads()
        assert count == 0
        mock_save.assert_not_called()


class TestLeadPollHandler:

    @pytest.fixture(autouse=True)
    def _env(self):
        with patch.dict(os.environ, {**ENV_VARS, "PAGE_IDS": "p1"}):
            yield

    @patch("lead_poll_service.poll_leads", return_value=3)
    def test_lead_poll_handler_returns_count(self, mock_poll):
        from lead_poll_function import lead_poll_handler
        resp = lead_poll_handler({}, None)
        mock_poll.assert_called_once()
        assert resp["statusCode"] == 200
        assert "3" in resp["body"]


# ═══════════════════════════════════════════════════════════════════════
#  Pipeline – SmartMoving action
# ═══════════════════════════════════════════════════════════════════════

class TestSmartMovingAction:

    @pytest.fixture(autouse=True)
    def _env(self):
        with patch.dict(os.environ, ENV_VARS):
            yield

    def _make_lead(self, **overrides):
        lead = {
            "leadgen_id": "L100",
            "page_id": "p1",
            "form_id": "F1",
            "full_name": "Jane Doe",
            "phone_number": "+15551234567",
            "email": "jane@example.com",
            "pickup_zip": "20001",
            "delivery_zip": "10001",
            "move_date": "2026-04-01",
            "move_size": "2_bedrooms",
            "source": "poll",
        }
        lead.update(overrides)
        return lead

    @patch("pipeline.actions.smartmoving.create_lead", return_value='"abc-123"')
    def test_send_to_smartmoving_builds_payload(self, mock_create):
        from pipeline.actions.smartmoving import send_to_smartmoving
        result = send_to_smartmoving(self._make_lead())
        mock_create.assert_called_once()
        payload = mock_create.call_args[0][0]
        assert payload["fullName"] == "Jane Doe"
        assert payload["phoneNumber"] == "5551234567"
        assert payload["email"] == "jane@example.com"
        assert payload["originZip"] == "20001"
        assert payload["destinationZip"] == "10001"
        assert payload["referralSource"] == "Facebook-Gorilla-HHG-Local"
        assert result["smartmoving_lead_id"] == '"abc-123"'

    @patch("pipeline.actions.smartmoving.create_lead", return_value='"xyz"')
    def test_campaign_sets_nationwide_referral(self, mock_create):
        from pipeline.actions.smartmoving import send_to_smartmoving
        send_to_smartmoving(self._make_lead(campaign="Northeast-Midwest"))
        payload = mock_create.call_args[0][0]
        assert payload["referralSource"] == "Facebook-Gorilla-HHG-Nationwide"

    @patch("pipeline.actions.smartmoving.create_lead", return_value='"xyz"')
    def test_campaign_sets_fl_ga_nc_referral(self, mock_create):
        from pipeline.actions.smartmoving import send_to_smartmoving
        send_to_smartmoving(self._make_lead(campaign="FL-GA-NC"))
        payload = mock_create.call_args[0][0]
        assert payload["referralSource"] == "Facebook-Gorilla-HHG-FL-GA-NC"

    def test_clean_phone_strips_plus1(self):
        from pipeline.actions.smartmoving import _clean_phone
        assert _clean_phone("+15551234567") == "5551234567"

    def test_clean_phone_strips_1(self):
        from pipeline.actions.smartmoving import _clean_phone
        assert _clean_phone("15551234567") == "5551234567"

    def test_clean_phone_leaves_10_digit(self):
        from pipeline.actions.smartmoving import _clean_phone
        assert _clean_phone("5551234567") == "5551234567"


class TestLeadPipeline:

    @pytest.fixture(autouse=True)
    def _env(self):
        with patch.dict(os.environ, ENV_VARS):
            yield

    @patch("pipeline.actions.smartmoving.create_lead", return_value='"ok"')
    def test_run_pipeline_calls_all_actions(self, mock_create):
        from pipeline import run_pipeline
        lead = {"leadgen_id": "L1", "full_name": "Test", "phone_number": "5551234567"}
        run_pipeline("new_lead", lead)
        mock_create.assert_called_once()

    @patch("pipeline.actions.smartmoving.create_lead", side_effect=Exception("API down"))
    def test_run_pipeline_handles_error(self, mock_create):
        from pipeline import run_pipeline
        lead = {"leadgen_id": "L1"}
        # Should not raise — errors are caught per action
        run_pipeline("new_lead", lead)

    def test_run_pipeline_unknown_name_returns_data(self):
        from pipeline import run_pipeline
        data = {"leadgen_id": "L1"}
        result = run_pipeline("nonexistent", data)
        assert result is data


# ═══════════════════════════════════════════════════════════════════════
#  Pipeline – date parser action
# ═══════════════════════════════════════════════════════════════════════

class TestDateParserAction:

    @pytest.fixture(autouse=True)
    def _env(self):
        with patch.dict(os.environ, ENV_VARS):
            yield

    @patch("pipeline.actions.date_parser.date")
    def test_iso_date_passthrough(self, mock_date):
        mock_date.today.return_value = date(2026, 3, 10)
        mock_date.side_effect = lambda *a, **kw: date(*a, **kw)
        from pipeline.actions.date_parser import format_move_date
        data = {"move_date": "2026-04-20"}
        result = format_move_date(data)
        assert result["move_date"] == "2026-04-20"

    @patch("pipeline.actions.date_parser.date")
    def test_slash_date(self, mock_date):
        mock_date.today.return_value = date(2026, 3, 10)
        mock_date.side_effect = lambda *a, **kw: date(*a, **kw)
        from pipeline.actions.date_parser import format_move_date
        data = {"move_date": "4/20"}
        result = format_move_date(data)
        assert result["move_date"] == "2026-04-20"

    @patch("pipeline.actions.date_parser.date")
    def test_slash_date_with_year(self, mock_date):
        mock_date.today.return_value = date(2026, 3, 10)
        mock_date.side_effect = lambda *a, **kw: date(*a, **kw)
        from pipeline.actions.date_parser import format_move_date
        data = {"move_date": "4/20/27"}
        result = format_move_date(data)
        assert result["move_date"] == "2027-04-20"

    @patch("pipeline.actions.date_parser.date")
    def test_month_name_and_day(self, mock_date):
        mock_date.today.return_value = date(2026, 3, 10)
        mock_date.side_effect = lambda *a, **kw: date(*a, **kw)
        from pipeline.actions.date_parser import format_move_date
        data = {"move_date": "April 20"}
        result = format_move_date(data)
        assert result["move_date"] == "2026-04-20"

    @patch("pipeline.actions.date_parser.date")
    def test_abbreviated_month(self, mock_date):
        mock_date.today.return_value = date(2026, 3, 10)
        mock_date.side_effect = lambda *a, **kw: date(*a, **kw)
        from pipeline.actions.date_parser import format_move_date
        data = {"move_date": "Apr 20"}
        result = format_move_date(data)
        assert result["move_date"] == "2026-04-20"

    @patch("pipeline.actions.date_parser.date")
    def test_month_only_uses_last_day(self, mock_date):
        mock_date.today.return_value = date(2026, 3, 10)
        mock_date.side_effect = lambda *a, **kw: date(*a, **kw)
        from pipeline.actions.date_parser import format_move_date
        data = {"move_date": "april"}
        result = format_move_date(data)
        assert result["move_date"] == "2026-04-30"

    @patch("pipeline.actions.date_parser.date")
    def test_past_date_bumps_to_next_year(self, mock_date):
        mock_date.today.return_value = date(2026, 3, 10)
        mock_date.side_effect = lambda *a, **kw: date(*a, **kw)
        from pipeline.actions.date_parser import format_move_date
        data = {"move_date": "1/15"}
        result = format_move_date(data)
        assert result["move_date"] == "2027-01-15"

    @patch("pipeline.actions.date_parser.date")
    def test_empty_move_date_uses_fallback(self, mock_date):
        mock_date.today.return_value = date(2026, 3, 10)
        mock_date.side_effect = lambda *a, **kw: date(*a, **kw)
        from pipeline.actions.date_parser import format_move_date
        data = {"move_date": ""}
        result = format_move_date(data)
        assert result["move_date"] == "2026-03-24"

    @patch("pipeline.actions.date_parser.parse_date", return_value=("2026-05-01", "Interpreted 'asap' as May 1"))
    @patch("pipeline.actions.date_parser.date")
    def test_unparseable_text_falls_back_to_ai(self, mock_date, mock_parse):
        mock_date.today.return_value = date(2026, 3, 10)
        mock_date.side_effect = lambda *a, **kw: date(*a, **kw)
        from pipeline.actions.date_parser import format_move_date
        data = {"move_date": "asap"}
        result = format_move_date(data)
        mock_parse.assert_called_once_with("asap", "2026-03-10")
        assert result["move_date"] == "2026-05-01"
        assert result["move_date_explanation"] == "Interpreted 'asap' as May 1"

    @patch("pipeline.actions.date_parser.parse_date", return_value=(None, None))
    @patch("pipeline.actions.date_parser.date")
    def test_unparseable_text_ai_fails_uses_fallback(self, mock_date, mock_parse):
        mock_date.today.return_value = date(2026, 3, 10)
        mock_date.side_effect = lambda *a, **kw: date(*a, **kw)
        from pipeline.actions.date_parser import format_move_date
        data = {"move_date": "asap"}
        result = format_move_date(data)
        mock_parse.assert_called_once()
        assert result["move_date"] == "2026-03-24"
        assert result["move_date_explanation"] == "AI unavailable, used fallback"

    @patch("pipeline.actions.date_parser.parse_date", return_value=("2026-07-20", "next Tuesday in July"))
    @patch("pipeline.actions.date_parser.date")
    def test_ai_date_sets_explanation(self, mock_date, mock_parse):
        mock_date.today.return_value = date(2026, 3, 10)
        mock_date.side_effect = lambda *a, **kw: date(*a, **kw)
        from pipeline.actions.date_parser import format_move_date
        data = {"move_date": "next tuesday in july"}
        result = format_move_date(data)
        assert result["move_date"] == "2026-07-20"
        assert result["move_date_explanation"] == "next Tuesday in July"

    @patch("pipeline.actions.date_parser.date")
    def test_when_is_the_move_field(self, mock_date):
        mock_date.today.return_value = date(2026, 3, 10)
        mock_date.side_effect = lambda *a, **kw: date(*a, **kw)
        from pipeline.actions.date_parser import format_move_date
        data = {"when_is_the_move": "June 15"}
        result = format_move_date(data)
        assert result["move_date"] == "2026-06-15"

    @patch("pipeline.actions.date_parser.date")
    def test_returns_data_dict(self, mock_date):
        mock_date.today.return_value = date(2026, 3, 10)
        mock_date.side_effect = lambda *a, **kw: date(*a, **kw)
        from pipeline.actions.date_parser import format_move_date
        data = {"move_date": "4/20", "full_name": "Jane"}
        result = format_move_date(data)
        assert result is data
        assert result["full_name"] == "Jane"


# ═══════════════════════════════════════════════════════════════════════
#  Conversation service
# ═══════════════════════════════════════════════════════════════════════

class TestConversationService:

    @pytest.fixture(autouse=True)
    def _env(self):
        with patch.dict(os.environ, ENV_VARS):
            yield

    def test_summarize_returns_conversation_when_below_threshold(self):
        from services.conversation_service import summarize
        msgs = [{"role": "user", "text": f"msg{i}"} for i in range(3)]
        with patch("services.conversation_service.replace_summary"), \
             patch("services.conversation_service.summarize_conversation"):
            result = summarize(msgs, "u1", "p1")
        # 3 messages < 10 threshold → returns raw conversation
        assert len(result) == 3
        assert result[0] == {"role": "user", "content": "msg0"}

    @patch("services.conversation_service.replace_summary")
    @patch("services.conversation_service.summarize_conversation", return_value="Summary text")
    def test_summarize_creates_summary_at_threshold(self, mock_summarize, mock_replace):
        from services.conversation_service import summarize
        msgs = [{"role": "user", "text": f"msg{i}"} for i in range(10)]
        result = summarize(msgs, "u1", "p1")
        mock_summarize.assert_called_once()
        mock_replace.assert_called_once()
        assert result == [{"role": "system", "content": "Summary text"}]

    @patch("services.conversation_service.replace_summary")
    @patch("services.conversation_service.summarize_conversation", return_value=None)
    def test_summarize_falls_back_when_openai_fails(self, mock_summarize, mock_replace):
        from services.conversation_service import summarize
        msgs = [{"role": "user", "text": f"msg{i}"} for i in range(10)]
        result = summarize(msgs, "u1", "p1")
        mock_replace.assert_not_called()
        assert len(result) == 10  # returns raw conversation

    def test_summarize_skips_messages_before_existing_summary(self):
        from services.conversation_service import summarize
        conv = [
            {"role": "summary", "text": "old summary"},
            {"role": "user", "text": "msg1"},
            {"role": "user", "text": "msg2"},
        ]
        with patch("services.conversation_service.replace_summary"), \
             patch("services.conversation_service.summarize_conversation"):
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

    @patch("services.messenger_service.save_message")
    def test_handle_echo_saves_as_sales(self, mock_save):
        from services.messenger_service import handle_echo
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

    @patch("services.messenger_service.send_messenger_message")
    @patch("services.messenger_service.generate_reply", return_value="AI reply")
    @patch("services.messenger_service.summarize", return_value=[{"role": "user", "content": "hello"}])
    @patch("services.messenger_service.log_conversation")
    @patch("services.messenger_service.fetch_conversation", return_value=[])
    @patch("services.messenger_service.save_message")
    def test_user_message_generates_reply_and_sends(self, mock_save, mock_fetch, mock_log, mock_summarize, mock_reply, mock_send):
        from services.messenger_service import handle_user_message
        handle_user_message(self._make_messaging(), {"id": "p1"})
        # Saves user message + AI answer = 2 calls
        assert mock_save.call_count == 2
        mock_reply.assert_called_once()
        mock_send.assert_called_once_with("u1", "AI reply", "p1")

    @patch("services.messenger_service.send_messenger_message")
    @patch("services.messenger_service.generate_reply", return_value="AI reply")
    @patch("services.messenger_service.summarize", return_value=[])
    @patch("services.messenger_service.log_conversation")
    @patch("services.messenger_service.fetch_conversation", return_value=[])
    @patch("services.messenger_service.save_message")
    def test_pattern_reply_overrides_openai(self, mock_save, mock_fetch, mock_log, mock_summarize, mock_reply, mock_send):
        from services.messenger_service import handle_user_message
        handle_user_message(self._make_messaging(text="move size: storage"), {"id": "p1"})
        # Pattern reply overrides — send_messenger_message called with storage question
        sent_text = mock_send.call_args[0][1]
        assert "storage unit" in sent_text

    @patch("services.messenger_service.send_messenger_message")
    @patch("services.messenger_service.generate_reply", return_value=None)
    @patch("services.messenger_service.summarize", return_value=[])
    @patch("services.messenger_service.log_conversation")
    @patch("services.messenger_service.fetch_conversation", return_value=[])
    @patch("services.messenger_service.save_message")
    def test_no_reply_when_openai_returns_none_and_no_pattern(self, mock_save, mock_fetch, mock_log, mock_summarize, mock_reply, mock_send):
        from services.messenger_service import handle_user_message
        handle_user_message(self._make_messaging(text="just chatting"), {"id": "p1"})
        mock_send.assert_not_called()

    @patch("services.messenger_service.save_message")
    def test_empty_message_skipped(self, mock_save):
        from services.messenger_service import handle_user_message
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

    @patch("ai.providers.openai.chat_completion", return_value="Bad")
    def test_classify_sentiment_bad(self, mock_cc):
        from ai.providers.openai import classify_sentiment
        assert classify_sentiment("terrible service") == "Bad"

    @patch("ai.providers.openai.chat_completion", return_value="Good")
    def test_classify_sentiment_good(self, mock_cc):
        from ai.providers.openai import classify_sentiment
        assert classify_sentiment("great job") == "Good"

    @patch("ai.providers.openai.chat_completion", return_value=None)
    def test_classify_sentiment_fallback_on_error(self, mock_cc):
        from ai.providers.openai import classify_sentiment
        assert classify_sentiment("anything") == "Good"

    @patch("ai.providers.openai.chat_completion", return_value="Short summary")
    def test_summarize_conversation(self, mock_cc):
        from ai.providers.openai import summarize_conversation
        assert summarize_conversation("long text") == "Short summary"

    @patch("ai.providers.openai.chat_completion", return_value="Hello!")
    def test_generate_reply(self, mock_cc):
        from ai.providers.openai import generate_reply
        assert generate_reply([{"role": "user", "content": "hi"}]) == "Hello!"

    @patch("ai.providers.openai.chat_completion", return_value="Date: 2026-05-15\nExplanation: Mid-May, closest upcoming")
    def test_parse_date_returns_date_and_explanation(self, mock_cc):
        from ai.providers.openai import parse_date
        iso_date, explanation = parse_date("mid may", "2026-03-10")
        assert iso_date == "2026-05-15"
        assert explanation == "Mid-May, closest upcoming"
        mock_cc.assert_called_once()

    @patch("ai.providers.openai.chat_completion", return_value=None)
    def test_parse_date_returns_none_on_failure(self, mock_cc):
        from ai.providers.openai import parse_date
        iso_date, explanation = parse_date("gibberish", "2026-03-10")
        assert iso_date is None
        assert explanation is None

    @patch("ai.providers.openai.chat_completion", return_value="Date: 2026-03-24\nExplanation: No valid date found, used fallback (today + 14 days)")
    def test_parse_date_fallback_response(self, mock_cc):
        from ai.providers.openai import parse_date
        iso_date, explanation = parse_date("asap", "2026-03-10")
        assert iso_date == "2026-03-24"
        assert "fallback" in explanation.lower()

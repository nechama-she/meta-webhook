"""Pure unit tests - every external dependency is mocked."""

import hashlib
import hmac
import json
import os
import sys
from datetime import date, timedelta
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
    "APP_ENV": "dev",
    "VERIFY_TOKEN": "test_verify_token",
    "OPENAI_API_KEY": "test-openai-key",
    "COMMENTS_DETECTION_USER_TOKEN": "test-user-token",
    "APP_SECRET": "test-app-secret",
    "ENABLE_OPENAI_ANSWER": "true",
}


def _signed_post(body: dict) -> dict:
    """Build a POST event with a valid Meta X-Hub-Signature-256 for the test APP_SECRET."""
    raw = json.dumps(body)
    sig = "sha256=" + hmac.new(b"test-app-secret", raw.encode("utf-8"), hashlib.sha256).hexdigest()
    return {
        "requestContext": {"http": {"method": "POST"}},
        "headers": {"x-hub-signature-256": sig},
        "body": raw,
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
        event = _signed_post({})
        assert self.handler(event, None) == {"statusCode": 200, "body": "OK"}

    def test_meta_event_log_includes_complete_event_and_context(self, capsys):
        event = _signed_post({"object": "page", "entry": [{"id": "p1"}]})
        context = MagicMock()
        context.aws_request_id = "request-123"
        context.invoked_function_arn = "arn:aws:lambda:us-east-1:123:function:test"
        context.function_name = "test"
        context.function_version = "$LATEST"
        context.memory_limit_in_mb = "256"
        context.log_group_name = "/aws/lambda/test"
        context.log_stream_name = "stream-123"
        context.get_remaining_time_in_millis.return_value = 25000

        assert self.handler(event, context)["statusCode"] == 200

        logs = capsys.readouterr().out
        assert "META_WEBHOOK_EVENT" in logs
        assert '"aws_request_id": "request-123"' in logs
        assert '"body": "{\\"object\\": \\"page\\"' in logs
        assert event["headers"]["x-hub-signature-256"] in logs

    def test_post_missing_signature_logs_reason_and_continues(self, capsys):
        event = _signed_post({"object": "page"})
        event["headers"] = {"user-agent": "Meta-Test"}

        assert self.handler(event, None)["statusCode"] == 200

        logs = capsys.readouterr().out
        assert "X-Hub-Signature-256 header is missing" in logs
        assert "object='page'" in logs
        assert "user_agent='Meta-Test'" in logs

    def test_post_invalid_signature_logs_hmac_mismatch_and_continues(self, capsys):
        event = _signed_post({"object": "page"})
        event["headers"]["x-hub-signature-256"] = "sha256=" + ("0" * 64)

        assert self.handler(event, None)["statusCode"] == 200
        assert "HMAC mismatch" in capsys.readouterr().out

    def test_prod_invalid_signature_is_rejected(self, capsys):
        event = _signed_post({"object": "page"})
        event["headers"]["x-hub-signature-256"] = "sha256=" + ("0" * 64)

        with patch.dict(os.environ, {"APP_ENV": "prod"}):
            assert self.handler(event, None)["statusCode"] == 403
        assert "failed - rejecting" in capsys.readouterr().out

    @patch("handler.process_comment")
    def test_post_feed_comment_dispatches(self, mock_comment):
        entry = {"id": "p1", "changes": [{"field": "feed", "value": {"item": "comment", "comment_id": "c1"}}]}
        event = _signed_post({"entry": [entry]})
        self.handler(event, None)
        mock_comment.assert_called_once()

    @patch("handler.process_leadgen")
    def test_post_leadgen_dispatches(self, mock_lead):
        entry = {"id": "p1", "changes": [{"field": "leadgen", "value": {"leadgen_id": "L1", "page_id": "p1"}}]}
        event = _signed_post({"entry": [entry]})
        self.handler(event, None)
        mock_lead.assert_called_once()

    @patch("handler.handle_user_message")
    def test_post_messenger_message_dispatches(self, mock_msg):
        entry = {
            "id": "p1",
            "messaging": [{"sender": {"id": "u1"}, "message": {"text": "hi", "mid": "m1"}}],
        }
        event = _signed_post({"entry": [entry]})
        self.handler(event, None)
        mock_msg.assert_called_once()

    @patch("handler.handle_echo")
    def test_post_echo_dispatches(self, mock_echo):
        entry = {
            "id": "p1",
            "messaging": [{"sender": {"id": "p1"}, "recipient": {"id": "u1"}, "message": {"text": "hi", "mid": "m1", "is_echo": True}}],
        }
        event = _signed_post({"entry": [entry]})
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
             patch("lead_poll_service.get_companies", return_value=[
                 {"id": "c1", "name": "Company 1", "facebook_page_id": "p1", "smartmoving_branch_id": "b1"},
                 {"id": "c2", "name": "Company 2", "facebook_page_id": "p2", "smartmoving_branch_id": "b2"},
             ]):
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

    @patch("lead_poll_service.lead_exists_by_leadgen_id", return_value=True)
    @patch("lead_poll_service.get_form_leads", return_value=[
        {"id": "L50", "field_data": []},
    ])
    @patch("lead_poll_service.get_leadgen_forms", return_value=[
        {"id": "F1"},
    ])
    def test_poll_duplicate_leads_not_counted(self, mock_forms, mock_leads, mock_exists):
        from lead_poll_service import poll_leads
        count = poll_leads()
        assert count == 0
        mock_exists.assert_called()

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

    @patch("lead_poll_service.get_companies", return_value=[])
    def test_poll_no_pages_configured(self, mock_get_companies):
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
        with patch.dict(os.environ, {"APP_ENV": "prod"}):
            resp = lead_poll_handler({}, None)
        mock_poll.assert_called_once()
        assert resp["statusCode"] == 200
        assert "3" in resp["body"]

    @patch("lead_poll_service.poll_leads")
    def test_dev_schedule_does_not_poll_live_leads(self, mock_poll):
        from lead_poll_function import lead_poll_handler
        with patch.dict(os.environ, {**ENV_VARS, "APP_ENV": "dev"}):
            resp = lead_poll_handler({}, None)
        mock_poll.assert_not_called()
        assert "disabled" in resp["body"].lower()

    @patch("lead_poll_function.run_pipeline")
    def test_dev_explicit_test_lead_runs_pipeline(self, mock_pipeline):
        from lead_poll_function import lead_poll_handler
        with patch.dict(os.environ, {**ENV_VARS, "APP_ENV": "dev"}):
            resp = lead_poll_handler({"test_lead": {"full_name": "Dev Test"}}, None)
        payload = mock_pipeline.call_args[0][1]
        assert payload["source"] == "dev_test"
        assert payload["referral_source"] == "DEV-TEST"
        assert payload["leadgen_id"].startswith("DEV-TEST-")
        assert resp["statusCode"] == 200


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
            "move_date": (date.today() + timedelta(days=30)).isoformat(),
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

    @patch("pipeline.actions.smartmoving.create_lead", return_value='"xyz"')
    def test_explicit_referral_source_overrides_default(self, mock_create):
        from pipeline.actions.smartmoving import send_to_smartmoving
        send_to_smartmoving(self._make_lead(referral_source="DEV-TEST"))
        payload = mock_create.call_args[0][0]
        assert payload["referralSource"] == "DEV-TEST"

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
    def test_in_service_area_runs_smartmoving(self, mock_create):
        from pipeline import run_pipeline
        lead = {"leadgen_id": "L1", "full_name": "Test", "phone_number": "5551234567",
            "pickup_zip": "10001", "smartmoving_branch_id": "branch-1", "company_name": "TestCo", "page_id": "101598038182773"}  # not in NC/SC/GA/FL/TN → in_service_area=True
        run_pipeline("new_lead", lead)
        mock_create.assert_called_once()

    @patch("pipeline.actions.log_to_borat_sheet.append_row", return_value=True)
    @patch("pipeline.actions.send_to_granot.send_lead", return_value="OK")
    @patch("pipeline.actions.smartmoving.create_lead", return_value='"ok"')
    def test_out_of_service_area_skips_smartmoving(self, mock_create, mock_hm, mock_sheet):
        from pipeline import run_pipeline
        lead = {"leadgen_id": "L1", "full_name": "Test", "phone_number": "5551234567",
                "pickup_zip": "33028", "smartmoving_branch_id": "branch-1", "company_name": "Gorilla", "page_id": "101598038182773"}  # FL → in_service_area=False
        run_pipeline("new_lead", lead)
        mock_create.assert_called_once()  # out-of-area leads also go to SmartMoving
        mock_hm.assert_not_called()

    @patch("pipeline.actions.smartmoving.create_lead", side_effect=Exception("API down"))
    def test_run_pipeline_handles_error(self, mock_create):
        from pipeline import run_pipeline
        lead = {"leadgen_id": "L1", "pickup_zip": "10001"}
        # Should not raise — errors are caught per action
        run_pipeline("new_lead", lead)

    def test_run_pipeline_unknown_name_returns_data(self):
        from pipeline import run_pipeline
        data = {"leadgen_id": "L1"}
        result = run_pipeline("nonexistent", data)
        assert result is data

    @patch("pipeline.actions.log_to_borat_sheet.append_row", return_value=True)
    @patch("pipeline.actions.send_to_granot.send_lead", return_value="OK")
    def test_branch_sets_flag_on_data(self, mock_hm, mock_sheet):
        from pipeline import run_pipeline
        lead = {"leadgen_id": "L1", "full_name": "Test", "phone_number": "5551234567",
                "pickup_zip": "27510", "smartmoving_branch_id": "branch-1", "company_name": "Gorilla", "page_id": "101598038182773"}  # NC → not in service
        result = run_pipeline("new_lead", lead)
        assert result["in_service_area"] is True


# ═══════════════════════════════════════════════════════════════════════
#  Pipeline – check_pickup_zip action
# ═══════════════════════════════════════════════════════════════════════

class TestCheckPickupZip:

    def test_nc_zip_not_in_service_area(self):
        from pipeline.actions.check_pickup_zip import check_pickup_zip
        assert check_pickup_zip({"pickup_zip": "27510"})["in_service_area"] is False

    def test_sc_zip_not_in_service_area(self):
        from pipeline.actions.check_pickup_zip import check_pickup_zip
        assert check_pickup_zip({"pickup_zip": "29401"})["in_service_area"] is False

    def test_ga_zip_not_in_service_area(self):
        from pipeline.actions.check_pickup_zip import check_pickup_zip
        assert check_pickup_zip({"pickup_zip": "30301"})["in_service_area"] is False

    def test_ga_secondary_range(self):
        from pipeline.actions.check_pickup_zip import check_pickup_zip
        assert check_pickup_zip({"pickup_zip": "39901"})["in_service_area"] is False

    def test_fl_zip_not_in_service_area(self):
        from pipeline.actions.check_pickup_zip import check_pickup_zip
        assert check_pickup_zip({"pickup_zip": "33028"})["in_service_area"] is False

    def test_tn_zip_not_in_service_area(self):
        from pipeline.actions.check_pickup_zip import check_pickup_zip
        assert check_pickup_zip({"pickup_zip": "37201"})["in_service_area"] is False

    def test_other_zip_in_service_area(self):
        from pipeline.actions.check_pickup_zip import check_pickup_zip
        assert check_pickup_zip({"pickup_zip": "10001"})["in_service_area"] is True

    def test_empty_zip(self):
        from pipeline.actions.check_pickup_zip import check_pickup_zip
        assert check_pickup_zip({"pickup_zip": ""})["in_service_area"] is False

    def test_missing_zip(self):
        from pipeline.actions.check_pickup_zip import check_pickup_zip
        assert check_pickup_zip({})["in_service_area"] is False


# ═══════════════════════════════════════════════════════════════════════
#  Pipeline – send_to_granot action
# ═══════════════════════════════════════════════════════════════════════

class TestSendToGranot:

    @pytest.fixture(autouse=True)
    def _env(self):
        env = {**ENV_VARS,
               "GRANOT_API_ID": "TESTID",
               "GRANOT_MOVER_REF": "test@test.com",
               "BORAT_LEADS_SPREADSHEET_ID": "fake-spreadsheet-id"}
        with patch.dict(os.environ, env):
            yield

    def _make_lead(self, **overrides):
        lead = {
            "leadgen_id": "L100",
            "full_name": "Jane Doe",
            "phone_number": "+15551234567",
            "email": "jane@example.com",
            "pickup_zip": "33028",
            "delivery_zip": "10001",
            "move_date": (date.today() + timedelta(days=30)).isoformat(),
            "company_name": "Facebook",
        }
        lead.update(overrides)
        return lead

    @patch("pipeline.actions.send_to_granot.send_lead", return_value="OK 12345")
    def test_builds_correct_payload(self, mock_send):
        from pipeline.actions.send_to_granot import send_to_granot
        result = send_to_granot(self._make_lead())
        mock_send.assert_called_once()
        payload = mock_send.call_args[0][0]
        assert payload["firstname"] == "Jane"
        assert payload["lastname"] == "Doe"
        assert payload["email"] == "jane@example.com"
        assert payload["phone1"] == "5551234567"
        assert payload["oaddr"] == "33028"
        assert payload["dzip"] == "10001"
        assert payload["leadno"] == "L100"
        assert payload["movedte"] == (date.today() + timedelta(days=30)).isoformat()
        assert payload["label"] == "Facebook"
        assert payload["notes"] == "Original Pickup: 33028, Original Delivery: 10001"
        assert result["granot_ok"] is True

    @patch("pipeline.actions.send_to_granot.send_lead", return_value="ERROR")
    def test_non_ok_response_sets_false(self, mock_send):
        from pipeline.actions.send_to_granot import send_to_granot
        result = send_to_granot(self._make_lead())
        assert result["granot_ok"] is False

    @patch("pipeline.actions.send_to_granot.send_lead", return_value=None)
    def test_none_response_sets_false(self, mock_send):
        from pipeline.actions.send_to_granot import send_to_granot
        result = send_to_granot(self._make_lead())
        assert result["granot_ok"] is False

    @patch("pipeline.actions.send_to_granot.send_lead", return_value="OK")
    def test_strips_plus1_from_phone(self, mock_send):
        from pipeline.actions.send_to_granot import send_to_granot
        send_to_granot(self._make_lead(phone_number="+15559999999"))
        assert mock_send.call_args[0][0]["phone1"] == "5559999999"

    @patch("pipeline.actions.send_to_granot.send_lead", return_value="OK")
    def test_strips_1_from_phone(self, mock_send):
        from pipeline.actions.send_to_granot import send_to_granot
        send_to_granot(self._make_lead(phone_number="15559999999"))
        assert mock_send.call_args[0][0]["phone1"] == "5559999999"

    @patch("pipeline.actions.send_to_granot.send_lead", return_value="OK")
    def test_single_name_uses_empty_lastname(self, mock_send):
        from pipeline.actions.send_to_granot import send_to_granot
        send_to_granot(self._make_lead(full_name="Madonna"))
        payload = mock_send.call_args[0][0]
        assert payload["firstname"] == "Madonna"
        assert payload["lastname"] == ""

    @patch("pipeline.actions.send_to_granot.send_lead", return_value="OK")
    @patch("pipeline.actions.send_to_granot.date")
    def test_past_date_uses_tomorrow(self, mock_date, mock_send):
        mock_date.today.return_value = date(2026, 3, 10)
        mock_date.fromisoformat = date.fromisoformat
        from pipeline.actions.send_to_granot import send_to_granot
        send_to_granot(self._make_lead(move_date="2026-03-10"))
        assert mock_send.call_args[0][0]["movedte"] == "2026-03-11"

    @patch("pipeline.actions.send_to_granot.send_lead", return_value="OK")
    def test_returns_data_dict(self, mock_send):
        from pipeline.actions.send_to_granot import send_to_granot
        lead = self._make_lead()
        result = send_to_granot(lead)
        assert result is lead

    @patch("pipeline.actions.log_to_borat_sheet.append_row", return_value=True)
    @patch("pipeline.actions.send_to_granot.send_lead", return_value="OK 99")
    @patch("pipeline.actions.smartmoving.create_lead", return_value=None)
    def test_pipeline_out_of_service_calls_granot(self, mock_sm, mock_hm, mock_sheet):
        from pipeline import run_pipeline
        lead = {"leadgen_id": "L1", "full_name": "Test User", "phone_number": "5551234567",
                "pickup_zip": "33028", "delivery_zip": "10001", "smartmoving_branch_id": "branch-1", "company_name": "Gorilla", "page_id": "101598038182773"}  # FL → not in service
        result = run_pipeline("new_lead", lead)
        mock_hm.assert_not_called()
        mock_sm.assert_called_once()  # out-of-area leads also go to SmartMoving
        assert "granot_ok" not in result
        mock_sheet.assert_not_called()


# ═══════════════════════════════════════════════════════════════════════
#  Pipeline – log_to_borat_sheet action
# ═══════════════════════════════════════════════════════════════════════

class TestLogToBoratSheet:

    @pytest.fixture(autouse=True)
    def _env(self):
        env = {**ENV_VARS,
               "BORAT_LEADS_SPREADSHEET_ID": "fake-spreadsheet-id"}
        with patch.dict(os.environ, env):
            yield

    def _make_lead(self, **overrides):
        lead = {
            "leadgen_id": "L100",
            "full_name": "Jane Doe",
            "phone_number": "(555) 123-4567",
            "email": "jane@example.com",
            "pickup_zip": "33028",
            "delivery_zip": "10001",
            "move_date": (date.today() + timedelta(days=30)).isoformat(),
            "move_size": "2 Bedrooms",
            "created_time": "2026-03-07T16:25:35+0000",
            "granot_id": "OK 12345",
        }
        lead.update(overrides)
        return lead

    @patch("pipeline.actions.log_to_borat_sheet.append_row", return_value=True)
    def test_appends_correct_row(self, mock_append):
        from pipeline.actions.log_to_borat_sheet import log_to_borat_sheet
        lead = self._make_lead()
        expected_move_date = (date.today() + timedelta(days=30)).isoformat()
        result = log_to_borat_sheet(lead)
        mock_append.assert_called_once_with(
            "fake-spreadsheet-id",
            "Leads",
            [
                "2026-03-07T16:25:35+0000",
                "L100",
                "OK 12345",
                "Jane Doe",
                "jane@example.com",
                "(555) 123-4567",
                "33028",
                "10001",
                expected_move_date,
                "2 Bedrooms",
                "Yes",
            ],
        )
        assert result["borat_sheet_logged"] is True

    @patch("pipeline.actions.log_to_borat_sheet.append_row", return_value=False)
    def test_failure_sets_false(self, mock_append):
        from pipeline.actions.log_to_borat_sheet import log_to_borat_sheet
        result = log_to_borat_sheet(self._make_lead())
        assert result["borat_sheet_logged"] is False

    def test_skips_when_no_spreadsheet_id(self):
        with patch.dict(os.environ, {"BORAT_LEADS_SPREADSHEET_ID": ""}):
            from pipeline.actions.log_to_borat_sheet import log_to_borat_sheet
            result = log_to_borat_sheet(self._make_lead())
            assert "borat_sheet_logged" not in result

    @patch("pipeline.actions.log_to_borat_sheet.append_row", return_value=True)
    def test_returns_data_dict(self, mock_append):
        from pipeline.actions.log_to_borat_sheet import log_to_borat_sheet
        lead = self._make_lead()
        result = log_to_borat_sheet(lead)
        assert result is lead


# ═══════════════════════════════════════════════════════════════════════
#  Pipeline – email_gorilla_notification action
# ═══════════════════════════════════════════════════════════════════════

class TestEmailGorillaNotification:

    @pytest.fixture(autouse=True)
    def _env(self):
        env = {**ENV_VARS,
               "HHG_NOTIFY_FROM": "Gorilla <sales@floridatopmovers.com>",
               "HHG_NOTIFY_TO": "sales@floridatopmovers.com"}
        with patch.dict(os.environ, env):
            yield

    @patch("mailer.ses.boto3")
    def test_sends_email_with_correct_fields(self, mock_boto3):
        mock_client = MagicMock()
        mock_boto3.client.return_value = mock_client
        mock_client.send_email.return_value = {"MessageId": "abc"}

        from pipeline.actions.send_lead_email import email_gorilla_notification
        lead = {
            "leadgen_id": "L100",
            "full_name": "Roe Cole",
            "phone_number": "(804) 433-5159",
            "email": "roecole@gmail.com",
            "pickup_zip": "23236",
            "delivery_zip": "33028",
            "move_date": "April 20",
            "created_time": "2026-03-07T16:25:35+0000",
        }
        result = email_gorilla_notification(lead)
        mock_client.send_email.assert_called_once()
        kw = mock_client.send_email.call_args[1]
        assert kw["Source"] == "Gorilla <sales@floridatopmovers.com>"
        assert kw["Destination"]["ToAddresses"] == ["sales@floridatopmovers.com"]
        assert kw["Message"]["Subject"]["Data"] == "New Lead on Facebook From Roe Cole, Gorilla"
        body = kw["Message"]["Body"]["Text"]["Data"]
        assert "Email: roecole@gmail.com" in body
        assert "Full Name: Roe Cole" in body
        assert "Phone Number: (804) 433-5159" in body
        assert "Created Date: 2026-03-07T16:25:35+0000" in body
        assert "Pickup Zip: 23236" in body
        assert "Delivery Zip: 33028" in body
        assert "Move Date: April 20" in body
        assert result is lead

    @patch("mailer.ses.boto3")
    def test_sends_to_multiple_recipients(self, mock_boto3):
        mock_client = MagicMock()
        mock_boto3.client.return_value = mock_client
        mock_client.send_email.return_value = {"MessageId": "abc"}

        with patch.dict(os.environ, {"HHG_NOTIFY_TO": "a@test.com,b@test.com"}):
            from pipeline.actions.send_lead_email import email_gorilla_notification
            email_gorilla_notification({"full_name": "Test"})
            kw = mock_client.send_email.call_args[1]
            assert kw["Destination"]["ToAddresses"] == ["a@test.com", "b@test.com"]

    @patch("mailer.ses.boto3")
    def test_skips_when_not_configured(self, mock_boto3):
        with patch.dict(os.environ, {"HHG_NOTIFY_FROM": "", "HHG_NOTIFY_TO": ""}):
            from pipeline.actions.send_lead_email import email_gorilla_notification
            result = email_gorilla_notification({"full_name": "Test"})
            mock_boto3.client.assert_not_called()
            assert result == {"full_name": "Test"}

    @patch("mailer.ses.boto3")
    def test_returns_data_unchanged(self, mock_boto3):
        mock_client = MagicMock()
        mock_boto3.client.return_value = mock_client
        mock_client.send_email.return_value = {"MessageId": "abc"}

        from pipeline.actions.send_lead_email import email_gorilla_notification
        lead = {"leadgen_id": "L1", "full_name": "Test User"}
        result = email_gorilla_notification(lead)
        assert result is lead


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

    @patch("services.messenger_service.chat_reply", return_value="AI reply")
    @patch("services.messenger_service.save_message")
    def test_user_message_generates_reply_and_saves(self, mock_save, mock_reply):
        from services.messenger_service import handle_user_message
        handle_user_message(self._make_messaging(), {"id": "p1"})
        # Saves user message + AI answer = 2 calls
        assert mock_save.call_count == 2
        mock_reply.assert_called_once_with("u1", "hello", "messenger")

    @patch("services.messenger_service.send_messenger_message")
    @patch("services.messenger_service.chat_reply", return_value="AI reply")
    @patch("services.messenger_service.save_message")
    def test_pattern_reply_sends_to_client(self, mock_save, mock_reply, mock_send):
        from services.messenger_service import handle_user_message
        handle_user_message(self._make_messaging(text="move size: storage"), {"id": "p1"})
        # Pattern reply MUST be sent to the client
        mock_send.assert_called_once()
        sent_text = mock_send.call_args[0][1]
        assert "storage unit" in sent_text

    @patch("services.messenger_service.send_messenger_message")
    @patch("services.messenger_service.chat_reply", return_value="AI reply")
    @patch("services.messenger_service.save_message")
    def test_ai_reply_not_sent_to_client(self, mock_save, mock_reply, mock_send):
        from services.messenger_service import handle_user_message
        handle_user_message(self._make_messaging(text="just chatting"), {"id": "p1"})
        # AI reply saved but NOT sent to client
        mock_send.assert_not_called()
        assert mock_save.call_count == 2

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


# ═══════════════════════════════════════════════════════════════════════
#  Pending notes – SmartMoving note retry
# ═══════════════════════════════════════════════════════════════════════

class TestPendingNotes:

    @pytest.fixture(autouse=True)
    def _env(self):
        with patch.dict(os.environ, ENV_VARS):
            yield

    @patch("pipeline.actions.smartmoving_note.save_pending_note")
    @patch("pipeline.actions.smartmoving_note.get_smartmoving_id", return_value=None)
    def test_messenger_note_saves_pending_when_no_lead(self, mock_rds, mock_save):
        from pipeline.actions.smartmoving_note import send_messenger_note
        data = {"sender_id": "u1", "text": "hello", "direction": "user"}
        send_messenger_note(data)
        mock_save.assert_called_once_with(
            source="messenger", lookup_key="u1", note="messenger (customer): hello"
        )

    @patch("pipeline.actions.smartmoving_note.add_note", return_value=True)
    @patch("pipeline.actions.smartmoving_note.save_pending_note")
    @patch("pipeline.actions.smartmoving_note.get_smartmoving_id", return_value="OPP-1")
    def test_messenger_note_posts_when_lead_exists(self, mock_rds, mock_save, mock_add):
        from pipeline.actions.smartmoving_note import send_messenger_note
        data = {"sender_id": "u1", "text": "hello", "direction": "user"}
        result = send_messenger_note(data)
        mock_add.assert_called_once_with("OPP-1", "messenger (customer): hello")
        mock_save.assert_not_called()
        assert result["smartmoving_id"] == "OPP-1"

    @patch("services.aircall_service.save_pending_note")
    @patch("services.aircall_service.get_smartmoving_id_by_phone", return_value=None)
    @patch("services.aircall_service.add_note")
    def test_aircall_note_saves_pending_when_no_lead(self, mock_add, mock_rds, mock_save):
        from services.aircall_service import _post_sms_note
        _post_sms_note("+12403586309", "+12405707987", "hi there", "received")
        mock_add.assert_not_called()
        mock_save.assert_called_once()
        args = mock_save.call_args[1]
        assert args["source"] == "sms"
        assert args["lookup_key"] == "2403586309"
        assert "hi there" in args["note"]

    @patch("services.aircall_service.save_pending_note")
    @patch("services.aircall_service.get_smartmoving_id_by_phone", return_value="OPP-2")
    @patch("services.aircall_service.add_note", return_value=True)
    def test_aircall_note_posts_when_lead_exists(self, mock_add, mock_rds, mock_save):
        from services.aircall_service import _post_sms_note
        _post_sms_note("+12403586309", "+12405707987", "hi there", "received")
        mock_add.assert_called_once()
        mock_save.assert_not_called()

    @patch("pending_notes_service.delete_pending_note")
    @patch("pending_notes_service.add_note", return_value=True)
    @patch("pending_notes_service.get_smartmoving_id_by_phone", return_value="OPP-3")
    @patch("pending_notes_service.scan_pending_notes", return_value=[
        {"note_id": "n1", "source": "sms", "lookup_key": "2403586309", "note": "sms: +1240 to +1240: hi"},
    ])
    def test_retry_posts_and_deletes(self, mock_scan, mock_rds, mock_add, mock_del):
        from pending_notes_service import retry_pending_notes
        count = retry_pending_notes()
        assert count == 1
        mock_add.assert_called_once_with("OPP-3", "sms: +1240 to +1240: hi")
        mock_del.assert_called_once_with("n1")

    @patch("pending_notes_service.delete_pending_note")
    @patch("pending_notes_service.add_note")
    @patch("pending_notes_service.get_smartmoving_id", return_value=None)
    @patch("pending_notes_service.scan_pending_notes", return_value=[
        {"note_id": "n2", "source": "messenger", "lookup_key": "u1", "note": "messenger (customer): hello"},
    ])
    def test_retry_skips_when_still_no_lead(self, mock_scan, mock_rds, mock_add, mock_del):
        from pending_notes_service import retry_pending_notes
        count = retry_pending_notes()
        assert count == 0
        mock_add.assert_not_called()
        mock_del.assert_not_called()


# ═══════════════════════════════════════════════════════════════════════
#  SmartMoving followup service
# ═══════════════════════════════════════════════════════════════════════

class TestSmartMovingFollowup:

    @pytest.fixture(autouse=True)
    def _env(self):
        with patch.dict(os.environ, ENV_VARS):
            yield

    @patch("services.smartmoving_service.save_followup")
    @patch("services.smartmoving_service.get_followups")
    def test_handler_routes_followup_created(self, mock_get, mock_save):
        mock_get.return_value = [{
            "id": "464da8b8-6509-461a-bdb4-b42e01025c87",
            "opportunityId": "04965da2-2647-43f7-8128-b4260137b7b2",
            "type": 2,
            "title": "Text aaa bbbb",
            "assignedToId": "1bde6105-87ba-452e-7281-08dcd42bc7e8",
            "dueDateTime": "2026-04-17T08:00:00-04:00",
            "completedAtUtc": None,
            "notes": "some notes",
            "completed": False,
        }]
        from handler import lambda_handler
        event = {
            "requestContext": {"http": {"method": "POST"}},
            "body": json.dumps({
                "event-type": "follow-up-created",
                "followup-id": "464da8b8-6509-461a-bdb4-b42e01025c87",
                "opportunity-id": "04965da2-2647-43f7-8128-b4260137b7b2",
            }),
        }
        resp = lambda_handler(event, None)
        assert resp["statusCode"] == 200
        mock_get.assert_called_once_with("04965da2-2647-43f7-8128-b4260137b7b2")
        mock_save.assert_called_once()

    @patch("services.smartmoving_service.save_followup")
    @patch("services.smartmoving_service.get_followups")
    def test_followup_api_failure_still_returns_200(self, mock_get, mock_save):
        mock_get.return_value = None
        from handler import lambda_handler
        event = {
            "requestContext": {"http": {"method": "POST"}},
            "body": json.dumps({
                "event-type": "follow-up-created",
                "followup-id": "abc",
                "opportunity-id": "def",
            }),
        }
        resp = lambda_handler(event, None)
        assert resp["statusCode"] == 200
        mock_save.assert_not_called()

    @patch("services.smartmoving_service.save_followup")
    @patch("services.smartmoving_service.get_followups")
    def test_followup_saves_all_returned(self, mock_get, mock_save):
        mock_get.return_value = [
            {"id": "a1", "opportunityId": "op1", "type": 1, "title": "First"},
            {"id": "a2", "opportunityId": "op1", "type": 2, "title": "Second"},
        ]
        from services.smartmoving_service import handle_followup_created
        handle_followup_created({
            "event-type": "follow-up-created",
            "followup-id": "a1",
            "opportunity-id": "op1",
        })
        assert mock_save.call_count == 2

    @patch("services.smartmoving_service.save_followup")
    @patch("services.smartmoving_service.get_followups")
    def test_handler_routes_followup_changed(self, mock_get, mock_save):
        mock_get.return_value = [{"id": "x1", "opportunityId": "op1"}]
        from handler import lambda_handler
        event = {
            "requestContext": {"http": {"method": "POST"}},
            "body": json.dumps({
                "event-type": "follow-up-changed",
                "followup-id": "x1",
                "opportunity-id": "op1",
            }),
        }
        resp = lambda_handler(event, None)
        assert resp["statusCode"] == 200
        mock_get.assert_called_once_with("op1")
        mock_save.assert_called_once()

    @patch("services.smartmoving_service.save_followup")
    @patch("services.smartmoving_service.get_followups")
    def test_handler_routes_followup_completed(self, mock_get, mock_save):
        mock_get.return_value = [{"id": "x1", "opportunityId": "op1", "completed": True}]
        from handler import lambda_handler
        event = {
            "requestContext": {"http": {"method": "POST"}},
            "body": json.dumps({
                "event-type": "follow-up-completed",
                "followup-id": "x1",
                "opportunity-id": "op1",
            }),
        }
        resp = lambda_handler(event, None)
        assert resp["statusCode"] == 200
        mock_save.assert_called_once()

    @patch("services.smartmoving_service.delete_followup")
    def test_handler_routes_followup_deleted(self, mock_del):
        from handler import lambda_handler
        event = {
            "requestContext": {"http": {"method": "POST"}},
            "body": json.dumps({
                "event-type": "follow-up-deleted",
                "followup-id": "abc-123",
                "opportunity-id": "op1",
            }),
        }
        resp = lambda_handler(event, None)
        assert resp["statusCode"] == 200
        mock_del.assert_called_once_with("abc-123")


class TestOpportunityChanged:

    _OPP_ID = "04965da2-2647-43f7-8128-b4260137b7b2"

    @pytest.fixture(autouse=True)
    def _env(self):
        with patch.dict(os.environ, ENV_VARS):
            yield

    def _event(self):
        return {
            "requestContext": {"http": {"method": "POST"}},
            "body": json.dumps({
                "event-type": "opportunity-changed",
                "opportunity-id": self._OPP_ID,
                "opportunity-status": 0,
            }),
        }

    def _event_with_status(self, status):
        event = self._event()
        payload = json.loads(event["body"])
        payload["opportunity-status"] = status
        event["body"] = json.dumps(payload)
        return event

    @patch("services.smartmoving_service.send_sms")
    @patch("services.smartmoving_service.get_user_id_by_name")
    @patch("services.smartmoving_service.get_lead_by_smartmoving_id")
    @patch("services.smartmoving_service.get_sales_rep")
    @patch("services.smartmoving_service.get_audit_activity")
    def test_sends_sms_on_sales_person_change(self, mock_audit, mock_rep, mock_lead, mock_user, mock_sms):
        mock_audit.return_value = [
            {"description": "Sales person changed to Eli Jones.", "activityType": 1}
        ]
        mock_user.return_value = "user-123"
        mock_rep.return_value = "645873"
        mock_lead.return_value = {
            "full_name": "John Smith",
            "phone": "2403586309",
            "company_name": "Gorilla Haulers",
        }
        from handler import lambda_handler
        resp = lambda_handler(self._event(), None)
        assert resp["statusCode"] == 200
        mock_sms.assert_called_once()
        args = mock_sms.call_args
        assert args[0][0] == 645873
        assert args[0][1] == "+12403586309"
        assert "Hi John Smith" in args[0][2]
        assert "Eli Jones" in args[0][2]
        assert "Gorilla Haulers" in args[0][2]

    @patch("services.smartmoving_service.send_sms")
    @patch("services.smartmoving_service.get_user_id_by_name")
    @patch("services.smartmoving_service.get_lead_by_smartmoving_id")
    @patch("services.smartmoving_service.get_sales_rep")
    @patch("services.smartmoving_service.get_audit_activity")
    def test_sends_sms_with_trailing_space_dot(self, mock_audit, mock_rep, mock_lead, mock_user, mock_sms):
        mock_audit.return_value = [
            {"description": "Sales person changed to Sean Edson .", "activityType": 1}
        ]
        mock_user.return_value = "user-123"
        mock_rep.return_value = "645873"
        mock_lead.return_value = {
            "full_name": "Jane Doe",
            "phone": "+12403586309",
            "company_name": "Gorilla Haulers",
        }
        from handler import lambda_handler
        resp = lambda_handler(self._event(), None)
        assert resp["statusCode"] == 200
        mock_sms.assert_called_once()
        assert "Sean Edson" in mock_sms.call_args[0][2]

    @patch("services.smartmoving_service.send_sms")
    @patch("services.smartmoving_service.get_audit_activity")
    def test_no_sms_when_not_sales_person_change(self, mock_audit, mock_sms):
        mock_audit.return_value = [
            {"description": "Opportunity reopened.", "activityType": 1}
        ]
        from handler import lambda_handler
        resp = lambda_handler(self._event(), None)
        assert resp["statusCode"] == 200
        mock_sms.assert_not_called()

    @patch("services.smartmoving_service.send_sms")
    @patch("services.smartmoving_service.get_sales_rep")
    @patch("services.smartmoving_service.get_audit_activity")
    def test_no_sms_when_rep_not_in_table(self, mock_audit, mock_rep, mock_sms):
        mock_audit.return_value = [
            {"description": "Sales person changed to Unknown Person.", "activityType": 1}
        ]
        mock_rep.return_value = None
        from handler import lambda_handler
        resp = lambda_handler(self._event(), None)
        assert resp["statusCode"] == 200
        mock_sms.assert_not_called()

    @patch("services.smartmoving_service.send_sms")
    @patch("services.smartmoving_service.get_user_id_by_name")
    @patch("services.smartmoving_service.get_lead_by_smartmoving_id")
    @patch("services.smartmoving_service.get_sales_rep")
    @patch("services.smartmoving_service.get_audit_activity")
    def test_no_sms_when_lead_not_found(self, mock_audit, mock_rep, mock_lead, mock_user, mock_sms):
        mock_audit.return_value = [
            {"description": "Sales person changed to Eli Jones.", "activityType": 1}
        ]
        mock_user.return_value = "user-123"
        mock_rep.return_value = "645873"
        mock_lead.return_value = None
        from handler import lambda_handler
        resp = lambda_handler(self._event(), None)
        assert resp["statusCode"] == 200
        mock_sms.assert_not_called()

    @patch("services.smartmoving_service.send_sms")
    @patch("services.smartmoving_service.get_user_id_by_name")
    @patch("services.smartmoving_service.get_lead_by_smartmoving_id")
    @patch("services.smartmoving_service.get_sales_rep")
    @patch("services.smartmoving_service.get_audit_activity")
    def test_no_sms_when_lead_missing_phone(self, mock_audit, mock_rep, mock_lead, mock_user, mock_sms):
        mock_audit.return_value = [
            {"description": "Sales person changed to Eli Jones.", "activityType": 1}
        ]
        mock_user.return_value = "user-123"
        mock_rep.return_value = "645873"
        mock_lead.return_value = {
            "full_name": "John Smith",
            "phone": None,
            "company_name": "Gorilla Haulers",
        }
        from handler import lambda_handler
        resp = lambda_handler(self._event(), None)
        assert resp["statusCode"] == 200
        mock_sms.assert_not_called()

    @patch("services.smartmoving_service.send_sms")
    @patch("services.smartmoving_service.get_audit_activity")
    def test_no_sms_when_no_audit_activity(self, mock_audit, mock_sms):
        mock_audit.return_value = []
        from handler import lambda_handler
        resp = lambda_handler(self._event(), None)
        assert resp["statusCode"] == 200
        mock_sms.assert_not_called()

    @patch("services.smartmoving_service.send_sms")
    @patch("services.smartmoving_service.get_user_id_by_name")
    @patch("services.smartmoving_service.get_lead_by_smartmoving_id")
    @patch("services.smartmoving_service.get_sales_rep")
    @patch("services.smartmoving_service.get_audit_activity")
    def test_no_sms_when_user_not_in_table_even_if_rep_number_exists(
        self, mock_audit, mock_rep, mock_lead, mock_user, mock_sms
    ):
        mock_audit.return_value = [
            {"description": "Sales person changed to Unknown Person.", "activityType": 1}
        ]
        mock_user.return_value = None
        mock_rep.return_value = "645873"
        mock_lead.return_value = {
            "full_name": "John Smith",
            "phone": "2403586309",
            "company_name": "Gorilla Haulers",
        }
        from handler import lambda_handler
        resp = lambda_handler(self._event(), None)
        assert resp["statusCode"] == 200
        mock_sms.assert_not_called()

    @patch("services.smartmoving_service._ensure_lead_exists")
    @patch("services.smartmoving_service.get_lead_by_smartmoving_id")
    @patch("services.smartmoving_service.get_opportunity")
    @patch("services.smartmoving_service.get_audit_activity")
    def test_creates_missing_lead_when_booked(self, mock_audit, mock_opp, mock_lead, mock_ensure):
        mock_audit.return_value = [{"description": "Opportunity booked.", "activityType": 1}]
        mock_opp.return_value = {
            "id": self._OPP_ID,
            "status": 4,
            "customer": {"name": "John Smith", "phoneNumber": "2403586309", "emailAddress": "john@example.com"},
        }
        mock_lead.side_effect = [None, {"id": "crm-1", "full_name": "John Smith", "phone": "2403586309", "company_name": "Gorilla Haulers"}]
        from handler import lambda_handler
        resp = lambda_handler(self._event_with_status(4), None)
        assert resp["statusCode"] == 200
        mock_ensure.assert_called_once_with(self._OPP_ID, "booked")

    @patch("services.smartmoving_service._ensure_lead_exists")
    @patch("services.smartmoving_service.get_lead_by_smartmoving_id")
    @patch("services.smartmoving_service.get_opportunity")
    @patch("services.smartmoving_service.get_audit_activity")
    def test_creates_missing_lead_when_completed(self, mock_audit, mock_opp, mock_lead, mock_ensure):
        mock_audit.return_value = [{"description": "Opportunity completed.", "activityType": 1}]
        mock_opp.return_value = {
            "id": self._OPP_ID,
            "status": 10,
            "customer": {"name": "John Smith", "phoneNumber": "2403586309", "emailAddress": "john@example.com"},
        }
        mock_lead.side_effect = [None, {"id": "crm-1", "full_name": "John Smith", "phone": "2403586309", "company_name": "Gorilla Haulers"}]
        from handler import lambda_handler
        resp = lambda_handler(self._event_with_status(10), None)
        assert resp["statusCode"] == 200
        mock_ensure.assert_called_once_with(self._OPP_ID, "completed")

    @patch("services.smartmoving_service._ensure_lead_exists")
    @patch("services.smartmoving_service.get_lead_by_smartmoving_id")
    @patch("services.smartmoving_service.get_audit_activity")
    def test_does_not_create_missing_lead_for_other_status(self, mock_audit, mock_lead, mock_ensure):
        mock_audit.return_value = [{"description": "Opportunity reopened.", "activityType": 1}]
        mock_lead.return_value = None
        from handler import lambda_handler
        resp = lambda_handler(self._event_with_status(0), None)
        assert resp["statusCode"] == 200
        mock_ensure.assert_not_called()

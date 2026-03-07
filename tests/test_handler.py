import json
import pytest
import os
from unittest.mock import patch, MagicMock


# ── Helpers ───────────────────────────────────────────────────────────

ENV_VARS = {
    "VERIFY_TOKEN": "test_verify_token",
    "OPENAI_API_KEY": "test-openai-key",
    "COMMENTS_DETECTION_USER_TOKEN": "test-user-token",
    "APP_SECRET": "test-app-secret",
}


# ── Webhook verification (GET) ───────────────────────────────────────

def test_webhook_verification():
    """Test GET request for webhook verification"""
    with patch.dict(os.environ, ENV_VARS):
        from meta_webhook.handler import lambda_handler

        event = {
            "requestContext": {"http": {"method": "GET"}},
            "queryStringParameters": {
                "hub.mode": "subscribe",
                "hub.verify_token": "test_verify_token",
                "hub.challenge": "test_challenge_string",
            },
        }

        response = lambda_handler(event, None)

        assert response["statusCode"] == 200
        assert response["body"] == "test_challenge_string"


def test_webhook_verification_fails_wrong_token():
    """Test GET request with wrong verify token"""
    with patch.dict(os.environ, ENV_VARS):
        from meta_webhook.handler import lambda_handler

        event = {
            "requestContext": {"http": {"method": "GET"}},
            "queryStringParameters": {
                "hub.mode": "subscribe",
                "hub.verify_token": "wrong_token",
                "hub.challenge": "test_challenge",
            },
        }

        response = lambda_handler(event, None)

        assert response["statusCode"] == 403
        assert response["body"] == "Forbidden"


# ── Method not allowed ────────────────────────────────────────────────

def test_method_not_allowed():
    """Non-GET/POST methods return 405."""
    with patch.dict(os.environ, ENV_VARS):
        from meta_webhook.handler import lambda_handler

        event = {"requestContext": {"http": {"method": "PUT"}}}
        response = lambda_handler(event, None)

        assert response["statusCode"] == 405


# ── POST: empty body returns 200 ─────────────────────────────────────

def test_post_empty_body():
    """POST with empty body should not crash."""
    with patch.dict(os.environ, ENV_VARS):
        from meta_webhook.handler import lambda_handler

        event = {
            "requestContext": {"http": {"method": "POST"}},
            "body": "{}",
        }
        response = lambda_handler(event, None)

        assert response["statusCode"] == 200
        assert response["body"] == "OK"


# ── Comment service unit test ─────────────────────────────────────────

def test_process_comment_good(monkeypatch):
    """Good comments are saved but not deleted."""
    with patch.dict(os.environ, ENV_VARS):
        from meta_webhook.services import comment_service
        from meta_webhook.clients import openai_client, dynamodb_client

        monkeypatch.setattr(openai_client, "classify_sentiment", lambda t: "Good")
        saved = []
        monkeypatch.setattr(dynamodb_client, "save_event", lambda body, **kw: saved.append(body))

        entry = {"id": "page123"}
        value = {"comment_id": "c1", "message": "Great post!", "from": {"id": "u1"}}
        comment_service.process_comment(entry, value)

        assert len(saved) == 1
        assert saved[0]["classifier"] == "Good"


def test_process_comment_bad_deletes_and_blocks(monkeypatch):
    """Bad comments trigger delete + block."""
    with patch.dict(os.environ, ENV_VARS):
        from meta_webhook.services import comment_service
        from meta_webhook.clients import openai_client, facebook_client, dynamodb_client

        monkeypatch.setattr(openai_client, "classify_sentiment", lambda t: "Bad")
        monkeypatch.setattr(facebook_client, "get_page_token", lambda pid: "tok")

        deleted, blocked, saved = [], [], []
        monkeypatch.setattr(facebook_client, "delete_comment", lambda cid, tok: deleted.append(cid))
        monkeypatch.setattr(facebook_client, "block_user", lambda uid, tok: blocked.append(uid))
        monkeypatch.setattr(dynamodb_client, "save_event", lambda body, **kw: saved.append(body))

        entry = {"id": "page123"}
        value = {"comment_id": "c2", "message": "Terrible!", "from": {"id": "u2"}}
        comment_service.process_comment(entry, value)

        assert deleted == ["c2"]
        assert blocked == ["u2"]
        assert saved[0]["classifier"] == "Bad"

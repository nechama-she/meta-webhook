import json
import pytest
import os
from unittest.mock import patch


@patch.dict(os.environ, {
    "VERIFY_TOKEN": "test_verify_token",
    "OPENAI_API_KEY": "test-openai-key",
    "COMMENTS_DETECTION_USER_TOKEN": "test-user-token",
    "APP_SECRET": "test-app-secret",
})
def test_webhook_verification():
    """Test GET request for webhook verification"""
    from meta_webhook.handler import lambda_handler
    
    event = {
        "requestContext": {
            "http": {
                "method": "GET"
            }
        },
        "queryStringParameters": {
            "hub.mode": "subscribe",
            "hub.verify_token": "test_verify_token",
            "hub.challenge": "test_challenge_string"
        }
    }
    
    response = lambda_handler(event, None)
    
    assert response["statusCode"] == 200
    assert response["body"] == "test_challenge_string"


@patch.dict(os.environ, {
    "VERIFY_TOKEN": "test_verify_token",
    "OPENAI_API_KEY": "test-openai-key",
    "COMMENTS_DETECTION_USER_TOKEN": "test-user-token",
    "APP_SECRET": "test-app-secret",
})
def test_webhook_verification_fails_wrong_token():
    """Test GET request with wrong verify token"""
    from meta_webhook.handler import lambda_handler
    
    event = {
        "requestContext": {
            "http": {
                "method": "GET"
            }
        },
        "queryStringParameters": {
            "hub.mode": "subscribe",
            "hub.verify_token": "wrong_token",
            "hub.challenge": "test_challenge"
        }
    }
    
    response = lambda_handler(event, None)
    
    assert response["statusCode"] == 403
    assert response["body"] == "Forbidden"
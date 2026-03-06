import pytest
from meta_webhook.handler import lambda_handler


def test_webhook_verification():
    """Test GET request for webhook verification"""
    event = {
        "requestContext": {
            "http": {
                "method": "GET"
            }
        },
        "queryStringParameters": {
            "hub.mode": "subscribe",
            "hub.verify_token": "badcommentsfacebook",
            "hub.challenge": "test_challenge_string"
        }
    }
    
    response = lambda_handler(event, None)
    
    assert response["statusCode"] == 200
    assert response["body"] == "test_challenge_string"


def test_webhook_verification_fails_wrong_token():
    """Test GET request with wrong verify token"""
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
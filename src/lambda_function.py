"""AWS Lambda entry point - keeps the SAM ``Handler: lambda_function.lambda_handler`` mapping."""

from meta_webhook.handler import lambda_handler  # noqa: F401
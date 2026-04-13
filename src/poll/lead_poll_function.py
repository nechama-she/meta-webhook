"""AWS Lambda entry point for the scheduled lead-polling function."""

from lead_poll_service import poll_leads
from pending_notes_service import retry_pending_notes


def lead_poll_handler(event, context):
    """EventBridge Scheduler invokes this every X minutes."""
    print("Lead poll triggered by schedule")
    count = poll_leads()
    retried = retry_pending_notes()
    return {"statusCode": 200, "body": f"Polled {count} new lead(s), retried {retried} note(s)"}

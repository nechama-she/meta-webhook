"""AWS Lambda entry point for the scheduled lead-polling function."""

from lead_poll_service import poll_leads
from pending_notes_service import retry_pending_notes


def lead_poll_handler(event, context):
    """EventBridge Scheduler invokes this every X minutes."""
    trace = {
        "request_id": getattr(context, "aws_request_id", "") if context else "",
        "invoked_arn": getattr(context, "invoked_function_arn", "") if context else "",
    }
    print(f"Lead poll triggered by schedule: {trace}")
    count = poll_leads(trace=trace)
    return {"statusCode": 200, "body": f"Polled {count} new lead(s)"}

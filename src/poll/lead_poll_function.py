"""AWS Lambda entry point for the scheduled lead-polling function."""

import os
import uuid

from lead_poll_service import poll_leads
from pending_notes_service import retry_pending_notes
from pipeline import run_pipeline


def lead_poll_handler(event, context):
    """EventBridge Scheduler invokes this every X minutes."""
    trace = {
        "request_id": getattr(context, "aws_request_id", "") if context else "",
        "invoked_arn": getattr(context, "invoked_function_arn", "") if context else "",
    }
    app_env = os.environ.get("APP_ENV", "prod").strip().lower()
    if app_env != "prod":
        test_lead = event.get("test_lead") if isinstance(event, dict) else None
        if not isinstance(test_lead, dict):
            print(f"Lead poll skipped: automatic polling is disabled in {app_env}")
            return {"statusCode": 200, "body": f"Automatic lead polling disabled in {app_env}"}

        item = dict(test_lead)
        item.setdefault("leadgen_id", f"DEV-TEST-{trace['request_id'] or uuid.uuid4()}")
        item["source"] = "dev_test"
        item["referral_source"] = "DEV-TEST"
        item["_lambda_request_id"] = trace["request_id"]
        item["_lambda_invoked_arn"] = trace["invoked_arn"]
        print(f"Running explicit dev test lead: leadgen_id={item['leadgen_id']} trace={trace}")
        run_pipeline("new_lead", item)
        return {"statusCode": 200, "body": f"Dev test lead processed: {item['leadgen_id']}"}

    print(f"Lead poll triggered by schedule: {trace}")
    count = poll_leads(trace=trace)
    return {"statusCode": 200, "body": f"Polled {count} new lead(s)"}

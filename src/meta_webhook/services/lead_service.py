"""Lead-generation webhook processing."""

import json

from meta_webhook.clients.facebook_client import fetch_lead_details
from meta_webhook.clients.dynamodb_client import save_event
from meta_webhook.config import LEADS_TABLE


def process_leadgen(entry: dict, lead_value: dict) -> None:
    """Fetch full lead data from Graph API and persist it."""
    leadgen_id = lead_value.get("leadgen_id")
    page_id = lead_value.get("page_id")

    print(f"Leadgen event: leadgen_id={leadgen_id}, page_id={page_id}")

    if not leadgen_id or not page_id:
        return

    lead_data = fetch_lead_details(leadgen_id, page_id)

    if lead_data:
        save_event(
            {
                "entry_id": entry.get("id"),
                "leadgen_id": leadgen_id,
                "page_id": page_id,
                "lead_data": lead_data,
                "raw_value": lead_value,
            },
            table_name=LEADS_TABLE,
            primary_key="leadgen_id",
        )
        print("Lead saved to leads table")

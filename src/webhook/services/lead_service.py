"""Lead-generation webhook processing."""

import json

from meta_api import fetch_lead_details
from db import save_event
from db.config import LEADS_TABLE


def process_leadgen(entry: dict, lead_value: dict) -> None:
    """Fetch full lead data from Graph API and persist it."""
    leadgen_id = lead_value.get("leadgen_id")
    page_id = lead_value.get("page_id")

    print(f"Leadgen event: leadgen_id={leadgen_id}, page_id={page_id}")

    if not leadgen_id or not page_id:
        return

    lead_data = fetch_lead_details(leadgen_id, page_id)

    if lead_data:
        # Flatten field_data into top-level keys
        item = {
            "leadgen_id": leadgen_id,
            "entry_id": entry.get("id"),
            "page_id": page_id,
            "created_time": lead_data.get("created_time"),
            "ad_id": lead_value.get("ad_id"),
            "adgroup_id": lead_value.get("adgroup_id"),
            "form_id": lead_value.get("form_id"),
        }
        for field in lead_data.get("field_data", []):
            name = field.get("name", "")
            values = field.get("values", [])
            item[name] = values[0] if len(values) == 1 else values

        print(f"Lead item: {json.dumps(item, default=str)}")
        save_event(item, table_name=LEADS_TABLE, primary_key="leadgen_id")
        print("Lead saved to leads table")

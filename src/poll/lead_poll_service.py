"""Periodic lead polling - pulls leads from Facebook page forms."""

import os
import time

from meta_api import get_leadgen_forms, get_form_leads
from db import save_lead_if_new
from pipeline import run_pipeline

PAGE_IDS = [
    pid.strip()
    for pid in os.environ.get("PAGE_IDS", "").split(",")
    if pid.strip()
]
LEAD_POLL_LOOKBACK_MINUTES = int(os.environ.get("LEAD_POLL_LOOKBACK_MINUTES", "30"))


def poll_leads() -> int:
    """Pull recent leads from all configured pages and save new ones.

    Returns the number of *new* leads saved (duplicates are skipped).
    """
    if not PAGE_IDS:
        print("Lead poll: no PAGE_IDS configured, skipping")
        return 0

    since = int(time.time()) - (LEAD_POLL_LOOKBACK_MINUTES * 60)
    total_saved = 0

    print(f"Lead poll: checking {len(PAGE_IDS)} page(s), lookback={LEAD_POLL_LOOKBACK_MINUTES}min (since {since})")

    for page_id in PAGE_IDS:
        forms = get_leadgen_forms(page_id)
        for form in forms:
            form_id = form.get("id")
            if not form_id:
                continue

            leads = get_form_leads(form_id, page_id, since)
            for lead in leads:
                leadgen_id = lead.get("id")
                if not leadgen_id:
                    continue

                item = {
                    "leadgen_id": leadgen_id,
                    "page_id": page_id,
                    "form_id": form_id,
                    "created_time": lead.get("created_time"),
                    "source": "poll",
                }
                for field in lead.get("field_data", []):
                    name = field.get("name", "")
                    values = field.get("values", [])
                    item[name] = values[0] if len(values) == 1 else values

                if save_lead_if_new(item):
                    print(f"NEW_LEAD_FOUND | leadgen_id={leadgen_id} | page_id={page_id} | form_id={form_id} | details={item}")
                    run_pipeline("new_lead", item)
                    total_saved += 1

    print(f"Lead poll: done, {total_saved} new lead(s) saved")
    return total_saved

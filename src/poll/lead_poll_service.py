"""Periodic lead polling - pulls leads from Facebook page forms."""

import time

from meta_api import get_leadgen_forms, get_form_leads
from db import save_lead_if_new, update_lead
from pipeline import run_pipeline
from crm.moving_crm import get_companies

LEAD_POLL_LOOKBACK_MINUTES = 30


def poll_leads() -> int:
    """Pull recent leads from all company pages and save new ones.

    Returns the number of *new* leads saved (duplicates are skipped).
    """
    companies = get_companies()
    if not companies:
        print("Lead poll: no companies found, skipping")
        return 0

    since = int(time.time()) - (LEAD_POLL_LOOKBACK_MINUTES * 60)
    total_saved = 0

    print(f"Lead poll: checking {len(companies)} company/page(s), lookback={LEAD_POLL_LOOKBACK_MINUTES}min (since {since})")

    for company in companies:
        company_id = company.get("id")
        company_name = company.get("name", "")
        page_id = company.get("facebook_page_id")
        branch_id = company.get("smartmoving_branch_id") or company.get("samrtmoving_branch_id")
        
        if not page_id:
            print(f"Lead poll: company {company_id} ({company_name}) has no facebook_page_id, skipping")
            continue
        
        if not branch_id:
            print(f"Lead poll: company {company_id} ({company_name}) has no smartmoving_branch_id, skipping")
            continue
        
        print(f"Lead poll: polling company {company_id} ({company_name}) page_id={page_id} branch_id={branch_id}")
        
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
                    "company_id": company_id,
                    "company_name": company_name,
                    "smartmoving_branch_id": branch_id,
                }
                for field in lead.get("field_data", []):
                    name = field.get("name", "")
                    values = field.get("values", [])
                    item[name] = values[0] if len(values) == 1 else values

                if save_lead_if_new(item):
                    print(f"NEW_LEAD_FOUND | leadgen_id={leadgen_id} | company={company_name} | page_id={page_id} | form_id={form_id}")
                    run_pipeline("new_lead", item)
                    update_lead(item)
                    total_saved += 1

    print(f"Lead poll: done, {total_saved} new lead(s) saved")
    return total_saved

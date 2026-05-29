"""Handle SmartMoving webhook events."""

import re

from aircall import send_sms
from crm.smartmoving_notes import add_note, get_audit_activity, get_followups
from db import try_claim_dedupe_key
from db.rds_client import (
    delete_followup,
    get_company_template,
    get_lead_by_smartmoving_id,
    get_sales_rep,
    get_user_id_by_name,
    save_followup,
    set_lead_assigned_to,
)

_SALES_PERSON_RE = re.compile(r"^Sales person changed to (.+?)\.?\s*$")


def handle_followup_created(body: dict) -> None:
    """Process a follow-up-created or follow-up-changed event."""
    opportunity_id = body.get("opportunity-id")
    followup_id = body.get("followup-id")
    if not opportunity_id or not followup_id:
        print("Missing opportunity-id or followup-id in SmartMoving event")
        return

    print(f"Fetching followups for opportunity {opportunity_id}")
    followups = get_followups(opportunity_id)
    if followups is None:
        print(f"Failed to fetch followups for {opportunity_id}")
        return

    for followup in followups:
        save_followup(followup)


def handle_followup_deleted(body: dict) -> None:
    """Process a follow-up-deleted event."""
    followup_id = body.get("followup-id")
    if not followup_id:
        print("Missing followup-id in SmartMoving delete event")
        return

    delete_followup(followup_id)


def handle_opportunity_changed(body: dict) -> None:
    """Process an opportunity-changed event.

    If the most recent audit activity is a sales person assignment,
    send an intro SMS from the rep's Aircall number to the lead.
    """
    opportunity_id = body.get("opportunity-id")
    if not opportunity_id:
        print("Missing opportunity-id in opportunity-changed event")
        return

    activities = get_audit_activity(opportunity_id)
    print(f"Audit activity response for {opportunity_id}: {activities!r}")
    if not activities:
        print(f"No audit activity for {opportunity_id}")
        return

    latest = activities[0]
    description = latest.get("description", "")
    match = _SALES_PERSON_RE.match(description)
    if not match:
        print(f"Not a sales person change: {description!r}")
        return

    rep_name = match.group(1).strip()
    print(f"Sales person changed to {rep_name!r} for {opportunity_id}")

    user_id = get_user_id_by_name(rep_name)
    if user_id:
        set_lead_assigned_to(opportunity_id, user_id)
    else:
        print(f"User {rep_name!r} not found in users table; assigned_to not updated")

    aircall_number_id = get_sales_rep(rep_name)
    if not aircall_number_id:
        print(f"Sales rep {rep_name!r} not found in users table")
        return

    lead = get_lead_by_smartmoving_id(opportunity_id)
    if not lead or not lead.get("phone") or not lead.get("company_name"):
        print(f"Lead not found or missing phone/company for {opportunity_id}")
        return

    full_name = lead.get("full_name") or ""
    if not full_name:
        print(f"Lead has no name for {opportunity_id}")
        return

    template = get_company_template(lead["company_id"], "rep_assignment_sms") if lead.get("company_id") else None
    first_name = full_name.split()[0] if full_name.strip() else ""
    if template:
        message = template.format(
            first_name=first_name,
            company_name=lead["company_name"],
            company_phone=lead.get("company_phone") or "",
            smartmoving_id=opportunity_id or "",
            rep_name=rep_name or "",
        )
    else:
        print(f"No rep_assignment_sms template for company_id={lead.get('company_id')!r}; using default")
        message = (
            f"Hi {full_name},\n"
            f"This is {rep_name} from {lead['company_name']}. "
            f"I've been assigned to help you with your upcoming move.\n\n"
            f"I'll be your point of contact and can assist with the estimate. "
            f"We can schedule a virtual in-home estimate, complete the estimate "
            f"over the phone with one of our estimators, or schedule a free "
            f"in-home estimate.\n\n"
            f"You can reply here or feel free to give me a call anytime."
        )

    phone = lead["phone"]
    if not phone.startswith("+"):
        phone = f"+1{phone}" if len(phone) == 10 else f"+{phone}"

    # Dedupe per rep+phone: same rep won't send intro twice to same number.
    # Different reps can each send their own intro once.
    rep_key = str(aircall_number_id).strip()
    dedupe_key = f"SMS_INTRO:{rep_key}:{phone}"
    is_first_intro_for_phone = try_claim_dedupe_key(dedupe_key)
    already_sent_intro_sms = not is_first_intro_for_phone
    print(
        f"Intro dedupe check: rep={rep_name}, rep_key={rep_key}, phone={phone}, key={dedupe_key}, "
        f"already_sent={already_sent_intro_sms}"
    )

    if already_sent_intro_sms:
        print(f"Intro SMS already sent by this rep to {phone} - adding note instead of duplicate send")
        note_text = (
            f"[DEDUPE] This contact may already be assigned to you in another company/opportunity. "
            "Intro SMS was not sent again."
        )
        add_note(opportunity_id, note_text)
        return

    print(f"Sending intro SMS to {phone} from Aircall number {aircall_number_id}")
    send_sms(int(aircall_number_id), phone, message)

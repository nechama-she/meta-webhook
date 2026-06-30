"""Handle SmartMoving webhook events."""

import re
from datetime import datetime, timezone

from aircall import send_sms
from crm.moving_crm import get_companies, patch_lead
from crm.smartmoving_notes import add_note, get_audit_activity, get_followups, get_opportunity
from db import try_claim_dedupe_key
from db.rds_client import (
    delete_followup,
    get_company_id_by_name,
    get_company_template,
    get_lead_by_smartmoving_id,
    get_sales_rep,
    get_user_id_by_name,
    save_followup,
    set_lead_assigned_to,
    set_lead_company_id,
    set_lead_status,
)
from pipeline.actions.send_to_moving_crm import send_to_moving_crm

_SALES_PERSON_RE = re.compile(r"^Sales person changed to (.+?)\.?\s*$")
_BRANCH_RE = re.compile(r"^Branch changed to (.+?)\.?\s*$")
_CHANGED_FROM_BOOKED_RE = re.compile(r"\bchanged\b.*\bfrom\s+Booked\b", re.IGNORECASE)
_CHANGED_TO_BOOKED_RE = re.compile(r"\bchanged\s+to\s+Booked\b", re.IGNORECASE)

_SMARTMOVING_STATUS_TO_CRM = {
    0: "new",
    1: "contacted",
    3: "quoted",
    4: "booked",
    10: "completed",
    11: "completed",
    20: "cancelled",
    30: "lost",
    50: "lost",
}


def _clean_phone(phone: str) -> str:
    """Strip +1 country code and non-digit characters, returning 10-digit number."""
    phone = "".join(c for c in phone if c.isdigit())
    if len(phone) == 11 and phone.startswith("1"):
        phone = phone[1:]
    return phone


_OPPORTUNITY_TYPE_MAP = {0: "Local", 1: "Intrastate", 2: "Interstate"}


def _add_if_value(payload: dict, key: str, value) -> None:
    if value is None:
        return
    if isinstance(value, str) and not value.strip():
        return
    payload[key] = value


def _format_smartmoving_date(value) -> str:
    if value is None or value == "":
        return ""
    text = str(value).strip()
    if text.endswith(".0"):
        text = text[:-2]
    if len(text) == 8 and text.isdigit():
        return f"{text[0:4]}-{text[4:6]}-{text[6:8]}"
    return text


def _parse_priority(lead_status) -> int | None:
    if not lead_status:
        return None
    match = re.search(r"(\d+)", str(lead_status))
    if not match:
        return None
    return int(match.group(1))


def _map_opportunity_status(opportunity_status) -> str:
    try:
        code = int(opportunity_status)
    except Exception:
        return ""
    return _SMARTMOVING_STATUS_TO_CRM.get(code, "")


def _build_notes_from_opportunity(opportunity: dict) -> str:
    parts = []
    quote_number = opportunity.get("quoteNumber")
    referral_source = opportunity.get("referralSource")
    branch = opportunity.get("branch") or {}
    move_size = opportunity.get("moveSize") or {}
    tariff = opportunity.get("tariff") or {}

    if quote_number not in ("", None):
        parts.append(f"quoteNumber: {quote_number}")
    if referral_source:
        parts.append(f"referralSource: {referral_source}")
    branch_name = (branch.get("name") or "").strip()
    if branch_name:
        parts.append(f"branchName: {branch_name}")
    if branch.get("phoneNumber"):
        parts.append(f"branchPhone: {branch.get('phoneNumber')}")
    if move_size.get("name"):
        parts.append(f"moveSize: {move_size.get('name')}")
    tariff_name = (tariff.get("name") or "").strip()
    if tariff_name:
        parts.append(f"tariff: {tariff_name}")

    return " | ".join(parts)


def _map_estimated_charges(charges: list) -> list[dict]:
    mapped = []
    for charge in charges or []:
        mapped_charge = {
            "sortOrder": charge.get("sortOrder", 0),
            "subtotal": charge.get("subtotal", 0),
            "discountAmount": charge.get("discountAmount", 0),
            "totalCost": charge.get("totalCost", 0),
        }
        _add_if_value(mapped_charge, "name", charge.get("name"))
        _add_if_value(mapped_charge, "description", charge.get("description"))
        _add_if_value(mapped_charge, "editableDescription", charge.get("editableDescription"))
        mapped.append(mapped_charge)
    return mapped


def _job_price_from_charges(job: dict) -> float:
    total = 0.0
    for charge in job.get("estimatedCharges") or []:
        try:
            total += float(charge.get("totalCost", 0) or 0)
        except Exception:
            pass
    return round(total, 2)


def _map_payments(payments: list) -> list[dict]:
    mapped = []
    for payment in payments or []:
        row = {"amount": payment.get("amount", 0)}
        _add_if_value(row, "takenByUser", payment.get("takenByUser"))
        mapped.append(row)
    return mapped


def _map_estimated_total(estimated_total: dict | None) -> dict:
    estimated_total = estimated_total or {}
    return {
        "subtotal": estimated_total.get("subtotal", 0),
        "taxableAmount": estimated_total.get("taxableAmount", 0),
        "tax": estimated_total.get("tax", 0),
        "finalTotal": estimated_total.get("finalTotal", 0),
    }


def _build_jobs_payload(opportunity: dict) -> list[dict]:
    opportunity_jobs = opportunity.get("jobs") or []
    jobs = []
    for job in opportunity_jobs:
        addresses = job.get("jobAddresses") or []
        pickup = addresses[0] if len(addresses) > 0 and addresses[0] else None
        delivery = addresses[1] if len(addresses) > 1 and addresses[1] else None
        move_date = _format_smartmoving_date(job.get("jobDate") or opportunity.get("serviceDate"))

        crm_job = {
            "smartmoving_job_id": job.get("id"),
            "estimatedCharges": _map_estimated_charges(job.get("estimatedCharges") or []),
            "price": _job_price_from_charges(job),
        }
        _add_if_value(crm_job, "pickup_zip", pickup)
        _add_if_value(crm_job, "delivery_zip", delivery)
        _add_if_value(crm_job, "move_date", move_date)
        _add_if_value(crm_job, "booked_move_date", move_date)
        jobs.append(crm_job)
    return jobs


def _build_crm_payload(opportunity_id: str, opportunity: dict, existing_lead: dict | None) -> dict:
    existing_lead = existing_lead or {}
    customer = opportunity.get("customer") or {}
    sales_assignee = opportunity.get("salesAssignee") or {}
    branch = opportunity.get("branch") or {}

    payload: dict = {}

    mapped_status = _map_opportunity_status(opportunity.get("status"))
    _add_if_value(payload, "status", mapped_status)

    priority = _parse_priority(opportunity.get("leadStatus"))
    if priority is not None:
        payload["priority"] = priority

    assigned_name = sales_assignee.get("name")
    _add_if_value(payload, "assigned_to_name", assigned_name)
    if assigned_name:
        user_id = get_user_id_by_name(assigned_name)
        if user_id:
            payload["assigned_to"] = user_id

    company_name = (branch.get("name") or existing_lead.get("company_name") or "").strip()
    _add_if_value(payload, "company_name", company_name)
    _add_if_value(payload, "notes", _build_notes_from_opportunity(opportunity))
    _add_if_value(payload, "full_name", customer.get("name") or existing_lead.get("full_name"))
    _add_if_value(payload, "smartmoving_id", opportunity.get("id") or opportunity_id)

    customer_phone = customer.get("phoneNumber") or ""
    phone = _clean_phone(customer_phone) if customer_phone else existing_lead.get("phone")
    _add_if_value(payload, "phone_number", phone)
    _add_if_value(payload, "email", customer.get("emailAddress"))

    payload["estimatedTotal"] = _map_estimated_total(opportunity.get("estimatedTotal"))
    payload["payments"] = _map_payments(opportunity.get("payments") or [])

    jobs = _build_jobs_payload(opportunity)
    if jobs:
        payload["jobs"] = jobs

    return payload


def _sync_opportunity_to_crm(opportunity_id: str) -> bool:
    opportunity = get_opportunity(opportunity_id, include_full=True)
    if not opportunity:
        print(f"Could not fetch full opportunity for {opportunity_id}")
        return False

    existing_lead = get_lead_by_smartmoving_id(opportunity_id)
    if not existing_lead:
        print(f"Lead not found for {opportunity_id}; skipping CRM sync (no create on update)")
        return False

    crm_lead_id = existing_lead.get("id")
    if not crm_lead_id:
        print(f"Lead id missing for {opportunity_id}; cannot patch CRM lead")
        return False

    payload = _build_crm_payload(opportunity_id, opportunity, existing_lead)
    ok = patch_lead(crm_lead_id, payload)
    if not ok and "assigned_to_name" in payload:
        retry_payload = dict(payload)
        retry_payload.pop("assigned_to_name", None)
        ok = patch_lead(crm_lead_id, retry_payload)

    print(f"Opportunity sync to CRM for {opportunity_id}: ok={ok}")
    return ok


def _handle_sales_person_assignment(opportunity_id: str, rep_name: str) -> None:
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


def _ensure_lead_exists(opportunity_id: str, status: str) -> None:
    """Fetch opportunity from SmartMoving and create the lead via Moving CRM API."""
    opp = get_opportunity(opportunity_id)
    if not opp:
        print(f"Could not fetch opportunity {opportunity_id}; lead not created")
        return
    customer = opp.get("customer") or {}
    full_name = customer.get("name", "")
    phone = _clean_phone(customer.get("phoneNumber", ""))
    email = customer.get("emailAddress", "")
    branch = opp.get("branch") or {}
    branch_id = str(branch.get("id") or "").strip()
    sm_branch_name = branch.get("name", "")
    company_name = sm_branch_name
    companies = get_companies()
    for c in companies:
        cid = str(c.get("smartmoving_branch_id") or c.get("samrtmoving_branch_id") or "").strip()
        if cid and cid == branch_id:
            company_name = c.get("name", sm_branch_name)
            print(f"_ensure_lead_exists: resolved branch {branch_id!r} -> company_name={company_name!r}")
            break
    else:
        print(f"_ensure_lead_exists: no Moving CRM company matched branch_id={branch_id!r}; using branch name {sm_branch_name!r}")
    referral_source = opp.get("referralSource", "")
    move_size = (opp.get("moveSize") or {}).get("name", "")
    move_type = _OPPORTUNITY_TYPE_MAP.get(opp.get("opportunityType"), "")

    service_date = opp.get("serviceDate")
    try:
        move_date = datetime.utcfromtimestamp(service_date).strftime("%Y-%m-%d") if service_date else ""
    except Exception:
        move_date = ""

    send_to_moving_crm({
        "full_name": full_name,
        "phone_number": phone,
        "email": email,
        "smartmoving_lead_id": opportunity_id,
        "company_name": company_name,
        "referral_source": referral_source,
        "move_size": move_size,
        "move_type": move_type,
        "move_date": move_date,
        "status": status,
        "source": "SmartMoving",
    })


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

    Always perform a full SmartMoving -> CRM lead sync. If the most recent
    audit activity is a sales person assignment, send an intro SMS.
    """
    opportunity_id = body.get("opportunity-id")
    if not opportunity_id:
        print("Missing opportunity-id in opportunity-changed event")
        return

    _sync_opportunity_to_crm(opportunity_id)

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
    _handle_sales_person_assignment(opportunity_id, rep_name)

"""Add Messenger/Instagram activity to a SmartMoving opportunity."""

import os
from datetime import datetime
from zoneinfo import ZoneInfo

from crm.moving_crm import get_users
from crm.smartmoving_notes import add_note, create_followup, get_followups, update_followup
from db import save_pending_note
from db.rds_client import get_smartmoving_followup_context


def _today_at_eight_eastern() -> str:
    return datetime.now(ZoneInfo("America/New_York")).strftime("%Y-%m-%dT08:00:00")


def _resolve_smartmoving_assignee(assigned_to_id: str | None) -> str | None:
    """Map a Moving CRM user ID to SmartMoving, falling back to an admin."""
    users = get_users()
    if users is None:
        print("SmartMoving follow-up: Moving CRM users lookup failed")
        return None

    if assigned_to_id:
        for user in users:
            if str(user.get("id") or "") == str(assigned_to_id):
                smartmoving_rep_id = str(user.get("smartmoving_rep_id") or "").strip()
                if smartmoving_rep_id:
                    return smartmoving_rep_id
                break

    for user in users:
        if str(user.get("role") or "").strip().lower() == "admin":
            smartmoving_rep_id = str(user.get("smartmoving_rep_id") or "").strip()
            if smartmoving_rep_id:
                return smartmoving_rep_id

    print("SmartMoving follow-up: no mapped assignee or admin SmartMoving rep ID")
    return None


def _sync_customer_message_followup(
    smartmoving_id: str,
    assigned_to_id: str | None,
    text: str,
) -> None:
    followups = get_followups(smartmoving_id)
    if followups is None:
        print(f"SmartMoving follow-up: lookup failed for {smartmoving_id}; skipping write")
        return

    due_date_time = _today_at_eight_eastern()
    existing = followups[0] if followups and isinstance(followups[0], dict) else None
    if existing and existing.get("id"):
        existing_notes = str(existing.get("notes") or "")
        notes = f"{text}\n{existing_notes}" if existing_notes else text
        payload = {
            "title": existing.get("title"),
            "type": existing.get("type"),
            "assignedToId": existing.get("assignedToId"),
            "dueDateTime": due_date_time,
            "notes": notes,
        }
        result = update_followup(
            smartmoving_id,
            str(existing["id"]),
            payload,
        )
        print(
            f"SmartMoving follow-up updated for {smartmoving_id}: "
            f"id={existing['id']} ok={result is not None}"
        )
        return

    smartmoving_assignee_id = _resolve_smartmoving_assignee(assigned_to_id)
    if not smartmoving_assignee_id:
        print(
            f"SmartMoving follow-up: lead {smartmoving_id} has no assignee "
            "and no admin fallback; skipping create"
        )
        return

    payload = {
        "type": 2,
        "title": "messenger",
        "assignedToId": smartmoving_assignee_id,
        "dueDateTime": due_date_time,
        "notes": text,
    }
    result = create_followup(smartmoving_id, payload)
    print(f"SmartMoving follow-up created for {smartmoving_id}: ok={result is not None}")


def send_messenger_note(data: dict) -> dict:
    """Look up the sender in RDS; if a SmartMoving lead exists, post the message as a note."""
    sender_id = data.get("sender_id", "")
    text = data.get("text", "")

    if not sender_id or not text:
        print("SmartMoving note: skipped (missing sender_id or text)")
        return data

    if os.environ.get("APP_ENV", "dev").strip().lower() != "prod":
        print("SmartMoving Messenger/Instagram note skipped outside prod")
        return data

    direction = data.get("direction", "user")
    platform = str(data.get("platform") or "messenger").strip().lower()
    role = "customer" if direction == "user" else "rep"
    environment = os.environ.get("APP_ENV", "dev").strip().lower()
    prefix = f"{platform}({role})({environment})"
    note = f"{prefix}: {text}"

    context = get_smartmoving_followup_context(sender_id)
    if not context:
        print(f"SmartMoving note: no lead found for {sender_id}, saving as pending")
        save_pending_note(source="messenger", lookup_key=sender_id, note=note)
        return data
    smartmoving_id = context["smartmoving_id"]

    print(f"SmartMoving note: posting to opportunity {smartmoving_id}")
    result = add_note(smartmoving_id, note)
    if result is not None:
        print(f"SmartMoving note: posted to {smartmoving_id} result={result!r}")
    else:
        print(f"SmartMoving note: failed to post to {smartmoving_id}")

    followups_enabled = os.environ.get("APP_ENV", "dev").strip().lower() == "prod"
    if (
        direction == "user"
        and not data.get("skip_followup", False)
        and followups_enabled
    ):
        _sync_customer_message_followup(
            smartmoving_id,
            context.get("assigned_to_id"),
            text,
        )
    elif direction == "user" and not followups_enabled:
        print("SmartMoving follow-up skipped outside prod")

    data["smartmoving_id"] = smartmoving_id
    return data

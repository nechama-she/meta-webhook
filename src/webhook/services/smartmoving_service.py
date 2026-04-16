"""Handle SmartMoving webhook events."""

from crm.smartmoving_notes import get_followups
from db.rds_client import delete_followup, save_followup


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

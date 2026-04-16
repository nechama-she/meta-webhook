"""Handle SmartMoving webhook events."""

from crm.smartmoving_notes import get_followups
from db.rds_client import save_followup


def handle_followup_created(body: dict) -> None:
    """Process a follow-up-created event from SmartMoving."""
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

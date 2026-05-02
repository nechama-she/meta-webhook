"""Action: add a messenger note to SmartMoving for an existing lead."""

from crm.smartmoving_notes import add_note
from db import save_pending_note
from db.rds_client import get_smartmoving_id


def send_messenger_note(data: dict) -> dict:
    """Look up the sender in RDS; if a SmartMoving lead exists, post the message as a note."""
    sender_id = data.get("sender_id", "")
    text = data.get("text", "")

    if not sender_id or not text:
        print("SmartMoving note: skipped (missing sender_id or text)")
        return data

    direction = data.get("direction", "user")
    prefix = "messenger (customer)" if direction == "user" else "messenger (rep)"
    note = f"{prefix}: {text}"

    smartmoving_id = get_smartmoving_id(sender_id)
    if not smartmoving_id:
        print(f"SmartMoving note: no lead found for {sender_id}, saving as pending")
        save_pending_note(source="messenger", lookup_key=sender_id, note=note)
        return data

    print(f"SmartMoving note: posting to opportunity {smartmoving_id}")
    result = add_note(smartmoving_id, note)
    if result is not None:
        print(f"SmartMoving note: posted to {smartmoving_id} result={result!r}")
    else:
        print(f"SmartMoving note: failed to post to {smartmoving_id}")
    data["smartmoving_id"] = smartmoving_id
    return data

"""Retry pending SmartMoving notes that failed because the lead didn't exist yet."""

from crm.smartmoving_notes import add_note
from db import scan_pending_notes, delete_pending_note
from db.rds_client import get_smartmoving_id, get_smartmoving_id_by_phone


def retry_pending_notes() -> int:
    """Scan all pending notes and retry posting to SmartMoving.

    Returns the number of notes successfully posted.
    """
    notes = scan_pending_notes()
    if not notes:
        print("Pending notes: none to retry")
        return 0

    print(f"Pending notes: retrying {len(notes)} note(s)")
    posted = 0

    for item in notes:
        note_id = item["note_id"]
        source = item.get("source", "")
        lookup_key = item.get("lookup_key", "")
        note = item.get("note", "")

        if source == "sms":
            smartmoving_id = get_smartmoving_id_by_phone(lookup_key)
        elif source == "messenger":
            smartmoving_id = get_smartmoving_id(lookup_key)
        else:
            print(f"Pending notes: unknown source '{source}', deleting {note_id}")
            delete_pending_note(note_id)
            continue

        if not smartmoving_id:
            print(f"Pending notes: still no lead for {source}:{lookup_key}")
            continue

        result = add_note(smartmoving_id, note)
        if result is not None:
            delete_pending_note(note_id)
            posted += 1
            print(f"Pending notes: posted {note_id} to {smartmoving_id} result={result!r}")
        else:
            print(f"Pending notes: failed to post {note_id}")

    print(f"Pending notes: {posted}/{len(notes)} posted")
    return posted

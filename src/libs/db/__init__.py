"""DynamoDB persistence layer."""

from db.client import (
    save_event,
    save_lead_if_new,
    update_lead,
    get_conversation,
    save_conversation_message,
    replace_summary,
    cache_get,
    cache_set,
    save_sender_info,
    save_pending_note,
    scan_pending_notes,
    delete_pending_note,
    save_sms_message,
    get_sms_messages,
    try_claim_dedupe_key,
)

"""CRM communication activity helpers."""

from datetime import datetime, timezone

from crm.moving_crm import send_communication_update


def record_communication(
    *,
    lead_id: str | None,
    channel: str,
    direction: str,
    timestamp: int | float,
    milliseconds: bool = False,
) -> bool:
    """Convert a webhook timestamp and record an exact-lead communication."""
    if not lead_id or not timestamp:
        print("CRM communication update skipped: lead_id and timestamp are required")
        return False
    try:
        seconds = float(timestamp) / 1000 if milliseconds else float(timestamp)
        occurred_at = datetime.fromtimestamp(seconds, timezone.utc).isoformat().replace(
            "+00:00", "Z"
        )
    except (TypeError, ValueError, OSError, OverflowError) as exc:
        print(f"CRM communication update skipped: invalid timestamp ({exc!r})")
        return False
    return send_communication_update(lead_id, channel, direction, occurred_at)

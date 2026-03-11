"""Action: send an out-of-service-area lead to Granot."""

from datetime import date, timedelta

from crm.granot import send_lead


def _clean_phone(phone: str) -> str:
    """Strip leading +1 / 1 country code and dashes."""
    phone = phone.replace("-", "").strip()
    if phone.startswith("+1"):
        phone = phone[2:]
    elif phone.startswith("1") and len(phone) == 11:
        phone = phone[1:]
    return phone


def _split_name(full_name: str) -> tuple[str, str]:
    """Split 'First Last' into (first, last). Falls back to (full, '')."""
    parts = full_name.strip().split(None, 1)
    if len(parts) == 2:
        return parts[0], parts[1]
    return parts[0] if parts else "", ""


def _ensure_future_date(move_date: str) -> str:
    """If move_date is today or in the past, return tomorrow instead."""
    if not move_date:
        return (date.today() + timedelta(days=1)).isoformat()
    try:
        dt = date.fromisoformat(move_date)
    except ValueError:
        return move_date
    if dt <= date.today():
        return (date.today() + timedelta(days=1)).isoformat()
    return move_date


def send_to_granot(data: dict) -> dict:
    """Transform lead data and POST to Granot.

    Returns the data dict enriched with ``granot_ok``.
    """
    full_name = data.get("full_name", "")
    firstname, lastname = _split_name(full_name)

    phone = _clean_phone(data.get("phone_number", ""))
    email = data.get("email", "")
    ozip = data.get("pickup_zip", data.get("ozip", ""))
    dzip = data.get("delivery_zip", data.get("dzip", ""))
    move_date = _ensure_future_date(data.get("move_date", ""))
    lead_id = data.get("leadgen_id", "")

    # Encode name as latin-1 safe
    firstname = firstname.encode("latin-1", "ignore").decode("latin-1")
    lastname = lastname.encode("latin-1", "ignore").decode("latin-1")

    payload = {
        "firstname": firstname,
        "lastname": lastname,
        "email": email,
        "phone1": phone,
        "oaddr": ozip,
        "dzip": dzip,
        "leadno": lead_id,
        "movedte": move_date,
        "label": "Borat",
        "notes": f"Original Pickup: {ozip}, Original Delivery: {dzip}",
    }

    print(f"Granot payload: {payload}")
    result = send_lead(payload)
    data["granot_ok"] = result is not None and "OK" in result
    data["granot_id"] = result or ""
    return data

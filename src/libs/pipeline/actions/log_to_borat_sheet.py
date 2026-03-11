"""Action: log an out-of-service-area lead to the Borat Leads Google Sheet."""

import os

from sheets.client import append_row


def log_to_borat_sheet(data: dict) -> dict:
    """Append a row to the 'Borat Leads' spreadsheet.

    Columns: Date Created, Lead Facebook Id, Granot Id, Full Name,
             Email, Phone, Pickup, Delivery, Date, Move Size, Qualified.

    Returns the data dict unchanged.
    """
    spreadsheet_id = os.environ.get("BORAT_LEADS_SPREADSHEET_ID", "")
    if not spreadsheet_id:
        print("log_to_borat_sheet: BORAT_LEADS_SPREADSHEET_ID not set, skipping")
        return data

    row = [
        data.get("created_time", ""),
        data.get("leadgen_id", ""),
        data.get("granot_id", ""),
        data.get("full_name", ""),
        data.get("email", ""),
        data.get("phone_number", ""),
        data.get("pickup_zip", ""),
        data.get("delivery_zip", ""),
        data.get("move_date", ""),
        data.get("move_size", ""),
        "Yes",
    ]

    ok = append_row(spreadsheet_id, "Leads", row)
    data["borat_sheet_logged"] = ok
    return data

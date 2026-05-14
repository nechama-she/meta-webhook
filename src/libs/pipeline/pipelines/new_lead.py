"""Pipeline: new_lead - actions to run when a fresh lead arrives."""

from pipeline.branch import Branch
from pipeline.actions.check_pickup_zip import check_pickup_zip
from pipeline.actions.date_parser import format_move_date
from pipeline.actions.log_to_borat_sheet import log_to_borat_sheet
from pipeline.actions.send_to_granot import send_to_granot
from pipeline.actions.send_to_moving_crm import send_to_moving_crm
from pipeline.actions.smartmoving import send_to_smartmoving, send_to_smartmoving_by_branch

# Gorilla Haulers - the only company that can send to main SmartMoving branch
_GORILLA_PAGE_ID = "101598038182773"


# Fields that should NOT have underscores replaced
_SKIP_NORMALIZE = frozenset({
    "leadgen_id", "page_id", "form_id", "email", "source",
    "phone_number", "inbox_url", "created_time",
})


def _normalize_facebook_fields(data: dict) -> dict:
    """Replace underscores with spaces in Facebook form text values."""
    for key, val in data.items():
        if key in _SKIP_NORMALIZE or not isinstance(val, str):
            continue
        if "_" in val:
            data[key] = val.replace("_", " ").title()
    return data


def _scope_in_service_area(data: dict) -> dict:
    """Apply in_service_area check only to Gorilla Haulers (primary company)."""
    page_id = data.get("page_id", "")
    if page_id == _GORILLA_PAGE_ID:
        check_pickup_zip(data)
    else:
        # Non-Gorilla companies always use their own company branch
        data["in_service_area"] = True
    return data


def _send_to_crm_by_company(data: dict) -> dict:
    """Route to SmartMoving or Granot based on company configuration."""
    if data.get("smartmoving_branch_id"):
        return send_to_smartmoving_by_branch(data)
    if data.get("granot_api_id") and data.get("granot_mover_ref"):
        return send_to_granot(data)
    print(f"CRM routing: no SmartMoving branch or Granot credentials for company={data.get('company_name')}, skipping")
    return data


ACTIONS = [
    _scope_in_service_area,
    format_move_date,
    _normalize_facebook_fields,
    Branch(
        "in_service_area",
        if_true=[
            _send_to_crm_by_company,
            send_to_moving_crm,
        ],
        if_false=[
            send_to_granot,
            log_to_borat_sheet,
            send_to_smartmoving,
            send_to_moving_crm,
        ],
    ),
]

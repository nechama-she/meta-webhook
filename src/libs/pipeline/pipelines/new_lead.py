"""Pipeline: new_lead – actions to run when a fresh lead arrives."""

from pipeline.branch import Branch
from pipeline.actions.check_pickup_zip import check_pickup_zip
from pipeline.actions.date_parser import format_move_date
from pipeline.actions.log_to_borat_sheet import log_to_borat_sheet
from pipeline.actions.send_to_granot import send_to_granot
from pipeline.actions.smartmoving import send_to_smartmoving, send_to_smartmoving_wilson


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


ACTIONS = [
    check_pickup_zip,
    format_move_date,
    _normalize_facebook_fields,
    Branch(
        "in_service_area",
        if_true=[
            send_to_smartmoving,
        ],
        if_false=[
            send_to_granot,
            log_to_borat_sheet,
            send_to_smartmoving_wilson,
        ],
    ),
]

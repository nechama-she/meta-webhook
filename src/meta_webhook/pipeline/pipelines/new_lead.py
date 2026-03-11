"""Pipeline: new_lead – actions to run when a fresh lead arrives."""

from meta_webhook.pipeline.actions.date_parser import format_move_date
from meta_webhook.pipeline.actions.smartmoving import send_to_smartmoving

ACTIONS = [
    format_move_date,
    send_to_smartmoving,
]

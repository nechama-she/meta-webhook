"""Lead action pipeline - runs a sequence of actions on each new lead.

To add a new action:
1. Create a module under pipeline/
2. Import the action function here
3. Add it to LEAD_ACTIONS

Each action receives the lead dict and should handle its own errors.
"""

from meta_webhook.pipeline.smartmoving import send_to_smartmoving

# ── Action registry ──────────────────────────────────────────────────
# Add or remove actions here. They run in order on every new lead.
LEAD_ACTIONS = [
    send_to_smartmoving,
]


def run_lead_actions(lead: dict) -> None:
    """Execute all registered actions for a new lead."""
    for action in LEAD_ACTIONS:
        name = action.__name__
        try:
            print(f"Running lead action: {name}")
            action(lead)
            print(f"Lead action {name}: done")
        except Exception as exc:
            print(f"Lead action {name} failed: {repr(exc)}")

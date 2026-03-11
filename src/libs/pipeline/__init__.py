"""Pipeline dispatcher – look up a pipeline by name and execute its actions."""

from pipeline.branch import Branch
from pipeline.pipelines import new_lead

_REGISTRY: dict[str, list] = {
    "new_lead": new_lead.ACTIONS,
}


def _run_actions(name: str, actions: list, data: dict) -> dict:
    """Execute a list of actions, handling Branch nodes."""
    for action in actions:
        try:
            if isinstance(action, Branch):
                branch_actions = action.resolve(data)
                data = _run_actions(name, branch_actions, data)
            else:
                data = action(data)
        except Exception as exc:
            action_name = getattr(action, "__name__", str(action))
            print(f"[pipeline:{name}] action {action_name} failed: {exc}")
    return data


def run_pipeline(name: str, data: dict) -> dict:
    """Run every action in the named pipeline, passing *data* through each.

    Each action receives the data dict, may mutate it, and must return it.
    If an action raises, it is logged and the pipeline continues.
    """
    actions = _REGISTRY.get(name)
    if actions is None:
        print(f"Pipeline '{name}' not found - skipping")
        return data

    return _run_actions(name, actions, data)

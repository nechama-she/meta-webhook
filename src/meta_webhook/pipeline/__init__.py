"""Pipeline dispatcher – look up a pipeline by name and execute its actions."""

from meta_webhook.pipeline.pipelines import new_lead

_REGISTRY: dict[str, list] = {
    "new_lead": new_lead.ACTIONS,
}


def run_pipeline(name: str, data: dict) -> dict:
    """Run every action in the named pipeline, passing *data* through each.

    Each action receives the data dict, may mutate it, and must return it.
    If an action raises, it is logged and the pipeline continues.
    """
    actions = _REGISTRY.get(name)
    if actions is None:
        print(f"Pipeline '{name}' not found – skipping")
        return data

    for action in actions:
        try:
            data = action(data)
        except Exception as exc:
            print(f"[pipeline:{name}] action {action.__name__} failed: {exc}")
    return data

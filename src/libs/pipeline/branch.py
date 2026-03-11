"""Branch – conditional fork for pipeline action lists."""


class Branch:
    """Run one list of actions or another based on a data key.

    Usage in a pipeline ACTIONS list::

        Branch("in_service_area",
               if_true=[action_a, action_b],
               if_false=[action_c])
    """

    def __init__(self, key: str, *, if_true: list = None, if_false: list = None):
        self.key = key
        self.if_true = if_true or []
        self.if_false = if_false or []
        self.__name__ = f"branch({key})"

    def resolve(self, data: dict) -> list:
        """Return the actions list for the current branch."""
        return self.if_true if data.get(self.key) else self.if_false

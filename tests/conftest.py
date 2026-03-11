"""Test configuration – add webhook and poll source directories to sys.path."""

import sys
from pathlib import Path

_repo = Path(__file__).resolve().parent.parent

for subdir in ("src/webhook", "src/poll"):
    path = str(_repo / subdir)
    if path not in sys.path:
        sys.path.insert(0, path)

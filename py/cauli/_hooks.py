"""Lifecycle hook runner (PROTOCOL.md section 4.8).

A hook that raises is reported on stderr and skipped, never failing the task
or the remaining hooks. worker/src/shim.py keeps its own copy because the
``cauli`` package may not be importable in the worker's interpreter.
"""

from __future__ import annotations

import sys
import traceback
from typing import Any, Callable, Iterable


def run_hooks(hooks: Iterable[Callable[[], Any]], where: str) -> None:
    """Call each hook in order. ``SystemExit`` and ``KeyboardInterrupt`` propagate."""
    for hook in hooks:
        try:
            hook()
        except Exception:
            print(f"cauli: {where} hook {hook!r} raised (ignored):", file=sys.stderr)
            traceback.print_exc()

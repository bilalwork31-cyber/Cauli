"""Lifecycle hook execution helper (PROTOCOL.md section 4.8).

One rule, shared by every execution context that runs registered hooks: a
hook that raises is reported on stderr and skipped — it must never fail the
task it wraps, and it must never prevent the remaining hooks from running.
The worker's embedded shim (worker/src/shim.py) carries its own copy of this
logic because it cannot assume the ``cauli`` package is importable inside the
worker's interpreter.
"""

from __future__ import annotations

import sys
import traceback
from typing import Any, Callable, Iterable


def run_hooks(hooks: Iterable[Callable[[], Any]], where: str) -> None:
    """Call each hook in order, isolating failures.

    Catches ``Exception`` (not ``BaseException``): a ``SystemExit`` or
    ``KeyboardInterrupt`` raised by a hook is allowed to propagate.
    """
    for hook in hooks:
        try:
            hook()
        except Exception:
            print(f"cauli: {where} hook {hook!r} raised (ignored):", file=sys.stderr)
            traceback.print_exc()

"""rupy._exec: the cpu child process (PROTOCOL.md section 5.1).

Spawned by the Rust worker as ``{python} -m rupy._exec --app module:attr``.
Prints exactly one ready line ``{"ready": true, "pid": N}`` on stdout, then
reads one JSON request per line from stdin and writes one JSON response per
line to stdout, flushing after every line. stderr is passthrough logging.

Robustness notes:
- fd 1 is re-pointed at stderr right after startup and the original stdout
  pipe is kept on a private fd, so task-level ``print()`` (or even C-level
  writes to fd 1) cannot corrupt the line protocol.
- The child never crashes on task exceptions; it reports them. EOF on stdin
  exits 0. Import/startup errors exit nonzero before the ready line.
- Soft timeout: SIGALRM via ``signal.setitimer(ITIMER_REAL)`` raising
  ``rupy.SoftTimeLimitExceeded`` in the task, disarmed in a finally block.
"""

from __future__ import annotations

import argparse
import asyncio
import importlib
import json
import os
import signal
import sys
import traceback
from typing import Any, TextIO

from rupy.exceptions import SoftTimeLimitExceeded

_TRACEBACK_CAP = 8192  # max chars of formatted traceback kept in error JSON (section 8)


def _format_traceback(exc: BaseException) -> str:
    text = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
    if len(text) > _TRACEBACK_CAP:
        text = text[-_TRACEBACK_CAP:]
    return text


def _error_json(exc: BaseException) -> dict[str, Any]:
    return {
        "type": type(exc).__name__,
        "message": str(exc),
        "traceback": _format_traceback(exc),
    }


def _on_alarm(signum: int, frame: Any) -> None:
    raise SoftTimeLimitExceeded("soft time limit exceeded")


def _is_retry(exc: BaseException) -> bool:
    """Duck-typed retry recognition (audit M6): an exception class named
    exactly "Retry" exposing a ``.countdown`` attribute is treated as a
    forced retry. This mirrors worker/src/shim.py's `_is_retry` exactly (by
    name, not `isinstance`), so cpu and io tasks agree on the SAME rule
    regardless of which `Retry` class raised it -- the app's own `rupy.Retry`,
    or an embedded duck-type in a test fixture with no rupy import at all.
    Previously this module used `isinstance(exc, rupy.exceptions.Retry)`,
    which silently disagreed with the shim's name-based rule and with the
    Rust cpu-mapping (ctx.rs), which only ever sees the type name string.
    """
    return type(exc).__name__ == "Retry" and hasattr(exc, "countdown")


def _load_app(spec: str) -> Any:
    module_name, sep, attr = spec.partition(":")
    if not sep or not module_name or not attr:
        print(
            f"rupy._exec: invalid --app {spec!r} (expected module:attr)",
            file=sys.stderr,
        )
        raise SystemExit(2)
    cwd = os.getcwd()
    if cwd not in sys.path:
        sys.path.insert(0, cwd)
    module = importlib.import_module(module_name)
    try:
        return getattr(module, attr)
    except AttributeError:
        print(
            f"rupy._exec: module {module_name!r} has no attribute {attr!r}",
            file=sys.stderr,
        )
        raise SystemExit(2) from None


def _execute(app: Any, request: dict[str, Any]) -> dict[str, Any]:
    """Run one task request and build the response dict. Never raises for task errors."""
    request_id = request.get("id")
    task_name = request.get("task")
    args = request.get("args") or []
    kwargs = request.get("kwargs") or {}
    soft_timeout_ms = request.get("soft_timeout_ms")

    task = getattr(app, "_tasks", {}).get(task_name)
    if task is None:
        return {
            "id": request_id,
            "ok": False,
            "error": {
                "type": "UnknownTask",
                "message": f"task {task_name!r} is not registered in this app",
                "traceback": "",
            },
        }

    use_timer = bool(soft_timeout_ms) and hasattr(signal, "setitimer")
    try:
        if use_timer:
            signal.setitimer(signal.ITIMER_REAL, float(soft_timeout_ms) / 1000.0)
        try:
            if task.is_async:
                result = asyncio.run(task.fn(*args, **kwargs))
            else:
                result = task.fn(*args, **kwargs)
        finally:
            if use_timer:
                signal.setitimer(signal.ITIMER_REAL, 0.0)
    except BaseException as exc:  # the child must never crash on task errors
        if _is_retry(exc):
            cd = getattr(exc, "countdown", None)
            countdown = None if cd is None else float(cd)
            return {
                "id": request_id,
                "ok": False,
                "retry": True,
                "countdown": countdown,
                "error": _error_json(exc),
            }
        return {"id": request_id, "ok": False, "error": _error_json(exc)}
    return {"id": request_id, "ok": True, "result": result}


def _write_line(out: TextIO, payload: dict[str, Any]) -> None:
    """Serialize and emit one protocol line, always flushing.

    A non JSON serializable result degrades to a SerializationError response
    for the same request id (section 5.1).
    """
    try:
        line = json.dumps(payload, separators=(",", ":"), allow_nan=False)
    except (TypeError, ValueError) as exc:
        error = {
            "type": "SerializationError",
            "message": f"task result is not JSON serializable: {exc}",
            "traceback": _format_traceback(exc),
        }
        line = json.dumps(
            {"id": payload.get("id"), "ok": False, "error": error},
            separators=(",", ":"),
        )
    out.write(line + "\n")
    out.flush()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m rupy._exec", description="rupy cpu child process"
    )
    parser.add_argument("--app", required=True, metavar="module:attr")
    ns = parser.parse_args(argv)

    # Reserve the real stdout pipe for protocol lines; point fd 1 at stderr so
    # stray prints from tasks (or C extensions) go to passthrough logging.
    proto_fd = os.dup(1)
    os.dup2(2, 1)
    proto = os.fdopen(proto_fd, "w", encoding="utf-8", newline="\n")
    sys.stdout = sys.stderr

    try:
        sys.stdin.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[union-attr]
    except Exception:
        pass

    app = _load_app(ns.app)  # import errors propagate: nonzero exit before ready line

    if hasattr(signal, "SIGALRM"):
        signal.signal(signal.SIGALRM, _on_alarm)

    _write_line(proto, {"ready": True, "pid": os.getpid()})

    while True:
        line = sys.stdin.readline()
        if line == "":  # EOF: parent closed the pipe, exit cleanly
            break
        line = line.strip()
        if not line:
            continue
        try:
            request = json.loads(line)
        except ValueError as exc:
            _write_line(
                proto,
                {
                    "id": None,
                    "ok": False,
                    "error": {
                        "type": "ProtocolError",
                        "message": f"malformed request line: {exc}",
                        "traceback": "",
                    },
                },
            )
            continue
        try:
            response = _execute(app, request)
        except (
            BaseException
        ) as exc:  # e.g. a SIGALRM landing at the exact task boundary
            request_id = request.get("id") if isinstance(request, dict) else None
            response = {"id": request_id, "ok": False, "error": _error_json(exc)}
        _write_line(proto, response)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

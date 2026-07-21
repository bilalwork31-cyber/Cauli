"""Shared test helpers: the exact envelope contract and the _exec child driver."""

from __future__ import annotations

import json
import queue
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any

# The 18 envelope fields of PROTOCOL.md section 2 — exact key set, no more, no less.
ENVELOPE_KEYS = {
    "v",
    "id",
    "task",
    "args",
    "kwargs",
    "queue",
    "kind",
    "retries",
    "max_retries",
    "backoff_base_ms",
    "backoff_factor",
    "backoff_max_ms",
    "jitter",
    "timeout_ms",
    "soft_timeout_ms",
    "idempotency_key",
    "store_result",
    "enqueued_at",
    "not_before",
}


def now_ms() -> int:
    return time.time_ns() // 1_000_000


def assert_default_option_fields(env: dict[str, Any]) -> None:
    """Assert the option fields a default @app.task() must produce."""
    assert env["v"] == 1
    assert env["kind"] == "io"
    assert env["retries"] == 0
    assert env["max_retries"] == 3
    assert env["backoff_base_ms"] == 500
    assert env["backoff_factor"] == 2.0
    assert env["backoff_max_ms"] == 60000
    assert env["jitter"] is True
    assert env["timeout_ms"] == 300000
    assert env["soft_timeout_ms"] is None
    assert env["idempotency_key"] is None
    assert env["store_result"] is True


class ExecChild:
    """Drives one `python -m rupy._exec` child over its line pipe protocol."""

    def __init__(self, cwd: Path, app_spec: str = "exec_fixture_app:app") -> None:
        self.proc = subprocess.Popen(
            [sys.executable, "-m", "rupy._exec", "--app", app_spec],
            cwd=str(cwd),
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            encoding="utf-8",
        )
        self._lines: queue.Queue[str | None] = queue.Queue()
        self._reader = threading.Thread(target=self._pump, daemon=True)
        self._reader.start()

    def _pump(self) -> None:
        assert self.proc.stdout is not None
        for line in self.proc.stdout:
            self._lines.put(line)
        self._lines.put(None)  # EOF sentinel

    def readline(self, timeout: float = 15.0) -> str:
        try:
            line = self._lines.get(timeout=timeout)
        except queue.Empty:
            raise AssertionError(
                f"timed out after {timeout}s waiting for a line from the child; "
                f"stderr so far: {self._stderr_snapshot()!r}"
            ) from None
        if line is None:
            raise AssertionError(
                f"child stdout closed unexpectedly; stderr: {self._stderr_snapshot()!r}"
            )
        return line

    def read_json(self, timeout: float = 15.0) -> dict[str, Any]:
        return json.loads(self.readline(timeout=timeout))

    def send(self, payload: dict[str, Any]) -> None:
        assert self.proc.stdin is not None
        self.proc.stdin.write(json.dumps(payload) + "\n")
        self.proc.stdin.flush()

    def request(self, payload: dict[str, Any], timeout: float = 15.0) -> dict[str, Any]:
        self.send(payload)
        return self.read_json(timeout=timeout)

    def close(self, timeout: float = 10.0) -> int:
        """Close stdin (EOF) and wait for exit; returns the exit code."""
        assert self.proc.stdin is not None
        self.proc.stdin.close()
        return self.proc.wait(timeout=timeout)

    def drain_eof(self, timeout: float = 5.0) -> None:
        """Assert no further protocol lines arrive before EOF."""
        try:
            line = self._lines.get(timeout=timeout)
        except queue.Empty:
            raise AssertionError("reader thread did not observe EOF") from None
        assert line is None, f"unexpected extra protocol line: {line!r}"

    def _stderr_snapshot(self) -> str:
        if self.proc.poll() is not None and self.proc.stderr is not None:
            try:
                return self.proc.stderr.read() or ""
            except Exception:
                return "<unreadable>"
        return "<child still running>"

    def terminate(self) -> None:
        if self.proc.poll() is None:
            self.proc.kill()
            self.proc.wait(timeout=5)
        for stream in (self.proc.stdin, self.proc.stdout, self.proc.stderr):
            try:
                if stream:
                    stream.close()
            except Exception:
                pass

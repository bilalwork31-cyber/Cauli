"""The exception docstrings are the contract a task author reads before writing
a handler, so they have to match what the lanes actually do.

``SoftTimeLimitExceeded`` shipped once documented as "injected into a running
task", which is true only of the sync lane. An ``async def`` task is cancelled
at the soft mark and never sees the class in its body, so anyone who wrote
``except SoftTimeLimitExceeded`` there got a handler that could not fire.
docs/CONFIGURATION.md's ``soft_timeout`` row states the divergence; this pins
the docstring to it.
"""

from __future__ import annotations

from pathlib import Path

from cauli import SoftTimeLimitExceeded

CONFIGURATION = (
    Path(__file__).resolve().parent.parent.parent / "docs" / "CONFIGURATION.md"
)


def _docstring() -> str:
    """The docstring on one line, so a rewrap cannot fail these assertions."""
    return " ".join((SoftTimeLimitExceeded.__doc__ or "").split())


def test_soft_limit_docstring_names_the_async_cancellation() -> None:
    doc = _docstring()
    assert "asyncio.CancelledError" in doc
    assert "async def" in doc
    assert "finally" in doc


def test_soft_limit_docstring_keeps_the_sync_lane_promise() -> None:
    doc = _docstring()
    assert "In a ``def`` task this class is raised into the running thread" in doc


def test_soft_limit_docstring_states_the_reported_failure_type() -> None:
    """Both lanes report SoftTimeLimitExceeded, whatever the body observed."""
    assert "reports is ``SoftTimeLimitExceeded`` on both lanes" in _docstring()


def test_soft_limit_docstring_points_at_the_configuration_row() -> None:
    assert "docs/CONFIGURATION.md" in _docstring()


def test_configuration_soft_timeout_row_still_documents_both_lanes() -> None:
    """The pointer is only useful while that row still carries the detail."""
    rows = [
        line
        for line in CONFIGURATION.read_text(encoding="utf-8").splitlines()
        if line.startswith("| `soft_timeout`")
    ]
    assert len(rows) == 1, "docs/CONFIGURATION.md no longer has one soft_timeout row"
    assert "asyncio.CancelledError" in rows[0]

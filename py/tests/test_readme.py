"""py/README.md has to stay truthful about statuses and about where the worker installs.

Both failures guarded here shipped once. The README listed four of the five
statuses ``status()`` can return, and the install section promised a
``cauli-worker`` binary without saying that its dependency marker DROPS the
requirement off Linux/CPython/x86_64/aarch64, so a macOS or Alpine reader
installed cleanly and only found out after writing tasks. Nothing else in CI
reads this file, which is exactly how it drifted.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from cauli import AsyncResult

README = Path(__file__).resolve().parent.parent / "README.md"

_QUOTED = re.compile(r'"(\w+)"')


def _readme() -> str:
    return README.read_text(encoding="utf-8")


def _status_line() -> str:
    for line in _readme().splitlines():
        if "r.status()" in line:
            return line
    raise AssertionError("README.md no longer shows an r.status() example")


def _documented_statuses() -> set[str]:
    """The statuses AsyncResult.status promises, read from its own docstring."""
    summary = (AsyncResult.status.__doc__ or "").splitlines()[0]
    return set(_QUOTED.findall(summary))


def test_status_example_lists_every_documented_status() -> None:
    documented = _documented_statuses()
    assert "expired" in documented, "the docstring itself lost a status"
    line = _status_line()
    missing = sorted(s for s in documented if f'"{s}"' not in line)
    assert not missing, f"README r.status() example omits {missing}"


def test_status_example_invents_no_status() -> None:
    extra = sorted(set(_QUOTED.findall(_status_line())) - _documented_statuses())
    assert not extra, f"README r.status() example shows unknown statuses {extra}"


@pytest.mark.parametrize(
    "phrase",
    ["Linux", "x86_64", "aarch64", "CPython", "3.14", "musl", "free threaded", "13.4"],
)
def test_install_section_states_the_worker_wheel_platforms(phrase: str) -> None:
    """The marker's limits (see test_packaging) must be readable from the README."""
    head = _readme().split("## Define an app")[0]
    assert phrase in head, f"the Install section never mentions {phrase!r}"


def test_readme_does_not_tell_the_reader_to_install_the_worker_separately() -> None:
    """`cauli-worker` is already a dependency of `cauli`; a second pip line is wrong."""
    assert "pip install cauli-worker" not in _readme()

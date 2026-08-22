"""Packaging metadata that has to hold on every platform we install on.

These assertions read py/pyproject.toml directly. They exist because a broken
dependency marker fails at the user's `pip install` (or, worse, at their app's
import) and never in a test run that already has the package present.
"""

from __future__ import annotations

import tomllib
from pathlib import Path

import pytest

PYPROJECT = Path(__file__).resolve().parent.parent / "pyproject.toml"


@pytest.fixture(scope="module")
def dependencies() -> list[str]:
    data = tomllib.loads(PYPROJECT.read_text(encoding="utf-8"))
    return list(data["project"]["dependencies"])


def _requirement(dependencies: list[str], name: str) -> str:
    for dep in dependencies:
        if dep.split(";")[0].strip().lower().startswith(name):
            return dep
    raise AssertionError(f"{name!r} is not declared in [project.dependencies]")


def test_tzdata_is_unconditional(dependencies: list[str]) -> None:
    """tzdata must carry no environment marker.

    zoneinfo needs a system tz database and CrontabSchedule raises at app
    import without one. Windows ships none, and neither do Alpine or
    distroless images, which are sys_platform == 'linux'. Any marker here
    drops the wheel on a platform that needs it.
    """
    requirement = _requirement(dependencies, "tzdata")
    assert ";" not in requirement, (
        f"tzdata must be unconditional, got {requirement!r}: a marker drops it "
        "on Alpine and distroless, where zoneinfo has no database either"
    )


def test_worker_binary_stays_marker_gated(dependencies: list[str]) -> None:
    """The counterpart: cauli-worker has wheels only for part of the matrix.

    Guards against 'remove the markers' being applied to the wrong line, which
    would make `pip install cauli` fail outright on macOS and Windows.
    """
    requirement = _requirement(dependencies, "cauli-worker")
    assert "sys_platform == 'linux'" in requirement

#!/usr/bin/env python3
"""Assert the four places that carry cauli's version agree.

`cauli-worker` pins `cauli==<version>` (worker/pyproject.toml), so a release
where the two drift produces a wheel that cannot resolve its own dependency --
and it fails at the user's `pip install`, not in our CI. Cheap to check, so it
runs on every push rather than only at tag time.

    python scripts/check_versions.py            # the four files agree
    python scripts/check_versions.py v0.2.0     # ...and match this git tag
"""

from __future__ import annotations

import re
import sys
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def _cargo_version(path: Path) -> str:
    return tomllib.loads(path.read_text(encoding="utf-8"))["package"]["version"]


def _project_version(path: Path) -> str:
    return tomllib.loads(path.read_text(encoding="utf-8"))["project"]["version"]


def _dunder_version(path: Path) -> str:
    match = re.search(
        r'^__version__\s*=\s*"([^"]+)"', path.read_text(encoding="utf-8"), re.M
    )
    if match is None:
        raise SystemExit(f"{path}: no __version__ assignment found")
    return match.group(1)


def _worker_pin(path: Path) -> str:
    """The `cauli==X` pin inside cauli-worker's dependencies."""
    deps = tomllib.loads(path.read_text(encoding="utf-8"))["project"]["dependencies"]
    for dep in deps:
        match = re.fullmatch(r"cauli\s*==\s*(\S+)", dep)
        if match:
            return match.group(1)
    raise SystemExit(f"{path}: dependencies must pin cauli==<version>, got {deps!r}")


def main(argv: list[str]) -> int:
    found = {
        "worker/Cargo.toml [package].version": _cargo_version(
            ROOT / "worker" / "Cargo.toml"
        ),
        "worker/pyproject.toml cauli== pin": _worker_pin(
            ROOT / "worker" / "pyproject.toml"
        ),
        "py/pyproject.toml [project].version": _project_version(
            ROOT / "py" / "pyproject.toml"
        ),
        "py/cauli/__init__.py __version__": _dunder_version(
            ROOT / "py" / "cauli" / "__init__.py"
        ),
    }

    # worker/pyproject.toml has `dynamic = ["version"]`; maturin takes it from
    # Cargo.toml, so the crate version IS the wheel version. Nothing to read.
    if len(set(found.values())) != 1:
        print("version mismatch:", file=sys.stderr)
        for where, value in found.items():
            print(f"  {value:<12} {where}", file=sys.stderr)
        return 1

    version = next(iter(found.values()))
    # An empty argument means "no tag to check" -- CI passes one unconditionally
    # and it is blank on a plain branch push.
    if len(argv) > 1 and argv[1]:
        tag = argv[1].removeprefix("refs/tags/")
        if tag.removeprefix("v") != version:
            print(f"tag {tag} does not match version {version}", file=sys.stderr)
            return 1
        print(f"ok: {version}, matching tag {tag}")
        return 0

    print(f"ok: {version} in all {len(found)} places")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))

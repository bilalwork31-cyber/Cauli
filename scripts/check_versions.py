#!/usr/bin/env python3
"""Assert the six places that carry cauli's version agree.

The two distributions pin each other. `cauli-worker` pins `cauli==<version>`
(worker/pyproject.toml) and `cauli` pins `cauli-worker==<version>` behind a
platform marker (py/pyproject.toml), so that a plain `pip install cauli` lands
a worker binary too. Either pin drifting produces a wheel that cannot resolve
its own dependency -- and it fails at the user's `pip install`, not in our CI.
Cheap to check, so it runs on every push rather than only at tag time.

README.md's Status section is checked too, and for a reason
this project already lived through: four artifacts shipped 1.0.0 marked
Production/Stable while the landing page still said "v0.1", and CI stayed green
the whole time because nothing read the README. The Status section must open
with a full `cauli X.Y.Z` version, and it must be the same one.

    python scripts/check_versions.py            # the six places agree
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


def _readme_status_version(path: Path) -> str:
    """The version the README's Status section states.

    Read from the Status section specifically rather than the whole file, so a
    version number quoted in an example or a dependency line cannot satisfy the
    check. A partial version such as "v0.1" does not match and fails here with
    a message naming what to write instead, which is the exact drift this
    function exists to catch.
    """
    text = path.read_text(encoding="utf-8")
    heading = re.search(r"^##\s+Status\s*$", text, re.M)
    if heading is None:
        raise SystemExit(f"{path}: no '## Status' section to read a version from")
    section = text[heading.end() :]
    end = re.search(r"^##\s", section, re.M)
    if end is not None:
        section = section[: end.start()]
    match = re.search(
        r"(?<![A-Za-z])cauli +([0-9]+[.][0-9]+[.][0-9]+)(?![0-9])", section
    )
    if match is None:
        raise SystemExit(
            f"{path}: the Status section must state the full version as "
            f"'cauli X.Y.Z'. A partial version such as 'v0.1' does not count: "
            f"it is what let the README drift away from the shipped artifacts."
        )
    return match.group(1)


def _pin(path: Path, name: str) -> str:
    """The `<name>==X` pin inside a pyproject's [project].dependencies.

    The version is read up to the PEP 508 marker, because the client's pin on
    the worker carries one: the worker wheels exist only for Linux on x86_64
    and aarch64, and off that set the requirement has to disappear rather than
    fail the client's install.
    """
    deps = tomllib.loads(path.read_text(encoding="utf-8"))["project"]["dependencies"]
    for dep in deps:
        match = re.match(rf"{re.escape(name)}\s*==\s*([^\s;]+)\s*(?:;|$)", dep)
        if match:
            return match.group(1)
    raise SystemExit(f"{path}: dependencies must pin {name}==<version>, got {deps!r}")


def main(argv: list[str]) -> int:
    found = {
        "worker/Cargo.toml [package].version": _cargo_version(
            ROOT / "worker" / "Cargo.toml"
        ),
        "worker/pyproject.toml cauli== pin": _pin(
            ROOT / "worker" / "pyproject.toml", "cauli"
        ),
        "py/pyproject.toml cauli-worker== pin": _pin(
            ROOT / "py" / "pyproject.toml", "cauli-worker"
        ),
        "py/pyproject.toml [project].version": _project_version(
            ROOT / "py" / "pyproject.toml"
        ),
        "py/cauli/__init__.py __version__": _dunder_version(
            ROOT / "py" / "cauli" / "__init__.py"
        ),
        "README.md Status section": _readme_status_version(ROOT / "README.md"),
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

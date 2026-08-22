#!/usr/bin/env python3
"""Assert the packaged license copies still match the repository originals.

cauli is dual licensed MIT OR Apache-2.0, and both wheels declare that. Apache
2.0 section 4(a) requires anyone distributing the work to ship a copy of the
license, so the license text has to be inside the artifact and not merely at
the root of the repository it was built from.

Both packages build from a subdirectory, and neither build backend will reach
outside its own project directory for data files, so `py/` and `worker/` each
carry a copy of the two root license files. Copies rot silently: this check is
what keeps them byte identical.

    python scripts/check_licenses.py
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
LICENSES = ("LICENSE-MIT", "LICENSE-APACHE")
PACKAGES = ("py", "worker")


def main() -> int:
    problems: list[str] = []
    for name in LICENSES:
        original = (ROOT / name).read_bytes()
        for package in PACKAGES:
            copy = ROOT / package / name
            if not copy.is_file():
                problems.append(f"{package}/{name} is missing")
            elif copy.read_bytes() != original:
                problems.append(f"{package}/{name} differs from the root {name}")

    if problems:
        print("license files out of sync:", file=sys.stderr)
        for problem in problems:
            print(f"  - {problem}", file=sys.stderr)
        print(
            "\nThe wheels declare MIT OR Apache-2.0 and must carry both texts.\n"
            "Fix with: cp LICENSE-MIT LICENSE-APACHE py/ && "
            "cp LICENSE-MIT LICENSE-APACHE worker/",
            file=sys.stderr,
        )
        return 1

    where = ", ".join(PACKAGES)
    print(f"ok: {len(LICENSES)} license files match in {where}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

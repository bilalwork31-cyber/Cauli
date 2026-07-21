#!/usr/bin/env bash
# One time setup for the benchmark harness. Run inside WSL Ubuntu-24.04:
#   cd /mnt/d/dev/projects/boring/rupy/bench && bash setup.sh
set -e

VENV="${BENCH_VENV:-/home/blackdevil/rupy-bench-venv}"
BENCH_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PY_PKG_DIR="$(dirname "$BENCH_DIR")/py"

if [ ! -x "$VENV/bin/python" ]; then
    echo "setup: creating venv at $VENV"
    python3 -m venv "$VENV"
fi
"$VENV/bin/pip" install -q --upgrade pip
"$VENV/bin/pip" install -q -r "$BENCH_DIR/requirements.txt"

# cauli python package: built in parallel by another agent; may not exist yet.
# Guarded so setup succeeds either way; rerun setup.sh once py/ appears.
if [ -f "$PY_PKG_DIR/pyproject.toml" ] || [ -f "$PY_PKG_DIR/setup.py" ]; then
    echo "setup: installing cauli package (editable) from $PY_PKG_DIR"
    "$VENV/bin/pip" install -q -e "$PY_PKG_DIR"
else
    echo "setup: cauli package not present at $PY_PKG_DIR yet; SKIPPED."
    echo "setup: cauli scenarios will be skipped by runner.sh until it exists (rerun setup.sh then)."
fi

echo "setup: done. venv at $VENV"
"$VENV/bin/pip" list 2>/dev/null | grep -Ei '^(celery|redis|requests|httpx|uvicorn|starlette|psutil|pytest|gevent|cauli) ' || true

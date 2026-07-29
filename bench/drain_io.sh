#!/usr/bin/env bash
# Drain-rate suite for the IO lanes. Same method as drain.sh (fill the queue
# with no worker, start the worker, take the slope over the middle 80% of
# completions), applied to io tasks so the async lane gets a measurement that
# is not the driver-artifact-prone exec_tps.
#
# Ceiling note, read before quoting: the io task is an HTTP GET against the
# local mock API's 50ms endpoint, so a fast worker can be bounded by the mock
# API and by the shared 6 cores rather than by the runtime. Treat these as
# "at least this fast", not as the worker's capacity.
#
# Usage: bash drain_io.sh [n]
set -u
set -o pipefail

BENCH_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV="${BENCH_VENV:-/home/blackdevil/rupy-bench-venv}"
PY="$VENV/bin/python"
CELERY_BIN="$VENV/bin/celery"
PORT="${BENCH_REDIS_PORT:-6390}"
export BENCH_REDIS_PORT="$PORT"
export PYTHONUNBUFFERED=1
CAULI_WORKER_BIN="${CAULI_WORKER_BIN:-/home/blackdevil/rupy-target/release/cauli-worker}"
RESULTS="$BENCH_DIR/results"
LOGS="$RESULTS/logs"
N="${1:-10000}"

mkdir -p "$LOGS"
cd "$BENCH_DIR"
ulimit -n 65535 2>/dev/null || true
log() { echo "[drain-io $(date +%H:%M:%S)] $*"; }

SITEPKG="$("$PY" -c 'import sysconfig; print(sysconfig.get_paths()["purelib"])' 2>/dev/null || true)"
[ -n "$SITEPKG" ] && export PYTHONPATH="$SITEPKG:$BENCH_DIR${PYTHONPATH:+:$PYTHONPATH}"
export PYTHONPATH="$(cd "$BENCH_DIR/.." && pwd)/py:$PYTHONPATH"
export VIRTUAL_ENV="$VENV"
export PATH="$VENV/bin:$PATH"

CAULI_URL="redis://127.0.0.1:$PORT/0"
CEL=("$CELERY_BIN" -A tasks_celery worker --loglevel=WARNING)
CAULI=("$CAULI_WORKER_BIN" --app tasks_cauli:app --redis-url "$CAULI_URL" --python "$PY")

cleanup() {
    local units
    units="$(systemctl --user list-units --plain --no-legend 'drainio-*.scope' 2>/dev/null | awk '{print $1}')"
    for u in $units; do
        systemctl --user kill -s SIGKILL "$u" 2>/dev/null || true
        systemctl --user stop "$u" 2>/dev/null || true
        systemctl --user reset-failed "$u" 2>/dev/null || true
    done
    [ -n "${MOCK_PID:-}" ] && kill "$MOCK_PID" 2>/dev/null || true
}
trap cleanup EXIT

redis-cli -p "$PORT" ping >/dev/null 2>&1 || {
    redis-server --port "$PORT" --save '' --appendonly no --daemonize yes; sleep 1
}
MOCK_PID=""
if ! "$PY" -c 'import urllib.request;urllib.request.urlopen("http://127.0.0.1:8077/health",timeout=2)' 2>/dev/null; then
    nohup "$PY" mock_api.py > "$LOGS/mock_api.log" 2>&1 &
    MOCK_PID=$!
    for _ in $(seq 1 40); do
        "$PY" -c 'import urllib.request;urllib.request.urlopen("http://127.0.0.1:8077/health",timeout=2)' 2>/dev/null && break
        sleep 0.25
    done
fi

run_arm() {               # run_arm <name> <stack> <task> <cmd...>
    local name="$1" stack="$2" task="$3"; shift 3
    log "=== $name (stack=$stack task=$task n=$N)"
    redis-cli -p "$PORT" flushall >/dev/null
    redis-cli -p "$PORT" -n 1 flushall >/dev/null 2>&1 || true

    "$PY" drain_driver.py --stack "$stack" --task "$task" --phase enqueue --n "$N" 2>&1 | tail -1

    systemctl --user reset-failed "drainio-$name.scope" 2>/dev/null || true
    systemd-run --user --scope --unit="drainio-$name" --collect \
        -p TimeoutStopSec=20 -p MemoryMax=1G -p MemorySwapMax=0 -- "$@" \
        > "$LOGS/drainio_$name.worker.log" 2>&1 &
    local scope_pid=$!

    "$PY" drain_driver.py --stack "$stack" --task "$task" --phase drain --n "$N" \
        --scenario "$name" --out "$RESULTS/drainio_$name.json" --timeout 300

    systemctl --user stop "drainio-$name.scope" 2>/dev/null || true
    for _ in $(seq 1 30); do
        systemctl --user is-active --quiet "drainio-$name.scope" 2>/dev/null || break
        sleep 0.5
    done
    systemctl --user kill -s SIGKILL "drainio-$name.scope" 2>/dev/null || true
    systemctl --user reset-failed "drainio-$name.scope" 2>/dev/null || true
    wait "$scope_pid" 2>/dev/null || true
    redis-cli -p "$PORT" flushall >/dev/null
}

run_arm celery_prefork16 celery io       "${CEL[@]}" -c 16 -P prefork
run_arm celery_gevent500 celery io       "${CEL[@]}" -c 500 -P gevent
run_arm cauli_sync       cauli  io       "${CAULI[@]}" --io-concurrency 500 --io-threads 64
run_arm cauli_async      cauli  io_async "${CAULI[@]}" --io-concurrency 500

log "done; results in $RESULTS/drainio_*.json"

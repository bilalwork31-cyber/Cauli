#!/usr/bin/env bash
# Drain-rate suite: measures WORKER capacity for cpu tasks, isolated from the
# client's enqueue speed. See drain_driver.py for why runner.sh's exec_tps
# cannot answer this for small tasks (it reports above the physical roofline
# once the worker outpaces the driver).
#
# Per arm: flushall -> enqueue N with NO worker -> start worker in a capped
# scope -> sample completions -> report steady-state slope + wall rate.
#
# Usage: bash drain.sh [iters] [n]
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

ITERS="${1:-3700}"
N="${2:-8000}"
export BENCH_CPU_ITER="$ITERS"

mkdir -p "$LOGS"
cd "$BENCH_DIR"
ulimit -n 65535 2>/dev/null || true
log() { echo "[drain $(date +%H:%M:%S)] $*"; }

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
    units="$(systemctl --user list-units --plain --no-legend 'drain-*.scope' 2>/dev/null | awk '{print $1}')"
    for u in $units; do
        systemctl --user kill -s SIGKILL "$u" 2>/dev/null || true
        systemctl --user stop "$u" 2>/dev/null || true
        systemctl --user reset-failed "$u" 2>/dev/null || true
    done
}
trap cleanup EXIT

redis-cli -p "$PORT" ping >/dev/null 2>&1 || {
    redis-server --port "$PORT" --save '' --appendonly no --daemonize yes
    sleep 1
}

run_arm() {               # run_arm <name> <stack> <cmd...>
    local name="$1" stack="$2"; shift 2
    log "=== $name (stack=$stack iters=$ITERS n=$N)"
    redis-cli -p "$PORT" flushall >/dev/null
    redis-cli -p "$PORT" -n 1 flushall >/dev/null 2>&1 || true

    # Fill the queue first, with nothing consuming it.
    "$PY" drain_driver.py --stack "$stack" --phase enqueue --n "$N" 2>&1 | tail -1

    systemctl --user reset-failed "drain-$name.scope" 2>/dev/null || true
    systemd-run --user --scope --unit="drain-$name" --collect \
        -p TimeoutStopSec=20 -p MemoryMax=1G -p MemorySwapMax=0 -- "$@" \
        > "$LOGS/drain_$name.worker.log" 2>&1 &
    local scope_pid=$!

    "$PY" drain_driver.py --stack "$stack" --phase drain --n "$N" \
        --scenario "$name" --out "$RESULTS/drain_$name.json" --timeout 300

    systemctl --user stop "drain-$name.scope" 2>/dev/null || true
    for _ in $(seq 1 30); do
        systemctl --user is-active --quiet "drain-$name.scope" 2>/dev/null || break
        sleep 0.5
    done
    systemctl --user kill -s SIGKILL "drain-$name.scope" 2>/dev/null || true
    systemctl --user reset-failed "drain-$name.scope" 2>/dev/null || true
    wait "$scope_pid" 2>/dev/null || true
    redis-cli -p "$PORT" flushall >/dev/null
}

ARMS="${DRAIN_ARMS:-celery,pf0,pf1,pf3,pf8}"
# Celery's worker_prefetch_multiplier is the direct analogue of cauli's
# --cpu-prefetch: how many messages a worker reserves beyond the one it is
# executing. tasks_celery.py pins it to 1 for semantic fairness against
# cauli's admission gate, which is equivalent to --cpu-prefetch 0. Comparing a
# prefetch-tuned cauli against a prefetch-1 Celery would be measuring the
# config, not the runtime, so BOTH sides get their depth swept (N1: compare
# each at its own optimum or do not compare).
case ",$ARMS," in *,celery,*) run_arm "celery_i${ITERS}" celery "${CEL[@]}" -c 6 -P prefork ;; esac
for d in 4 16 64; do
    case ",$ARMS," in
        *",celerypf$d,"*) run_arm "celerypf${d}_i${ITERS}" celery \
            "${CEL[@]}" -c 6 -P prefork --prefetch-multiplier "$d" ;;
    esac
done
for d in 0 1 3 8 16 32 64; do
    case ",$ARMS," in
        *",pf$d,"*) run_arm "cauli_pf${d}_i${ITERS}" cauli \
            "${CAULI[@]}" --cpu-workers 6 --cpu-prefetch "$d" ;;
    esac
done

log "done; results in $RESULTS/drain_*.json"

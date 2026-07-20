#!/usr/bin/env bash
# Mini end-to-end verification run (NOT the benchmark): redis 6393 (started if
# absent), fake_graph 8078 (started if absent, left running), UNCAPPED workers,
# N=300, 5 pages, SEND_DELAY=0.2, TICK_SECONDS=3, production topology.
#
# Usage: bash verify_mini.sh celery|rupy_sync|rupy_async
set -u
set -o pipefail

STACK="${1:?usage: verify_mini.sh celery|rupy_sync|rupy_async}"
CAMP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BENCH_DIR="$(cd "$CAMP_DIR/.." && pwd)"
ROOT_DIR="$(cd "$BENCH_DIR/.." && pwd)"
VENV="${BENCH_VENV:-/home/blackdevil/rupy-bench-venv}"
PY="$VENV/bin/python"
CELERY_BIN="$VENV/bin/celery"
RUPY_WORKER_BIN="${RUPY_WORKER_BIN:-/home/blackdevil/rupy-target/release/rupy-worker}"
PORT="${BENCH_REDIS_PORT:-6393}"
export BENCH_REDIS_PORT="$PORT"
export PYTHONUNBUFFERED=1
GRAPH_PORT=8078
export FAKE_GRAPH_URL="http://127.0.0.1:$GRAPH_PORT"
export SEND_DELAY=0.2 TICK_SECONDS=3 N_PAGES=5 \
       APP_MAX_PER_MINUTE=200 MAX_BATCHES_PER_DISPATCH=10 ERROR_RATE=0.02
LOGS="$CAMP_DIR/results/logs"
mkdir -p "$LOGS"
cd "$CAMP_DIR"

SITEPKG="$("$PY" -c 'import sysconfig; print(sysconfig.get_paths()["purelib"])')"
export PYTHONPATH="$SITEPKG:$CAMP_DIR:$BENCH_DIR:$ROOT_DIR/py${PYTHONPATH:+:$PYTHONPATH}"
export VIRTUAL_ENV="$VENV"
export PATH="$VENV/bin:$PATH"

log() { echo "[mini $(date +%H:%M:%S)] $*"; }

if ! redis-cli -p "$PORT" ping >/dev/null 2>&1; then
    redis-server --port "$PORT" --save '' --appendonly no --daemonize yes
    sleep 0.5
    redis-cli -p "$PORT" ping >/dev/null 2>&1 || { log "FATAL: redis $PORT"; exit 1; }
    log "started throwaway redis on $PORT"
fi

graph_ok() {
    "$PY" -c "import urllib.request,sys
try:
    r=urllib.request.urlopen('http://127.0.0.1:$GRAPH_PORT/health',timeout=2)
    sys.exit(0 if r.status==200 else 1)
except Exception:
    sys.exit(1)"
}
if ! graph_ok; then
    nohup "$PY" fake_graph.py > "$LOGS/fake_graph.mini.log" 2>&1 &
    for _ in $(seq 1 40); do graph_ok && break; sleep 0.25; done
    graph_ok || { log "FATAL: fake_graph did not come up"; exit 1; }
    log "started fake_graph on $GRAPH_PORT (left running afterwards)"
fi

redis-cli -p "$PORT" flushall >/dev/null

WPID=""
teardown() {
    if [ -n "$WPID" ]; then
        kill -TERM -- "-$WPID" 2>/dev/null || kill -TERM "$WPID" 2>/dev/null || true
        sleep 3
        kill -KILL -- "-$WPID" 2>/dev/null || true
    fi
    pkill -9 -f "celery.*-A campaign_celery" 2>/dev/null || true
    if [ "$STACK" != "celery" ]; then
        pkill -9 -f "rupy-worker.*campaign_rupy" 2>/dev/null || true
    fi
    redis-cli -p "$PORT" flushall >/dev/null 2>&1 || true
}
trap teardown EXIT

case "$STACK" in
  celery)
    DRIVER_STACK=celery
    setsid bash -c "
\"$CELERY_BIN\" -A campaign_celery worker --loglevel=WARNING -n default@%h  -Q celery,backfill_heavy,webhook_ingest -c 2 &
\"$CELERY_BIN\" -A campaign_celery worker --loglevel=WARNING -n long@%h     -Q campaign_long -c 4 --max-tasks-per-child=1000 &
\"$CELERY_BIN\" -A campaign_celery worker --loglevel=WARNING -n short@%h    -Q campaign_short -c 2 &
\"$CELERY_BIN\" -A campaign_celery worker --loglevel=WARNING -n dispatch@%h -Q dispatch --pool=solo &
wait" > "$LOGS/mini_celery.worker.log" 2>&1 &
    WPID=$!
    log "celery topology starting (pgid $WPID); waiting for 4 pongs"
    ok=0
    for _ in $(seq 1 60); do
        n="$("$CELERY_BIN" -A campaign_celery inspect ping --timeout 2 2>/dev/null | grep -c 'OK' || true)"
        [ "$n" -ge 4 ] && { ok=1; break; }
        sleep 1
    done
    [ "$ok" = "1" ] || { log "FATAL: celery workers not ready"; exit 1; }
    ;;
  rupy_sync|rupy_async)
    DRIVER_STACK=rupy
    if [ "$STACK" = "rupy_async" ]; then
        export RUPY_VARIANT=async
        FLAGS="--io-concurrency 1000 --io-threads 8"
    else
        export RUPY_VARIANT=sync
        FLAGS="--io-concurrency 200 --io-threads 96"
    fi
    # shellcheck disable=SC2086
    setsid "$RUPY_WORKER_BIN" --app campaign_rupy:app \
        --redis-url "redis://127.0.0.1:$PORT/0" --python "$PY" \
        --queues default,dispatch,campaign_short,campaign_long,backfill_heavy,webhook_ingest \
        --cpu-workers 1 --visibility-timeout 300 $FLAGS \
        > "$LOGS/mini_$STACK.worker.log" 2>&1 &
    WPID=$!
    sleep 3
    kill -0 "$WPID" 2>/dev/null || { log "FATAL: rupy worker died (see $LOGS/mini_$STACK.worker.log)"; exit 1; }
    ;;
  *) log "unknown stack $STACK"; exit 1 ;;
esac

log "worker ready; driving mini run (n=300, pages=5, tick=3)"
"$PY" campaign_driver.py --stack "$DRIVER_STACK" --scenario "mini_$STACK" \
    --n 300 --pages 5 --tick 3 --timeout 300 --pid "$WPID" \
    2>&1 | tee "$LOGS/mini_$STACK.driver.log" | grep -E '^\[campaign\]'
rc=${PIPESTATUS[0]}
log "driver exit=$rc (json: results/mini_$STACK.json)"
exit "$rc"

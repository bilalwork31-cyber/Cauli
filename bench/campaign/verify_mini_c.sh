#!/usr/bin/env bash
# Mini scenario C verification (NOT the benchmark): 5 campaigns x 300
# recipients, UNCAPPED workers, redis 6393, fake_graph on 8079 with
# ERROR_RATE=0.05, same bench pg db (truncated by the driver).
#
# Usage: bash verify_mini_c.sh celery|cauli
set -u
set -o pipefail

STACK="${1:?usage: verify_mini_c.sh celery|cauli}"
CAMP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BENCH_DIR="$(cd "$CAMP_DIR/.." && pwd)"
ROOT_DIR="$(cd "$BENCH_DIR/.." && pwd)"
VENV="${BENCH_VENV:-$HOME/rupy-bench-venv}"
PY="$VENV/bin/python"
CELERY_BIN="$VENV/bin/celery"
CAULI_WORKER_BIN="${CAULI_WORKER_BIN:-$HOME/rupy-target/release/cauli-worker}"
PORT="${BENCH_REDIS_PORT:-6393}"
export BENCH_REDIS_PORT="$PORT"
export PYTHONUNBUFFERED=1
GRAPH_PORT=8079
export FAKE_GRAPH_PORT="$GRAPH_PORT"
export FAKE_GRAPH_URL="http://127.0.0.1:$GRAPH_PORT"
export SEND_DELAY=0 TICK_SECONDS=3 N_PAGES=20 ERROR_RATE=0.05 \
       APP_MAX_PER_MINUTE=1000000000 MAX_BATCHES_PER_DISPATCH=1000000 \
       LEASE_MS=600000 CAULI_VARIANT=async
LOGS="$CAMP_DIR/results/logs"
mkdir -p "$LOGS"
cd "$CAMP_DIR"

SITEPKG="$("$PY" -c 'import sysconfig; print(sysconfig.get_paths()["purelib"])')"
export PYTHONPATH="$SITEPKG:$CAMP_DIR:$BENCH_DIR:$ROOT_DIR/py${PYTHONPATH:+:$PYTHONPATH}"
export VIRTUAL_ENV="$VENV"
export PATH="$VENV/bin:$PATH"

log() { echo "[mini-c $(date +%H:%M:%S)] $*"; }

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
    nohup "$PY" fake_graph.py > "$LOGS/fake_graph.mini_c.log" 2>&1 &
    for _ in $(seq 1 40); do graph_ok && break; sleep 0.25; done
    graph_ok || { log "FATAL: fake_graph 8079 did not come up"; exit 1; }
    log "started fake_graph on $GRAPH_PORT (ERROR_RATE=0.05, left running)"
fi

redis-cli -p "$PORT" flushall >/dev/null

WPID=""
teardown() {
    if [ -n "$WPID" ]; then
        kill -TERM -- "-$WPID" 2>/dev/null || kill -TERM "$WPID" 2>/dev/null || true
        sleep 3
        kill -KILL -- "-$WPID" 2>/dev/null || true
    fi
    pkill -9 -f "celery.*-A campaign_celery_c" 2>/dev/null || true
    if [ "$STACK" != "celery" ]; then
        pkill -9 -f "cauli-worker.*campaign_cauli_c" 2>/dev/null || true
    fi
    redis-cli -p "$PORT" flushall >/dev/null 2>&1 || true
}
trap teardown EXIT

case "$STACK" in
  celery)
    setsid bash -c "
\"$CELERY_BIN\" -A campaign_celery_c worker --loglevel=WARNING -n default@%h  -Q celery,backfill_heavy,webhook_ingest -c 2 &
\"$CELERY_BIN\" -A campaign_celery_c worker --loglevel=WARNING -n long@%h     -Q campaign_long -c 4 --max-tasks-per-child=1000 &
\"$CELERY_BIN\" -A campaign_celery_c worker --loglevel=WARNING -n short@%h    -Q campaign_short -c 2 &
\"$CELERY_BIN\" -A campaign_celery_c worker --loglevel=WARNING -n dispatch@%h -Q dispatch --pool=solo &
\"$CELERY_BIN\" -A campaign_celery_c worker --loglevel=WARNING -n persist@%h  -Q persist -c 2 &
wait" > "$LOGS/mini_c_celery.worker.log" 2>&1 &
    WPID=$!
    log "celery C topology starting (pgid $WPID); waiting for 5 pongs"
    ok=0
    for _ in $(seq 1 90); do
        n="$("$CELERY_BIN" -A campaign_celery_c inspect ping --timeout 2 2>/dev/null | grep -c 'OK' || true)"
        [ "$n" -ge 5 ] && { ok=1; break; }
        sleep 1
    done
    [ "$ok" = "1" ] || { log "FATAL: celery workers not ready"; exit 1; }
    ;;
  cauli)
    setsid "$CAULI_WORKER_BIN" --app campaign_cauli_c:app \
        --redis-url "redis://127.0.0.1:$PORT/0" --python "$PY" \
        --queues default,dispatch,campaign_short,campaign_long,backfill_heavy,webhook_ingest,persist \
        --io-concurrency 1000 --io-threads 16 --cpu-workers 1 \
        --visibility-timeout 300 \
        > "$LOGS/mini_c_cauli.worker.log" 2>&1 &
    WPID=$!
    sleep 3
    kill -0 "$WPID" 2>/dev/null || { log "FATAL: cauli worker died (see $LOGS/mini_c_cauli.worker.log)"; exit 1; }
    ;;
  *) log "unknown stack $STACK"; exit 1 ;;
esac

log "worker ready; driving mini C (5 campaigns x 300, pages=20, tick=3)"
"$PY" driver_c.py --stack "$STACK" --scenario "mini_c_$STACK" \
    --campaigns 5 --min-n 300 --max-n 300 --pages 20 --seed 7 \
    --tick 3 --timeout 300 --pid "$WPID" \
    2>&1 | tee "$LOGS/mini_c_$STACK.driver.log" | grep -E '^\[c\]'
rc=${PIPESTATUS[0]}
log "driver exit=$rc (json: results/mini_c_$STACK.json)"
exit "$rc"

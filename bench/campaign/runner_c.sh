#!/usr/bin/env bash
# Scenario C orchestrator: 100-campaign chaos + stage-2 Postgres persister.
# Conventions of runner_campaign.sh (own redis 6390, fake_graph 8078 with
# ERROR_RATE=0.05, 1G scopes, FLUSHALL+TRUNCATE between scenarios).
#
# REFUSES to start if the A/B suite appears active (camp-*.scope units, or
# something already listening on 6390/8078).
#
# Usage: bash runner_c.sh [filter]     filter: celery | cauli | C_...
set -u
set -o pipefail

CAMP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BENCH_DIR="$(cd "$CAMP_DIR/.." && pwd)"
ROOT_DIR="$(cd "$BENCH_DIR/.." && pwd)"
VENV="${BENCH_VENV:-$HOME/rupy-bench-venv}"
PY="$VENV/bin/python"
CELERY_BIN="$VENV/bin/celery"
PORT="${BENCH_REDIS_PORT:-6390}"
export BENCH_REDIS_PORT="$PORT"
export PYTHONUNBUFFERED=1
CAULI_WORKER_BIN="${CAULI_WORKER_BIN:-$HOME/rupy-target/release/cauli-worker}"
RESULTS="$CAMP_DIR/results"
LOGS="$RESULTS/logs"
DRIVER_TIMEOUT="${BENCH_DRIVER_TIMEOUT:-3600}"
FILTER="${1:-}"
GRAPH_PORT="${FAKE_GRAPH_PORT:-8078}"
export FAKE_GRAPH_URL="http://127.0.0.1:$GRAPH_PORT"

# ---- scenario C knobs (env-driven; CONFIG reads these at import) ----
export SEND_DELAY=0 TICK_SECONDS=5 N_PAGES=200 ERROR_RATE=0.05 \
       APP_MAX_PER_MINUTE=1000000000 MAX_BATCHES_PER_DISPATCH=1000000 \
       LEASE_MS=600000 CAULI_VARIANT=async
C_CAMPAIGNS="${C_CAMPAIGNS:-100}"
C_MIN_N="${C_MIN_N:-4000}"
C_MAX_N="${C_MAX_N:-5000}"
C_SEED="${C_SEED:-42}"

mkdir -p "$LOGS"
cd "$CAMP_DIR"
ulimit -n 65535 2>/dev/null || true

log() { echo "[runner-c $(date +%H:%M:%S)] $*" | tee -a "$LOGS/runner_c.log"; }

[ -x "$PY" ] || { echo "venv missing at $VENV"; exit 1; }

# ---- coordination guard: refuse to run over the A/B suite ----
if systemctl --user list-units --plain --no-legend 'camp-*.scope' 2>/dev/null | grep -q .; then
    log "FATAL: camp-*.scope units active (A/B suite running?). Aborting."
    exit 1
fi
if redis-cli -p "$PORT" ping >/dev/null 2>&1; then
    log "FATAL: redis already listening on $PORT (A/B suite running?). Aborting."
    exit 1
fi
if curl -s -m 2 "http://127.0.0.1:$GRAPH_PORT/health" >/dev/null 2>&1; then
    log "FATAL: something on $GRAPH_PORT (A/B fake_graph?). Aborting."
    exit 1
fi
if ! "$PY" -c "import psycopg2, persist_common; persist_common.ensure_schema()" 2>/dev/null; then
    log "FATAL: postgres not reachable ($PY -c 'import psycopg2...'); run pg_setup.sh as root first."
    exit 1
fi

CAULI_URL="redis://127.0.0.1:$PORT/0"
SITEPKG="$("$PY" -c 'import sysconfig; print(sysconfig.get_paths()["purelib"])' 2>/dev/null || true)"
if [ -n "$SITEPKG" ]; then
    export PYTHONPATH="$SITEPKG:$CAMP_DIR:$BENCH_DIR${PYTHONPATH:+:$PYTHONPATH}"
else
    export PYTHONPATH="$CAMP_DIR:$BENCH_DIR${PYTHONPATH:+:$PYTHONPATH}"
fi
export PYTHONPATH="$ROOT_DIR/py:$PYTHONPATH"
export VIRTUAL_ENV="$VENV"
export PATH="$VENV/bin:$PATH"

STARTED_REDIS=0
GRAPH_PID=""
SCOPE_PID=""
CGPATH=""
WORKER_PID=""

cleanup() {
    log "cleanup: tearing down scopes, fake_graph, redis"
    local units
    units="$(systemctl --user list-units --plain --no-legend 'camp-*.scope' 2>/dev/null | awk '{print $1}')"
    for u in $units; do
        systemctl --user kill -s SIGKILL "$u" 2>/dev/null || true
        systemctl --user stop "$u" 2>/dev/null || true
        systemctl --user reset-failed "$u" 2>/dev/null || true
    done
    if [ -n "$GRAPH_PID" ]; then
        kill "$GRAPH_PID" 2>/dev/null || true
        sleep 1
        kill -9 "$GRAPH_PID" 2>/dev/null || true
        pkill -9 -f "fake_graph:app" 2>/dev/null || true
    fi
    if [ "$STARTED_REDIS" = "1" ]; then
        redis-cli -p "$PORT" shutdown nosave 2>/dev/null || true
    fi
}
trap cleanup EXIT

redis-server --port "$PORT" --save '' --appendonly no --daemonize yes
STARTED_REDIS=1
for _ in $(seq 1 20); do redis-cli -p "$PORT" ping >/dev/null 2>&1 && break; sleep 0.2; done
redis-cli -p "$PORT" ping >/dev/null 2>&1 || { log "FATAL: redis did not start"; exit 1; }
log "started throwaway redis on $PORT"

nohup "$PY" fake_graph.py > "$LOGS/fake_graph_c.log" 2>&1 &
GRAPH_PID=$!
gok=0
for _ in $(seq 1 40); do
    curl -s -m 2 "http://127.0.0.1:$GRAPH_PORT/health" >/dev/null 2>&1 && { gok=1; break; }
    sleep 0.25
done
[ "$gok" = "1" ] || { log "FATAL: fake_graph did not start"; exit 1; }
log "fake_graph up on $GRAPH_PORT (ERROR_RATE=$ERROR_RATE, pid $GRAPH_PID)"

start_scoped() {          # start_scoped <name> <cap> <cmd...>
    local unit="$1" cap="$2"; shift 2
    systemctl --user reset-failed "camp-$unit.scope" 2>/dev/null || true
    systemd-run --user --scope --unit="camp-$unit" --collect -p TimeoutStopSec=20 \
        -p "MemoryMax=$cap" -p "MemorySwapMax=0" -- "$@" \
        > "$LOGS/$unit.worker.log" 2>&1 &
    SCOPE_PID=$!
    CGPATH=""; WORKER_PID=""
    local cg=""
    for _ in $(seq 1 50); do
        cg="$(systemctl --user show "camp-$unit.scope" -p ControlGroup --value 2>/dev/null || true)"
        if [ -n "$cg" ] && [ -d "/sys/fs/cgroup$cg" ]; then break; fi
        cg=""; sleep 0.2
    done
    [ -n "$cg" ] || return 1
    CGPATH="/sys/fs/cgroup$cg"
    for _ in $(seq 1 25); do
        WORKER_PID="$(head -1 "$CGPATH/cgroup.procs" 2>/dev/null || true)"
        [ -n "$WORKER_PID" ] && break
        sleep 0.2
    done
    [ -n "$WORKER_PID" ] || return 1
    return 0
}

stop_scoped() {
    local unit="camp-$1.scope"
    systemctl --user stop "$unit" 2>/dev/null || true
    for _ in $(seq 1 30); do
        systemctl --user is-active --quiet "$unit" 2>/dev/null || break
        sleep 1
    done
    systemctl --user kill -s SIGKILL "$unit" 2>/dev/null || true
    systemctl --user reset-failed "$unit" 2>/dev/null || true
    if [ -n "$SCOPE_PID" ]; then wait "$SCOPE_PID" 2>/dev/null || true; fi
    SCOPE_PID=""
}

celery_c_cmd() {   # production topology + persist worker, one scope
    local c="\"$CELERY_BIN\" -A campaign_celery_c worker --loglevel=WARNING"
    echo "$c -n default@%h  -Q celery,backfill_heavy,webhook_ingest -c 2 & \
$c -n long@%h     -Q campaign_long -c 4 --max-tasks-per-child=1000 & \
$c -n short@%h    -Q campaign_short -c 2 & \
$c -n dispatch@%h -Q dispatch --pool=solo & \
$c -n persist@%h  -Q persist -c 2 & \
wait"
}

wait_ready() {            # wait_ready <stack> (celery: 5 pongs)
    if [ "$1" = "celery" ]; then
        local n=0
        for _ in $(seq 1 90); do
            n="$("$CELERY_BIN" -A campaign_celery_c inspect ping --timeout 2 2>/dev/null | grep -c 'OK' || true)"
            [ "$n" -ge 5 ] && return 0
            kill -0 "$WORKER_PID" 2>/dev/null || return 1
            sleep 1
        done
        return 1
    else
        sleep 3
        kill -0 "$WORKER_PID" 2>/dev/null
    fi
}

match_filter() {
    [ -z "$FILTER" ] && return 0
    case "$1" in "$FILTER"*) return 0 ;; esac
    [ "$2" = "$FILTER" ] && return 0
    return 1
}

run_c() {                 # run_c <name> <stack>
    local name="$1" stack="$2"
    match_filter "$name" "$stack" || return 0
    if [ "$stack" = "cauli" ] && [ ! -x "$CAULI_WORKER_BIN" ]; then
        log "SKIP $name: no cauli binary"
        return 0
    fi
    log "=== $name stack=$stack campaigns=$C_CAMPAIGNS n=$C_MIN_N-$C_MAX_N pages=$N_PAGES error_rate=$ERROR_RATE"
    redis-cli -p "$PORT" flushall >/dev/null
    local started=1
    if [ "$stack" = "celery" ]; then
        start_scoped "$name" 1G bash -c "$(celery_c_cmd)" && started=0
    else
        start_scoped "$name" 1G "$CAULI_WORKER_BIN" --app campaign_cauli_c:app \
            --redis-url "$CAULI_URL" --python "$PY" \
            --queues default,dispatch,campaign_short,campaign_long,backfill_heavy,webhook_ingest,persist \
            --io-concurrency 1000 --io-threads 16 --cpu-workers 1 \
            --visibility-timeout 300 && started=0
    fi
    if [ "$started" != "0" ] || ! wait_ready "$stack"; then
        log "ERROR $name: worker failed/never ready (see $LOGS/$name.worker.log)"
        printf '{"scenario":"%s","status":"start_failed"}\n' "$name" > "$RESULTS/$name.json"
        stop_scoped "$name"
        redis-cli -p "$PORT" flushall >/dev/null
        return 1
    fi
    log "$name: worker ready pid=$WORKER_PID cgroup=$CGPATH"
    "$PY" driver_c.py --stack "$stack" --scenario "$name" \
        --campaigns "$C_CAMPAIGNS" --min-n "$C_MIN_N" --max-n "$C_MAX_N" \
        --pages "$N_PAGES" --seed "$C_SEED" --tick "$TICK_SECONDS" \
        --timeout "$DRIVER_TIMEOUT" --cgroup-path "$CGPATH" --pid "$WORKER_PID" \
        2>&1 | tee -a "$LOGS/$name.driver.log" | grep -E '^\[c\]' \
        || log "warn: $name driver exited nonzero (recorded in JSON)"
    stop_scoped "$name"
    redis-cli -p "$PORT" flushall >/dev/null
}

log "scenario C suite start (filter='${FILTER:-all}') redis=$PORT graph=$GRAPH_PORT timeout=${DRIVER_TIMEOUT}s"
run_c C_celery celery
run_c C_cauli   cauli
log "suite done; results in $RESULTS"

"$PY" - <<'EOF'
import glob, json, os
for p in sorted(glob.glob(os.path.join('results', 'C_*.json'))):
    try:
        d = json.load(open(p))
    except Exception:
        continue
    v = d.get('validations') or {}
    lat = d.get('latency_e2e_ms') or {}
    lag = d.get('persist_lag_ms') or {}
    mem = d.get('memory') or {}
    peak = mem.get('memory_peak_file_bytes') or mem.get('peak_cgroup_sampled_bytes') or 0
    print(f"{d.get('scenario',''):<12} {d.get('status',''):<9} drain={d.get('drain_wall_s',0):>7.1f}s "
          f"sent={v.get('sent','')} pg={v.get('pg_count','')} match={v.get('pg_matches_sent','')} "
          f"dup={v.get('duplicates','')} fail={v.get('failed','')} "
          f"p50={lat.get('p50','')}ms lag_p50={lag.get('p50','')}ms "
          f"peak={peak/1048576:.0f}MiB oom={mem.get('oom_kills')}")
EOF

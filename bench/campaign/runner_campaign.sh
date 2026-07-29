#!/usr/bin/env bash
# Campaign pipeline benchmark orchestrator: Celery (production topology) vs
# cauli, sequential, 1G-capped, on WSL2. Same conventions as bench/runner.sh:
# throwaway redis on $BENCH_REDIS_PORT (default 6390, NEVER 6379), FLUSHALL
# between scenarios, fake Graph API uncapped on 8078 for the whole suite,
# workers inside `systemd-run --user --scope -p MemoryMax=1G -p MemorySwapMax=0`.
#
# Usage:   bash runner_campaign.sh [filter]
#   filter matches scenario name prefixes (A, B, A_prod_celery, ...) or a
#   stack name (celery | cauli). No filter runs everything.
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
DRIVER_TIMEOUT="${BENCH_DRIVER_TIMEOUT:-1800}"
FILTER="${1:-}"
GRAPH_PORT="${FAKE_GRAPH_PORT:-8078}"
export FAKE_GRAPH_URL="http://127.0.0.1:$GRAPH_PORT"

mkdir -p "$LOGS"
cd "$CAMP_DIR"
ulimit -n 65535 2>/dev/null || true

log() { echo "[runner $(date +%H:%M:%S)] $*" | tee -a "$LOGS/runner.log"; }

[ -x "$PY" ] || { echo "venv missing at $VENV; run: bash ../setup.sh"; exit 1; }

CAULI_URL="redis://127.0.0.1:$PORT/0"

# PYTHONPATH/VIRTUAL_ENV exports copied from bench/runner.sh: the cauli worker
# embeds CPython; venv site-packages + this dir + bench dir on PYTHONPATH,
# the raw py/ source dir for the editable cauli install (.pth hooks never run
# from PYTHONPATH), VIRTUAL_ENV so the worker shim does real site processing.
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

start_redis() {
    if redis-cli -p "$PORT" ping >/dev/null 2>&1; then
        log "redis already up on $PORT (reusing)"
    else
        redis-server --port "$PORT" --save '' --appendonly no --daemonize yes
        STARTED_REDIS=1
        for _ in $(seq 1 20); do
            redis-cli -p "$PORT" ping >/dev/null 2>&1 && break
            sleep 0.2
        done
        redis-cli -p "$PORT" ping >/dev/null 2>&1 || { log "FATAL: redis on $PORT did not start"; exit 1; }
        log "started throwaway redis on port $PORT"
    fi
}

check_graph() {
    "$PY" -c "import urllib.request,sys
try:
    r=urllib.request.urlopen('http://127.0.0.1:$GRAPH_PORT/health',timeout=2)
    sys.exit(0 if r.status==200 else 1)
except Exception:
    sys.exit(1)"
}

start_graph() {
    if check_graph; then
        log "fake_graph already up on $GRAPH_PORT (reusing)"
        return 0
    fi
    nohup "$PY" fake_graph.py > "$LOGS/fake_graph.log" 2>&1 &
    GRAPH_PID=$!
    for _ in $(seq 1 40); do
        check_graph && { log "fake_graph up on $GRAPH_PORT (pid $GRAPH_PID, uncapped)"; return 0; }
        sleep 0.25
    done
    log "FATAL: fake_graph did not become healthy (see $LOGS/fake_graph.log)"
    exit 1
}

start_scoped() {          # start_scoped <name> <cap|none> <cmd...>
    local unit="$1" cap="$2"; shift 2
    systemctl --user reset-failed "camp-$unit.scope" 2>/dev/null || true
    local props=(--collect -p TimeoutStopSec=20)
    if [ "$cap" != "none" ]; then
        props+=(-p "MemoryMax=$cap" -p "MemorySwapMax=0")
    fi
    systemd-run --user --scope --unit="camp-$unit" "${props[@]}" -- "$@" \
        > "$LOGS/$unit.worker.log" 2>&1 &
    SCOPE_PID=$!
    CGPATH=""
    WORKER_PID=""
    local cg=""
    for _ in $(seq 1 50); do
        cg="$(systemctl --user show "camp-$unit.scope" -p ControlGroup --value 2>/dev/null || true)"
        if [ -n "$cg" ] && [ -d "/sys/fs/cgroup$cg" ]; then break; fi
        cg=""
        sleep 0.2
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

stop_scoped() {           # stop_scoped <name>
    local unit="camp-$1.scope"
    systemctl --user stop "$unit" 2>/dev/null || true
    for _ in $(seq 1 30); do
        systemctl --user is-active --quiet "$unit" 2>/dev/null || break
        sleep 1
    done
    if systemctl --user is-active --quiet "$unit" 2>/dev/null; then
        systemctl --user kill -s SIGKILL "$unit" 2>/dev/null || true
    fi
    systemctl --user reset-failed "$unit" 2>/dev/null || true
    if [ -n "$SCOPE_PID" ]; then wait "$SCOPE_PID" 2>/dev/null || true; fi
    SCOPE_PID=""
}

wait_ready() {            # wait_ready <stack>  (celery: all 4 nodes pong)
    local stack="$1"
    if [ "$stack" = "celery" ]; then
        local n=0
        for _ in $(seq 1 60); do
            n="$("$CELERY_BIN" -A campaign_celery inspect ping --timeout 2 2>/dev/null | grep -c 'OK' || true)"
            [ "$n" -ge 4 ] && return 0
            kill -0 "$WORKER_PID" 2>/dev/null || return 1
            sleep 1
        done
        return 1
    else
        sleep 3
        kill -0 "$WORKER_PID" 2>/dev/null
    fi
}

match_filter() {          # match_filter <name> <stack>
    [ -z "$FILTER" ] && return 0
    case "$1" in "$FILTER"*) return 0 ;; esac
    [ "$2" = "$FILTER" ] && return 0
    return 1
}

# Production topology (chatsx profile, scaled): 4 celery worker processes in
# ONE scope; default worker also owns backfill_heavy + webhook_ingest.
celery_topology_cmd() {
    local c="\"$CELERY_BIN\" -A campaign_celery worker --loglevel=WARNING"
    echo "$c -n default@%h  -Q celery,backfill_heavy,webhook_ingest -c 2 & \
$c -n long@%h     -Q campaign_long -c 4 --max-tasks-per-child=1000 & \
$c -n short@%h    -Q campaign_short -c 2 & \
$c -n dispatch@%h -Q dispatch --pool=solo & \
wait"
}

CAULI_QUEUES="default,dispatch,campaign_short,campaign_long,backfill_heavy,webhook_ingest"

run_campaign() {          # run_campaign <name> <stack> <n> <pages>
    local name="$1" stack="$2" n="$3" pages="$4"
    match_filter "$name" "$stack" || return 0
    if [ "$stack" = "cauli" ] && [ ! -x "$CAULI_WORKER_BIN" ]; then
        log "SKIP $name: cauli binary not found at $CAULI_WORKER_BIN"
        printf '{"scenario":"%s","status":"skipped","reason":"cauli binary not found"}\n' "$name" > "$RESULTS/$name.json"
        return 0
    fi
    log "=== $name  stack=$stack n=$n pages=$pages send_delay=$SEND_DELAY tick=$TICK_SECONDS variant=${CAULI_VARIANT:-sync}"
    redis-cli -p "$PORT" flushall >/dev/null
    local started=1
    if [ "$stack" = "celery" ]; then
        start_scoped "$name" 1G bash -c "$(celery_topology_cmd)" && started=0
    else
        local flags
        if [ "${CAULI_VARIANT:-sync}" = "async" ]; then
            flags="--io-concurrency 1000 --io-threads 8"
        else
            flags="--io-concurrency 200 --io-threads 96"
        fi
        # shellcheck disable=SC2086
        start_scoped "$name" 1G "$CAULI_WORKER_BIN" --app campaign_cauli:app \
            --redis-url "$CAULI_URL" --python "$PY" --queues "$CAULI_QUEUES" \
            --cpu-workers 1 --visibility-timeout 300 $flags && started=0
    fi
    if [ "$started" != "0" ]; then
        log "ERROR $name: worker scope failed to start (see $LOGS/$name.worker.log)"
        printf '{"scenario":"%s","status":"start_failed"}\n' "$name" > "$RESULTS/$name.json"
        stop_scoped "$name"
        return 1
    fi
    if ! wait_ready "$stack"; then
        log "ERROR $name: worker never became ready (see $LOGS/$name.worker.log)"
        printf '{"scenario":"%s","status":"start_failed"}\n' "$name" > "$RESULTS/$name.json"
        stop_scoped "$name"
        redis-cli -p "$PORT" flushall >/dev/null
        return 1
    fi
    log "$name: worker ready pid=$WORKER_PID cgroup=$CGPATH"
    "$PY" campaign_driver.py --stack "$stack" --scenario "$name" --n "$n" \
        --pages "$pages" --tick "$TICK_SECONDS" --timeout "$DRIVER_TIMEOUT" \
        --cgroup-path "$CGPATH" --pid "$WORKER_PID" \
        2>&1 | tee -a "$LOGS/$name.driver.log" | grep -E '^\[campaign\]' \
        || log "warn: $name driver exited nonzero (status recorded in JSON)"
    stop_scoped "$name"
    redis-cli -p "$PORT" flushall >/dev/null
}

# ---------------------------------------------------------------- scenarios --
set_A() {   # production profile
    export SEND_DELAY=1.0 APP_MAX_PER_MINUTE=200 MAX_BATCHES_PER_DISPATCH=10 \
           ERROR_RATE=0.02 TICK_SECONDS=15 N_PAGES=20
    N=4000; PAGES=20
}
set_B() {   # uncorked profile
    export SEND_DELAY=0.2 APP_MAX_PER_MINUTE=6000 MAX_BATCHES_PER_DISPATCH=40 \
           ERROR_RATE=0.02 TICK_SECONDS=5 N_PAGES=50
    N=10000; PAGES=50
}

log "suite start (filter='${FILTER:-all}') redis=$PORT graph=$GRAPH_PORT cauli_bin=$CAULI_WORKER_BIN driver_timeout=${DRIVER_TIMEOUT}s"
start_redis
start_graph

set_A
export CAULI_VARIANT=sync
run_campaign A_prod_celery      celery "$N" "$PAGES"
run_campaign A_prod_cauli_sync   cauli   "$N" "$PAGES"
export CAULI_VARIANT=async
run_campaign A_prod_cauli_async  cauli   "$N" "$PAGES"

set_B
export CAULI_VARIANT=sync
run_campaign B_uncorked_celery    celery "$N" "$PAGES"
run_campaign B_uncorked_cauli_sync cauli   "$N" "$PAGES"
export CAULI_VARIANT=async
run_campaign B_uncorked_cauli_async cauli  "$N" "$PAGES"

log "suite done; results in $RESULTS"
"$PY" - <<'EOF'
import glob, json, os
print(f"{'scenario':<24} {'status':<11} {'drain_s':>8} {'sends_ps':>8} {'sent':>6} "
      f"{'fail':>5} {'dup':>4} {'p50s':>7} {'p95s':>7} {'peakMiB':>8} {'oom':>4}")
for p in sorted(glob.glob(os.path.join('results', '*.json'))):
    try:
        d = json.load(open(p))
    except Exception:
        continue
    if not isinstance(d, dict) or 'scenario' not in d:
        continue
    lat = d.get('latency_e2e_ms') or {}
    mem = d.get('memory') or {}
    cnt = d.get('counts') or {}
    inv = d.get('invariants') or {}
    peak = mem.get('memory_peak_file_bytes') or mem.get('peak_cgroup_sampled_bytes') or mem.get('peak_rss_sampled_bytes') or 0
    ds = d.get('drain_wall_s') or 0
    sent = cnt.get('sent', 0)
    p50 = lat.get('p50'); p95 = lat.get('p95')
    print(f"{d.get('scenario',''):<24} {d.get('status',''):<11} {ds:>8.1f} "
          f"{(sent/ds if ds else 0):>8.1f} {sent:>6} {cnt.get('failed',0):>5} "
          f"{str(inv.get('duplicates','')):>4} "
          f"{(p50/1000 if p50 else 0):>7.1f} {(p95/1000 if p95 else 0):>7.1f} "
          f"{peak/1048576:>8.1f} {str(mem.get('oom_kills','')):>4}")
EOF

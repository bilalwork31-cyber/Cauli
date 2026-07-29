#!/usr/bin/env bash
# django_real orchestrator: Meta-Chat campaign chaos on a REAL Django app,
# 10-minute time box, uncorked dispatcher, NO memory cap.
#
# Conventions of runner_c.sh with: throwaway redis 6396, fake_graph 8078,
# Postgres db bench_django, systemd-run user scopes WITHOUT MemoryMax (scope
# exists only for cgroup memory accounting), celery first then cauli.
#
# Usage: bash runner_django.sh smoke     [celery|cauli]  2-min window, 20k, ORM layer
#        bash runner_django.sh real      [celery|cauli]  10-min window, 1M, ORM layer
#        bash runner_django.sh smoke_raw [celery|cauli]  2-min window, 20k, raw layer
#        bash runner_django.sh real_raw  [celery|cauli]  10-min window, 1M, raw layer
#        bash runner_django.sh sym_smoke [celery_frozen|cauli_fork|cauli_async]
#        bash runner_django.sh sym       [celery_frozen|cauli_fork|cauli_async]
#
# raw layer (DJ_DATA_LAYER=raw, both stacks identically): one-statement raw
# SQL claim, redis intent locks/sent flags, outcomes LPUSHed to redis (send
# hot path touches no Postgres), bg persisters draining DJ_PERSIST_BATCH=50
# per drain with raw executemany, DJ_PERSIST_TASKS=4 chains (celery persist
# worker scaled to -c 4 to match).
#
# sym / sym_smoke: symmetric-topology, both-frozen comparison on the raw
# layer. Same process count and concurrency budget on both stacks:
#   celery_frozen  ONE celery invocation, ALL queues, prefork -c 6, acks_late,
#                  prefetch 1, gc.collect()+gc.freeze() in the master before
#                  forking (CAULI_BENCH_GC_FREEZE=1 hook in celery_app_django)
#   cauli_fork     ONE cauli worker, every task kind=cpu (DJ_CAULI_KIND=cpu):
#                  --cpu-workers 6 --cpu-child-threads 15 fork-server children
#                  forked from a gc.freeze()-d preloaded parent (§5.1)
#   cauli_async    ONE cauli worker, the io-lane raw variant (the DJR
#                  real_raw cauli leg: io-concurrency 40, cpu-workers 1)
set -u
set -o pipefail

DJ_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BENCH_DIR="$(cd "$DJ_DIR/.." && pwd)"
CAMP_DIR="$BENCH_DIR/campaign"
ROOT_DIR="$(cd "$BENCH_DIR/.." && pwd)"
VENV="${BENCH_VENV:-/home/blackdevil/rupy-bench-venv}"
PY="$VENV/bin/python"
CELERY_BIN="$VENV/bin/celery"
PORT="${BENCH_REDIS_PORT:-6396}"
export BENCH_REDIS_PORT="$PORT"
export PYTHONUNBUFFERED=1
export DJANGO_SETTINGS_MODULE=benchsite.settings
CAULI_WORKER_BIN="${CAULI_WORKER_BIN:-/home/blackdevil/rupy-target/release/cauli-worker}"
RESULTS="$DJ_DIR/results"
LOGS="$RESULTS/logs"
GRAPH_PORT="${FAKE_GRAPH_PORT:-8078}"
export FAKE_GRAPH_PORT="$GRAPH_PORT"
export FAKE_GRAPH_URL="http://127.0.0.1:$GRAPH_PORT"

# ---- scenario knobs: uncorked dispatcher, PRODUCTION 10-min lease ----
export SEND_DELAY=0 TICK_SECONDS=5 N_PAGES=200 ERROR_RATE=0.05 \
       APP_MAX_PER_MINUTE=1000000000 MAX_BATCHES_PER_DISPATCH=1000000 \
       LEASE_MS=600000

MODE="${1:-smoke}"
FILTER="${2:-}"

DATA_LAYER=orm
SUFFIX=""
SYM=0
case "$MODE" in
    smoke)     N_CAMPAIGNS=20;  N_PER=1000;  WARMUP=30; WINDOW=120; PREFIX=smoke_dj ;;
    real)      N_CAMPAIGNS=100; N_PER=10000; WARMUP=60; WINDOW=600; PREFIX=DJ ;;
    smoke_raw) N_CAMPAIGNS=20;  N_PER=1000;  WARMUP=30; WINDOW=120; PREFIX=smoke_dj
               DATA_LAYER=raw; SUFFIX=_raw ;;
    real_raw)  N_CAMPAIGNS=100; N_PER=10000; WARMUP=60; WINDOW=600; PREFIX=DJR
               DATA_LAYER=raw; SUFFIX=_raw ;;
    sym_smoke) N_CAMPAIGNS=20;  N_PER=1000;  WARMUP=30; WINDOW=120; PREFIX=smoke_sym
               DATA_LAYER=raw; SYM=1 ;;
    sym)       N_CAMPAIGNS=100; N_PER=10000; WARMUP=60; WINDOW=600; PREFIX=SYM
               DATA_LAYER=raw; SYM=1 ;;
    *) echo "usage: runner_django.sh smoke|real|smoke_raw|real_raw|sym_smoke|sym [filter]"; exit 2 ;;
esac

export DJ_DATA_LAYER="$DATA_LAYER"
export DJ_PERSIST_BATCH="${DJ_PERSIST_BATCH:-50}"
if [ "$DATA_LAYER" = "raw" ]; then PERSIST_C_DEFAULT=12; else PERSIST_C_DEFAULT=2; fi
PERSIST_C="${DJ_PERSIST_TASKS:-$PERSIST_C_DEFAULT}"
export DJ_PERSIST_TASKS="$PERSIST_C"
# celery persist worker concurrency: enough slots for the chains, capped at 4
# forks so the raw run does not inflate celery RAM (4 concurrent drains at
# batch 50 measured 144 ms p50 lag in smoke - celery persisters are not
# admission-starved the way one-process cauli chains are).
CELERY_PERSIST_C="$PERSIST_C"
[ "$CELERY_PERSIST_C" -gt 4 ] && CELERY_PERSIST_C=4

mkdir -p "$LOGS"
cd "$DJ_DIR"
ulimit -n 65535 2>/dev/null || true

log() { echo "[runner-dj $(date +%H:%M:%S)] $*" | tee -a "$LOGS/runner_django.log"; }

[ -x "$PY" ] || { echo "venv missing at $VENV"; exit 1; }

SITEPKG="$("$PY" -c 'import sysconfig; print(sysconfig.get_paths()["purelib"])' 2>/dev/null || true)"
export PYTHONPATH="${SITEPKG:+$SITEPKG:}$DJ_DIR:$CAMP_DIR:$BENCH_DIR:$ROOT_DIR/py${PYTHONPATH:+:$PYTHONPATH}"
export VIRTUAL_ENV="$VENV"
export PATH="$VENV/bin:$PATH"

# ---- coordination guards ----
if systemctl --user list-units --plain --no-legend 'dj-*.scope' 2>/dev/null | grep -q .; then
    log "FATAL: dj-*.scope units active (previous run?). Aborting."
    exit 1
fi
if redis-cli -p "$PORT" ping >/dev/null 2>&1; then
    log "FATAL: redis already listening on $PORT. Aborting."
    exit 1
fi
if curl -s -m 2 "http://127.0.0.1:$GRAPH_PORT/health" >/dev/null 2>&1; then
    log "FATAL: something on $GRAPH_PORT. Aborting."
    exit 1
fi
if ! "$PY" -c "import django_boot" 2>"$LOGS/pgcheck.err"; then
    log "FATAL: django_boot import failed (see $LOGS/pgcheck.err)"
    exit 1
fi
"$PY" manage.py migrate --noinput >"$LOGS/migrate.log" 2>&1 \
    || { log "FATAL: migrate failed (see $LOGS/migrate.log)"; exit 1; }

CAULI_URL="redis://127.0.0.1:$PORT/0"
CAULI_QUEUES="default,dispatch,campaign_short,campaign_long,backfill_heavy,webhook_ingest,persist"

STARTED_REDIS=0
GRAPH_PID=""
SCOPE_PID=""
CGPATH=""
WORKER_PID=""

cleanup() {
    log "cleanup: tearing down scopes, fake_graph, redis"
    local units
    units="$(systemctl --user list-units --plain --no-legend 'dj-*.scope' 2>/dev/null | awk '{print $1}')"
    for u in $units; do
        systemctl --user kill -s SIGKILL "$u" 2>/dev/null || true
        systemctl --user stop "$u" 2>/dev/null || true
        systemctl --user reset-failed "$u" 2>/dev/null || true
    done
    if [ -n "$GRAPH_PID" ]; then
        kill "$GRAPH_PID" 2>/dev/null || true
        sleep 1
        kill -9 "$GRAPH_PID" 2>/dev/null || true
        pkill -9 -f "fake_graph" 2>/dev/null || true
        fuser -k -9 "$GRAPH_PORT/tcp" 2>/dev/null || true
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

( cd "$CAMP_DIR" && exec nohup "$PY" fake_graph.py > "$LOGS/fake_graph.log" 2>&1 ) &
GRAPH_PID=$!
gok=0
for _ in $(seq 1 40); do
    curl -s -m 2 "http://127.0.0.1:$GRAPH_PORT/health" >/dev/null 2>&1 && { gok=1; break; }
    sleep 0.25
done
[ "$gok" = "1" ] || { log "FATAL: fake_graph did not start"; exit 1; }
log "fake_graph up on $GRAPH_PORT (ERROR_RATE=$ERROR_RATE)"

start_scoped() {          # start_scoped <name> <cmd...>  -- NO MemoryMax
    local unit="$1"; shift
    systemctl --user reset-failed "dj-$unit.scope" 2>/dev/null || true
    systemd-run --user --scope --unit="dj-$unit" --collect -p TimeoutStopSec=240 \
        -p "MemorySwapMax=0" -- "$@" \
        > "$LOGS/$unit.worker.log" 2>&1 &
    SCOPE_PID=$!
    CGPATH=""; WORKER_PID=""
    local cg=""
    for _ in $(seq 1 50); do
        cg="$(systemctl --user show "dj-$unit.scope" -p ControlGroup --value 2>/dev/null || true)"
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

stop_scoped() {           # graceful TERM (celery warm shutdown / cauli drain)
    local unit="dj-$1.scope"
    log "stopping $unit (graceful, TimeoutStopSec=240)"
    systemctl --user stop "$unit" 2>/dev/null || true
    for _ in $(seq 1 60); do
        systemctl --user is-active --quiet "$unit" 2>/dev/null || break
        sleep 1
    done
    systemctl --user kill -s SIGKILL "$unit" 2>/dev/null || true
    systemctl --user reset-failed "$unit" 2>/dev/null || true
    if [ -n "$SCOPE_PID" ]; then wait "$SCOPE_PID" 2>/dev/null || true; fi
    SCOPE_PID=""
}

celery_cmd() {   # production topology + persist + DEDICATED bg-fill workers
    local c="\"$CELERY_BIN\" -A celery_app_django worker --loglevel=WARNING"
    echo "$c -n default@%h  -Q celery -c 2 & \
$c -n long@%h     -Q campaign_long -c 4 --max-tasks-per-child=1000 & \
$c -n short@%h    -Q campaign_short -c 2 & \
$c -n dispatch@%h -Q dispatch --pool=solo & \
$c -n persist@%h  -Q persist -c $CELERY_PERSIST_C & \
$c -n backfill@%h -Q backfill_heavy -c 2 & \
$c -n webhook@%h  -Q webhook_ingest -c 2 & \
wait"
}

CELERY_PONGS=7            # nodes expected to pong (prod topology: 7; sym: 1)
wait_ready() {            # wait_ready <stack>
    if [ "$1" = "celery" ]; then
        local n=0
        for _ in $(seq 1 120); do
            n="$("$CELERY_BIN" -A celery_app_django inspect ping --timeout 2 2>/dev/null | grep -c 'OK' || true)"
            [ "$n" -ge "$CELERY_PONGS" ] && return 0
            kill -0 "$WORKER_PID" 2>/dev/null || return 1
            sleep 1
        done
        return 1
    else
        sleep 5
        kill -0 "$WORKER_PID" 2>/dev/null
    fi
}

match_filter() {
    [ -z "$FILTER" ] && return 0
    [ "$1" = "$FILTER" ] && return 0
    return 1
}

run_stack() {             # run_stack <stack>
    local stack="$1" name="${PREFIX}_$1${SUFFIX}"
    match_filter "$stack" || return 0
    if [ "$stack" = "cauli" ] && [ ! -x "$CAULI_WORKER_BIN" ]; then
        log "SKIP $name: no cauli binary"
        return 0
    fi
    log "=== $name stack=$stack campaigns=$N_CAMPAIGNS per=$N_PER pages=$N_PAGES warmup=${WARMUP}s window=${WINDOW}s"
    redis-cli -p "$PORT" flushall >/dev/null
    log "$name: seeding fresh schema"
    "$PY" driver_django.py seed --campaigns "$N_CAMPAIGNS" --per "$N_PER" \
        --pages "$N_PAGES" 2>&1 | tee -a "$LOGS/$name.driver.log" | grep -E '^\[dj\]' \
        || { log "ERROR $name: seed failed"; return 1; }
    local started=1
    if [ "$stack" = "celery" ]; then
        start_scoped "$name" bash -c "$(celery_cmd)" && started=0
    else
        # io-concurrency must stay close to io-threads: sync ORM send_batch
        # tasks BLOCK their admission slot, and hundreds of admitted-but-
        # blocked batches starve the short persist/dispatch tasks (seen in
        # smoke). 40 slots on 48 sync threads recycle in seconds; the batch
        # backlog stays in redis streams instead. --batch 8 keeps per-round
        # over-fetch small so every queue gets slots fairly.
        export DJ_SEND_POOL=240
        start_scoped "$name" "$CAULI_WORKER_BIN" --app cauli_app_django:app \
            --redis-url "$CAULI_URL" --python "$PY" --queues "$CAULI_QUEUES" \
            --io-concurrency 40 --io-threads 48 --batch 8 --cpu-workers 1 \
            --visibility-timeout 300 && started=0
        unset DJ_SEND_POOL
    fi
    if [ "$started" != "0" ] || ! wait_ready "$stack"; then
        log "ERROR $name: worker failed/never ready (see $LOGS/$name.worker.log)"
        printf '{"scenario":"%s","status":"start_failed"}\n' "$name" > "$RESULTS/$name.json"
        stop_scoped "$name"
        redis-cli -p "$PORT" flushall >/dev/null
        return 1
    fi
    log "$name: worker ready pid=$WORKER_PID cgroup=$CGPATH"
    "$PY" driver_django.py run --stack "$stack" --scenario "$name" \
        --warmup "$WARMUP" --window "$WINDOW" --tick "$TICK_SECONDS" \
        --cgroup-path "$CGPATH" --pid "$WORKER_PID" \
        2>&1 | tee -a "$LOGS/$name.driver.log" | grep -E '^\[dj\]' \
        || log "warn: $name run exited nonzero (recorded in JSON)"
    stop_scoped "$name"
    log "$name: finalizing (post-stop drain + integrity)"
    "$PY" driver_django.py finalize --scenario "$name" \
        2>&1 | tee -a "$LOGS/$name.driver.log" | grep -E '^\[dj\]' \
        || log "warn: $name finalize exited nonzero (recorded in JSON)"
    redis-cli -p "$PORT" flushall >/dev/null
}

# ---- symmetric topology, both frozen (sym / sym_smoke modes) ----------------
CELERY_Q_ALL="celery,dispatch,campaign_short,campaign_long,backfill_heavy,webhook_ingest,persist"

run_sym() {               # run_sym <celery_frozen|cauli_fork|cauli_async>
    local cfg="$1" name="${PREFIX}_$1" stack=cauli started=1 rc=0
    [ "$cfg" = "celery_frozen" ] && stack=celery
    match_filter "$cfg" || return 0
    if [ "$stack" = "cauli" ] && [ ! -x "$CAULI_WORKER_BIN" ]; then
        log "SKIP $name: no cauli binary"
        return 0
    fi
    log "=== $name stack=$stack campaigns=$N_CAMPAIGNS per=$N_PER pages=$N_PAGES warmup=${WARMUP}s window=${WINDOW}s"
    redis-cli -p "$PORT" flushall >/dev/null
    log "$name: seeding fresh schema"
    "$PY" driver_django.py seed --campaigns "$N_CAMPAIGNS" --per "$N_PER" \
        --pages "$N_PAGES" 2>&1 | tee -a "$LOGS/$name.driver.log" | grep -E '^\[dj\]' \
        || { log "ERROR $name: seed failed"; return 1; }
    case "$cfg" in
        celery_frozen)
            # ONE worker invocation, ALL queues, prefork -c 6, gc.freeze in
            # the master pre-fork (CAULI_BENCH_GC_FREEZE hook). Each child
            # still runs the ThreadPoolExecutor(15) send pool per batch.
            CELERY_PONGS=1
            start_scoped "$name" bash -c "CAULI_BENCH_GC_FREEZE=1 exec \"$CELERY_BIN\" \
-A celery_app_django worker --loglevel=WARNING -Q $CELERY_Q_ALL -c 6" && started=0
            ;;
        cauli_fork)
            # Same 6 GILs / same per-child 15-thread budget as celery -c 6:
            # every task kind=cpu on 6 fork-server children (forked from ONE
            # preloaded gc.freeze()-d parent), 15 in-flight tasks per child,
            # DJ_SEND_POOL=15 per child process = celery's per-child pool.
            export DJ_CAULI_KIND=cpu DJ_SEND_POOL=15
            start_scoped "$name" "$CAULI_WORKER_BIN" --app cauli_app_django:app \
                --redis-url "$CAULI_URL" --python "$PY" --queues "$CAULI_QUEUES" \
                --io-concurrency 64 --io-threads 8 --batch 8 \
                --cpu-workers 6 --cpu-child-threads 15 \
                --visibility-timeout 300 && started=0
            ;;
        cauli_fork40)
            # Tuned fork lane: same 6 GILs but 40 in-flight tasks per child
            # (sends are ~300ms GIL-free HTTP wait + ~3-4ms Python, so one
            # GIL supports ~40 threads before it saturates).
            export DJ_CAULI_KIND=cpu DJ_SEND_POOL=40
            start_scoped "$name" "$CAULI_WORKER_BIN" --app cauli_app_django:app \
                --redis-url "$CAULI_URL" --python "$PY" --queues "$CAULI_QUEUES" \
                --io-concurrency 64 --io-threads 8 --batch 16 \
                --cpu-workers 6 --cpu-child-threads 40 \
                --visibility-timeout 300 && started=0
            ;;
        cauli_async)
            # The DJR real_raw cauli leg: one process, io lane, sync send
            # threads over the raw redis/SQL layer (see real_raw notes above).
            export DJ_SEND_POOL=240
            start_scoped "$name" "$CAULI_WORKER_BIN" --app cauli_app_django:app \
                --redis-url "$CAULI_URL" --python "$PY" --queues "$CAULI_QUEUES" \
                --io-concurrency 40 --io-threads 48 --batch 8 --cpu-workers 1 \
                --visibility-timeout 300 && started=0
            ;;
        *) log "unknown sym config $cfg"; return 2 ;;
    esac
    if [ "$started" != "0" ] || ! wait_ready "$stack"; then
        log "ERROR $name: worker failed/never ready (see $LOGS/$name.worker.log)"
        printf '{"scenario":"%s","status":"start_failed"}\n' "$name" > "$RESULTS/$name.json"
        rc=1
    else
        log "$name: worker ready pid=$WORKER_PID cgroup=$CGPATH"
        "$PY" driver_django.py run --stack "$stack" --scenario "$name" \
            --warmup "$WARMUP" --window "$WINDOW" --tick "$TICK_SECONDS" \
            --cgroup-path "$CGPATH" --pid "$WORKER_PID" \
            2>&1 | tee -a "$LOGS/$name.driver.log" | grep -E '^\[dj\]' \
            || log "warn: $name run exited nonzero (recorded in JSON)"
    fi
    stop_scoped "$name"
    if [ "$rc" = "0" ]; then
        log "$name: finalizing (post-stop drain + integrity)"
        "$PY" driver_django.py finalize --scenario "$name" \
            2>&1 | tee -a "$LOGS/$name.driver.log" | grep -E '^\[dj\]' \
            || log "warn: $name finalize exited nonzero (recorded in JSON)"
    fi
    unset DJ_CAULI_KIND DJ_SEND_POOL
    CELERY_PONGS=7
    redis-cli -p "$PORT" flushall >/dev/null
    return $rc
}

log "django_real $MODE suite start (filter='${FILTER:-all}') layer=$DATA_LAYER redis=$PORT graph=$GRAPH_PORT lease=${LEASE_MS}ms"
if [ "$SYM" = "1" ]; then
    run_sym celery_frozen
    run_sym cauli_fork
    run_sym cauli_fork40
    run_sym cauli_async
else
    run_stack celery
    run_stack cauli
fi
log "suite done; results in $RESULTS"

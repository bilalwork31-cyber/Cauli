#!/usr/bin/env bash
# Benchmark orchestrator: Celery vs cauli, sequential, resource capped, on WSL2.
#
# Usage:   bash runner.sh [filter]
#   filter matches scenario name prefixes (S1, S1a, S3, ...) or a stack name
#   (celery | cauli). No filter runs everything.
#
# One system under test at a time. Throwaway redis on $BENCH_REDIS_PORT
# (default 6390, NEVER 6379), FLUSHALL between scenarios, mock_api running
# uncapped for the whole suite. Memory caps use the VALIDATED user scope form:
#
#   systemd-run --user --scope --unit=NAME -p MemoryMax=CAP -p MemorySwapMax=0 -- CMD
#
# (MemorySwapMax=0 is required: WSL2 has 8G swap which would silently absorb
# the cap. Validated on this machine: a hog under MemoryMax=100M gets OOM
# killed, exit 137.) CPU is left uncapped for both stacks: same 6 cores,
# strictly sequential runs, so CPU contention is identical by construction.
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
DRIVER_TIMEOUT="${BENCH_DRIVER_TIMEOUT:-600}"
FILTER="${1:-}"

mkdir -p "$LOGS"
cd "$BENCH_DIR"
ulimit -n 65535 2>/dev/null || true

log() { echo "[runner $(date +%H:%M:%S)] $*" | tee -a "$LOGS/runner.log"; }

[ -x "$PY" ] || { echo "venv missing at $VENV; run: bash setup.sh"; exit 1; }

CAULI_URL="redis://127.0.0.1:$PORT/0"

# The cauli worker embeds CPython and spawns `python -m cauli._exec` children;
# both must be able to import cauli, tasks_cauli, common and their deps from the
# bench venv. PYTHONPATH covers the embedded interpreter; --python (added to
# the cauli invocations below) makes the cpu children use the venv interpreter
# directly. Harmless for Celery (it already runs from the venv).
SITEPKG="$("$PY" -c 'import sysconfig; print(sysconfig.get_paths()["purelib"])' 2>/dev/null || true)"
if [ -n "$SITEPKG" ]; then
    export PYTHONPATH="$SITEPKG:$BENCH_DIR${PYTHONPATH:+:$PYTHONPATH}"
fi
# Editable installs (pip install -e) rely on .pth hooks, which PYTHONPATH
# entries never execute; expose the cauli package source dir directly, and
# export VIRTUAL_ENV so the worker shim performs real site processing
# (site.addsitedir) for the venv as well.
export PYTHONPATH="$(cd "$BENCH_DIR/.." && pwd)/py:$PYTHONPATH"
export VIRTUAL_ENV="$VENV"
export PATH="$VENV/bin:$PATH"

STARTED_REDIS=0
MOCK_PID=""
SCOPE_PID=""
CGPATH=""
WORKER_PID=""

# ---------------------------------------------------------------- cleanup ---
cleanup() {
    log "cleanup: tearing down scopes, mock api, redis"
    local units
    units="$(systemctl --user list-units --plain --no-legend 'bench-*.scope' 2>/dev/null | awk '{print $1}')"
    for u in $units; do
        systemctl --user kill -s SIGKILL "$u" 2>/dev/null || true
        systemctl --user stop "$u" 2>/dev/null || true
        systemctl --user reset-failed "$u" 2>/dev/null || true
    done
    if [ -n "$MOCK_PID" ]; then
        kill "$MOCK_PID" 2>/dev/null || true
        sleep 1
        kill -9 "$MOCK_PID" 2>/dev/null || true
    fi
    if [ "$STARTED_REDIS" = "1" ]; then
        redis-cli -p "$PORT" shutdown nosave 2>/dev/null || true
    fi
}
trap cleanup EXIT

# ------------------------------------------------------------ infrastructure
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

check_mock() {
    "$PY" -c 'import urllib.request,sys
try:
    r=urllib.request.urlopen("http://127.0.0.1:8077/health",timeout=2)
    sys.exit(0 if r.status==200 else 1)
except Exception:
    sys.exit(1)'
}

start_mock() {
    if check_mock; then
        log "mock_api already up on 8077 (reusing)"
        return 0
    fi
    nohup "$PY" mock_api.py > "$LOGS/mock_api.log" 2>&1 &
    MOCK_PID=$!
    for _ in $(seq 1 40); do
        check_mock && { log "mock_api up on 8077 (pid $MOCK_PID, uncapped)"; return 0; }
        sleep 0.25
    done
    log "FATAL: mock_api did not become healthy (see $LOGS/mock_api.log)"
    exit 1
}

# ------------------------------------------------------- scope worker mgmt --
start_scoped() {          # start_scoped <name> <cap|none> <cmd...>
    local unit="$1" cap="$2"; shift 2
    systemctl --user reset-failed "bench-$unit.scope" 2>/dev/null || true
    local props=(--collect -p TimeoutStopSec=20)
    if [ "$cap" != "none" ]; then
        props+=(-p "MemoryMax=$cap" -p "MemorySwapMax=0")
    fi
    systemd-run --user --scope --unit="bench-$unit" "${props[@]}" -- "$@" \
        > "$LOGS/$unit.worker.log" 2>&1 &
    SCOPE_PID=$!
    CGPATH=""
    WORKER_PID=""
    local cg=""
    for _ in $(seq 1 50); do
        cg="$(systemctl --user show "bench-$unit.scope" -p ControlGroup --value 2>/dev/null || true)"
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
    local unit="bench-$1.scope"
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

wait_ready() {            # wait_ready <stack>
    local stack="$1"
    if [ "$stack" = "celery" ]; then
        for _ in $(seq 1 45); do
            if "$CELERY_BIN" -A tasks_celery inspect ping --timeout 1 >/dev/null 2>&1; then
                return 0
            fi
            kill -0 "$WORKER_PID" 2>/dev/null || return 1
            sleep 1
        done
        return 1
    else
        # cauli: no ping protocol; give it 3s to import the app and settle
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

skip_if_no_cauli() {       # skip_if_no_cauli <name> <stack>
    if [ "$2" = "cauli" ] && [ ! -x "$CAULI_WORKER_BIN" ]; then
        log "SKIP $1: cauli binary not found at $CAULI_WORKER_BIN (set CAULI_WORKER_BIN to override)"
        printf '{"scenario":"%s","status":"skipped","reason":"cauli binary not found"}\n' "$1" > "$RESULTS/$1.json"
        return 0
    fi
    return 1
}

# ------------------------------------------------------------- scenarios ----
run_scenario() {          # run_scenario <name> <stack> <cap> <task> <n> -- <worker cmd...>
    local name="$1" stack="$2" cap="$3" task="$4" n="$5"; shift 5
    [ "${1:-}" = "--" ] && shift
    match_filter "$name" "$stack" || return 0
    skip_if_no_cauli "$name" "$stack" && return 0
    log "=== $name  stack=$stack cap=$cap task=$task n=$n"
    redis-cli -p "$PORT" flushall >/dev/null
    if ! start_scoped "$name" "$cap" "$@"; then
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
    # 200 task warmup, never recorded
    "$PY" driver.py --stack "$stack" --task "$task" --n 200 --warmup --timeout 180 \
        --pid "$WORKER_PID" >> "$LOGS/$name.driver.log" 2>&1 \
        || log "warn: $name warmup incomplete (continuing)"
    # measured run
    "$PY" driver.py --stack "$stack" --task "$task" --n "$n" --scenario "$name" \
        --cgroup-path "$CGPATH" --pid "$WORKER_PID" --timeout "$DRIVER_TIMEOUT" \
        2>&1 | tee -a "$LOGS/$name.driver.log" | grep -E '^\[driver\]' \
        || log "warn: $name driver exited nonzero (status recorded in JSON)"
    stop_scoped "$name"
    redis-cli -p "$PORT" flushall >/dev/null
}

run_idle() {              # run_idle <name> <stack> <cap> -- <worker cmd...>
    local name="$1" stack="$2" cap="$3"; shift 3
    [ "${1:-}" = "--" ] && shift
    match_filter "$name" "$stack" || return 0
    skip_if_no_cauli "$name" "$stack" && return 0
    log "=== $name (idle ram) stack=$stack cap=$cap"
    redis-cli -p "$PORT" flushall >/dev/null
    if ! start_scoped "$name" "$cap" "$@"; then
        log "ERROR $name: worker scope failed to start"
        printf '{"scenario":"%s","status":"start_failed"}\n' "$name" > "$RESULTS/$name.json"
        stop_scoped "$name"
        return 1
    fi
    if ! wait_ready "$stack"; then
        log "ERROR $name: worker never became ready"
        printf '{"scenario":"%s","status":"start_failed"}\n' "$name" > "$RESULTS/$name.json"
        stop_scoped "$name"
        return 1
    fi
    "$PY" driver.py --stack "$stack" --idle --idle-duration 20 --scenario "$name" \
        --cgroup-path "$CGPATH" --pid "$WORKER_PID" \
        2>&1 | tee -a "$LOGS/$name.driver.log" | grep -E '^\[driver\]' || true
    stop_scoped "$name"
    redis-cli -p "$PORT" flushall >/dev/null
}

# ------------------------------------------------------------------ suite ---
log "suite start (filter='${FILTER:-all}') redis=$PORT cauli_bin=$CAULI_WORKER_BIN driver_timeout=${DRIVER_TIMEOUT}s"
start_redis
start_mock

CEL=("$CELERY_BIN" -A tasks_celery worker --loglevel=WARNING)
CAULI=("$CAULI_WORKER_BIN" --app tasks_cauli:app --redis-url "$CAULI_URL" --python "$PY")

# S1: 10k io tasks, 1G cap
run_scenario S1a_celery_prefork8   celery 1G io       10000 -- "${CEL[@]}" -c 8   -P prefork
run_scenario S1b_celery_prefork16  celery 1G io       10000 -- "${CEL[@]}" -c 16  -P prefork
run_scenario S1c_celery_gevent500  celery 1G io       10000 -- "${CEL[@]}" -c 500 -P gevent
run_scenario S1d_cauli_sync_io500   cauli   1G io       10000 -- "${CAULI[@]}" --io-concurrency 500 --io-threads 64
run_scenario S1e_cauli_async_io500  cauli   1G io_async 10000 -- "${CAULI[@]}" --io-concurrency 500

# S2: 2k cpu tasks, 1G cap
run_scenario S2a_celery_prefork6   celery 1G cpu 2000 -- "${CEL[@]}" -c 6 -P prefork
run_scenario S2b_cauli_cpu6         cauli   1G cpu 2000 -- "${CAULI[@]}" --cpu-workers 6

# S3: 10k io tasks under 512M stress (celery prefork expected to OOM/thrash;
# the driver survives that and records status=stalled/worker_dead + counts)
run_scenario S3a_celery_prefork8_512M   celery 512M io       10000 -- "${CEL[@]}" -c 8   -P prefork
run_scenario S3b_celery_gevent500_512M  celery 512M io       10000 -- "${CEL[@]}" -c 500 -P gevent
run_scenario S3c_cauli_async_io1000_512M cauli   512M io_async 10000 -- "${CAULI[@]}" --io-concurrency 1000

# S4: idle RAM per worker configuration (20s settle, no tasks)
run_idle S4a_celery_prefork8_idle  celery 1G -- "${CEL[@]}" -c 8   -P prefork
run_idle S4b_celery_prefork16_idle celery 1G -- "${CEL[@]}" -c 16  -P prefork
run_idle S4c_celery_gevent500_idle celery 1G -- "${CEL[@]}" -c 500 -P gevent
run_idle S4d_cauli_io_idle          cauli   1G -- "${CAULI[@]}" --io-concurrency 500 --io-threads 64
run_idle S4e_cauli_cpu_idle         cauli   1G -- "${CAULI[@]}" --cpu-workers 6

# ---------------------------------------------------------------- summary ---
log "suite done; results in $RESULTS"
"$PY" - <<'EOF'
import glob, json, os
print(f"{'scenario':<28} {'status':<12} {'exec_tps':>9} {'p50ms':>8} {'p95ms':>8} {'p99ms':>8} {'peakMiB':>8} {'oom':>4} {'done':>6}")
for p in sorted(glob.glob(os.path.join('results', '*.json'))):
    try:
        d = json.load(open(p))
    except Exception:
        continue
    if not isinstance(d, dict) or 'scenario' not in d:
        continue
    lat = d.get('latency_ms') or {}
    mem = d.get('memory') or {}
    tp = d.get('throughput') or {}
    peak = mem.get('memory_peak_file_bytes') or mem.get('peak_cgroup_sampled_bytes') or mem.get('peak_rss_sampled_bytes') or 0
    idle = d.get('idle_memory_current_bytes')
    if idle is not None:
        peak = idle
    print(f"{d.get('scenario',''):<28} {d.get('status',''):<12} "
          f"{tp.get('exec_tps','') if tp else '':>9} {lat.get('p50','') if lat else '':>8} "
          f"{lat.get('p95','') if lat else '':>8} {lat.get('p99','') if lat else '':>8} "
          f"{peak/1048576:>8.1f} {str(mem.get('oom_kills','')):>4} {str(d.get('completed','')):>6}")
EOF

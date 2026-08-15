#!/usr/bin/env bash
# Run one drain-rate measurement: enqueue N no-op tasks with no worker
# running, start the given worker command, poll until drained. See
# RESULTS.md for the method and why this beats a live producer/consumer
# race (exec_tps becomes invalid once the worker outpaces the enqueuer).
#
# Usage: run.sh <framework> <N> <timeout_s> <result_file> <worker_cmd...>
#   framework: cauli_sync | cauli_async | celery | taskiq
#
# Requires on PATH (or override via env): redis-server, redis-cli, python3
# with cauli/celery/taskiq installed, and cauli-worker if benching cauli.
# BENCH_REDIS_PORT (default 6395) picks a dedicated Redis instance so this
# never touches a real broker on 6379; the script starts one if not already
# listening.
set -uo pipefail
cd "$(dirname "$0")"

FRAMEWORK=$1
N=$2
TIMEOUT=$3
RESULT_FILE=$4
shift 4
WORKER_CMD=("$@")

PORT=${BENCH_REDIS_PORT:-6395}
export BENCH_REDIS_URL="redis://127.0.0.1:${PORT}/0"
PY=${BENCH_PYTHON:-python3}

# cauli-worker embeds the system libpython it was linked against, not
# whichever venv is active in this shell -- so its site-packages (redis,
# msgspec, ...) need to be on PYTHONPATH explicitly. Resolved from
# BENCH_PYTHON so this works for any venv, not just the one used to build it.
# Also: cauli installed editable (pip install -e py/) needs its .pth
# processed by site, which a bare PYTHONPATH entry skips -- put the source
# dir on PYTHONPATH directly instead. Harmless if cauli is a real
# (non-editable) install, and no-op for celery/taskiq which don't need it.
REPO_ROOT="$(cd .. && pwd)"
VENV_SITE_PACKAGES="$("$PY" -c 'import sysconfig; print(sysconfig.get_path("purelib"))')"
export PYTHONPATH="${REPO_ROOT}/py:${VENV_SITE_PACKAGES}${PYTHONPATH:+:$PYTHONPATH}"

if ! redis-cli -p "$PORT" ping > /dev/null 2>&1; then
    echo "[redis] starting dedicated instance on port $PORT" >&2
    redis-server --port "$PORT" --save '' --appendonly no --daemonize yes --logfile /tmp/bench-redis-${PORT}.log
    sleep 0.5
fi

redis-cli -p "$PORT" flushall > /dev/null
redis-cli -p "$PORT" set bench:done 0 > /dev/null

echo "[enqueue] $FRAMEWORK N=$N ${TASK_ARG:+arg=$TASK_ARG}" >&2
"$PY" enqueue.py "$FRAMEWORK" "$N" ${TASK_ARG:+"$TASK_ARG"} >&2

echo "[worker] starting: ${WORKER_CMD[*]}" >&2
setsid "${WORKER_CMD[@]}" > /tmp/bench-worker-${PORT}.log 2>&1 &
WORKER_PID=$!
sleep 0.3

echo "[monitor] polling for drain (timeout ${TIMEOUT}s)" >&2
"$PY" monitor.py "$N" "$TIMEOUT" > "$RESULT_FILE"
cat "$RESULT_FILE" >&2
echo >&2

kill -TERM -- -$WORKER_PID 2>/dev/null
sleep 1
kill -KILL -- -$WORKER_PID 2>/dev/null
wait $WORKER_PID 2>/dev/null
exit 0

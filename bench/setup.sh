#!/usr/bin/env bash
# One-command setup for reproducing this benchmark. Linux only (cauli-worker
# requirement). Builds cauli-worker fresh, creates a dedicated venv, starts
# a dedicated Redis instance, and creates the Postgres table the I/O lanes
# use. Does not touch any Redis/Postgres you already run -- everything here
# is on its own port/role/database.
set -euo pipefail
cd "$(dirname "$0")"
REPO_ROOT="$(cd .. && pwd)"

VENV=${BENCH_VENV:-$HOME/cauli-bench-venv}
REDIS_PORT=${BENCH_REDIS_PORT:-6395}
PG_ROLE=${BENCH_PG_ROLE:-bench}
PG_DB=${BENCH_PG_DB:-bench}

echo "[1/5] building cauli-worker (release, fresh)"
CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$HOME/cauli-target}"
export CARGO_TARGET_DIR
(cd "$REPO_ROOT/worker" && cargo build --release --bin cauli-worker)
echo "cauli-worker commit: $(cd "$REPO_ROOT" && git rev-parse HEAD)"

echo "[2/5] creating venv at $VENV"
python3 -m venv "$VENV"
"$VENV/bin/pip" install -q --upgrade pip
"$VENV/bin/pip" install -q -e "$REPO_ROOT/py"
"$VENV/bin/pip" install -q -r requirements.txt

echo "[3/5] installing uvloop from GitHub main (not the last PyPI release)"
"$VENV/bin/pip" install -q --upgrade --force-reinstall --no-deps \
  'uvloop @ git+https://github.com/MagicStack/uvloop.git'
echo "uvloop version: $("$VENV/bin/python3" -c 'import uvloop; print(uvloop.__version__)')"

echo "[4/5] starting dedicated Redis on port $REDIS_PORT"
if ! redis-cli -p "$REDIS_PORT" ping > /dev/null 2>&1; then
    redis-server --port "$REDIS_PORT" --save '' --appendonly no --daemonize yes \
      --logfile "/tmp/bench-redis-${REDIS_PORT}.log"
    sleep 0.5
fi
redis-cli -p "$REDIS_PORT" ping

echo "[5/5] creating Postgres bench_io table (role/db: $PG_ROLE/$PG_DB, assumed to already exist)"
PGPASSWORD="$PG_ROLE" psql -h 127.0.0.1 -U "$PG_ROLE" -d "$PG_DB" -c "
    CREATE TABLE IF NOT EXISTS bench_io (
        id BIGSERIAL PRIMARY KEY,
        payload TEXT NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );
"

cat <<EOF

Done. To run a measurement:
  export BENCH_PYTHON="$VENV/bin/python3"
  export CAULI_WORKER_BIN="$CARGO_TARGET_DIR/release/cauli-worker"
  ./run.sh cauli_sync 100000 60 /tmp/result.json \\
    "\$CAULI_WORKER_BIN" -A tasks_cauli_sync:app --procs 12 --io-threads 80 \\
    --io-concurrency 80 --redis-url redis://127.0.0.1:$REDIS_PORT/0

Or reproduce the pinned campaign numbers from RESULTS.md:
  python3 campaign.py --reps 3
EOF

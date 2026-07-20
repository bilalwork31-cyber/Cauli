#!/usr/bin/env bash
# One-time Postgres 16 setup for scenario C. Run as root:
#   wsl.exe -d Ubuntu-24.04 -u root -e bash -lc "bash /mnt/d/dev/projects/boring/rupy/bench/campaign/pg_setup.sh"
# Idempotent: ensures cluster 16/main runs, role bench/bench, db bench,
# md5 host auth for bench on 127.0.0.1.
set -e

if ! pg_lsclusters 2>/dev/null | awk '{print $1" "$2}' | grep -q '^16 main$'; then
    echo "[pg_setup] creating cluster 16 main"
    pg_createcluster 16 main
fi
if ! pg_lsclusters | grep -E '^16 +main' | grep -q online; then
    echo "[pg_setup] starting cluster 16 main"
    pg_ctlcluster 16 main start
fi

su postgres -c "psql -tAc \"SELECT 1 FROM pg_roles WHERE rolname='bench'\"" | grep -q 1 \
    || su postgres -c "psql -c \"CREATE ROLE bench LOGIN PASSWORD 'bench'\""
su postgres -c "psql -tAc \"SELECT 1 FROM pg_database WHERE datname='bench'\"" | grep -q 1 \
    || su postgres -c "createdb -O bench bench"

HBA="$(su postgres -c "psql -tAc 'SHOW hba_file'")"
if ! grep -Eq '^host +all +bench +127\.0\.0\.1/32 +(md5|scram-sha-256)' "$HBA"; then
    echo "[pg_setup] adding md5 host line for bench to $HBA"
    # insert before the first generic host line so it wins
    sed -i '0,/^host/s//host    all             bench           127.0.0.1\/32            md5\n&/' "$HBA"
    pg_ctlcluster 16 main reload
fi

echo "[pg_setup] verifying password auth on 127.0.0.1"
PGPASSWORD=bench psql -h 127.0.0.1 -U bench -d bench -tAc "SELECT 'pg_ok'" \
    && echo "[pg_setup] DONE"

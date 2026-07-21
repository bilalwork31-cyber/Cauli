#!/usr/bin/env bash
# One-time Postgres setup for the django_real benchmark. Run as root:
#   wsl.exe -d Ubuntu-24.04 -u root -e bash -lc "bash /mnt/d/dev/projects/boring/rupy/bench/django_real/pg_setup_django.sh"
# Follows bench/campaign/pg_setup.sh conventions. Idempotent:
#   - ensures cluster 16/main is online (created by pg_setup.sh already)
#   - creates database bench_django owned by role bench (role exists from pg_setup.sh)
#   - raises max_connections to 400 (cauli's single process runs a large send
#     thread pool; every Django ORM thread holds one connection) and restarts
#     the cluster if the setting changed.
set -e

if ! pg_lsclusters | grep -E '^16 +main' | grep -q online; then
    echo "[pg_setup_django] starting cluster 16 main"
    pg_ctlcluster 16 main start
fi

su postgres -c "psql -tAc \"SELECT 1 FROM pg_roles WHERE rolname='bench'\"" | grep -q 1 \
    || su postgres -c "psql -c \"CREATE ROLE bench LOGIN PASSWORD 'bench'\""
su postgres -c "psql -tAc \"SELECT 1 FROM pg_database WHERE datname='bench_django'\"" | grep -q 1 \
    || su postgres -c "createdb -O bench bench_django"

CUR="$(su postgres -c "psql -tAc 'SHOW max_connections'")"
if [ "$CUR" -lt 400 ]; then
    echo "[pg_setup_django] max_connections $CUR -> 400 (restart)"
    su postgres -c "psql -c \"ALTER SYSTEM SET max_connections = 400\""
    pg_ctlcluster 16 main restart
fi

# hba line from pg_setup.sh is 'host all bench 127.0.0.1/32 md5' -> covers bench_django
PGPASSWORD=bench psql -h 127.0.0.1 -U bench -d bench_django -tAc "SELECT 'pg_django_ok'" \
    && echo "[pg_setup_django] DONE (max_connections=$(su postgres -c "psql -tAc 'SHOW max_connections'"))"

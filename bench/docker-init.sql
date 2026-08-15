-- Runs automatically on first postgres container start (docker-entrypoint-initdb.d).
-- Mirrors what setup.sh creates for a non-Docker run of this suite.
CREATE TABLE IF NOT EXISTS bench_io (
    id BIGSERIAL PRIMARY KEY,
    payload TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

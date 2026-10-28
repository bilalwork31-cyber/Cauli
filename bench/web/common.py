"""Constants shared by the Django and FastAPI sides of the web benchmark."""

import os

LATENCY_KEY = "web:e2e_ns"
PAYLOAD = "x" * 200
# Connections per FastAPI worker process. 4 processes x 8 keeps the web tier at
# 32 Postgres backends, well under the box's max_connections of 400.
WEB_POOL = int(os.environ.get("BENCH_WEB_POOL", "8"))

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS web_job (
    id BIGSERIAL PRIMARY KEY,
    payload TEXT NOT NULL,
    processed INTEGER NOT NULL DEFAULT 0
)
"""

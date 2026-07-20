"""Central knob table for the campaign benchmark (mimic of production settings).

One CONFIG dict, every knob overridable via an environment variable of the
same name. Values are read at import time, so the runner exports the scenario
env BEFORE starting workers or the driver (systemd-run --scope inherits the
caller's environment).

LEASE_MS deviation (pre-approved, documented): production uses a 10 minute
lease; the bench uses 60s so orphan reclaim is actually exercisable inside a
benchmark-scale run.
"""
import os


def _f(name, default):
    try:
        return float(os.environ[name])
    except (KeyError, ValueError):
        return default


def _i(name, default):
    try:
        return int(os.environ[name])
    except (KeyError, ValueError):
        return default


CONFIG = {
    "BATCH_SIZE": _i("BATCH_SIZE", 50),
    "MAX_BATCHES_PER_DISPATCH": _i("MAX_BATCHES_PER_DISPATCH", 10),
    "APP_MAX_PER_MINUTE": _i("APP_MAX_PER_MINUTE", 200),
    "CONCURRENT_GLOBAL": _i("CONCURRENT_GLOBAL", 15),
    "CONCURRENT_PER_PAGE": _i("CONCURRENT_PER_PAGE", 3),
    "SEND_DELAY": _f("SEND_DELAY", 1.0),
    "MAX_ATTEMPTS": _i("MAX_ATTEMPTS", 3),
    "LEASE_MS": _i("LEASE_MS", 60000),      # production: 600000 (10 min); bench-scaled
    "ERROR_RATE": _f("ERROR_RATE", 0.02),   # consumed by fake_graph.py (own process)
    "N_PAGES": _i("N_PAGES", 20),
    "RETRY_SCALE": _f("RETRY_SCALE", 1.0),
    "BACKOFF_SCALE": _f("BACKOFF_SCALE", 1.0),
    "TICK_SECONDS": _f("TICK_SECONDS", 15),
}

# Fake Graph API base URL (fake_graph.py, port 8078 by default).
GRAPH_URL = os.environ.get("FAKE_GRAPH_URL", "http://127.0.0.1:8078")

# Recipient store redis: same instance as the broker, dedicated db 3.
REDIS_PORT = int(os.environ.get("CAMPAIGN_REDIS_PORT",
                                os.environ.get("BENCH_REDIS_PORT", "6390")))
STORE_DB = int(os.environ.get("CAMPAIGN_STORE_DB", "3"))

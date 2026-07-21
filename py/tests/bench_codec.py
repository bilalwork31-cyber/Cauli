"""Microbench: encode+decode ops/s of a representative envelope, per backend.

Not a pytest file (no test_ prefix). Run directly:

    python tests/bench_codec.py                 # ambient backend
    CAULI_DISABLE_MSGSPEC=1 python tests/bench_codec.py

or let it fork itself to print both backends side by side (default).
"""

from __future__ import annotations

import os
import subprocess
import sys
import time

ENVELOPE = {
    "v": 1,
    "id": "d41d8cd98f00b204e9800998ecf8427e",
    "task": "myapp.tasks.send_email",
    "args": [42, "a@b.com", {"subject": "hello", "cc": ["x@y.z", "p@q.r"]}],
    "kwargs": {"retry": True, "priority": 3, "note": "résumé attached"},
    "queue": "default",
    "kind": "io",
    "retries": 0,
    "max_retries": 3,
    "backoff_base_ms": 500,
    "backoff_factor": 2.0,
    "backoff_max_ms": 60000,
    "jitter": True,
    "timeout_ms": 300000,
    "soft_timeout_ms": None,
    "idempotency_key": None,
    "store_result": True,
    "enqueued_at": 1721471234567,
    "not_before": None,
}

N = 200_000


def bench_one() -> None:
    from cauli import _codec

    encoded = _codec.encode(ENVELOPE)
    assert _codec.decode(encoded) == ENVELOPE

    # warmup
    for _ in range(5_000):
        _codec.decode(_codec.encode(ENVELOPE))

    t0 = time.perf_counter()
    for _ in range(N):
        _codec.encode(ENVELOPE)
    t_enc = time.perf_counter() - t0

    t0 = time.perf_counter()
    for _ in range(N):
        _codec.decode(encoded)
    t_dec = time.perf_counter() - t0

    t0 = time.perf_counter()
    for _ in range(N):
        _codec.decode(_codec.encode(ENVELOPE))
    t_rt = time.perf_counter() - t0

    print(
        f"backend={_codec.backend:<7} encode={N / t_enc:>12,.0f} ops/s  "
        f"decode={N / t_dec:>12,.0f} ops/s  roundtrip={N / t_rt:>12,.0f} ops/s"
    )


def main() -> None:
    if len(sys.argv) > 1 and sys.argv[1] == "--one":
        bench_one()
        return
    for disable in ("", "1"):
        env = dict(os.environ)
        if disable:
            env["CAULI_DISABLE_MSGSPEC"] = disable
        else:
            env.pop("CAULI_DISABLE_MSGSPEC", None)
        subprocess.run([sys.executable, os.path.abspath(__file__), "--one"], env=env)


if __name__ == "__main__":
    main()

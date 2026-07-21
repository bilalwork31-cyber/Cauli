"""Calibrate CPU_ITER for common.cpu_call so one call takes ~50ms on THIS machine.

Prints ms/call and tasks/sec for candidate iteration counts, then suggests the
iteration count for the 50ms target. Bake the suggested value into
common.CPU_ITER (with the measured ms in the comment).

Run:  python calibrate.py [target_ms]
"""

import hashlib
import statistics
import sys
import time

TARGET_MS = float(sys.argv[1]) if len(sys.argv) > 1 else 50.0

CANDIDATES = [20_000, 40_000, 60_000, 80_000, 100_000, 120_000, 150_000, 200_000]


def time_iters(n: int, reps: int = 9) -> float:
    """Median wall ms for one pbkdf2_hmac call with n iterations."""
    samples = []
    for _ in range(reps):
        t0 = time.perf_counter()
        hashlib.pbkdf2_hmac("sha256", b"password", b"salt", n)
        samples.append((time.perf_counter() - t0) * 1000.0)
    return statistics.median(samples)


def main() -> None:
    # warm up (first OpenSSL call can be slower)
    hashlib.pbkdf2_hmac("sha256", b"password", b"salt", 10_000)

    print(f"{'ITER':>9}  {'ms/call':>9}  {'tasks/sec (1 core)':>19}")
    per_ms = []
    for n in CANDIDATES:
        ms = time_iters(n)
        print(f"{n:>9}  {ms:>9.2f}  {1000.0 / ms:>19.1f}")
        per_ms.append(n / ms)

    # iterations per ms is essentially linear; use the median rate
    rate = statistics.median(per_ms)
    suggested = int(round(rate * TARGET_MS / 1000.0) * 1000)
    measured = time_iters(suggested, reps=11)
    print()
    print(f"target {TARGET_MS:.0f} ms  ->  suggested CPU_ITER = {suggested}")
    print(
        f"verification: {suggested} iterations = {measured:.1f} ms median "
        f"({1000.0 / measured:.1f} tasks/sec/core)"
    )
    print(f"bake into common.py:  CPU_ITER = {suggested}  # {measured:.1f} ms measured")


if __name__ == "__main__":
    main()

"""Soak test: sustained load for a long duration, watching for RSS/PSS
growth that would indicate a reference leak. Not a peak-throughput test --
this suite's producer tops out around 4-6k enqueues/s in a tight loop
(measured elsewhere in RESULTS.md), well below cauli's peak drain rate, so
the soak runs at a rate comfortably inside that ceiling rather than at 70%
of drain-rate throughput. The point is sustained execution over millions of
tasks and many hours, not saturating the worker.

Runs two things concurrently until `duration_s` elapses or it's killed:
1. A steady open-loop enqueue loop (reuses latency_producer's scheduling,
   ignores the timestamp it produces -- this lane doesn't need latency).
2. A memory sampler: PSS (see memory_report.py for why PSS not RSS) summed
   across matching worker processes, appended to a CSV every sample_interval_s.

Designed to survive independently of the session that launched it: run via
setsid/nohup, check progress by tailing the CSV. Does not manage the worker
process itself -- start that separately first.

CLI: soak_driver.py <lane> <rate> <duration_s> <sample_interval_s> <pgrep_pattern> <csv_path>
  lane: cauli_async (only lane wired up currently; same pattern extends to others)
"""

import csv
import subprocess
import sys
import threading
import time

from common import REDIS_URL
from latency_producer import run as producer_run


def pss_kb(pid):
    try:
        with open(f"/proc/{pid}/smaps_rollup") as f:
            for line in f:
                if line.startswith("Pss:"):
                    return int(line.split()[1])
    except (FileNotFoundError, ProcessLookupError):
        return 0
    return 0


def sample_memory_loop(pgrep_pattern, csv_path, interval_s, stop_event):
    with open(csv_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["elapsed_s", "unix_ts", "n_processes", "total_pss_mib"])
        f.flush()
        t0 = time.monotonic()
        while not stop_event.is_set():
            out = subprocess.run(["pgrep", "-x", pgrep_pattern], capture_output=True, text=True)
            pids = [int(p) for p in out.stdout.split()]
            total_kb = sum(pss_kb(p) for p in pids)
            writer.writerow([f"{time.monotonic() - t0:.1f}", f"{time.time():.0f}", len(pids), f"{total_kb / 1024:.1f}"])
            f.flush()
            stop_event.wait(interval_s)


def make_enqueue_fn(lane):
    if lane == "cauli_async":
        from tasks_cauli_async import noop

        return lambda ts: noop.delay()
    raise SystemExit(f"unknown lane {lane!r}")


def main():
    lane = sys.argv[1]
    rate = float(sys.argv[2])
    duration_s = float(sys.argv[3])
    sample_interval_s = float(sys.argv[4])
    pgrep_pattern = sys.argv[5]
    csv_path = sys.argv[6]

    enqueue_fn = make_enqueue_fn(lane)

    stop_event = threading.Event()
    mem_thread = threading.Thread(
        target=sample_memory_loop, args=(pgrep_pattern, csv_path, sample_interval_s, stop_event)
    )
    mem_thread.start()

    print(f"[soak] lane={lane} rate={rate}/s duration={duration_s}s sampling every {sample_interval_s}s to {csv_path}", flush=True)
    t_start = time.time()
    stats = producer_run(enqueue_fn, rate, duration_s)
    elapsed = time.time() - t_start

    stop_event.set()
    mem_thread.join()

    print(f"[soak] done: sent {stats['sent']} in {elapsed:.1f}s, max producer drift {stats['max_drift_s']*1000:.1f}ms", flush=True)


if __name__ == "__main__":
    main()

"""Campaign benchmark driver (one scenario = one invocation).

Flow:
  1. flush store db (3), seed N recipients round-robin over --pages pages,
     seed 500 webhook inbox rows, set the bg:run flag,
  2. record t0, enqueue ONE bgfill.ghost_job (it re-enqueues itself while
     bg:run is set), then every --tick seconds enqueue campaign.dispatch AND
     webhook.drain - identical beat replacement for both stacks (production
     uses celery beat; here the driver ticks so both stacks get the exact
     same schedule),
  3. poll the store every --poll seconds until every recipient is terminal
     (sent/failed/skipped), or --timeout (-> status=stalled with counts), or
     the worker dies (-> status=worker_dead after a 10s grace),
  4. collect per-recipient rows: e2e latency percentiles (enqueued_first_ms
     -> sent_at_ms, includes queue wait by design), sends/s timeline in 10s
     buckets, attempts histogram, retry counters, DUPLICATES (must be 0),
     bg-fill counters, cgroup memory peak/oom (MemorySampler imported from
     bench/driver.py, unmodified) + psutil worker-tree RSS fallback.

Output: results/{scenario}.json + one human `[campaign]` summary line.
"""
import argparse
import importlib
import json
import os
import sys
import time
from datetime import datetime, timezone

CAMP_DIR = os.path.dirname(os.path.abspath(__file__))
BENCH_DIR = os.path.dirname(CAMP_DIR)
for _p in (CAMP_DIR, BENCH_DIR):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import driver as bench_driver   # bench/driver.py: sampler + percentile code

import bgfill_common
import campconfig
import store

RESULTS_DIR = os.path.join(CAMP_DIR, "results")
TERMINAL = ("sent", "failed", "skipped")


def _task_module(stack):
    return importlib.import_module(
        "campaign_celery" if stack == "celery" else "campaign_rupy")


def _tick(mod, cid):
    mod.dispatch.apply_async(args=(cid,), queue="dispatch")
    mod.webhook_drain.apply_async(queue="webhook_ingest")


def main():
    ap = argparse.ArgumentParser(description="campaign benchmark driver")
    ap.add_argument("--stack", required=True, choices=["celery", "rupy"])
    ap.add_argument("--scenario", required=True)
    ap.add_argument("--n", type=int, default=300)
    ap.add_argument("--pages", type=int, default=campconfig.CONFIG["N_PAGES"])
    ap.add_argument("--tick", type=float,
                    default=campconfig.CONFIG["TICK_SECONDS"])
    ap.add_argument("--timeout", type=float, default=1800.0)
    ap.add_argument("--poll", type=float, default=2.0)
    ap.add_argument("--campaign-id", default="c1")
    ap.add_argument("--cgroup-path", default=None)
    ap.add_argument("--pid", type=int, default=None)
    args = ap.parse_args()

    cid = args.campaign_id
    os.makedirs(RESULTS_DIR, exist_ok=True)
    out_path = os.path.join(RESULTS_DIR, f"{args.scenario}.json")
    mod = _task_module(args.stack)

    r3 = store.conn()
    r3.ping()
    store.flush_store_db()
    store.seed_campaign(cid, args.n, args.pages)
    bgfill_common.seed_webhook_inbox(500)
    store.set_bg_active(True)
    print(f"[campaign] seeded n={args.n} pages={args.pages} webhook_rows=500",
          file=sys.stderr)

    sampler = bench_driver.MemorySampler(args.cgroup_path, args.pid, 0.25)
    sampler.start()

    t0 = time.time()
    mod.ghost_job.apply_async(queue="backfill_heavy")
    next_tick = t0
    ticks = 0
    status = "ok"
    dead_since = None
    counts = {}
    last_progress = 0.0
    while True:
        now = time.time()
        if now >= next_tick:
            _tick(mod, cid)
            ticks += 1
            next_tick += args.tick
        counts = store.count_by_status(cid)
        done = sum(counts.get(s, 0) for s in TERMINAL)
        if done >= args.n:
            break
        if now - last_progress >= 10.0:
            last_progress = now
            print(f"[campaign] t={now - t0:6.1f}s ticks={ticks} {counts}",
                  file=sys.stderr)
        if now - t0 >= args.timeout:
            status = "stalled"
            break
        alive = bench_driver.worker_alive(args.pid)
        if alive is False:
            if dead_since is None:
                dead_since = now
                print("[campaign] worker gone; 10s grace for stragglers",
                      file=sys.stderr)
            elif now - dead_since > 10.0:
                status = "worker_dead"
                break
        else:
            dead_since = None
        time.sleep(max(0.05, min(args.poll, next_tick - time.time())))
    t_done = time.time()
    store.set_bg_active(False)   # stops the ghost re-enqueue chain

    sampler.stop()
    sampler.join(timeout=2)

    # ---------------- final collection ----------------
    rows = store.collect_rows(cid)
    lat = [float(r["sent_at_ms"] - r["enqueued_first_ms"]) for r in rows
           if r["status"] == "sent" and r["sent_at_ms"] > 0
           and r["enqueued_first_ms"] > 0]
    lat_summary = bench_driver.summarize_latencies(lat)

    t0_ms = t0 * 1000.0
    buckets = {}
    for r in rows:
        if r["status"] == "sent" and r["sent_at_ms"] > 0:
            b = int((r["sent_at_ms"] - t0_ms) // 10000)
            buckets[b] = buckets.get(b, 0) + 1
    timeline = [buckets.get(i, 0) for i in range(max(buckets) + 1)] \
        if buckets else []

    att_hist = {}
    for r in rows:
        att_hist[r["attempts"]] = att_hist.get(r["attempts"], 0) + 1
    dup = store.duplicates(cid)
    retries = {
        "http_retries": store.get_counter(cid, "http_retries"),
        "rows_with_multiple_attempts":
            sum(v for k, v in att_hist.items() if k > 1),
        "attempts_histogram": {str(k): v for k, v in sorted(att_hist.items())},
        "lock_skips": store.get_counter(cid, "lock_skips"),
        "already_sent_repairs": store.get_counter(cid, "already_sent"),
        "claimed_total": store.get_counter(cid, "claimed_total"),
    }
    bg = bgfill_common.webhook_counts()
    counts = store.count_by_status(cid)
    mem = sampler.summary()
    drain_s = t_done - t0

    blob = {
        "scenario": args.scenario,
        "stack": args.stack,
        "rupy_variant": os.environ.get("RUPY_VARIANT", "sync")
            if args.stack == "rupy" else None,
        "n": args.n,
        "pages": args.pages,
        "status": status,
        "started_at": datetime.fromtimestamp(t0, timezone.utc).isoformat(),
        "config": dict(campconfig.CONFIG),
        "args": vars(args).copy(),
        "drain_wall_s": round(drain_s, 3),
        "ticks": ticks,
        "counts": counts,
        "invariants": {"duplicates": dup, "duplicates_zero": dup == 0},
        "retries": retries,
        "latency_e2e_ms": lat_summary,
        "sends_per_10s": timeline,
        "bgfill": bg,
        "memory": {**mem,
                   "worker_alive_at_end": bench_driver.worker_alive(args.pid)},
        "samples": {
            "memory": sampler.samples,
            "latencies_ms": [round(x, 1) for x in lat],
        },
    }
    with open(out_path, "w") as f:
        json.dump(blob, f, indent=1)

    sent = counts.get("sent", 0)
    peak = mem.get("memory_peak_file_bytes") or sampler.peak_cgroup \
        or sampler.peak_rss or 0
    sends_ps = sent / max(drain_s, 1e-9)
    print(f"[campaign] {args.scenario} stack={args.stack} status={status} "
          f"n={args.n} drain_s={drain_s:.1f} sends_ps={sends_ps:.1f} "
          f"sent={sent} failed={counts.get('failed', 0)} "
          f"skipped={counts.get('skipped', 0)} dup={dup} "
          f"http_retries={retries['http_retries']} "
          f"p50={lat_summary['p50']}ms p95={lat_summary['p95']}ms "
          f"p99={lat_summary['p99']}ms "
          f"ghost_calls={bg['ghost_calls']} "
          f"webhook_processed={bg['webhook_processed']} "
          f"peak_mem={peak / 1048576:.1f}MiB oom={mem.get('oom_kills')}")
    return 0 if (status == "ok" and dup == 0) else 3


if __name__ == "__main__":
    sys.exit(main())

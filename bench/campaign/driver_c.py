# ruff: noqa: E402 -- imports intentionally follow sys.path/env setup
"""Scenario C driver: 100-campaign full-throughput chaos + stage-2 persist.

Seeds C campaigns (sizes uniform in [min_n, max_n], seeded RNG) with GLOBALLY
UNIQUE recipient ids `{cid}r{i:06d}` (pg PK is recipient_id alone) spread
round-robin over a GLOBAL pool of --pages pages via one running cursor.
Ticks campaign.dispatch for EVERY campaign each --tick seconds, webhook.drain
once per tick, one ghost_job at start, TWO persist.drain chains at start.

Cheap progress polling (the 450k-row full scan would perturb the system):
non-terminal ~= ZCARD(due) + ZCARD(leased) per campaign (terminal rows are in
neither zset). The exact full status pass runs only in the endgame.
Completion = all recipients terminal AND results_raw empty AND
pg_count == sent. Timeout -> status=stalled with counts (a result, not a
crash).

JSON: latency + persist-lag percentiles, sends/s + persists/s 10s timelines,
attempts histogram, counters, validations (pg_count==sent, duplicates==0),
memory (1s sampling). Raw per-recipient arrays are NOT stored (450k rows).
"""

import argparse
import importlib
import json
import os
import random
import sys
import time
from datetime import datetime, timezone

CAMP_DIR = os.path.dirname(os.path.abspath(__file__))
BENCH_DIR = os.path.dirname(CAMP_DIR)
for _p in (CAMP_DIR, BENCH_DIR):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import driver as bench_driver

import bgfill_common
import campconfig
import persist_common
import store

RESULTS_DIR = os.path.join(CAMP_DIR, "results")
TERMINAL = ("sent", "failed", "skipped")


def seed_all(n_campaigns, min_n, max_n, pages, seed):
    rng = random.Random(seed)
    sizes = {f"c{i:03d}": rng.randint(min_n, max_n) for i in range(n_campaigns)}
    r = store.conn()
    now = store.now_ms()
    cursor = 0
    pipe = r.pipeline(transaction=False)
    for cid, n in sizes.items():
        for i in range(n):
            rid = f"{cid}r{i:06d}"  # globally unique recipient id
            page = f"p{cursor % pages}"
            cursor += 1
            pipe.hset(
                store.k_hash(cid, rid),
                mapping={
                    "status": "pending",
                    "attempts": 0,
                    "page_id": page,
                    "next_due_ms": now,
                    "lease_until_ms": 0,
                    "sent_at_ms": 0,
                    "enqueued_first_ms": 0,
                },
            )
            pipe.zadd(store.k_due(cid), {rid: now})
            pipe.sadd(store.k_ids(cid), rid)
            if len(pipe) >= 3000:
                pipe.execute()
                pipe = r.pipeline(transaction=False)
        r.hset(
            store.k_meta(cid),
            mapping={"total": n, "n_pages": pages, "seeded_at_ms": now},
        )
        r.sadd(store.ACTIVE_SET, cid)
    pipe.execute()
    return sizes


def nonterminal_total(cids):
    pipe = store.conn().pipeline(transaction=False)
    for cid in cids:
        pipe.zcard(store.k_due(cid))
        pipe.zcard(store.k_leased(cid))
    return sum(pipe.execute())


def exact_counts(cids):
    total = {}
    for cid in cids:
        for st, n in store.count_by_status(cid).items():
            total[st] = total.get(st, 0) + n
    return total


def aggregate_rows(cids, t0_ms):
    """Stream per-campaign rows; return latencies summary, timeline, hists."""
    lat = []
    att_hist = {}
    counts = {}
    buckets = {}
    for cid in cids:
        for row in store.collect_rows(cid):
            counts[row["status"]] = counts.get(row["status"], 0) + 1
            att_hist[row["attempts"]] = att_hist.get(row["attempts"], 0) + 1
            if (
                row["status"] == "sent"
                and row["sent_at_ms"] > 0
                and row["enqueued_first_ms"] > 0
            ):
                lat.append(float(row["sent_at_ms"] - row["enqueued_first_ms"]))
                b = int((row["sent_at_ms"] - t0_ms) // 10000)
                buckets[b] = buckets.get(b, 0) + 1
    timeline = [buckets.get(i, 0) for i in range(max(buckets) + 1)] if buckets else []
    return lat, att_hist, counts, timeline


def sum_counter(cids, name):
    return sum(store.get_counter(cid, name) for cid in cids)


def main():
    ap = argparse.ArgumentParser(description="scenario C chaos driver")
    ap.add_argument("--stack", required=True, choices=["celery", "cauli"])
    ap.add_argument("--scenario", required=True)
    ap.add_argument("--campaigns", type=int, default=100)
    ap.add_argument("--min-n", type=int, default=4000)
    ap.add_argument("--max-n", type=int, default=5000)
    ap.add_argument("--pages", type=int, default=200)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--tick", type=float, default=5.0)
    ap.add_argument("--timeout", type=float, default=3600.0)
    ap.add_argument("--poll", type=float, default=5.0)
    ap.add_argument("--cgroup-path", default=None)
    ap.add_argument("--pid", type=int, default=None)
    args = ap.parse_args()

    os.makedirs(RESULTS_DIR, exist_ok=True)
    out_path = os.path.join(RESULTS_DIR, f"{args.scenario}.json")
    mod = importlib.import_module(
        "campaign_celery_c" if args.stack == "celery" else "campaign_cauli_c"
    )

    store.conn().ping()
    persist_common.ensure_schema()
    persist_common.truncate()
    store.flush_store_db()
    persist_common.results_conn().flushdb()

    seed_t = time.time()
    sizes = seed_all(args.campaigns, args.min_n, args.max_n, args.pages, args.seed)
    cids = list(sizes)
    total = sum(sizes.values())
    bgfill_common.seed_webhook_inbox(500)
    store.set_bg_active(True)
    print(
        f"[c] seeded campaigns={args.campaigns} recipients={total} "
        f"pages={args.pages} seed={args.seed} in {time.time() - seed_t:.1f}s",
        file=sys.stderr,
    )

    sampler = bench_driver.MemorySampler(args.cgroup_path, args.pid, 1.0)
    sampler.start()

    t0 = time.time()
    mod.ghost_job.apply_async(queue="backfill_heavy")
    mod.persist_drain.apply_async(queue="persist")
    mod.persist_drain.apply_async(queue="persist")
    next_tick = t0
    ticks = 0
    status = "ok"
    dead_since = None
    prev_done, prev_t = 0, t0
    last_log = 0.0
    exact = None
    exact_ts = 0.0
    while True:
        now = time.time()
        if now >= next_tick:
            for cid in cids:
                mod.dispatch.apply_async(args=(cid,), queue="dispatch")
            mod.webhook_drain.apply_async(queue="webhook_ingest")
            ticks += 1
            next_tick += args.tick
        nt = nonterminal_total(cids)
        backlog = persist_common.backlog_len()
        done_est = total - nt
        if nt == 0 and backlog == 0:
            if exact is None or now - exact_ts > 10.0:
                exact = exact_counts(cids)
                exact_ts = now
            pgc = persist_common.pg_count()
            if sum(exact.get(s, 0) for s in TERMINAL) >= total and pgc >= exact.get(
                "sent", 0
            ):
                break
        if now - last_log >= 10.0:
            rate = (done_est - prev_done) / max(now - prev_t, 1e-9)
            prev_done, prev_t = done_est, now
            print(
                f"[c] t={now - t0:7.1f}s done={done_est}/{total} "
                f"rate={rate:6.1f}/s pg={persist_common.pg_count()} "
                f"backlog={backlog} nonterminal={nt}",
                file=sys.stderr,
            )
            last_log = now
        if now - t0 >= args.timeout:
            status = "stalled"
            break
        alive = bench_driver.worker_alive(args.pid)
        if alive is False:
            if dead_since is None:
                dead_since = now
                print("[c] worker gone; 10s grace", file=sys.stderr)
            elif now - dead_since > 10.0:
                status = "worker_dead"
                break
        else:
            dead_since = None
        time.sleep(max(0.05, min(args.poll, next_tick - time.time())))
    t_done = time.time()
    store.set_bg_active(False)

    sampler.stop()
    sampler.join(timeout=2)

    print("[c] run ended; aggregating final state", file=sys.stderr)
    lat, att_hist, counts, send_tl = aggregate_rows(cids, t0 * 1000.0)
    lat_summary = bench_driver.summarize_latencies(lat)
    lags = persist_common.collect_lags()
    lag_summary = bench_driver.summarize_latencies(lags)
    ptl_abs = persist_common.persist_timeline()
    base = int((t0 * 1000.0) // 10000)
    ptl = {}
    for k, v in ptl_abs.items():
        ptl[max(0, k - base)] = ptl.get(max(0, k - base), 0) + v
    persist_tl = [ptl.get(i, 0) for i in range(max(ptl) + 1)] if ptl else []
    dup = sum(store.duplicates(cid) for cid in cids)
    sent = counts.get("sent", 0)
    pgc = persist_common.pg_count()
    backlog = persist_common.backlog_len()
    retries = {
        "http_retries": sum_counter(cids, "http_retries"),
        "rows_with_multiple_attempts": sum(v for k, v in att_hist.items() if k > 1),
        "attempts_histogram": {str(k): v for k, v in sorted(att_hist.items())},
        "lock_skips": sum_counter(cids, "lock_skips"),
        "already_sent_repairs": sum_counter(cids, "already_sent"),
        "claimed_total": sum_counter(cids, "claimed_total"),
    }
    mem = sampler.summary()
    drain_s = t_done - t0
    validations = {
        "pg_count": pgc,
        "sent": sent,
        "pg_matches_sent": pgc == sent,
        "results_raw_left": backlog,
        "duplicates": dup,
        "duplicates_zero": dup == 0,
        "failed": counts.get("failed", 0),
        "skipped": counts.get("skipped", 0),
    }

    blob = {
        "scenario": args.scenario,
        "stack": args.stack,
        "cauli_variant": os.environ.get("CAULI_VARIANT", "async")
        if args.stack == "cauli"
        else None,
        "campaigns": args.campaigns,
        "n_total": total,
        "pages": args.pages,
        "seed": args.seed,
        "status": status,
        "started_at": datetime.fromtimestamp(t0, timezone.utc).isoformat(),
        "config": dict(campconfig.CONFIG),
        "args": vars(args).copy(),
        "campaign_sizes": sizes,
        "drain_wall_s": round(drain_s, 3),
        "ticks": ticks,
        "counts": counts,
        "validations": validations,
        "retries": retries,
        "latency_e2e_ms": lat_summary,
        "persist_lag_ms": lag_summary,
        "sends_per_10s": send_tl,
        "persists_per_10s": persist_tl,
        "bgfill": bgfill_common.webhook_counts(),
        "memory": {**mem, "worker_alive_at_end": bench_driver.worker_alive(args.pid)},
        "samples": {"memory": sampler.samples},
    }
    with open(out_path, "w") as f:
        json.dump(blob, f, indent=1)

    peak = (
        mem.get("memory_peak_file_bytes")
        or sampler.peak_cgroup
        or sampler.peak_rss
        or 0
    )
    ok = status == "ok" and validations["pg_matches_sent"] and dup == 0
    print(
        f"[c] {args.scenario} stack={args.stack} status={status} "
        f"campaigns={args.campaigns} n={total} drain_s={drain_s:.1f} "
        f"sends_ps={sent / max(drain_s, 1e-9):.1f} sent={sent} "
        f"failed={validations['failed']} skipped={validations['skipped']} "
        f"dup={dup} pg={pgc} pg_match={validations['pg_matches_sent']} "
        f"http_retries={retries['http_retries']} "
        f"p50={lat_summary['p50']}ms p95={lat_summary['p95']}ms "
        f"p99={lat_summary['p99']}ms lag_p50={lag_summary['p50']}ms "
        f"lag_p95={lag_summary['p95']}ms "
        f"peak_mem={peak / 1048576:.1f}MiB oom={mem.get('oom_kills')}"
    )
    return 0 if ok else 3


if __name__ == "__main__":
    sys.exit(main())

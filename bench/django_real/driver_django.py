# ruff: noqa: E402 -- imports intentionally follow sys.path/env setup
"""django_real driver: seed / run / finalize phases (runner orchestrates).

seed:     reset the bench_django tables, bulk_create campaigns + recipients
          (fresh schema per stack run), seed the webhook inbox, clear the
          bench redis dbs (store counters db3, results buffer db4).
run:      time-boxed chaos. Ticks campaign.dispatch for EVERY campaign each
          --tick seconds and webhook.drain once per tick, one ghost_job chain
          + two persist.drain chains at start (same shape as driver_c). After
          --warmup seconds records a marker timestamp; EXACTLY --window
          seconds later stops enqueuing and exits with status timebox_ok.
          Samples cgroup memory (1s) and per-process RSS (5s; USS/PSS pass
          every 60s) the whole time. Writes results/<scenario>.json.
finalize: runs AFTER the runner stopped the workers. Drains any leftover
          results_raw backlog into SendLog (the persister's crash-recovery
          contract: at-least-once + unique-key dedup), then measures from
          Postgres: in-window sends, sends/s, wait percentiles, timelines,
          attempts histogram, duplicate + integrity validations, bg-fill
          liveness. Merges into the same JSON.

Window accounting is timestamp-based (sent_at_ms in [marker, marker+window)),
so warmup sends never count and post-window stragglers never count.
"""

import argparse
import importlib
import json
import os
import sys
import time
from datetime import datetime, timezone

DJ_DIR = os.path.dirname(os.path.abspath(__file__))
BENCH_DIR = os.path.dirname(DJ_DIR)
CAMP_DIR = os.path.join(BENCH_DIR, "campaign")
ROOT_DIR = os.path.dirname(BENCH_DIR)
for _p in (DJ_DIR, CAMP_DIR, BENCH_DIR, os.path.join(ROOT_DIR, "py")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import django_boot  # noqa: F401

from django.db import connection
from django.db.models import Count

import driver as bench_driver  # bench/driver.py (sampler, percentiles)

import campconfig
import persist_common
import store as redis_store
import tasks_shared as ts
from campaigns.models import BackfillJob, Campaign, Recipient, SendLog, WebhookInboxItem

RESULTS_DIR = os.path.join(DJ_DIR, "results")

TABLES = [
    "campaigns_sendlog",
    "campaigns_recipient",
    "campaigns_webhookinboxitem",
    "campaigns_backfilljob",
    "campaigns_campaign",
]


def log(msg):
    print(f"[dj] {msg}", file=sys.stderr, flush=True)


def out_path(scenario):
    return os.path.join(RESULTS_DIR, f"{scenario}.json")


def cids():
    return list(Campaign.objects.order_by("cid").values_list("cid", flat=True))


# ------------------------------------------------------------------- seed ----
def cmd_seed(args):
    t0 = time.time()
    with connection.cursor() as cur:
        cur.execute("TRUNCATE %s RESTART IDENTITY CASCADE" % ", ".join(TABLES))
    redis_store.flush_store_db()
    persist_common.results_conn().flushdb()

    now = redis_store.now_ms()
    Campaign.objects.bulk_create(
        [
            Campaign(
                cid=f"c{i:03d}",
                name=f"campaign {i}",
                status="active",
                total_recipients=args.per,
                n_pages=args.pages,
                seeded_at_ms=now,
            )
            for i in range(args.campaigns)
        ]
    )
    cursor = 0
    buf = []
    made = 0
    for i in range(args.campaigns):
        cid = f"c{i:03d}"
        for j in range(args.per):
            buf.append(
                Recipient(
                    rid=f"{cid}r{j:06d}",
                    campaign_id=cid,
                    page_id=f"p{cursor % args.pages}",
                    status="pending",
                    attempts=0,
                    next_due_ms=now,
                    lease_until_ms=0,
                    enqueued_first_ms=0,
                    sent_at_ms=0,
                    sent_flag=False,
                    lock_until_ms=0,
                    last_attempt_ms=0,
                )
            )
            cursor += 1
            if len(buf) >= 5000:
                Recipient.objects.bulk_create(buf)
                made += len(buf)
                buf = []
        if made and made % 100000 < 5000:
            log(f"seeded {made} recipients...")
    if buf:
        Recipient.objects.bulk_create(buf)
        made += len(buf)
    ts.seed_webhook_inbox(500)
    with connection.cursor() as cur:
        cur.execute("ANALYZE campaigns_recipient")
    log(
        f"seed done: campaigns={args.campaigns} recipients={made} "
        f"pages={args.pages} webhook_inbox=500 in {time.time() - t0:.1f}s"
    )
    return 0


# ---------------------------------------------------------------- proc RSS ---
class ProcSampler:
    """Per-process RSS from the worker cgroup: the Celery fork-weight story."""

    def __init__(self, cgroup_path, interval=5.0, full_every=60.0):
        self.cgroup_path = cgroup_path
        self.interval = interval
        self.full_every = full_every
        self.procs = {}  # pid -> {"cmd", "peak_rss", "last_rss", ...}
        self._stop = False
        self._thread = None
        try:
            import psutil

            self._ps = psutil
        except ImportError:
            self._ps = None

    def _pids(self):
        try:
            with open(os.path.join(self.cgroup_path, "cgroup.procs")) as f:
                return [int(x) for x in f.read().split()]
        except (OSError, ValueError):
            return []

    @staticmethod
    def _smaps_private(pid):
        """Private_Clean + Private_Dirty (+ Private_Hugetlb) from
        /proc/PID/smaps_rollup, bytes: the pages this process does NOT share,
        i.e. the copy-on-write / gc.freeze proof for forked children."""
        try:
            total = 0
            with open(f"/proc/{pid}/smaps_rollup") as f:
                for line in f:
                    if line.startswith("Private_"):
                        total += int(line.split()[1])  # kB
            return total * 1024
        except (OSError, ValueError, IndexError):
            return None

    def _sample(self, full):
        for pid in self._pids():
            try:
                p = self._ps.Process(pid)
                rss = p.memory_info().rss
                d = self.procs.get(pid)
                if d is None:
                    cmd = " ".join(p.cmdline())[:160]
                    d = {
                        "pid": pid,
                        "cmd": cmd,
                        "peak_rss": 0,
                        "last_rss": 0,
                        "uss": None,
                        "pss": None,
                        "private": None,
                        "peak_private": 0,
                    }
                    self.procs[pid] = d
                d["last_rss"] = rss
                d["peak_rss"] = max(d["peak_rss"], rss)
                if full:
                    try:
                        mfi = p.memory_full_info()
                        d["uss"] = mfi.uss
                        d["pss"] = getattr(mfi, "pss", None)
                    except Exception:
                        pass
                    priv = self._smaps_private(pid)
                    if priv is not None:
                        d["private"] = priv
                        d["peak_private"] = max(d["peak_private"], priv)
            except Exception:
                continue

    def run_bg(self):
        import threading

        def loop():
            last_full = 0.0
            while not self._stop:
                now = time.time()
                full = now - last_full >= self.full_every
                if full:
                    last_full = now
                self._sample(full)
                time.sleep(self.interval)
            self._sample(True)

        self._thread = threading.Thread(target=loop, daemon=True)
        self._thread.start()

    def stop(self):
        self._stop = True
        if self._thread:
            self._thread.join(timeout=10)

    def summary(self):
        rows = sorted(self.procs.values(), key=lambda d: -d["peak_rss"])
        return {
            "n_processes_seen": len(rows),
            "total_peak_rss_bytes": sum(d["peak_rss"] for d in rows),
            "total_last_uss_bytes": sum(d["uss"] for d in rows if d["uss"]) or None,
            "total_last_private_bytes": sum(d["private"] for d in rows if d["private"])
            or None,
            "processes": rows,
        }


# -------------------------------------------------------------------- run ----
def cmd_run(args):
    os.makedirs(RESULTS_DIR, exist_ok=True)
    mod = importlib.import_module(
        "celery_app_django" if args.stack == "celery" else "cauli_app_django"
    )
    all_cids = cids()
    total = sum(Campaign.objects.values_list("total_recipients", flat=True))
    connection.close()  # no ORM traffic from the driver during the run
    redis_store.conn().ping()

    sampler = bench_driver.MemorySampler(args.cgroup_path, args.pid, 1.0)
    sampler.start()
    procs = ProcSampler(args.cgroup_path) if args.cgroup_path else None
    if procs and procs._ps:
        procs.run_bg()

    redis_store.set_bg_active(True)
    t0 = time.time()
    mod.ghost_job.apply_async(queue="backfill_heavy")
    n_persist = int(os.environ.get("DJ_PERSIST_TASKS", "2"))
    for _ in range(n_persist):
        mod.persist_drain.apply_async(queue="persist")
    marker_ms = None
    end_ms = None
    next_tick = t0
    ticks = 0
    status = "timebox_ok"
    dead_since = None
    last_log = 0.0
    prev_done, prev_t = 0, t0
    log(
        f"run start stack={args.stack} layer={ts.DATA_LAYER} "
        f"campaigns={len(all_cids)} n={total} persist_tasks={n_persist} "
        f"warmup={args.warmup}s window={args.window}s tick={args.tick}s"
    )
    while True:
        now = time.time()
        if marker_ms is None and now - t0 >= args.warmup:
            marker_ms = int(now * 1000)
            log(f"warmup over, measured window starts (marker={marker_ms})")
        if marker_ms is not None and (now * 1000 - marker_ms) >= args.window * 1000:
            end_ms = marker_ms + int(args.window * 1000)
            break
        if now >= next_tick:
            for cid in all_cids:
                mod.dispatch.apply_async(args=(cid,), queue="dispatch")
            mod.webhook_drain.apply_async(queue="webhook_ingest")
            ticks += 1
            next_tick += args.tick
        if now - last_log >= 10.0:
            drained = int(persist_common.results_conn().get("persist:drained") or 0)
            backlog = persist_common.backlog_len()
            done_est = drained + backlog
            rate = (done_est - prev_done) / max(now - prev_t, 1e-9)
            prev_done, prev_t = done_est, now
            log(
                f"t={now - t0:6.1f}s sent_est={done_est}/{total} "
                f"rate={rate:6.1f}/s persisted={drained} backlog={backlog}"
            )
            last_log = now
        alive = bench_driver.worker_alive(args.pid)
        if alive is False:
            if dead_since is None:
                dead_since = now
                log("worker gone; 10s grace")
            elif now - dead_since > 10.0:
                status = "worker_dead"
                end_ms = int(now * 1000)
                break
        else:
            dead_since = None
        time.sleep(0.25)

    redis_store.set_bg_active(False)
    sampler.stop()
    sampler.join(timeout=2)
    if procs:
        procs.stop()

    blob = {
        "scenario": args.scenario,
        "stack": args.stack,
        "data_layer": ts.DATA_LAYER,
        "persist_tasks": n_persist,
        "persist_batch": (
            int(os.environ.get("DJ_PERSIST_BATCH", "50"))
            if ts.RAW
            else persist_common.DRAIN_LIMIT
        ),
        "status": status,
        "campaigns": len(all_cids),
        "n_total": total,
        "started_at": datetime.fromtimestamp(t0, timezone.utc).isoformat(),
        "t0_ms": int(t0 * 1000),
        "marker_ms": marker_ms,
        "end_ms": end_ms,
        "warmup_s": args.warmup,
        "window_s": args.window,
        "tick_s": args.tick,
        "ticks": ticks,
        "config": dict(campconfig.CONFIG),
        "memory": {
            **sampler.summary(),
            "worker_alive_at_end": bench_driver.worker_alive(args.pid),
        },
        "per_process": procs.summary() if procs else None,
        "samples": {"memory": sampler.samples},
    }
    with open(out_path(args.scenario), "w") as f:
        json.dump(blob, f, indent=1)
    peak = blob["memory"].get("memory_peak_file_bytes") or sampler.peak_cgroup or 0
    log(
        f"run done status={status} ticks={ticks} "
        f"peak_cgroup={peak / 1048576:.1f}MiB "
        f"procs_seen={blob['per_process']['n_processes_seen'] if procs else 0}"
    )
    return 0 if status == "timebox_ok" else 3


# ---------------------------------------------------------------- finalize ---
def pct(vals, p):
    return bench_driver.percentile(vals, p)


def cmd_finalize(args):
    path = out_path(args.scenario)
    with open(path) as f:
        blob = json.load(f)
    marker = blob["marker_ms"]
    end = blob["end_ms"]
    window_s = (end - marker) / 1000.0

    # post-stop drain: persister crash-recovery contract (idempotent insert)
    leftover = 0
    while True:
        n = ts.persist_drain_once()
        if n == 0:
            break
        leftover += n
    log(f"finalize: drained {leftover} leftover results_raw records")

    all_cids = cids()
    sendlog_total = SendLog.objects.count()
    flags_total = Recipient.objects.filter(sent_flag=True).count()
    status_counts = {
        r["status"]: r["n"]
        for r in Recipient.objects.values("status").annotate(n=Count("rid"))
    }
    att_rows = Recipient.objects.values("attempts").annotate(n=Count("rid"))
    att_hist = {str(r["attempts"]): r["n"] for r in att_rows}
    dup = sum(redis_store.duplicates(cid) for cid in all_cids)

    win = SendLog.objects.filter(sent_at_ms__gte=marker, sent_at_ms__lt=end)
    sends_in_window = win.count()
    waits = [
        float(a - b)
        for a, b in win.values_list("sent_at_ms", "enqueued_first_ms")
        if a and b
    ]
    wait_summary = bench_driver.summarize_latencies(waits)

    buckets = {}
    with connection.cursor() as cur:
        cur.execute(
            "SELECT (sent_at_ms - %s) / 10000 AS bucket, count(*) "
            "FROM campaigns_sendlog WHERE sent_at_ms >= %s AND sent_at_ms < %s"
            " GROUP BY 1 ORDER BY 1",
            [marker, marker, end],
        )
        for b, n in cur.fetchall():
            buckets[int(b)] = int(n)
    send_tl = [buckets.get(i, 0) for i in range(max(buckets) + 1)] if buckets else []

    lags = persist_common.collect_lags()
    lag_summary = bench_driver.summarize_latencies(lags)
    ptl_abs = persist_common.persist_timeline()
    base = marker // 10000
    ptl = {}
    for k, v in ptl_abs.items():
        ptl[max(0, k - base)] = ptl.get(max(0, k - base), 0) + v
    persist_tl = [ptl.get(i, 0) for i in range(max(ptl) + 1)] if ptl else []

    def ctr(name):
        return sum(redis_store.get_counter(cid, name) for cid in all_cids)

    wh = {
        r["status"]: r["n"]
        for r in WebhookInboxItem.objects.values("status").annotate(n=Count("id"))
    }
    bg = {
        "ghost_runs": redis_store.get_bg("ghost_runs"),
        "ghost_calls": redis_store.get_bg("ghost_calls"),
        "ghost_errors": redis_store.get_bg("ghost_errors"),
        "backfill_jobs": BackfillJob.objects.count(),
        "backfill_pages_fetched": sum(
            BackfillJob.objects.values_list("pages_fetched", flat=True)
        ),
        "webhook_processed": redis_store.get_bg("webhook_processed"),
        "webhook_failures": redis_store.get_bg("webhook_failures"),
        "webhook_dead": redis_store.get_bg("webhook_dead"),
        "webhook_orm_status": wh,
    }

    validations = {
        "sendlog_rows": sendlog_total,
        "sent_flags": flags_total,
        "sendlog_matches_sent": sendlog_total == flags_total,
        "sendlog_minus_flags": sendlog_total - flags_total,
        "results_raw_left": persist_common.backlog_len(),
        "post_stop_drained": leftover,
        "duplicates": dup,
        "duplicates_zero": dup == 0,
        "failed": status_counts.get("failed", 0),
        "skipped": status_counts.get("skipped", 0),
    }
    retries = {
        "http_retries": ctr("http_retries"),
        "lock_skips": ctr("lock_skips"),
        "already_sent_repairs": ctr("already_sent"),
        "claimed_total": ctr("claimed_total"),
        "attempts_histogram": dict(sorted(att_hist.items())),
    }
    blob.update(
        {
            "window": {
                "window_s": window_s,
                "sends_in_window": sends_in_window,
                "sends_per_s": round(sends_in_window / window_s, 1),
                "wait_ms": wait_summary,
                "sends_per_10s": send_tl,
            },
            "totals": {
                "sends_total_run": sendlog_total,
                "status_counts": status_counts,
            },
            "persist_lag_ms": lag_summary,
            "persists_per_10s": persist_tl,
            "validations": validations,
            "retries": retries,
            "bgfill": bg,
        }
    )
    with open(path, "w") as f:
        json.dump(blob, f, indent=1)

    ok = (
        blob["status"] == "timebox_ok"
        and dup == 0
        and validations["sendlog_matches_sent"]
    )
    mem = blob.get("memory") or {}
    peak = (
        mem.get("memory_peak_file_bytes") or mem.get("peak_cgroup_sampled_bytes") or 0
    )
    log(
        f"FINAL {args.scenario} stack={blob['stack']} status={blob['status']} "
        f"window={window_s:.0f}s sends={sends_in_window} "
        f"sends_ps={sends_in_window / window_s:.1f} "
        f"p50_wait={wait_summary['p50']}ms p95_wait={wait_summary['p95']}ms "
        f"dup={dup} sendlog={sendlog_total} flags={flags_total} "
        f"match={validations['sendlog_matches_sent']} "
        f"peak={peak / 1048576:.1f}MiB "
        f"ghost_calls={bg['ghost_calls']} webhook_done={bg['webhook_processed']}"
    )
    return 0 if ok else 3


def main():
    ap = argparse.ArgumentParser(description="django_real driver")
    sub = ap.add_subparsers(dest="cmd", required=True)

    s = sub.add_parser("seed")
    s.add_argument("--campaigns", type=int, default=100)
    s.add_argument("--per", type=int, default=10000)
    s.add_argument("--pages", type=int, default=200)

    r = sub.add_parser("run")
    r.add_argument("--stack", required=True, choices=["celery", "cauli"])
    r.add_argument("--scenario", required=True)
    r.add_argument("--warmup", type=float, default=60.0)
    r.add_argument("--window", type=float, default=600.0)
    r.add_argument("--tick", type=float, default=5.0)
    r.add_argument("--cgroup-path", default=None)
    r.add_argument("--pid", type=int, default=None)

    f = sub.add_parser("finalize")
    f.add_argument("--scenario", required=True)

    args = ap.parse_args()
    if args.cmd == "seed":
        return cmd_seed(args)
    if args.cmd == "run":
        return cmd_run(args)
    return cmd_finalize(args)


if __name__ == "__main__":
    sys.exit(main())

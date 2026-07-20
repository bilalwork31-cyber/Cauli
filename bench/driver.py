"""Benchmark measurement engine (one scenario = one invocation).

Flow:
  1. import the stack's task module and enqueue N tasks as fast as possible
     via the native API (celery .delay / rupy .delay), recording a wall clock
     enqueue timestamp (ms) per task id,
  2. wait for all N results in the backend:
       celery: DBSIZE delta on backend db 1 as the cheap gate, then MGET of
               celery-task-meta-{id} to confirm and to read date_done,
       rupy:   DBSIZE delta on db 0 as the cheap gate, then MGET of
               rupy:result:{id} to confirm and to read finished_at,
  3. compute throughput two ways:
       exec_tps = N / (t_done - enqueue_end)   (pure execution window)
       full_tps = N / (t_done - t_start)       (includes enqueue time)
  4. per task latency in ms:
       celery: date_done (stored UTC by the backend) minus the driver's
               enqueue wall time for that id
       rupy:   finished_at (result JSON, epoch ms) minus the driver's enqueue
               wall time for that id (the driver stamps it immediately before
               .delay, i.e. when the client sets envelope enqueued_at)
     summarized as p50/p90/p95/p99/max/mean over successful tasks,
  5. memory sampling thread every 250ms: cgroup memory.current (via
     --cgroup-path) plus RSS of the worker process tree via psutil (--pid) as
     fallback; after the run also memory.peak and memory.events oom_kill.

Robustness:
  --timeout bounds the whole wait (default 600s). On expiry the scenario is
  recorded with status "stalled" plus the completed count (OOM thrash signal).
  A dead worker (pid gone / cgroup path gone) is detected, given a short grace
  period for straggler results, and recorded as status "worker_dead".

Output: one JSON blob to results/{scenario}.json (raw samples + summary) and
one human summary line on stdout.

Modes:
  normal        enqueue + measure (default)
  --warmup      enqueue + wait but record nothing (JIT/pool/connection warmup)
  --idle        no tasks; sample memory for --idle-duration seconds (S4)
"""
import argparse
import json
import math
import os
import sys
import threading
import time
from datetime import datetime, timezone

BENCH_DIR = os.path.dirname(os.path.abspath(__file__))
RESULTS_DIR = os.path.join(BENCH_DIR, "results")


# ---------------------------------------------------------------------------
# percentile math (unit tested in test_driver.py)
# ---------------------------------------------------------------------------
def percentile(values, p):
    """Linear interpolation percentile (numpy default method).

    values: iterable of numbers (need not be sorted). p in [0, 100].
    """
    vals = sorted(values)
    if not vals:
        return None
    if len(vals) == 1:
        return float(vals[0])
    if p <= 0:
        return float(vals[0])
    if p >= 100:
        return float(vals[-1])
    rank = (p / 100.0) * (len(vals) - 1)
    lo = math.floor(rank)
    hi = math.ceil(rank)
    if lo == hi:
        return float(vals[lo])
    frac = rank - lo
    return float(vals[lo] * (1.0 - frac) + vals[hi] * frac)


def summarize_latencies(lat_ms):
    if not lat_ms:
        return {"count": 0, "p50": None, "p90": None, "p95": None, "p99": None,
                "max": None, "mean": None}
    return {
        "count": len(lat_ms),
        "p50": round(percentile(lat_ms, 50), 2),
        "p90": round(percentile(lat_ms, 90), 2),
        "p95": round(percentile(lat_ms, 95), 2),
        "p99": round(percentile(lat_ms, 99), 2),
        "max": round(max(lat_ms), 2),
        "mean": round(sum(lat_ms) / len(lat_ms), 2),
    }


# ---------------------------------------------------------------------------
# memory sampling
# ---------------------------------------------------------------------------
def _read_int_file(path):
    try:
        with open(path) as f:
            return int(f.read().strip())
    except (OSError, ValueError):
        return None


def read_oom_kills(cgroup_path):
    try:
        with open(os.path.join(cgroup_path, "memory.events")) as f:
            for line in f:
                parts = line.split()
                if len(parts) == 2 and parts[0] == "oom_kill":
                    return int(parts[1])
    except OSError:
        return None
    return None


class MemorySampler(threading.Thread):
    """Every `interval` seconds: cgroup memory.current and psutil tree RSS."""

    def __init__(self, cgroup_path=None, pid=None, interval=0.25):
        super().__init__(daemon=True)
        self.cgroup_path = cgroup_path
        self.pid = pid
        self.interval = interval
        self.samples = []          # [{"t": epoch_s, "cgroup": bytes|None, "rss": bytes|None}]
        self.peak_cgroup = 0
        self.peak_rss = 0
        self.cgroup_gone = False
        self.worker_seen_dead = False
        # name must not shadow threading.Thread._stop (join() calls it)
        self._stop_evt = threading.Event()
        self._psutil = None
        if pid:
            try:
                import psutil
                self._psutil = psutil
            except ImportError:
                pass

    def _tree_rss(self):
        if not (self._psutil and self.pid):
            return None
        try:
            root = self._psutil.Process(self.pid)
            procs = [root] + root.children(recursive=True)
            total = 0
            for p in procs:
                try:
                    total += p.memory_info().rss
                except (self._psutil.NoSuchProcess, self._psutil.AccessDenied):
                    pass
            return total
        except (self._psutil.NoSuchProcess, self._psutil.AccessDenied):
            self.worker_seen_dead = True
            return None

    def sample_once(self):
        cg = None
        if self.cgroup_path:
            cg = _read_int_file(os.path.join(self.cgroup_path, "memory.current"))
            if cg is None:
                self.cgroup_gone = True
            else:
                self.peak_cgroup = max(self.peak_cgroup, cg)
        rss = self._tree_rss()
        if rss is not None:
            self.peak_rss = max(self.peak_rss, rss)
        self.samples.append({"t": round(time.time(), 3), "cgroup": cg, "rss": rss})

    def run(self):
        while not self._stop_evt.is_set():
            self.sample_once()
            self._stop_evt.wait(self.interval)
        self.sample_once()

    def stop(self):
        self._stop_evt.set()

    def summary(self):
        peak_file = None
        oom = None
        if self.cgroup_path:
            peak_file = _read_int_file(os.path.join(self.cgroup_path, "memory.peak"))
            oom = read_oom_kills(self.cgroup_path)
        return {
            "peak_cgroup_sampled_bytes": self.peak_cgroup or None,
            "memory_peak_file_bytes": peak_file,
            "oom_kills": oom,
            "peak_rss_sampled_bytes": self.peak_rss or None,
            "cgroup_gone": self.cgroup_gone,
        }


def worker_alive(pid):
    if not pid:
        return None
    try:
        import psutil
        if not psutil.pid_exists(pid):
            return False
        p = psutil.Process(pid)
        return p.is_running() and p.status() != psutil.STATUS_ZOMBIE
    except Exception:
        return None


# ---------------------------------------------------------------------------
# stack adapters
# ---------------------------------------------------------------------------
def get_task(stack, task_name):
    if stack == "celery":
        import tasks_celery as mod
        table = {"io": mod.io_task, "cpu": mod.cpu_task}
        if task_name not in table:
            sys.exit(f"driver: task '{task_name}' not valid for celery (io|cpu)")
        return table[task_name]
    elif stack == "rupy":
        import tasks_rupy as mod
        table = {"io": mod.io_task, "io_async": mod.io_task_async, "cpu": mod.cpu_task}
        if task_name not in table:
            sys.exit(f"driver: task '{task_name}' not valid for rupy (io|io_async|cpu)")
        return table[task_name]
    sys.exit(f"driver: unknown stack '{stack}'")


def redis_client(port, db):
    import redis
    return redis.Redis(host="127.0.0.1", port=port, db=db)


def parse_celery_date_done(s):
    """celery date_done ISO string -> epoch ms. Naive datetimes are UTC."""
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.timestamp() * 1000.0


def collect_results(stack, r, ids):
    """MGET all result keys. Returns (n_present, latencies_ms, n_failed).

    latencies only for successful tasks; failed tasks counted separately.
    """
    if stack == "celery":
        keys = [f"celery-task-meta-{i}" for i in ids]
    else:
        keys = [f"rupy:result:{i}" for i in ids]
    present = 0
    failed = 0
    lat = []
    id_list = list(ids.keys())
    CHUNK = 1000
    for off in range(0, len(keys), CHUNK):
        vals = r.mget(keys[off:off + CHUNK])
        for idx, raw in enumerate(vals):
            if raw is None:
                continue
            present += 1
            tid = id_list[off + idx]
            try:
                meta = json.loads(raw)
            except (ValueError, TypeError):
                failed += 1
                continue
            if stack == "celery":
                if meta.get("status") == "SUCCESS":
                    dd = meta.get("date_done")
                    if dd:
                        lat.append(parse_celery_date_done(dd) - ids[tid])
                else:
                    failed += 1
            else:
                if meta.get("status") == "success":
                    fa = meta.get("finished_at")
                    if fa is not None:
                        lat.append(float(fa) - ids[tid])
                else:
                    failed += 1
    return present, lat, failed


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser(description="benchmark driver")
    ap.add_argument("--stack", required=True, choices=["celery", "rupy"])
    ap.add_argument("--task", default="io", choices=["io", "io_async", "cpu"])
    ap.add_argument("--n", type=int, default=1000)
    ap.add_argument("--scenario", default=None, help="name for results/{scenario}.json")
    ap.add_argument("--cgroup-path", default=None,
                    help="cgroup v2 dir of the worker scope (memory.current etc.)")
    ap.add_argument("--pid", type=int, default=None,
                    help="worker main pid for psutil tree RSS fallback")
    ap.add_argument("--timeout", type=float, default=600.0,
                    help="whole wait bound in seconds (default 600)")
    ap.add_argument("--poll", type=float, default=0.2, help="poll interval seconds")
    ap.add_argument("--redis-port", type=int,
                    default=int(os.environ.get("BENCH_REDIS_PORT", "6390")))
    ap.add_argument("--warmup", action="store_true",
                    help="run but do not record (no JSON output)")
    ap.add_argument("--idle", action="store_true",
                    help="no tasks; just sample memory for --idle-duration")
    ap.add_argument("--idle-duration", type=float, default=20.0)
    args = ap.parse_args()

    scenario = args.scenario or f"{args.stack}_{args.task}_{args.n}"
    os.makedirs(RESULTS_DIR, exist_ok=True)
    out_path = os.path.join(RESULTS_DIR, f"{scenario}.json")

    blob = {
        "scenario": scenario,
        "stack": args.stack,
        "task": args.task,
        "n": args.n,
        "status": None,
        "config": vars(args).copy(),
        "started_at": datetime.now(timezone.utc).isoformat(),
    }

    # ---------------- idle mode (S4) ----------------
    if args.idle:
        sampler = MemorySampler(args.cgroup_path, args.pid, 0.25)
        sampler.start()
        time.sleep(args.idle_duration)
        sampler.stop()
        sampler.join(timeout=2)
        mem = sampler.summary()
        current = _read_int_file(os.path.join(args.cgroup_path, "memory.current")) \
            if args.cgroup_path else None
        blob.update({
            "status": "idle_ok",
            "memory": mem,
            "idle_memory_current_bytes": current,
            "samples": {"memory": sampler.samples},
        })
        with open(out_path, "w") as f:
            json.dump(blob, f, indent=1)
        mib = (current or sampler.peak_rss or 0) / (1024 * 1024)
        print(f"[driver] {scenario} status=idle_ok idle_mem={mib:.1f}MiB "
              f"(cgroup={current} rss_peak={sampler.peak_rss})")
        return 0

    # ---------------- normal / warmup ----------------
    task = get_task(args.stack, args.task)
    backend_db = 1 if args.stack == "celery" else 0
    r = redis_client(args.redis_port, backend_db)
    r.ping()

    baseline_dbsize = r.dbsize()

    sampler = None
    if not args.warmup:
        sampler = MemorySampler(args.cgroup_path, args.pid, 0.25)
        sampler.start()

    # enqueue as fast as possible, native API, stamping wall ms per id
    ids = {}
    t_start = time.time()
    try:
        for _ in range(args.n):
            ts_ms = time.time() * 1000.0
            res = task.delay()
            ids[res.id] = ts_ms
    except Exception as e:
        blob["status"] = "enqueue_error"
        blob["error"] = f"{type(e).__name__}: {e}"
        if sampler:
            sampler.stop()
        if not args.warmup:
            with open(out_path, "w") as f:
                json.dump(blob, f, indent=1)
        print(f"[driver] {scenario} status=enqueue_error after {len(ids)} tasks: {e}",
              file=sys.stderr)
        return 2
    enqueue_end = time.time()
    enqueue_s = enqueue_end - t_start
    print(f"[driver] enqueued {args.n} tasks in {enqueue_s:.2f}s "
          f"({args.n / max(enqueue_s, 1e-9):.0f} enq/s)", file=sys.stderr)

    # wait for completion
    deadline = t_start + args.timeout
    status = "ok"
    n_done = 0
    lat = []
    n_failed = 0
    t_done = None
    last_full_check = 0.0
    dead_since = None
    while True:
        now = time.time()
        gate = (r.dbsize() - baseline_dbsize) >= args.n
        if gate or (now - last_full_check) >= 5.0:
            last_full_check = now
            n_done, lat, n_failed = collect_results(args.stack, r, ids)
            if n_done >= args.n:
                t_done = time.time()
                break
            print(f"[driver] progress {n_done}/{args.n}", file=sys.stderr)
        if now >= deadline:
            status = "stalled"
            t_done = time.time()
            n_done, lat, n_failed = collect_results(args.stack, r, ids)
            break
        alive = worker_alive(args.pid)
        if alive is False:
            if dead_since is None:
                dead_since = now
                print("[driver] worker process gone; grace period for stragglers",
                      file=sys.stderr)
            elif now - dead_since > 10.0:
                status = "worker_dead"
                t_done = time.time()
                n_done, lat, n_failed = collect_results(args.stack, r, ids)
                break
        time.sleep(args.poll)

    if sampler:
        sampler.stop()
        sampler.join(timeout=2)

    exec_window = max(t_done - enqueue_end, 1e-9)
    full_window = max(t_done - t_start, 1e-9)
    lat_summary = summarize_latencies(lat)
    mem = sampler.summary() if sampler else {}
    if sampler and sampler.cgroup_gone:
        # scope vanished mid run: worker was killed hard (OOM or teardown)
        if status == "ok" and n_done < args.n:
            status = "worker_dead"
    blob.update({
        "status": status,
        "completed": n_done,
        "failed_tasks": n_failed,
        "timing": {
            "t_start_epoch": round(t_start, 3),
            "enqueue_end_epoch": round(enqueue_end, 3),
            "t_done_epoch": round(t_done, 3),
            "enqueue_s": round(enqueue_s, 3),
            "exec_window_s": round(exec_window, 3),
            "full_window_s": round(full_window, 3),
        },
        "throughput": {
            "exec_tps": round(n_done / exec_window, 2),
            "full_tps": round(n_done / full_window, 2),
            "enqueue_rate": round(args.n / max(enqueue_s, 1e-9), 2),
        },
        "latency_ms": lat_summary,
        "memory": {**mem, "worker_alive_at_end": worker_alive(args.pid)},
        "samples": {
            "memory": sampler.samples if sampler else [],
            "latencies_ms": [round(x, 2) for x in lat],
        },
    })

    if not args.warmup:
        with open(out_path, "w") as f:
            json.dump(blob, f, indent=1)

    peak = 0
    if sampler:
        peak = sampler.peak_cgroup or sampler.peak_rss or 0
    tag = "warmup " if args.warmup else ""
    print(f"[driver] {tag}{scenario} stack={args.stack} task={args.task} "
          f"n={args.n} status={status} done={n_done} failed={n_failed} "
          f"exec_tps={n_done / exec_window:.1f} full_tps={n_done / full_window:.1f} "
          f"p50={lat_summary['p50']}ms p95={lat_summary['p95']}ms "
          f"p99={lat_summary['p99']}ms max={lat_summary['max']}ms "
          f"peak_mem={peak / (1024 * 1024):.1f}MiB "
          f"oom={mem.get('oom_kills') if mem else None}")
    return 0 if status == "ok" else 3


if __name__ == "__main__":
    sys.exit(main())

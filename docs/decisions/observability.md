# Decision: observability surface at 1.0
> **Historical design note, not current documentation.** This is a record of how one
> pre 1.0 decision was reached and what was known when it was reached. It is kept
> because the reasoning is worth reading, not because it describes today's behaviour.
> Where it disagrees with the code, with [PROTOCOL.md](../../PROTOCOL.md) or with
> [docs/CONFIGURATION.md](../CONFIGURATION.md), those win. The status line below was
> checked against the source, not carried over.
>
> **Status: shipped in 1.0.0.** The stats line gained per lane latency percentiles,
> `oldest_ms`, `cpu_rss_mb`, `sync_live`, `sync_abandoned`, `async_rejected`,
> `cpu_backlog` and `loop_lag_ms`, then `pid`, `host` and `duplicate` in the release audit
> pass. `pending_async` was removed. cpu child recycling now defaults to 1000 tasks.
> The rejections still stand: no metrics endpoint, no JSON logging, no health endpoint.

**Keep the single stats line and make it a documented logfmt contract. Add 9 fields, remove 1, and
flip the cpu child recycle default. Zero new dependencies and zero new flags.**

The 14 current fields answer "is something broken" well and "is it degrading" not at all. This
project's own measured degradations, a 2.00s task body becoming 3.82s and p99 going 20ms to 92ms,
are invisible in the current line by construction. Everything needed to fix that already exists in
the tree: `enqueued_at` in the envelope, `child_pids` in `CpuPool`, XACK and XDEL on every finish.

## Additions

- **Six latency fields**, `sync_p50 sync_p99 async_p50 async_p99 cpu_p50 cpu_p99`, from a hand rolled
  log2 bucket histogram per lane. 24 buckets from 1ms to about 4 hours, one AtomicU64 each, snapshot
  and reset at each stats tick. Cost is two clock reads and one relaxed `fetch_add`, under 100ns per
  task, which is 0.02 percent of even a 0.5ms task body and noise against the measured 0.1 percent
  dispatch share. About 600 bytes of memory total.

  Rejected: hdrhistogram, because it is a dependency; and mean plus max, because a mean hides the
  tail and the 3.82s case is a MEDIAN shift that a max cannot show. Bucket resolution is 2x worst
  case, which is knee detection rather than precision, and knee detection is the operator's job.
  Precision stays in bench/.

- **`oldest_ms`**, the age of the oldest unacked entry, via `XRANGE q - + COUNT 1` and the millisecond
  component of the stream id. One probe per queue per tick. This is the leading indicator, and the
  reason it earns its place over per task sampling is subtle and worth keeping: **it still works
  while fetching is paused**, which is exactly the situation where per task latency sampling goes
  blind because nothing is being sampled.

- **`cpu_rss_mb`**, summed from `/proc/PID/status` over `child_pids` at the stats tick, closing the
  measured 331.8 MB blind spot at zero hot path cost.

- **`cpu_lost`**, counting `CpuOutcome::Lost`, so repeated child death becomes alertable rather than
  a scrolling warning. Today an OOM killed child folds into `failed` as a generic WorkerLost.

## Removal, and the one breaking behaviour change

- **Remove `pending_async`.** It is the one field whose own documentation names its replacement:
  loops.rs admits `async_rejected` is what actually moves during a wedge. Removing it post 1.0 costs
  a major version; now it costs a changelog line.

- **Flip `--cpu-max-tasks-per-child` from 0 to 1000.** An unbounded child by default contradicts this
  audit's own finding that nothing else bounds child memory. The fork server makes recycling a fork
  of the preloaded parent with no re import, so the cost is milliseconds per thousand tasks; even the
  stdio fallback pays one import per 1000, under 0.5 percent duty. Keep 0 as an explicit opt out.
  This is a behaviour change and 1.0 is the last cheap moment for it.

## Format: the line IS the API

It is already strict `k=v` logfmt. Declare that in PROTOCOL section 7 as a stable parsing contract:
space separated, integer values, counters cumulative, latency keys interval scoped, key set frozen
per major version. Vector, promtail and awk then consume it with no further work.

No HTTP `/metrics` endpoint at 1.0: it means a new listener, a new port flag and new attack surface.
A stable logfmt contract now makes a post 1.0 Prometheus endpoint purely additive. No `--log-json`
either; tracing-subscriber's json feature stays available if it is ever wanted.

## Ranked by value against cost

1. Per lane p50 and p99: closes the top gap, about 100ns per task, no dependency
2. `oldest_ms`: the leading indicator, one redis probe per tick
3. `cpu_rss_mb`: closes a measured blind spot, stats tick only
4. Recycle default 1000: behaviour change, must land before the freeze or never
5. `cpu_lost` plus removing `pending_async`: one atomic and one deletion
6. PROTOCOL section 7 rewritten to declare the logfmt contract

Explicitly not added: any metrics crate, a `/metrics` endpoint, `--log-json`, per queue breakdowns,
per task structured events, OpenTelemetry, or configurable memory threshold warnings. Each is either
a dependency, a configuration surface, or answers no question the proposed key set leaves open.

## Needs your approval

The two breaking items: removing `pending_async`, and changing the recycle default. Everything else
is additive.

# Observability through one stats line

Status: Accepted (0.1.0)

## Context

The worker's only operator interface is a periodic `stats:` log line. Before
0.1.0 it carried counters and inflight gauges. They showed whether something was broken, but not
whether the worker was degrading: there was no latency of any kind, nothing
showed a growing backlog while fetching was paused, cpu child memory was not
counted, and a cpu child killed by the OOM killer was folded into `failed` as a
generic `WorkerLost`. The data to fix each of these already existed in the
worker: the stream ids, the cpu pool's child pids, and the timing around each
task body.

## Decision

Keep the single stats line, make it a documented logfmt contract (PROTOCOL.md
section 7), and add fields rather than a new interface. No new dependency and no
new flag.

- Per lane latency, `sync_p50 sync_p99 async_p50 async_p99 cpu_p50 cpu_p99`, in
  ms over the interval just ended. Each lane has a 24 bucket log2 histogram of
  `AtomicU64` counters covering 1 ms to about 4 hours, drained at every tick
  (`LatencyHist` in `worker/src/stats.rs`). Recording is one relaxed
  `fetch_add` plus the two clock reads around the body. Resolution is 2x worst
  case: enough to see a knee. Precise latency work belongs in `bench/`.
- `oldest_ms`: age of the oldest outstanding entry in any of the worker's
  queues, the older of the oldest pending entry (`XPENDING`) and the oldest
  undelivered one. It is a broker probe rather than a per task sample, so it
  keeps reporting while fetching is paused, which is exactly when per task
  sampling has nothing to sample.
- `cpu_rss_mb`: resident memory summed over the cpu children from
  `/proc/PID/status`, read at the stats tick only. `rss_mb` covers the worker
  process alone.
- `cpu_lost`: cpu children that died mid task.
- `sync_live`, `sync_abandoned`, `async_rejected`, `cpu_backlog`, `loop_lag_ms`:
  sync pool size and hard timeout abandonments, async submissions rejected at
  the shim's queue cap, depth behind the fetch gate, and embedded event loop lag
  (see [process-model.md](process-model.md)).
- `pid` and `host` lead every line, so the independent lines from `--procs`
  above 1 can be told apart. `duplicate` counts executions suppressed by an
  idempotency key.
- `pending_async` was removed. `async_rejected` is the number that moves when an
  event loop wedges; `pending_async` stayed flat through that failure.
- `--cpu-max-tasks-per-child` defaults to 10000 instead of 0, because nothing
  else bounds cpu child memory. Under the fork server a recycle is a fork of the
  preloaded parent with no re import. `0` remains an explicit opt out.

The contract: `key=value` pairs separated by spaces, integer values except
`host`, counters cumulative, gauges instantaneous, latency keys interval scoped.
A minor release may add a key; renaming or removing one is a major version
change.

## Consequences

- Vector, promtail or awk can consume the line without a custom parser.
- A Prometheus endpoint can be added later without changing the line.
- The latency cost per task and the histogram's memory (24 counters per lane)
  are small, but the per task cost has not been benchmarked.
- Removing `pending_async` and changing the recycle default were breaking
  changes, made before the first release because afterwards each would need a
  major version.

## Alternatives considered

- `hdrhistogram`. Rejected: a dependency for what a fixed array of counters does.
- Mean plus max latency. Rejected: a mean hides the tail, and a shift of the
  median does not show in the max.
- An HTTP `/metrics` endpoint. Rejected: a new listener, a new port flag and new
  attack surface.
- `--log-json`, per queue breakdowns, per task events, OpenTelemetry,
  configurable memory warnings, a health endpoint. Rejected: each is a
  dependency or a configuration surface, or answers no question the key set
  leaves open.

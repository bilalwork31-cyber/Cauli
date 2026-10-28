# Redis Cluster is not supported

Status: Accepted (0.1.0)

## Context

cauli's keys were designed for a single Redis dataset. On Redis Cluster several
paths break:

- The delayed mover script touches `cauli:q:{queue}` and
  `cauli:delayed:{queue}`, which never share a hash slot, so it fails with
  CROSSSLOT on every tick.
- The retry write queues a `ZADD` to the delayed set in the same flush as the
  `XACK` and `XDEL` on the stream. On a cluster with several masters those land
  on different nodes: the delete can succeed while the `ZADD` is redirected,
  which loses the task silently. This follows from the key layout and has not
  been reproduced on a real multi master cluster.
- The fetch loop reads every queue in one `XREADGROUP`, which is CROSSSLOT as
  soon as a worker consumes two or more queues.
- Neither the worker nor the Python client follows MOVED redirects. The worker
  enables only the `tokio-comp`, `connection-manager` and `tokio-rustls-comp`
  features of the redis crate, with no cluster support. The client builds a
  standalone `redis.Redis`.

So no real multi master deployment could have been relying on cauli, and
refusing Cluster breaks nobody.

## Decision

The worker refuses to start on Redis Cluster. Standalone Redis and Sentinel are
the supported topologies.

- At startup, before touching any consumer group, the worker sends
  `INFO cluster` and exits 1 when the reply contains `cluster_enabled:1`
  (`probe_cluster_info`, `cluster_info_says_enabled` and `cluster_decision` in
  `worker/src/lib.rs`). The message names the topology and the supported ones.
- The probe uses `INFO cluster`, not `CLUSTER INFO`. `CLUSTER INFO` reports
  cluster health (`cluster_state`, slot counts) with no `cluster_enabled` field,
  and a standalone server refuses it, so it cannot prove a cluster. A test pins
  the probe against a live server of each topology
  (`the_probe_reads_the_topology_off_a_real_server`).
- A failed probe does not block startup. Every Redis answers `INFO`, so a
  failure means an ACL or proxy in front of a deployment that already works.
- `CAULI_ALLOW_REDIS_CLUSTER=1` (also `true`, `yes`, `on`) starts the worker
  anyway, with a warning that delayed tasks, retries and beat lose work and that
  fetching two or more queues fails.
- `cauli-beat` refuses to claim and publish on a cluster.

## Consequences

- Sentinel works from the Python client and `cauli-beat` through
  `Cauli(redis_client=...)` with a `master_for` factory. The worker connects by
  URL and does not look the master up again after a failover.
- Standalone key names never changed, so there is no migration.
- The override exists for someone running one queue, no delayed tasks, no
  retries and no beat, who accepts the risk.

## Alternatives considered

- Hash tag keys by queue (`{queue}`). This fixes the mover and the retry path,
  but beat's claim script updates beat's own schedule keys and the target
  queue's key in one atomic call, for every queue, and making that work needs
  one global hash tag. That pins every key to one slot and
  removes the reason to run Cluster. Rejected.
- Support Cluster for throughput. Not worth it: a consumer group on one stream
  lives in one slot regardless, so Cluster offers memory headroom and per queue
  sharding, not throughput per queue.
- A `--allow-redis-cluster` flag. Replaced by the environment variable, which
  keeps the CLI surface unchanged.

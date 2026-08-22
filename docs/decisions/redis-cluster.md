# Decision: Redis Cluster support at 1.0
> **Historical design note, not current documentation.** This is a record of how one
> pre 1.0 decision was reached and what was known when it was reached. It is kept
> because the reasoning is worth reading, not because it describes today's behaviour.
> Where it disagrees with the code, with [PROTOCOL.md](../../PROTOCOL.md) or with
> [docs/CONFIGURATION.md](../CONFIGURATION.md), those win. The status line below was
> checked against the source, not carried over.
>
> **Status: shipped in 1.0.0, with one correction to the recommendation below.** The worker
> probes the topology at startup, before it touches a consumer group, and exits 1 with a
> message naming the topology when the reply says the node runs in cluster mode
> (`probe_cluster_info`, `cluster_info_says_enabled` and `cluster_decision` in
> `worker/src/lib.rs`). The escape hatch shipped as the environment variable
> `CAULI_ALLOW_REDIS_CLUSTER=1`, not as the `--allow-redis-cluster` flag this document
> proposed. The command matters, and this document originally named the wrong one: the probe
> sends `INFO cluster`, which is the only reply carrying `cluster_enabled:0` or
> `cluster_enabled:1`. `CLUSTER INFO` is a different command. A cluster node answers it with
> `cluster_state` and `cluster_slots_*` and no `cluster_enabled` field at all, and a
> standalone refuses it outright, so a probe built on `CLUSTER INFO` can never prove a
> cluster. The Sentinel half of the recommendation also changed: Sentinel is reachable from
> the Python client and `cauli-beat` through `Cauli(redis_client=...)`, but the worker
> connects by URL and never looks a master up again after a failover.

**Do not support Cluster. 1.0 refuses at startup. Standalone and Sentinel only.**

## The decisive fact: refusing breaks nobody

The audit assumed refusal was a real tradeoff, because the non delayed, non periodic paths appeared
to work on Cluster and refusing would break a user happily relying on them. **That assumption was
wrong.** Three further breaks were found in source beyond the two the audit had:

- `fetch_loop` XREADGROUPs all queues in ONE command, so it is CROSSSLOT with two or more queues.
- `finish_retry`'s pipeline lands on two masters: the XDEL succeeds and the ZADD returns MOVED.
  **Silent loss**, and a different one from the mover bug.
- Neither client follows MOVED redirects at all, so even result reads fail.

So "cauli works on Redis Cluster" today means single node dev clusters only. There is no user with a
real multi master deployment to break, which turns the decision from a tradeoff into an easy call.

Reproduce the `finish_retry` loss on three masters before any document claims it. That is the one
piece of this that rests on reading rather than measurement.

## Why not fix it properly

The `{queue}` hash tag design does work for the mover and the retry path, slot 9917 verified. It was
still rejected, for a reason that is worth recording because it is not obvious: **beat's claim
atomicity cannot be fixed without a single global hash tag**, and a global tag pins every key in the
system to one slot, which removes the only thing Cluster was for. The fix and the motivation cancel
each other out.

There is also little to gain. A Streams consumer group on one queue lives in one slot regardless, so
Cluster buys a task queue memory headroom and per queue sharding, not throughput.

Migration cost is avoided entirely, because standalone key names never change.

## What 1.0 should do

Refuse at startup when a Cluster is detected, with a message naming the topology and pointing at the
supported ones. Provide `--allow-redis-cluster` as an explicit override for anyone who genuinely runs
single queue, no delayed, no periodic and wants to accept the risk knowingly.

Needs your approval: the startup refusal and the override flag.

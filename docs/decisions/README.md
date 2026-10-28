# Architecture decisions

Each record states the context, the decision and its consequences as of the
release in its status line. Where a record and the code disagree, the code and
[PROTOCOL.md](../../PROTOCOL.md) win.

- [Worker clock anchored on Redis TIME](clock-architecture.md): absolute instants come from a sampled Redis clock so every worker agrees on when delayed work fires and expires.
- [Delivery guarantee](delivery-guarantee.md): at least once execution within one Redis dataset, with the idempotency key as an admission lease rather than a dedup guarantee.
- [Error taxonomy in the result document](error-taxonomy.md): `error.origin` added, the worker timeout renamed `TimeLimitExceeded`, everything else frozen.
- [Observability through one stats line](observability.md): one logfmt stats line as a stable contract, with no metrics endpoint and no new dependency.
- [Packaging and distribution of the worker](packaging.md): one wheel per CPython minor version, launched through a wrapper that fixes the libpython loader path and `VIRTUAL_ENV`.
- [Process, teardown and threading model](process-model.md): exit through `_exit`, persistent sync thread states, self exit on a wedged event loop, and three execution lanes.
- [Redis Cluster is not supported](redis-cluster.md): the worker refuses to start on a cluster unless `CAULI_ALLOW_REDIS_CLUSTER` is set.
- [Retry is recognised by name, not identity](retry-name-matching.md): `Retry` plus `countdown` is matched by class name on every lane, because identity cannot be checked.

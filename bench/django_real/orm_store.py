"""Django-ORM recipient store: same API shape as bench/campaign/store.py, but
the recipient table, leases, intent locks and sent flags live in Postgres via
the ORM (the C variant faked this table in redis).

Duplicate-protection contract (identical to store.py, DB-atomic now):
  1. claim_batch() is a real SELECT ... FOR UPDATE SKIP LOCKED claim of due
     pending/retry rows plus expired-lease orphans inside one transaction, so
     concurrent dispatchers can never double-claim a live lease.
  2. The intent lock is a conditional UPDATE on lock_until_ms (SET NX EX
     analog): only one sender per recipient at a time.
  3. The sent flag (Recipient.sent_flag) is checked under the lock; if set the
     sender skips silently (already_sent = guard working, NOT a duplicate).
  4. record_send_attempt() is the tripwire before every actual POST: an atomic
     UPDATE ... WHERE sent_flag = false. Rowcount 0 means the flag was already
     up: the duplicates counter increments and the send is refused.

Bench accounting (counters, bg flags, duplicates tripwire counter) stays in
redis via bench/campaign/store.py exactly like the C run, so driver-side
reporting code keeps working and the accounting never perturbs Postgres.
"""

import time

from django.db import transaction
from django.db.models import F, Q

import store as redis_store  # bench/campaign/store.py (counters, bg flags)

import django_boot  # noqa: F401  (safety: ORM needs apps loaded)
from campaigns.models import Campaign, Recipient

import campconfig

CONFIG = campconfig.CONFIG
LOCK_TTL_MS = 300_000  # SET NX EX 300 analog
CLAIMABLE_DUE = ("pending", "retry")
CLAIMABLE_ORPHAN = ("queued", "sending")


def now_ms():
    return int(time.time() * 1000)


# ------------------------------------------------------------------ claiming -
def claim_batch(cid, at_ms=None, limit=50, lease_ms=None):
    """SELECT FOR UPDATE SKIP LOCKED claim of due rows + expired-lease orphans.

    Sets status=queued, lease_until=now+lease, stamps enqueued_first_ms on
    first claim. Returns [(rid, page_id), ...] like store.claim_batch.
    """
    now = at_ms if at_ms is not None else now_ms()
    lease = lease_ms if lease_ms is not None else CONFIG["LEASE_MS"]
    due = Q(status__in=CLAIMABLE_DUE, next_due_ms__lte=now)
    orphan = Q(status__in=CLAIMABLE_ORPHAN, lease_until_ms__lte=now)
    with transaction.atomic():
        rows = list(
            Recipient.objects.select_for_update(skip_locked=True)
            .filter(campaign_id=cid)
            .filter(due | orphan)
            .order_by("next_due_ms")
            .values_list("rid", "page_id")[: int(limit)]
        )
        ids = [r[0] for r in rows]
        if ids:
            Recipient.objects.filter(rid__in=ids).update(
                status="queued", lease_until_ms=now + int(lease)
            )
            Recipient.objects.filter(rid__in=ids, enqueued_first_ms=0).update(
                enqueued_first_ms=now
            )
    if ids:
        redis_store.incr_counter(cid, "claimed_total", len(ids))
    return rows


# ------------------------------------------------------------------ send path
def acquire_lock(cid, rid):
    """Intent lock: conditional UPDATE, true iff this caller took the lock."""
    now = now_ms()
    return (
        Recipient.objects.filter(rid=rid, lock_until_ms__lt=now).update(
            lock_until_ms=now + LOCK_TTL_MS
        )
        == 1
    )


def begin_send(rid):
    """attempts += 1, status=sending. Returns (new_attempts, sent_flag,
    enqueued_first_ms). Read-then-update is race-free under the intent lock
    (only the lock holder mutates these fields)."""
    row = (
        Recipient.objects.filter(rid=rid)
        .values("attempts", "sent_flag", "enqueued_first_ms")
        .first()
    )
    Recipient.objects.filter(rid=rid).update(
        attempts=F("attempts") + 1, status="sending"
    )
    return row["attempts"] + 1, row["sent_flag"], row["enqueued_first_ms"]


def record_send_attempt(cid, rid):
    """Tripwire before an actual POST: atomic UPDATE WHERE sent_flag=false.
    False = flag already up, duplicates counter incremented, caller MUST NOT
    send."""
    ok = (
        Recipient.objects.filter(rid=rid, sent_flag=False).update(
            last_attempt_ms=now_ms()
        )
        == 1
    )
    if not ok:
        redis_store.conn().incr(redis_store.k_dup(cid))
    return ok


def set_sent_flag(rid):
    Recipient.objects.filter(rid=rid).update(sent_flag=True)


# ------------------------------------------------------------------ marking --
def mark_results(cid, results):
    """Grouped outcome application: one transaction per batch, one UPDATE per
    row (the ORM twin of store.mark_results' single redis pipeline)."""
    already = 0
    with transaction.atomic():
        for res in results:
            o = res["outcome"]
            if o == "lock_skip":
                continue
            qs = Recipient.objects.filter(rid=res["rid"])
            att = res.get("attempts", 0)
            if o == "sent":
                qs.update(
                    status="sent",
                    attempts=att,
                    sent_at_ms=res["sent_at_ms"],
                    lease_until_ms=0,
                    lock_until_ms=0,
                )
            elif o == "already_sent":
                qs.update(status="sent", lease_until_ms=0, lock_until_ms=0)
                already += 1
            elif o == "retry":
                qs.update(
                    status="retry",
                    attempts=att,
                    next_due_ms=res["next_due_ms"],
                    lease_until_ms=0,
                    lock_until_ms=0,
                )
            elif o in ("failed", "skipped"):
                qs.update(status=o, attempts=att, lease_until_ms=0, lock_until_ms=0)
            else:
                raise ValueError(f"unknown outcome {o!r}")
    if already:
        redis_store.incr_counter(cid, "already_sent", already)


# ------------------------------------------------------------------ queries --
def campaign_total(cid):
    return (
        Campaign.objects.filter(cid=cid)
        .values_list("total_recipients", flat=True)
        .first()
        or 0
    )


def active_campaigns():
    return max(1, Campaign.objects.filter(status="active").count())

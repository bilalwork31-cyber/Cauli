"""ORM models mirroring what the campaign scenario-C variant stores.

Recipient is the Django twin of the redis hash in bench/campaign/store.py
(status/lease/attempts) plus the idempotency fields that lived as separate
redis keys there: sent_flag (send:sent:*), lock_until_ms (send:lock:*, the
intent lock, now a conditional-UPDATE row lock) and last_attempt_ms (the
tripwire stamp). Millisecond epoch bigints are kept instead of DateTimeFields
so the metric math stays byte-identical to the campaign C suite.

SendLog mirrors the scenario-C `messages` Postgres table (stage-2 persist
target, unique recipient = ON CONFLICT DO NOTHING dedup).

WebhookInboxItem / BackfillJob are the ORM homes for the bgfill_common
components (webhook inbox drain rows, ghost backfill pacing job rows).
"""

from django.db import models

STATUS_CHOICES = [
    ("pending", "pending"),
    ("retry", "retry"),
    ("queued", "queued"),
    ("sending", "sending"),
    ("sent", "sent"),
    ("failed", "failed"),
    ("skipped", "skipped"),
]


class Campaign(models.Model):
    cid = models.CharField(max_length=16, primary_key=True)
    name = models.CharField(max_length=64, default="")
    status = models.CharField(max_length=12, default="active", db_index=True)
    total_recipients = models.IntegerField(default=0)
    n_pages = models.IntegerField(default=0)
    seeded_at_ms = models.BigIntegerField(default=0)
    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return self.cid


class Recipient(models.Model):
    rid = models.CharField(max_length=32, primary_key=True)
    campaign = models.ForeignKey(
        Campaign, on_delete=models.CASCADE, related_name="recipients"
    )
    page_id = models.CharField(max_length=16)
    status = models.CharField(max_length=12, choices=STATUS_CHOICES, default="pending")
    attempts = models.SmallIntegerField(default=0)
    next_due_ms = models.BigIntegerField(default=0)
    lease_until_ms = models.BigIntegerField(default=0)
    enqueued_first_ms = models.BigIntegerField(default=0)
    sent_at_ms = models.BigIntegerField(default=0)
    # idempotency / double-send guards (redis keys in the C variant)
    sent_flag = models.BooleanField(default=False)
    lock_until_ms = models.BigIntegerField(default=0)
    last_attempt_ms = models.BigIntegerField(default=0)

    class Meta:
        indexes = [
            models.Index(
                fields=["campaign", "status", "next_due_ms"], name="recip_claim_due_idx"
            ),
            models.Index(
                fields=["campaign", "status", "lease_until_ms"],
                name="recip_claim_lease_idx",
            ),
            models.Index(fields=["sent_flag"], name="recip_sentflag_idx"),
            models.Index(fields=["sent_at_ms"], name="recip_sentat_idx"),
        ]

    def __str__(self):
        return self.rid


class SendLog(models.Model):
    recipient_rid = models.CharField(max_length=32, unique=True)
    campaign_cid = models.CharField(max_length=16)
    page_id = models.CharField(max_length=16)
    message_id = models.CharField(max_length=64)
    sent_at_ms = models.BigIntegerField(default=0)
    enqueued_first_ms = models.BigIntegerField(default=0)

    class Meta:
        indexes = [
            models.Index(fields=["sent_at_ms"], name="sendlog_sentat_idx"),
        ]

    def __str__(self):
        return self.recipient_rid


class WebhookInboxItem(models.Model):
    item_key = models.CharField(max_length=32, unique=True)
    status = models.CharField(
        max_length=12, default="pending"
    )  # pending|processing|done|dead
    attempts = models.SmallIntegerField(default=0)
    next_due_ms = models.BigIntegerField(default=0)
    payload = models.JSONField(default=dict)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        indexes = [
            models.Index(fields=["status", "next_due_ms"], name="webhook_claim_idx"),
        ]

    def __str__(self):
        return self.item_key


class BackfillJob(models.Model):
    worker = models.CharField(max_length=64, default="")
    started_ms = models.BigIntegerField(default=0)
    pages_fetched = models.IntegerField(default=0)
    errors = models.IntegerField(default=0)
    finished = models.BooleanField(default=False)

    def __str__(self):
        return f"backfill:{self.pk}"

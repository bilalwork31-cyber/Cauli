"""RabbitMQ and SQS publishers for the ready queue (PROTOCOL.md section 14).

Redis stays the store for results, delayed tasks, idempotency claims and beat.
Only an enqueue that is due now goes to the broker, and a publish is at least
once: a connection lost mid publish is retried and can deliver twice.
"""

from __future__ import annotations

import os
import threading
from typing import Any, Sequence
from urllib.parse import unquote, urlsplit

#: Seconds for a connect, a publish and its confirm; the worker's
#: ``--redis-timeout`` default.
PUBLISH_TIMEOUT = 5
#: Declared by every producer and worker, so both sides must send the same.
AMQP_QUEUE_ARGUMENTS = {"x-queue-type": "quorum"}
#: SendMessageBatch limits: entries per call, and bytes over the whole call.
SQS_BATCH_ENTRIES = 10
SQS_BATCH_BYTES = 1_048_576
SQS_QUEUE_NAME_MAX = 80

_SCHEMES = ("amqp", "amqps", "sqs")


def amqp_queue_name(queue: str) -> str:
    return f"cauli.q.{queue}"


def sqs_queue_name(queue: str) -> str:
    name = f"cauli-{queue}"
    if "." in queue or len(name) > SQS_QUEUE_NAME_MAX:
        raise ValueError(
            f"queue {queue!r} cannot be an SQS queue: SQS names allow letters, "
            f"digits, '-' and '_' only, and {name!r} must fit in "
            f"{SQS_QUEUE_NAME_MAX} characters"
        )
    return name


def check_url(url: str) -> None:
    """Raise ValueError for a broker URL no publisher here can open."""
    parts = urlsplit(url)
    if parts.scheme not in _SCHEMES:
        raise ValueError(
            f"broker_url scheme {parts.scheme!r} is not supported: use amqp://, "
            "amqps:// or sqs://. Redis is the broker when broker_url is unset "
            "(configure it with redis_url)"
        )
    if parts.scheme == "sqs" and (
        parts.username or parts.password or parts.path.strip("/")
    ):
        raise ValueError(
            "an sqs:// broker_url is sqs:// or sqs://<region> and nothing else. "
            "Credentials come from the AWS credential chain (environment, "
            "profile, instance or task role), never from the URL"
        )


def open_broker(url: str) -> "AmqpBroker | SqsBroker":
    check_url(url)
    return SqsBroker(url) if url.startswith("sqs:") else AmqpBroker(url)


def redact_url(url: str) -> str:
    """The URL without its userinfo, for logs and repr."""
    parts = urlsplit(url)
    if parts.username is None:
        return url
    host = parts.hostname or ""
    port = f":{parts.port}" if parts.port else ""
    return f"{parts.scheme}://***@{host}{port}{parts.path}"


class AmqpBroker:
    """Confirmed, persistent publishes to quorum queues over one connection.

    A batch is written first and its confirms read after, which is what makes
    a large ``enqueue_many`` cost one round of fsyncs instead of one each.
    py-amqp connections are not thread safe, so a lock serializes publishers.
    A forked child opens its own connection: the inherited socket is the
    parent's.
    """

    def __init__(self, url: str) -> None:
        parts = urlsplit(url)
        self._tls = parts.scheme == "amqps"
        self._host = parts.hostname or "localhost"
        self._port = parts.port or (5671 if self._tls else 5672)
        self._user = unquote(parts.username or "guest")
        self._password = unquote(parts.password or "guest")
        self._vhost = unquote(parts.path[1:]) or "/"
        self._lock = threading.Lock()
        self._connection: Any = None
        self._channel: Any = None
        self._declared: set[str] = set()
        self._pid = 0
        self._next_tag = 1
        # Delivery tag to message index, for publishes awaiting a confirm.
        self._unconfirmed: dict[int, int] = {}
        # Message indexes of the current call with no confirm yet, sent or not.
        self._outstanding: set[int] = set()
        self._nacked = 0

    def _open(self) -> None:
        try:
            import amqp
        except ImportError as exc:
            raise ImportError(
                "an amqp:// broker_url needs py-amqp: pip install 'cauli[rabbitmq]'"
            ) from exc
        connection = amqp.Connection(
            host=f"{self._host}:{self._port}",
            userid=self._user,
            password=self._password,
            virtual_host=self._vhost,
            # Verifies the certificate and the hostname against the system CAs.
            ssl={"server_hostname": self._host} if self._tls else False,
            connect_timeout=PUBLISH_TIMEOUT,
            read_timeout=PUBLISH_TIMEOUT,
            write_timeout=PUBLISH_TIMEOUT,
        )
        connection.connect()
        channel = connection.channel()
        channel.confirm_select()
        channel.events["basic_ack"].add(self._on_ack)
        channel.events["basic_nack"].add(self._on_nack)
        self._connection, self._channel = connection, channel
        self._declared = set()
        self._pid = os.getpid()
        self._next_tag = 1
        self._unconfirmed = {}

    def _settle(self, delivery_tag: int, multiple: bool) -> int:
        """Forget the publishes a confirm covers; returns how many."""
        tags = (
            [tag for tag in self._unconfirmed if tag <= delivery_tag]
            if multiple
            else [delivery_tag]
        )
        settled = [
            self._unconfirmed.pop(tag) for tag in tags if tag in self._unconfirmed
        ]
        self._outstanding.difference_update(settled)
        return len(settled)

    def _on_ack(self, delivery_tag: int, multiple: bool) -> None:
        self._settle(delivery_tag, multiple)

    def _on_nack(self, delivery_tag: int, multiple: bool) -> None:
        self._nacked += self._settle(delivery_tag, multiple)

    def _publish_and_confirm(
        self, messages: Sequence[tuple[str, bytes]], indexes: list[int]
    ) -> None:
        import amqp

        for index in indexes:
            queue, raw = messages[index]
            name = amqp_queue_name(queue)
            if name not in self._declared:
                self._channel.queue_declare(
                    queue=name,
                    durable=True,
                    auto_delete=False,
                    arguments=AMQP_QUEUE_ARGUMENTS,
                )
                self._declared.add(name)
            self._unconfirmed[self._next_tag] = index
            self._next_tag += 1
            self._channel.basic_publish(
                amqp.Message(raw, delivery_mode=2, content_type="application/json"),
                exchange="",
                routing_key=name,
                mandatory=True,
            )
        while self._unconfirmed:
            self._connection.drain_events(timeout=PUBLISH_TIMEOUT)

    def publish_many(self, messages: Sequence[tuple[str, bytes]]) -> None:
        """Publish ``(queue, envelope)`` pairs and wait for every confirm.

        A connection that fails is reopened once and the unconfirmed
        messages are sent again, so one of them may be delivered twice.
        """
        import amqp

        with self._lock:
            self._outstanding = set(range(len(messages)))
            self._nacked = 0
            for attempt in (1, 2):
                try:
                    if self._channel is None or self._pid != os.getpid():
                        self._open()
                    self._publish_and_confirm(messages, sorted(self._outstanding))
                    break
                except (OSError, amqp.exceptions.ConnectionError):
                    self._channel = None
                    if attempt == 2:
                        raise
                except BaseException:
                    # A channel error closes the channel on the server side.
                    self._channel = None
                    raise
            if self._nacked:
                raise RuntimeError(
                    f"RabbitMQ refused {self._nacked} of {len(messages)} publishes "
                    "(a nack: a queue is over its length limit or the broker "
                    "could not persist the message). Those tasks were not enqueued"
                )


class SqsBroker:
    """SendMessageBatch to standard queues through boto3."""

    def __init__(self, url: str) -> None:
        self._region = urlsplit(url).hostname or None
        self._lock = threading.Lock()
        self._client: Any = None
        self._queue_urls: dict[str, str] = {}
        self._pid = 0

    def _sqs(self) -> Any:
        if self._client is None or self._pid != os.getpid():
            with self._lock:
                if self._client is None or self._pid != os.getpid():
                    self._client = self._new_client()
                    self._queue_urls = {}
                    self._pid = os.getpid()
        return self._client

    def _new_client(self) -> Any:
        try:
            import boto3
            from botocore.config import Config
        except ImportError as exc:
            raise ImportError(
                "an sqs:// broker_url needs boto3: pip install 'cauli[sqs]'"
            ) from exc
        return boto3.client(
            "sqs",
            region_name=self._region,
            config=Config(
                connect_timeout=PUBLISH_TIMEOUT,
                read_timeout=PUBLISH_TIMEOUT,
                retries={"mode": "standard", "max_attempts": 3},
            ),
        )

    def _queue_url(self, client: Any, queue: str) -> str:
        name = sqs_queue_name(queue)
        url = self._queue_urls.get(name)
        if url is None:
            try:
                url = client.get_queue_url(QueueName=name)["QueueUrl"]
            except client.exceptions.QueueDoesNotExist:
                url = client.create_queue(QueueName=name)["QueueUrl"]
            self._queue_urls[name] = url
        return url

    def publish_many(self, messages: Sequence[tuple[str, bytes]]) -> None:
        """Send ``(queue, envelope)`` pairs, batched per queue within the SQS limits."""
        client = self._sqs()
        batch: list[dict[str, str]] = []
        batch_queue = ""
        batch_bytes = 0
        for queue, raw in messages:
            if batch and (
                queue != batch_queue
                or len(batch) == SQS_BATCH_ENTRIES
                or batch_bytes + len(raw) > SQS_BATCH_BYTES
            ):
                self._send_batch(client, batch_queue, batch)
                batch, batch_bytes = [], 0
            batch_queue = queue
            batch_bytes += len(raw)
            batch.append({"Id": str(len(batch)), "MessageBody": raw.decode("utf-8")})
        if batch:
            self._send_batch(client, batch_queue, batch)

    def _send_batch(
        self, client: Any, queue: str, entries: list[dict[str, str]]
    ) -> None:
        reply = client.send_message_batch(
            QueueUrl=self._queue_url(client, queue), Entries=entries
        )
        failed = reply.get("Failed")
        if failed:
            first = failed[0]
            raise RuntimeError(
                f"SQS refused {len(failed)} of {len(entries)} messages for queue "
                f"{sqs_queue_name(queue)!r}: {first.get('Code')}: {first.get('Message')}. "
                "Those tasks were not enqueued"
            )

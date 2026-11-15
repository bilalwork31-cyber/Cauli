"""RabbitMQ and SQS as the ready queue (PROTOCOL.md section 14), client side.

The routing tests use a recording stand in for the publisher. The publisher
tests need a real server and skip without one:

    CAULI_TEST_AMQP_URL=amqp://guest:guest@127.0.0.1:5672/
    CAULI_TEST_SQS_ENDPOINT=http://127.0.0.1:9324   (ElasticMQ or LocalStack)
"""

from __future__ import annotations

import asyncio
import json
import os
import uuid

import pytest

from cauli import AsyncCauli, Cauli, _broker

AMQP_URL = os.environ.get("CAULI_TEST_AMQP_URL")
SQS_ENDPOINT = os.environ.get("CAULI_TEST_SQS_ENDPOINT")


class RecordingBroker:
    def __init__(self):
        self.calls = []

    def publish_many(self, messages):
        self.calls.append(list(messages))


def _with_broker(app):
    app.broker_url = "amqp://guest:guest@broker.invalid/"
    app._broker = RecordingBroker()
    return app._broker


@pytest.mark.parametrize(
    "url",
    ["amqp://u:p@host:5672/vhost", "amqps://host", "sqs://", "sqs://eu-west-1"],
)
def test_supported_urls_are_accepted(url):
    _broker.check_url(url)


@pytest.mark.parametrize(
    "url, needle",
    [
        ("redis://localhost:6379/0", "redis_url"),
        ("kafka://host", "not supported"),
        ("host:5672", "not supported"),
        ("sqs://AKIA:secret@us-east-1", "credential chain"),
        ("sqs://us-east-1/my-queue", "sqs://<region>"),
    ],
)
def test_unsupported_urls_fail_at_construction(redis_url, url, needle):
    with pytest.raises(ValueError, match=needle):
        Cauli(redis_url=redis_url, broker_url=url)


def test_the_url_comes_from_the_environment_when_not_passed(redis_url, monkeypatch):
    monkeypatch.setenv("CAULI_BROKER_URL", "sqs://eu-west-1")
    assert Cauli(redis_url=redis_url).broker_url == "sqs://eu-west-1"
    assert Cauli(redis_url=redis_url, broker_url="amqp://h/").broker_url == "amqp://h/"
    monkeypatch.delenv("CAULI_BROKER_URL")
    assert Cauli(redis_url=redis_url).broker_url is None


def test_repr_never_shows_the_broker_password(redis_url):
    app = Cauli(
        redis_url=redis_url, broker_url="amqp://svc:hunter2@mq.internal:5672/prod"
    )
    assert "hunter2" not in repr(app)
    assert "amqp://***@mq.internal:5672/prod" in repr(app)


def test_sqs_queue_names_are_validated():
    assert _broker.sqs_queue_name("emails") == "cauli-emails"
    with pytest.raises(ValueError, match="cannot be an SQS queue"):
        _broker.sqs_queue_name("emails.high")
    with pytest.raises(ValueError, match="80 characters"):
        _broker.sqs_queue_name("q" * 75)


def test_a_task_due_now_goes_to_the_broker_and_not_to_redis(app, redis_client):
    broker = _with_broker(app)

    @app.task(name="t")
    def t(x):
        return x

    result = t.delay(1)

    [[(queue, raw)]] = broker.calls
    assert queue == "default"
    assert json.loads(raw)["id"] == result.id
    assert redis_client.exists("cauli:q:default") == 0


def test_a_delayed_task_stays_in_redis_for_the_worker_to_publish(app, redis_client):
    broker = _with_broker(app)

    @app.task(name="t")
    def t(x):
        return x

    t.apply_async(args=(1,), countdown=30)

    assert broker.calls == []
    assert redis_client.zcard("cauli:delayed:default") == 1


def test_enqueue_many_splits_a_batch_by_destination(app, redis_client):
    broker = _with_broker(app)

    @app.task(name="t")
    def t(x):
        return x

    results = app.enqueue_many(
        [(t, (1,)), (t, (2,), {}, {"countdown": 30}), (t, (3,), {}, {"queue": "bulk"})]
    )

    [published] = broker.calls
    assert [queue for queue, _raw in published] == ["default", "bulk"]
    assert [json.loads(raw)["id"] for _q, raw in published] == [
        results[0].id,
        results[2].id,
    ]
    [delayed] = redis_client.zrange("cauli:delayed:default", 0, -1)
    assert json.loads(delayed)["id"] == results[1].id
    assert redis_client.exists("cauli:q:default", "cauli:q:bulk") == 0


def test_the_async_calls_route_the_same_way(redis_url, redis_client):
    app = AsyncCauli(redis_url=redis_url)
    broker = _with_broker(app)

    @app.task(name="t")
    async def t(x):
        return x

    async def run():
        now = await t.adelay(1)
        later = await t.aapply_async(args=(2,), countdown=30)
        many = await app.aenqueue_many([(t, (3,)), (t, (4,), {}, {"countdown": 30})])
        await app.aclose()
        return now, later, many

    now, later, many = asyncio.run(run())

    published = [json.loads(raw)["id"] for call in broker.calls for _q, raw in call]
    assert published == [now.id, many[0].id]
    delayed = {
        json.loads(raw)["id"]
        for raw in redis_client.zrange("cauli:delayed:default", 0, -1)
    }
    assert delayed == {later.id, many[1].id}


def test_a_failed_publish_raises_at_the_call_site(app):
    class Refusing:
        def publish_many(self, messages):
            raise RuntimeError("broker said no")

    app.broker_url = "amqp://guest:guest@broker.invalid/"
    app._broker = Refusing()

    @app.task(name="t")
    def t():
        return None

    with pytest.raises(RuntimeError, match="broker said no"):
        t.delay()


class RecordingSqsClient:
    """Stands in for boto3's client: records every SendMessageBatch."""

    def __init__(self):
        self.batches = []

    def get_queue_url(self, QueueName):
        return {"QueueUrl": f"https://sqs.invalid/{QueueName}"}

    def send_message_batch(self, QueueUrl, Entries):
        self.batches.append((QueueUrl, Entries))
        return {}


def _sqs_with(client):
    broker = _broker.SqsBroker("sqs://us-east-1")
    broker._client, broker._pid = client, os.getpid()
    return broker


def test_sqs_batches_stay_within_ten_entries():
    client = RecordingSqsClient()
    _sqs_with(client).publish_many([("q", b'{"n":%d}' % n) for n in range(25)])

    assert [len(entries) for _url, entries in client.batches] == [10, 10, 5]
    sent = [
        json.loads(e["MessageBody"])["n"] for _url, es in client.batches for e in es
    ]
    assert sent == list(range(25))


def test_sqs_batches_stay_within_the_byte_limit_of_one_call():
    client = RecordingSqsClient()
    big = b'{"pad":"' + b"x" * 400_000 + b'"}'
    _sqs_with(client).publish_many([("q", big)] * 5)

    sizes = [
        sum(len(e["MessageBody"]) for e in entries) for _url, entries in client.batches
    ]
    assert sizes == [2 * len(big), 2 * len(big), len(big)]
    assert max(sizes) <= _broker.SQS_BATCH_BYTES


def test_sqs_entry_ids_are_unique_within_a_batch():
    client = RecordingSqsClient()
    _sqs_with(client).publish_many([("q", b"{}")] * 10)

    [(_url, entries)] = client.batches
    assert len({entry["Id"] for entry in entries}) == 10


def test_a_message_sqs_refuses_raises_and_names_the_reason():
    class Refusing(RecordingSqsClient):
        def send_message_batch(self, QueueUrl, Entries):
            return {
                "Failed": [
                    {"Id": "0", "Code": "InvalidMessageContents", "Message": "bad char"}
                ]
            }

    with pytest.raises(RuntimeError, match="InvalidMessageContents: bad char"):
        _sqs_with(Refusing()).publish_many([("q", b"{}")])


def _amqp_drain(queue):
    """Every message body on a RabbitMQ queue, acked."""
    import amqp

    broker = _broker.AmqpBroker(AMQP_URL)
    connection = amqp.Connection(
        host=f"{broker._host}:{broker._port}",
        userid=broker._user,
        password=broker._password,
        virtual_host=broker._vhost,
    )
    connection.connect()
    channel = connection.channel()
    bodies = []
    while True:
        message = channel.basic_get(_broker.amqp_queue_name(queue), no_ack=True)
        if message is None:
            break
        bodies.append(message.body)
    connection.close()
    return bodies


@pytest.mark.skipif(AMQP_URL is None, reason="CAULI_TEST_AMQP_URL is not set")
class TestAmqpBroker:
    def test_a_batch_arrives_whole_and_in_order(self):
        queue = f"t{uuid.uuid4().hex[:12]}"
        broker = _broker.open_broker(AMQP_URL)

        broker.publish_many([(queue, b'{"n":%d}' % n) for n in range(500)])

        assert [json.loads(b)["n"] for b in _amqp_drain(queue)] == list(range(500))

    def test_the_queue_is_a_durable_quorum_queue(self):
        import amqp

        queue = f"t{uuid.uuid4().hex[:12]}"
        broker = _broker.open_broker(AMQP_URL)
        broker.publish_many([(queue, b"{}")])

        # A classic or transient declare of the same name must be refused.
        with pytest.raises(amqp.exceptions.PreconditionFailed):
            broker._channel.queue_declare(
                queue=_broker.amqp_queue_name(queue), durable=True, auto_delete=False
            )

    def test_a_dead_connection_is_replaced_and_the_publish_lands(self):
        queue = f"t{uuid.uuid4().hex[:12]}"
        broker = _broker.open_broker(AMQP_URL)
        broker.publish_many([(queue, b'{"n":0}')])
        broker._connection.transport.sock.close()

        broker.publish_many([(queue, b'{"n":1}')])

        assert [json.loads(b)["n"] for b in _amqp_drain(queue)] == [0, 1]

    def test_a_forked_child_does_not_publish_on_the_parents_socket(self):
        queue = f"t{uuid.uuid4().hex[:12]}"
        broker = _broker.open_broker(AMQP_URL)
        broker.publish_many([(queue, b'{"who":"parent"}')])

        pid = os.fork()
        if pid == 0:
            try:
                broker.publish_many([(queue, b'{"who":"child"}')])
            finally:
                os._exit(0)
        os.waitpid(pid, 0)
        broker.publish_many([(queue, b'{"who":"parent again"}')])

        assert [json.loads(b)["who"] for b in _amqp_drain(queue)] == [
            "parent",
            "child",
            "parent again",
        ]

    def test_an_unreachable_broker_raises_within_the_timeout(self):
        broker = _broker.open_broker("amqp://guest:guest@127.0.0.1:1/")
        with pytest.raises(OSError):
            broker.publish_many([("q", b"{}")])


@pytest.fixture()
def sqs_env(monkeypatch):
    monkeypatch.setenv("AWS_ENDPOINT_URL_SQS", SQS_ENDPOINT)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")


def _sqs_drain(broker, queue):
    client = broker._sqs()
    url = broker._queue_url(client, queue)
    bodies = []
    while True:
        reply = client.receive_message(QueueUrl=url, MaxNumberOfMessages=10)
        messages = reply.get("Messages", [])
        if not messages:
            return bodies
        for message in messages:
            bodies.append(message["Body"])
            client.delete_message(QueueUrl=url, ReceiptHandle=message["ReceiptHandle"])


@pytest.mark.skipif(SQS_ENDPOINT is None, reason="CAULI_TEST_SQS_ENDPOINT is not set")
class TestSqsBroker:
    def test_a_batch_larger_than_one_call_arrives_whole(self, sqs_env):
        queue = f"t{uuid.uuid4().hex[:12]}"
        broker = _broker.open_broker("sqs://us-east-1")

        broker.publish_many([(queue, b'{"n":%d}' % n) for n in range(35)])

        assert sorted(json.loads(b)["n"] for b in _sqs_drain(broker, queue)) == list(
            range(35)
        )

    def test_queues_are_kept_apart(self, sqs_env):
        first, second = (f"t{uuid.uuid4().hex[:12]}" for _ in range(2))
        broker = _broker.open_broker("sqs://us-east-1")

        broker.publish_many(
            [(first, b'{"q":1}'), (second, b'{"q":2}'), (first, b'{"q":1}')]
        )

        assert [json.loads(b)["q"] for b in _sqs_drain(broker, first)] == [1, 1]
        assert [json.loads(b)["q"] for b in _sqs_drain(broker, second)] == [2]

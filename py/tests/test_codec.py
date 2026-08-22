"""cauli._codec: roundtrip fidelity, and the rejections msgspec does NOT make
on its own.

msgspec is the only backend (there is no stdlib fallback), and it is more
permissive than the protocol: left alone it encodes NaN/Infinity as null,
accepts set and bytes, and coerces int dict keys. The tests below therefore
mostly pin down `_validate_json_types`, which is what actually enforces the
JSON type set.
"""

from __future__ import annotations

import math

import pytest

# A representative section-2 envelope: every field type the client produces.
ENVELOPE = {
    "v": 1,
    "id": "a" * 32,
    "task": "myapp.tasks.send_email",
    "args": [1, "a@b.com", 2.5, None, True, {"nested": [1, 2]}],
    "kwargs": {"k": True, "text": "café — unicode"},
    "queue": "default",
    "kind": "io",
    "retries": 0,
    "max_retries": 3,
    "backoff_base_ms": 500,
    "backoff_factor": 2.0,
    "backoff_max_ms": 60000,
    "jitter": True,
    "timeout_ms": 300000,
    "soft_timeout_ms": None,
    "idempotency_key": None,
    "store_result": True,
    "enqueued_at": 1721471234567,
    "not_before": None,
}


@pytest.fixture
def codec():
    """The codec module. Single backend: msgspec is a hard requirement, so
    there is no parameterization and no skip — if it is missing, importing
    cauli fails outright and that is the intended behavior."""
    import cauli._codec

    assert cauli._codec.backend == "msgspec"
    return cauli._codec


def test_envelope_roundtrip(codec):
    encoded = codec.encode(ENVELOPE)
    assert isinstance(encoded, (bytes, str))
    assert codec.decode(encoded) == ENVELOPE
    # compact separators: no spaces after , or :
    text = encoded.decode("utf-8") if isinstance(encoded, bytes) else encoded
    assert ", " not in text and ": " not in text
    # UTF-8, not \u escapes, for non-ASCII text on both backends
    assert "café" in text


def test_encode_str_always_text(codec):
    line = codec.encode_str({"ready": True, "pid": 1234})
    assert isinstance(line, str)
    assert codec.decode(line) == {"ready": True, "pid": 1234}


def test_nan_and_infinity_rejected(codec):
    for bad in (float("nan"), float("inf"), float("-inf")):
        with pytest.raises(ValueError):
            codec.encode({"x": bad})
        with pytest.raises(ValueError):
            codec.encode([1, [2, [bad]]])


def test_non_json_types_rejected(codec):
    # msgspec would happily serialize sets/datetimes; stdlib raises TypeError.
    # Both backends must reject them identically (PROTOCOL section 8: a non
    # serializable value is a SerializationError, not a silent coercion).
    import datetime

    for bad in ({1, 2}, datetime.datetime(2020, 1, 1), object(), b"bytes"):
        with pytest.raises(TypeError):
            codec.encode({"x": bad})


def test_decode_accepts_bytes_and_str(codec):
    assert codec.decode(b'{"a": 1}') == {"a": 1}
    assert codec.decode('{"a": 1}') == {"a": 1}
    assert codec.decode(b"[1,2.5,null,true]") == [1, 2.5, None, True]


def test_decode_malformed_raises_decode_errors(codec):
    for garbage in ("{not json", "", '{"a":'):
        with pytest.raises(codec.DECODE_ERRORS):
            codec.decode(garbage)


def test_encode_output_is_valid_utf8_json(codec):
    import json

    encoded = codec.encode(ENVELOPE)
    raw = encoded if isinstance(encoded, bytes) else encoded.encode("utf-8")
    assert json.loads(raw.decode("utf-8")) == ENVELOPE


def test_scalars_and_bignums(codec):
    for value in (0, -1, 2**80, 1.5, "", "x", True, False, None, [], {}):
        assert codec.decode(codec.encode(value)) == value
    assert math.isclose(codec.decode(codec.encode(1e308)), 1e308)


def test_encode_errors_covers_raw_msgspec_encode_failure(codec):
    """`msgspec.EncodeError` does NOT subclass ValueError (its MRO is
    EncodeError -> MsgspecError -> Exception), unlike `msgspec.DecodeError`
    which does. A call site catching only (TypeError, ValueError) would let a
    raw encode failure escape, so ENCODE_ERRORS must name MsgspecError."""
    import msgspec

    assert not issubclass(msgspec.EncodeError, (ValueError, TypeError))
    assert issubclass(msgspec.EncodeError, codec.ENCODE_ERRORS)
    assert issubclass(msgspec.DecodeError, codec.DECODE_ERRORS)


def test_validator_catches_what_msgspec_would_wave_through(codec):
    """msgspec is more permissive than the protocol. Without the validation
    walk these would silently reach the wire: NaN/Infinity as `null` (a
    corrupted value, not an error), `set` as an array, and `bytes` as base64.
    Each must raise instead."""
    import msgspec.json as mj

    raw = mj.Encoder()
    # Confirm the premise: bare msgspec really does accept all of these.
    assert raw.encode({"a": float("nan")}) == b'{"a":null}'
    assert raw.encode({"a": {1, 2}}) in (b'{"a":[1,2]}', b'{"a":[2,1]}')
    assert raw.encode({"a": b"x"}) == b'{"a":"eA=="}'
    # The codec must not.
    for bad in ({"a": float("nan")}, {"a": {1, 2}}, {"a": b"x"}):
        with pytest.raises(codec.ENCODE_ERRORS):
            codec.encode(bad)


def test_lone_surrogate_rejected(codec):
    """CD-1: a result carrying an unpaired surrogate (e.g. from
    surrogateescape filename decoding) must fail at encode, inside the
    guarded region, rather than at some later transport boundary."""
    with pytest.raises(codec.ENCODE_ERRORS):
        codec.encode({"a": "\ud800"})


def test_non_str_dict_keys_rejected_identically(codec):
    """CD-3: msgspec and the stdlib do NOT agree on non-str dict key
    handling -- the stdlib silently coerces (bool -> "true"/"false", etc.)
    but msgspec rejects some of those outright (verified empirically: a
    bool key raises "Only dicts with str-like or number-like keys are
    supported" there). Replicating the stdlib's exact coercion for msgspec
    would need a full tree rebuild on every encode for an edge case with no
    real caller (task args/kwargs keys are always str already). Simpler and
    safer: both backends must reject a non-str key the same way, rather
    than one silently succeeding where the other raises."""
    for key in (1, 2.5, True, False, None):
        with pytest.raises(TypeError):
            codec.encode({key: "v"})


def test_rejection_names_the_offending_argument(codec):
    """The whole complaint about `send_receipt.delay(order.id)` on a UUID pk
    was that the error named the type and nothing else. The walk now records
    the key/index of every container it unwinds through, so the message points
    at the argument."""
    import datetime
    import decimal
    import uuid

    for bad in (uuid.uuid4(), datetime.datetime(2020, 1, 1), decimal.Decimal("1.5")):
        with pytest.raises(TypeError) as info:
            codec.encode({"args": [bad], "kwargs": {}})
        message = str(info.value)
        assert message.startswith("args[0]: ")
        assert type(bad).__name__ in message
        # and it says what the wire format WOULD take
        assert "allowed types:" in message


def test_path_walks_nested_containers(codec):
    with pytest.raises(TypeError) as info:
        codec.encode({"kwargs": {"meta": {"tags": [1, {2, 3}]}}})
    assert str(info.value).startswith("kwargs['meta']['tags'][1]: ")

    with pytest.raises(ValueError) as info:
        codec.encode({"args": [0, [1, [float("inf")]]]})
    assert str(info.value).startswith("args[1][1][0]: ")


def test_non_str_dict_key_path_points_at_the_containing_dict(codec):
    """The bad key is not itself a traversable segment, so the path stops at
    the dict that holds it."""
    with pytest.raises(TypeError) as info:
        codec.encode({"kwargs": {"meta": {1: "v"}}})
    assert str(info.value) == "kwargs['meta']: dict keys must be str, got int"


def test_top_level_rejection_keeps_its_bare_message(codec):
    """Nothing was traversed, so there is no path to prepend and the message
    must not grow a stray `: ` prefix."""
    with pytest.raises(TypeError) as info:
        codec.encode(object())
    assert str(info.value).startswith("Object of type object is not JSON serializable")

    with pytest.raises(ValueError) as info:
        codec.encode(float("nan"))
    assert str(info.value) == "Out of range float values are not JSON compliant"


def test_path_annotation_survives_the_envelope_shape(codec):
    """The real call site encodes the whole envelope, not just the args, so
    the outermost segment is an envelope field and reads bare."""
    envelope = dict(ENVELOPE)
    envelope["args"] = [1, {"attachment": b"pdf"}]
    with pytest.raises(TypeError) as info:
        codec.encode(envelope)
    assert str(info.value).startswith("args[1]['attachment']: ")


def test_cyclic_payload_still_raises_recursion_error(codec):
    """RecursionError is deliberately not annotated with a path (there is no
    finite one), and must still reach the caller as a member of
    ENCODE_ERRORS rather than being swallowed by the annotation handler."""
    loop: list = [1]
    loop.append(loop)
    with pytest.raises(RecursionError):
        codec.encode({"args": loop})

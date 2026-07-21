"""cauli._codec: both backends must agree on accept/reject and roundtrip."""

from __future__ import annotations

import importlib
import importlib.util
import math
import sys

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


def _reload_codec(monkeypatch, disable: bool):
    if disable:
        monkeypatch.setenv("CAULI_DISABLE_MSGSPEC", "1")
    else:
        monkeypatch.delenv("CAULI_DISABLE_MSGSPEC", raising=False)
    import cauli._codec

    return importlib.reload(cauli._codec)


@pytest.fixture(params=["msgspec", "json"])
def codec(request, monkeypatch):
    """The codec module under each backend; restores the ambient one after."""
    if request.param == "msgspec" and importlib.util.find_spec("msgspec") is None:
        pytest.skip("msgspec not installed")
    mod = _reload_codec(monkeypatch, disable=(request.param == "json"))
    assert mod.backend == request.param
    yield mod
    # Re-import under the ORIGINAL environment so later tests (and the rest
    # of this session's cauli modules) see the ambient backend again.
    monkeypatch.undo()
    importlib.reload(sys.modules["cauli._codec"])


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

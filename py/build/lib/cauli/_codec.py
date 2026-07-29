"""cauli._codec: JSON encode/decode, backed by msgspec.

The wire format is plain JSON (PROTOCOL.md sections 2, 5.1, 8); this module
only decides HOW that JSON is produced and parsed, never WHAT crosses the
wire.

msgspec is a hard requirement, not an accelerator. There is deliberately no
stdlib ``json`` fallback: a fallback means the slow path can silently survive
in production, and — worse here — the two backends did not agree. They
disagreed on lone surrogates, on dict-key coercion (the stdlib silently turns
``{1: "a"}`` into ``{"1": "a"}`` while msgspec rejects a ``bool`` key
outright), and on which exception type surfaces. One implementation means one
set of semantics to reason about and to test.

Note that msgspec is PERMISSIVE where the stdlib is strict, so the validation
walk below is load-bearing rather than belt-and-braces. Measured against
msgspec 0.21: it encodes ``NaN``/``Infinity`` to ``null`` (silently corrupting
the value), accepts ``set`` (as an array) and ``bytes`` (as base64), and
coerces int dict keys to strings. All of those must be rejected to keep the
protocol honest, so ``_validate_json_types`` walks the object tree first and
enforces the JSON type set. What msgspec does handle correctly on its own is a
lone surrogate, which raises ``UnicodeEncodeError`` at encode time.

``encode`` returns ``bytes``; call sites either hand that straight to redis
(which accepts bytes) or funnel it through :func:`encode_str` /
:func:`encode_bytes`. ``decode`` accepts ``bytes`` or ``str``.
"""

from __future__ import annotations

import math
from typing import Any

try:
    import msgspec
    import msgspec.json as _msgspec_json
except ImportError as exc:  # pragma: no cover - environment error, not logic
    raise ImportError(
        "cauli requires msgspec. Install it with:  pip install msgspec\n"
        "(it is a declared dependency, so `pip install cauli` normally brings "
        "it in; this usually means an editable/partial install)"
    ) from exc

__all__ = [
    "backend",
    "encode",
    "encode_str",
    "encode_bytes",
    "decode",
    "ENCODE_ERRORS",
    "DECODE_ERRORS",
]

backend = "msgspec"

# Exception tuples call sites catch around encode()/decode().
#
# `msgspec.MsgspecError` is listed explicitly and is NOT redundant:
# `msgspec.DecodeError` subclasses ValueError, but `msgspec.EncodeError` does
# NOT (its MRO is EncodeError -> MsgspecError -> Exception). Catching only
# (TypeError, ValueError) would therefore let a raw encode failure escape
# every guarded call site.
#
# TypeError covers unsupported types, ValueError covers the non-finite float
# rejection and UnicodeEncodeError (lone surrogates), and RecursionError
# covers a self-referential or pathologically deep object tree — the validator
# below recurses without cycle detection, and RecursionError is neither a
# ValueError nor a TypeError, so it has to be named.
ENCODE_ERRORS: tuple = (TypeError, ValueError, RecursionError, msgspec.MsgspecError)
DECODE_ERRORS: tuple = (TypeError, ValueError, RecursionError, msgspec.MsgspecError)

_encoder = _msgspec_json.Encoder()
_decoder = _msgspec_json.Decoder()


def _validate_json_types(obj: Any) -> None:
    """Reject anything outside the JSON type set, non-finite floats, and
    non-str dict keys.

    Not optional: msgspec would otherwise silently encode ``NaN``/``Infinity``
    as ``null``, accept a ``set`` as an array and ``bytes`` as base64, and
    coerce int dict keys — all of which would put values on the wire that the
    protocol does not define. Exact-type checks come first because the common
    case is plain builtins.

    Dict keys must be ``str``. Coercing them (as the stdlib does) is a footgun
    in its own right: ``{1: "a", "1": "b"}`` would silently collapse to a
    single entry.
    """
    t = type(obj)
    if t is str or t is int or t is bool or obj is None:
        return
    if t is float:
        if math.isfinite(obj):
            return
        raise ValueError("Out of range float values are not JSON compliant")
    if t is dict:
        for k, v in obj.items():
            if type(k) is not str:
                raise TypeError(f"dict keys must be str, got {type(k).__name__}")
            _validate_json_types(v)
        return
    if t is list or t is tuple:
        for v in obj:
            _validate_json_types(v)
        return
    # Subclasses of the builtin scalar/container types (rare path).
    if isinstance(obj, (str, int)):
        return
    if isinstance(obj, float):
        if math.isfinite(obj):
            return
        raise ValueError("Out of range float values are not JSON compliant")
    if isinstance(obj, dict):
        for k, v in obj.items():
            if not isinstance(k, str):
                raise TypeError(f"dict keys must be str, got {type(k).__name__}")
            _validate_json_types(v)
        return
    if isinstance(obj, (list, tuple)):
        for v in obj:
            _validate_json_types(v)
        return
    raise TypeError(f"Object of type {t.__name__} is not JSON serializable")


def encode(obj: Any) -> bytes:
    """Encode ``obj`` as compact UTF-8 JSON bytes.

    Raises for non-JSON types (TypeError), NaN/Infinity and lone surrogates
    (ValueError), per :data:`ENCODE_ERRORS`.
    """
    _validate_json_types(obj)
    return _encoder.encode(obj)


def decode(data: bytes | str) -> Any:
    """Parse JSON from bytes or str. Raises on malformed input."""
    return _decoder.decode(data)


def encode_str(obj: Any) -> str:
    """:func:`encode`, as ``str`` (for text-stream transports)."""
    return encode(obj).decode("utf-8")


def encode_bytes(obj: Any) -> bytes:
    """:func:`encode`, as ``bytes`` (for byte-stream transports).

    Identical to :func:`encode` now that msgspec is the only backend; kept as
    a named entry point so the cpu child's socket path states its intent (and
    so it stays correct if a backend returning ``str`` is ever reintroduced).
    """
    return encode(obj)

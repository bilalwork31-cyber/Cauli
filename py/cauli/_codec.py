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

# Tail of a non-JSON-type rejection. The point of naming the set inline is
# that a Celery refugee arrives with a UUID primary key or a datetime, and the
# error should say once what the wire format actually takes.
_ALLOWED = "allowed types: str, int, float, bool, None, list, tuple, dict with str keys"


def _push_path(exc: BaseException, segment: object) -> None:
    """Record one location segment on an in-flight validation failure.

    Segments are attached to the exception as the walk unwinds, so they arrive
    innermost-first and :func:`_render_path` reverses them. Carrying the
    location on the exception rather than threading a path string down the
    walk keeps the success path — which runs on every single enqueue — free of
    the per-element string building that would otherwise cost more than the
    type checks it annotates.

    Only the plain ``TypeError``/``ValueError`` raised by this module are
    annotated, so :func:`encode` can rebuild the message as ``type(exc)(msg)``
    without ever meeting an exception class that takes other constructor
    arguments.
    """
    if type(exc) is not TypeError and type(exc) is not ValueError:
        return
    path = getattr(exc, "_cauli_path", None)
    if path is None:
        exc._cauli_path = [segment]  # type: ignore[attr-defined]
    else:
        path.append(segment)


def _render_path(segments: list[Any]) -> str:
    """Render innermost-first segments as e.g. ``args[0]['meta']``.

    The outermost segment is a top-level envelope field (``args``, ``kwargs``,
    a result key), so it reads bare; everything under it is subscripted.
    """
    parts: list[str] = []
    for i, seg in enumerate(reversed(segments)):
        if isinstance(seg, str):
            parts.append(seg if i == 0 else f"[{seg!r}]")
        else:
            parts.append(f"[{seg}]")
    return "".join(parts)


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

    Every container re-raises a failure from below after recording its own key
    or index, so :func:`encode` can say WHICH argument was rejected rather than
    only naming the type. ``RecursionError`` is deliberately not annotated: a
    cyclic object has no finite path to report, and running more code at the
    depth limit only invites a second one.
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
            try:
                _validate_json_types(v)
            except (TypeError, ValueError) as exc:
                _push_path(exc, k)
                raise
        return
    if t is list or t is tuple:
        for i, v in enumerate(obj):
            try:
                _validate_json_types(v)
            except (TypeError, ValueError) as exc:
                _push_path(exc, i)
                raise
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
            try:
                _validate_json_types(v)
            except (TypeError, ValueError) as exc:
                _push_path(exc, k)
                raise
        return
    if isinstance(obj, (list, tuple)):
        for i, v in enumerate(obj):
            try:
                _validate_json_types(v)
            except (TypeError, ValueError) as exc:
                _push_path(exc, i)
                raise
        return
    raise TypeError(f"Object of type {t.__name__} is not JSON serializable ({_ALLOWED})")


def encode(obj: Any) -> bytes:
    """Encode ``obj`` as compact UTF-8 JSON bytes.

    Raises for non-JSON types (TypeError), NaN/Infinity and lone surrogates
    (ValueError), per :data:`ENCODE_ERRORS`. A rejection from inside a
    container carries its location, e.g. ``args[0]: Object of type UUID is not
    JSON serializable (allowed types: ...)``.
    """
    try:
        _validate_json_types(obj)
    except (TypeError, ValueError) as exc:
        segments = getattr(exc, "_cauli_path", None)
        if not segments:
            raise
        raise type(exc)(f"{_render_path(segments)}: {exc}") from exc
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

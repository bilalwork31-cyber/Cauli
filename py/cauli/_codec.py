"""JSON encode and decode for the wire format (PROTOCOL.md sections 2, 5.1, 8).

msgspec is required, with no stdlib ``json`` fallback: the two disagree on
lone surrogates, dict key coercion and exception types. msgspec is also
permissive (``NaN`` to ``null``, ``set`` as array, ``bytes`` as base64, int
keys coerced), so :func:`_validate_json_types` enforces the JSON type set
before every encode.
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

# msgspec.EncodeError is not a ValueError, so MsgspecError must be listed.
# RecursionError covers cyclic or very deep trees in the validator.
ENCODE_ERRORS: tuple = (TypeError, ValueError, RecursionError, msgspec.MsgspecError)
DECODE_ERRORS: tuple = (TypeError, ValueError, RecursionError, msgspec.MsgspecError)

_encoder = _msgspec_json.Encoder()
_decoder = _msgspec_json.Decoder()

_ALLOWED = "allowed types: str, int, float, bool, None, list, tuple, dict with str keys"

# serde_json's recursion limit in the worker: deeper JSON fails to parse and
# is dead lettered as malformed, so the client rejects it at the call site.
_MAX_NESTING = 127


def _push_path(exc: BaseException, segment: object) -> None:
    """Record one location segment on a validation failure as the walk unwinds.

    Carrying the path on the exception keeps string building off the success
    path, which runs on every enqueue. Only this module's plain ``TypeError``
    and ``ValueError`` are annotated, so :func:`encode` can rebuild them as
    ``type(exc)(msg)``.
    """
    if type(exc) is not TypeError and type(exc) is not ValueError:
        return
    path = getattr(exc, "_cauli_path", None)
    if path is None:
        exc._cauli_path = [segment]  # type: ignore[attr-defined]
    else:
        path.append(segment)


def _render_path(segments: list[Any]) -> str:
    """Render innermost first segments as e.g. ``args[0]['meta']``."""
    parts: list[str] = []
    for i, seg in enumerate(reversed(segments)):
        if isinstance(seg, str):
            parts.append(seg if i == 0 else f"[{seg!r}]")
        else:
            parts.append(f"[{seg}]")
    return "".join(parts)


def _validate_json_types(obj: Any, depth: int = 1) -> None:
    """Reject non JSON types, non finite floats and non str dict keys.

    Keys are not coerced: ``{1: "a", "1": "b"}`` would collapse to one entry.
    ``RecursionError`` is not annotated: a cycle has no finite path.
    """
    t = type(obj)
    if t is str or t is int or t is bool or obj is None:
        return
    if t is float:
        if math.isfinite(obj):
            return
        raise ValueError("Out of range float values are not JSON compliant")
    if depth > _MAX_NESTING and isinstance(obj, (dict, list, tuple)):
        raise ValueError(f"nested deeper than {_MAX_NESTING} arrays/objects")
    if t is dict:
        for k, v in obj.items():
            if type(k) is not str:
                raise TypeError(f"dict keys must be str, got {type(k).__name__}")
            try:
                _validate_json_types(v, depth + 1)
            except (TypeError, ValueError) as exc:
                _push_path(exc, k)
                raise
        return
    if t is list or t is tuple:
        for i, v in enumerate(obj):
            try:
                _validate_json_types(v, depth + 1)
            except (TypeError, ValueError) as exc:
                _push_path(exc, i)
                raise
        return
    # Subclasses of the builtin types, after the exact type fast path.
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
                _validate_json_types(v, depth + 1)
            except (TypeError, ValueError) as exc:
                _push_path(exc, k)
                raise
        return
    if isinstance(obj, (list, tuple)):
        for i, v in enumerate(obj):
            try:
                _validate_json_types(v, depth + 1)
            except (TypeError, ValueError) as exc:
                _push_path(exc, i)
                raise
        return
    raise TypeError(
        f"Object of type {t.__name__} is not JSON serializable ({_ALLOWED})"
    )


def encode(obj: Any) -> bytes:
    """Encode ``obj`` as compact UTF-8 JSON bytes.

    Raises one of :data:`ENCODE_ERRORS`; a rejection inside a container names
    its location, e.g. ``args[0]: Object of type UUID is not JSON
    serializable (allowed types: ...)``.
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
    """:func:`encode`, as ``str``."""
    return encode(obj).decode("utf-8")


def encode_bytes(obj: Any) -> bytes:
    """:func:`encode`, named for the cpu child's byte socket path."""
    return encode(obj)

"""cauli._codec: JSON encode/decode with optional msgspec acceleration.

The wire format is plain JSON (PROTOCOL.md sections 2, 5.1, 8); this module
only changes HOW that JSON is produced/parsed, never WHAT crosses the wire.
When ``msgspec`` is importable and the env var ``CAULI_DISABLE_MSGSPEC`` is
unset, ``msgspec.json`` is used; otherwise the stdlib ``json`` module. Both
backends emit UTF-8, compact (no-whitespace) JSON and reject NaN/Infinity
(the stdlib's ``allow_nan=False`` semantics; msgspec would otherwise encode
non-finite floats as ``null``, silently corrupting values).

Because msgspec natively serializes many non-JSON types (set, datetime,
UUID, dataclasses, ...) that the stdlib rejects with TypeError, the msgspec
path first validates the object tree against the JSON type set so both
backends accept and reject the same inputs (a task returning a ``set`` must
be a SerializationError regardless of which codec is installed). Known
micro-divergences, accepted: ``str`` subclasses encode via the stdlib but
raise TypeError under msgspec; the stdlib *decoder* accepts the non-standard
``NaN``/``Infinity`` literals while msgspec rejects them (nothing in cauli
ever produces those on the wire).

``encode`` returns ``bytes`` (msgspec) or ``str`` (stdlib); every call site
either passes the value straight to redis (which accepts both) or funnels it
through :func:`encode_str`. ``decode`` accepts ``bytes`` or ``str``.
"""

from __future__ import annotations

import json as _json
import math
import os
from typing import Any, Callable

__all__ = [
    "backend",
    "encode",
    "encode_str",
    "decode",
    "ENCODE_ERRORS",
    "DECODE_ERRORS",
]

encode: Callable[[Any], "bytes | str"]
decode: Callable[["bytes | str"], Any]

# Exception tuples call sites catch around encode()/decode(). ValueError
# covers the NaN/Inf rejection on both backends (msgspec.EncodeError and
# msgspec.DecodeError both subclass ValueError, as does UnicodeEncodeError);
# TypeError covers unsupported types.
ENCODE_ERRORS: tuple = (TypeError, ValueError)
DECODE_ERRORS: tuple = (TypeError, ValueError)


def _validate_json_types(obj: Any) -> None:
    """Reject anything outside the JSON type set, and non-finite floats.

    Mirrors ``json.dumps(obj, allow_nan=False)``'s acceptance rules so the
    msgspec backend cannot silently serialize types (set, datetime, ...) the
    stdlib backend would loudly reject, and vice versa never encodes NaN as
    null. Exact-type checks first: the common case is plain builtins.
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
            _validate_json_types(k)
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
            _validate_json_types(k)
            _validate_json_types(v)
        return
    if isinstance(obj, (list, tuple)):
        for v in obj:
            _validate_json_types(v)
        return
    raise TypeError(f"Object of type {t.__name__} is not JSON serializable")


_msgspec_json = None
if not os.environ.get("CAULI_DISABLE_MSGSPEC"):
    try:
        import msgspec.json as _msgspec_json  # type: ignore[import-not-found]
    except ImportError:  # optional dependency: pip install 'cauli[speed]'
        _msgspec_json = None

if _msgspec_json is not None:
    backend = "msgspec"
    _encoder = _msgspec_json.Encoder()
    _decoder = _msgspec_json.Decoder()

    def encode(obj: Any) -> bytes:
        """Encode ``obj`` as compact UTF-8 JSON bytes. Raises for non-JSON
        types (TypeError) and NaN/Infinity (ValueError)."""
        _validate_json_types(obj)
        return _encoder.encode(obj)

    def decode(data: "bytes | str") -> Any:
        """Parse JSON from bytes or str. Raises ValueError on malformed input."""
        return _decoder.decode(data)

else:
    backend = "json"

    def encode(obj: Any) -> str:
        """Encode ``obj`` as compact JSON text. Raises for non-JSON types
        (TypeError) and NaN/Infinity (ValueError). ``ensure_ascii=False``
        so both backends emit identical UTF-8 output for non-ASCII text."""
        return _json.dumps(
            obj, separators=(",", ":"), allow_nan=False, ensure_ascii=False
        )

    def decode(data: "bytes | str") -> Any:
        """Parse JSON from bytes or str. Raises ValueError on malformed input."""
        return _json.loads(data)


def encode_str(obj: Any) -> str:
    """:func:`encode`, always as ``str`` (for text-stream transports)."""
    out = encode(obj)
    if isinstance(out, bytes):
        return out.decode("utf-8")
    return out

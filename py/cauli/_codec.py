"""cauli._codec: JSON encode/decode with optional msgspec acceleration.

The wire format is plain JSON (PROTOCOL.md sections 2, 5.1, 8); this module
only changes HOW that JSON is produced/parsed, never WHAT crosses the wire.
When ``msgspec`` is importable and the env var ``CAULI_DISABLE_MSGSPEC`` is
unset, ``msgspec.json`` is used; otherwise the stdlib ``json`` module. Both
backends emit UTF-8, compact (no-whitespace) JSON and reject NaN/Infinity
(the stdlib's ``allow_nan=False`` semantics; msgspec would otherwise encode
non-finite floats as ``null``, silently corrupting values).

Because msgspec natively serializes many non-JSON types (set, datetime,
UUID, dataclasses, ...) that the stdlib rejects with TypeError, and the two
backends don't even agree on which dict KEY types they'll silently coerce
(msgspec rejects a bool key outright; the stdlib coerces it to "true"/
"false"), every encode call -- both backends -- first validates the object
tree against the JSON type set (values AND keys) so both backends accept
and reject the same inputs (a task returning a ``set``, or a dict keyed by
anything other than ``str``, must be a SerializationError regardless of
which codec is installed). Known micro-divergences, accepted: ``str``
subclasses encode via the stdlib but raise TypeError under msgspec; the
stdlib *decoder* accepts the non-standard ``NaN``/``Infinity`` literals
while msgspec rejects them (nothing in cauli ever produces those on the
wire).

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
# TypeError covers unsupported types. RecursionError (CD-2) covers a
# self-referential or pathologically deep object tree: _validate_json_types
# recurses with no cycle detection, and the stdlib/msgspec (de)serializers
# are themselves recursive, so either direction can hit Python's recursion
# limit -- a normal, catchable exception, but not a ValueError/TypeError, so
# it must be listed explicitly or it escapes every call site's guard.
ENCODE_ERRORS: tuple = (TypeError, ValueError, RecursionError)
DECODE_ERRORS: tuple = (TypeError, ValueError, RecursionError)


def _validate_json_types(obj: Any) -> None:
    """Reject anything outside the JSON type set, non-finite floats, and
    non-str dict keys.

    Mirrors ``json.dumps(obj, allow_nan=False)``'s acceptance rules so the
    msgspec backend cannot silently serialize types (set, datetime, ...) the
    stdlib backend would loudly reject, and vice versa never encodes NaN as
    null. Exact-type checks first: the common case is plain builtins.

    Dict keys (CD-2): the stdlib silently coerces a non-str key (bool -> "true"
    /"false", int/float -> str(key), None -> "null"); msgspec rejects some of
    those outright ("Only dicts with str-like or number-like keys are
    supported" -- verified: a bool key raises TypeError there). Replicating
    the stdlib's exact coercion for the msgspec backend would mean rebuilding
    the whole object tree on every encode to fix an edge case with no real
    caller today (task args/kwargs keys are always str -- **kwargs' keys are
    identifiers -- so a non-str key can only come from a task's return value).
    Simpler and safer: require str keys on BOTH backends. This also avoids
    the stdlib's own footgun where two coerced keys can silently collide,
    e.g. ``{1: "a", "1": "b"}`` -> ``{"1": "b"}``.
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
                raise TypeError(
                    f"dict keys must be str, got {type(k).__name__} "
                    "(both codec backends require this; the stdlib's "
                    "silent int/bool/None -> str key coercion is not "
                    "replicated here since msgspec doesn't match it)"
                )
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
                raise TypeError(
                    f"dict keys must be str, got {type(k).__name__} "
                    "(both codec backends require this; the stdlib's "
                    "silent int/bool/None -> str key coercion is not "
                    "replicated here since msgspec doesn't match it)"
                )
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
        (TypeError), non-str dict keys (TypeError), and NaN/Infinity
        (ValueError). ``ensure_ascii=False`` so both backends emit identical
        UTF-8 output for non-ASCII text."""
        # CD-3: the stdlib encoder would otherwise silently coerce a non-str
        # dict key (bool/int/float/None) where msgspec rejects it outright;
        # run the same validator here so both backends reject it identically
        # instead of only msgspec raising a confusing native TypeError. This
        # duplicates the stdlib's own (already-adequate) non-JSON-type
        # rejection too, but that's cheap next to json.dumps itself.
        _validate_json_types(obj)
        text = _json.dumps(
            obj, separators=(",", ":"), allow_nan=False, ensure_ascii=False
        )
        # CD-1: a task result can legally be a Python str containing lone
        # (unpaired) surrogates -- e.g. os.fsdecode() output under
        # surrogateescape. ensure_ascii=False passes those through into
        # `text` unescaped; the eventual UTF-8 encode at whatever transport
        # boundary consumes this string (a socket write, a text-mode file
        # write) would then raise UnicodeEncodeError *outside* every call
        # site's `except ENCODE_ERRORS` guard. Fail fast here instead, at the
        # one place already wrapped by every caller.
        text.encode("utf-8")
        return text

    def decode(data: "bytes | str") -> Any:
        """Parse JSON from bytes or str. Raises ValueError on malformed input."""
        return _json.loads(data)


def encode_str(obj: Any) -> str:
    """:func:`encode`, always as ``str`` (for text-stream transports)."""
    out = encode(obj)
    if isinstance(out, bytes):
        return out.decode("utf-8")
    return out


def encode_bytes(obj: Any) -> bytes:
    """:func:`encode`, always as ``bytes`` (for byte-stream transports).

    Preferred over ``encode_str`` on the cpu child's socket path, which needs
    bytes to hand to ``sendall``. Going through ``encode_str`` there costs a
    full transcode of every response on both backends: msgspec produces bytes
    which would be decoded to str and immediately re-encoded, and the stdlib
    would encode to UTF-8 twice (once for the surrogate check in ``encode``,
    once for the socket). Each branch below does exactly one conversion.
    """
    out = encode(obj)
    if isinstance(out, str):
        # The stdlib branch already proved this string is UTF-8 encodable
        # (the CD-1 check); this is the encode whose result actually ships.
        return out.encode("utf-8")
    return out

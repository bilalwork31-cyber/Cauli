"""Mock external API for the benchmark. Runs UNCAPPED on 127.0.0.1:8077.

Simulates an external network service:
    GET /io        -> await asyncio.sleep(0.05) then {"ok": true}   (50ms latency)
    GET /io?ms=N   -> variable latency (N milliseconds, float ok, 0 = immediate)
    GET /health    -> {"ok": true} immediately (readiness probe)

It must never be the bottleneck: 2 uvicorn workers, loop=asyncio, http=h11,
access log off, large backlog. Verified to sustain >2000 rps by verify_api.py.

Run:  python mock_api.py
"""
import asyncio
import socket

from starlette.applications import Starlette
from starlette.responses import JSONResponse
from starlette.routing import Route

# ---------------------------------------------------------------------------
# TCP_NODELAY fix. uvicorn's h11 implementation (0.51) never sets TCP_NODELAY
# on accepted sockets, and it writes each response as two small writes
# (headers, then body). Nagle + the client's delayed ACK then stalls every
# request on a REUSED keepalive connection by ~40ms, which silently caps a
# keepalive client at ~25 rps per connection. Measured here before the fix:
# requests.Session reuse = 44ms/req, fresh connection = 1.5ms/req. The patch
# disables Nagle per accepted connection so the mock is never the bottleneck.
# ---------------------------------------------------------------------------
from uvicorn.protocols.http.h11_impl import H11Protocol

_orig_connection_made = H11Protocol.connection_made


def _connection_made_nodelay(self, transport):
    _orig_connection_made(self, transport)
    sock = transport.get_extra_info("socket")
    if sock is not None:
        try:
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        except OSError:
            pass


H11Protocol.connection_made = _connection_made_nodelay

_OK = {"ok": True}


async def io(request):
    ms = request.query_params.get("ms")
    delay = (float(ms) / 1000.0) if ms is not None else 0.05
    if delay > 0:
        await asyncio.sleep(delay)
    return JSONResponse(_OK)


async def health(request):
    return JSONResponse(_OK)


app = Starlette(routes=[Route("/io", io), Route("/health", health)])


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "mock_api:app",
        host="127.0.0.1",
        port=8077,
        workers=2,
        loop="asyncio",
        http="h11",
        access_log=False,
        log_level="warning",
        backlog=4096,
        timeout_keep_alive=75,
        limit_concurrency=None,
        limit_max_requests=None,
    )

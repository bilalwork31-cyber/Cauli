"""Fake Graph API for the campaign benchmark. Runs UNCAPPED on 127.0.0.1:8078.

Endpoints:
    POST /me/messages       uniform 200-500ms latency, then {"message_id": "m_<uuid>"}.
                            With probability ERROR_RATE (default 0.02) returns an
                            injected error instead: 500 or 429 (50/50); the 429
                            carries retry_after in body + Retry-After header.
    GET  /conversations     ?page=N -> 100 fake messages, 150-300ms latency
                            (background-fill noise traffic).
    GET  /health            immediate {"ok": true} (readiness probe).

Env knobs: ERROR_RATE, FAKE_GRAPH_SEED (deterministic RNG; with 2 uvicorn
workers each process seeds the same RNG, so per-process sequences are
reproducible but request interleaving across workers is not), FAKE_GRAPH_PORT,
FAKE_GRAPH_WORKERS.

Latency is asyncio.sleep so the server is never CPU-bound; sustains >2000 rps
(verified by verify_fake_graph.py).
"""

import asyncio
import os
import random
import socket
import uuid

from starlette.applications import Starlette
from starlette.responses import JSONResponse
from starlette.routing import Route

# ---------------------------------------------------------------------------
# TCP_NODELAY fix, copied from bench/mock_api.py (measured on this box):
# uvicorn's h11 impl never sets TCP_NODELAY on accepted sockets and writes
# headers+body as two small writes; Nagle + delayed ACK then stalls every
# request on a REUSED keepalive connection by ~40ms, capping a keepalive
# client at ~25 rps per connection. Disable Nagle per accepted connection.
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

ERROR_RATE = float(os.environ.get("ERROR_RATE", "0.02"))
_SEED = os.environ.get("FAKE_GRAPH_SEED")
rng = random.Random(int(_SEED)) if _SEED else random.Random()


async def me_messages(request):
    await asyncio.sleep(rng.uniform(0.200, 0.500))
    if rng.random() < ERROR_RATE:
        if rng.random() < 0.5:
            return JSONResponse(
                {"error": {"code": 500, "message": "internal server error"}},
                status_code=500,
            )
        return JSONResponse(
            {"error": {"code": 429, "message": "rate limited"}, "retry_after": 1},
            status_code=429,
            headers={"Retry-After": "1"},
        )
    return JSONResponse({"message_id": f"m_{uuid.uuid4().hex}"})


async def conversations(request):
    try:
        page = int(request.query_params.get("page", "0"))
    except ValueError:
        page = 0
    await asyncio.sleep(rng.uniform(0.150, 0.300))
    msgs = [
        {"id": f"c{page}_{i}", "from": f"user{i}", "text": "fake message body"}
        for i in range(100)
    ]
    return JSONResponse({"page": page, "messages": msgs})


async def health(request):
    return JSONResponse({"ok": True})


app = Starlette(
    routes=[
        Route("/me/messages", me_messages, methods=["POST"]),
        Route("/conversations", conversations, methods=["GET"]),
        Route("/health", health, methods=["GET"]),
    ]
)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "fake_graph:app",
        host="127.0.0.1",
        port=int(os.environ.get("FAKE_GRAPH_PORT", "8078")),
        workers=int(os.environ.get("FAKE_GRAPH_WORKERS", "2")),
        loop="asyncio",
        http="h11",
        access_log=False,
        log_level="warning",
        backlog=4096,
        timeout_keep_alive=75,
        limit_concurrency=None,
        limit_max_requests=None,
    )

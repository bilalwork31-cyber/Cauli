"""Raw-asyncio HTTP/1.1 keepalive POST client for the fake Graph API.

Pattern copied from bench/common.py `_AsyncHTTPPool` (measured on this box:
httpx.AsyncClient ANTI-scales with concurrency, so the async send path uses a
minimal keepalive pool on raw asyncio streams; see the deviation note in
bench/common.py). This variant does POST with a JSON body and returns the
HTTP status code (the campaign logic only needs the status, same as the sync
path which only checks requests' status_code).

One pool per running event loop, connections reused LIFO, created on demand
(in-flight count is gated by the per-page semaphores + rupy --io-concurrency).
"""
import asyncio
import json
from urllib.parse import urlsplit


class AsyncGraphPool:
    def __init__(self, base_url: str, path: str = "/me/messages"):
        u = urlsplit(base_url)
        self.host = u.hostname or "127.0.0.1"
        self.port = u.port or 80
        self.path = path
        self._idle = []

    def _request_bytes(self, body: bytes) -> bytes:
        head = (
            f"POST {self.path} HTTP/1.1\r\n"
            f"host: {self.host}:{self.port}\r\n"
            f"content-type: application/json\r\n"
            f"content-length: {len(body)}\r\n"
            f"connection: keep-alive\r\n\r\n"
        ).encode()
        return head + body

    async def _open(self):
        return await asyncio.open_connection(self.host, self.port)

    async def _roundtrip(self, conn, req: bytes) -> int:
        reader, writer = conn
        writer.write(req)
        await writer.drain()
        line = await reader.readline()
        if not line.startswith(b"HTTP/1.1 "):
            raise ConnectionError(f"bad status line: {line!r}")
        status = int(line.split(b" ", 2)[1])
        clen = 0
        while True:
            h = await reader.readline()
            if h in (b"\r\n", b"\n", b""):
                break
            if h.lower().startswith(b"content-length:"):
                clen = int(h.split(b":", 1)[1])
        if clen:
            await reader.readexactly(clen)
        return status

    @staticmethod
    def _close(conn) -> None:
        try:
            conn[1].close()
        except Exception:
            pass

    async def post_json(self, payload: dict, timeout: float = 30.0) -> int:
        req = self._request_bytes(json.dumps(payload).encode())
        reused = bool(self._idle)
        conn = self._idle.pop() if reused else await self._open()
        try:
            status = await asyncio.wait_for(self._roundtrip(conn, req), timeout)
        except Exception:
            self._close(conn)
            if not reused:
                raise
            # pooled connection went stale (server keepalive close); one retry
            conn = await self._open()
            try:
                status = await asyncio.wait_for(self._roundtrip(conn, req),
                                                timeout)
            except Exception:
                self._close(conn)
                raise
        self._idle.append(conn)
        return status


_pools = {}


def get_pool(base_url: str) -> AsyncGraphPool:
    loop = asyncio.get_running_loop()
    key = (id(loop), base_url)
    pool = _pools.get(key)
    if pool is None:
        pool = AsyncGraphPool(base_url)
        _pools[key] = pool
    return pool

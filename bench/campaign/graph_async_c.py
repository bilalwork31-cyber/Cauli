"""Scenario C async HTTP client: like graph_async.AsyncGraphPool but returns
(status, body_bytes) so the send path can capture message_id for the stage-2
persist record. Standalone copy (additive; graph_async.py stays untouched
while the A/B suite runs from it). Same raw-asyncio keepalive pattern from
bench/common.py (httpx anti-scales on this box).
"""
import asyncio
import json
from urllib.parse import urlsplit


class CGraphPool:
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

    async def _roundtrip(self, conn, req: bytes):
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
        body = await reader.readexactly(clen) if clen else b""
        return status, body

    @staticmethod
    def _close(conn) -> None:
        try:
            conn[1].close()
        except Exception:
            pass

    async def post_json(self, payload: dict, timeout: float = 30.0):
        """POST payload; returns (status:int, body:bytes)."""
        req = self._request_bytes(json.dumps(payload).encode())
        reused = bool(self._idle)
        conn = self._idle.pop() if reused else await self._open()
        try:
            res = await asyncio.wait_for(self._roundtrip(conn, req), timeout)
        except Exception:
            self._close(conn)
            if not reused:
                raise
            conn = await self._open()
            try:
                res = await asyncio.wait_for(self._roundtrip(conn, req), timeout)
            except Exception:
                self._close(conn)
                raise
        self._idle.append(conn)
        return res


_pools = {}


def get_pool(base_url: str) -> CGraphPool:
    loop = asyncio.get_running_loop()
    key = (id(loop), base_url)
    pool = _pools.get(key)
    if pool is None:
        pool = CGraphPool(base_url)
        _pools[key] = pool
    return pool

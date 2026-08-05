# Standard Library
import asyncio
import socket
from unittest.mock import patch

# Third Party
import pytest

# Project Modules
from proxy.metrics import ProxyMetrics
from proxy.models import (
    CircuitBreakerConfig,
    KeepAliveConfig,
    RetryConfig,
    Upstream,
    UpstreamKeepAliveConfig,
)
from proxy.proxy_server import ReverseProxy
from proxy.timeouts import Timeouts
from proxy.upstream_pool import UpstreamsPool


async def _read_http_response(reader, timeout=5.0):
    """Read a complete HTTP response (headers + body by Content-Length)."""
    data = b""
    while b"\r\n\r\n" not in data:
        chunk = await asyncio.wait_for(reader.read(4096), timeout=timeout)
        if not chunk:
            break
        data += chunk

    headers_end = data.find(b"\r\n\r\n") + 4
    raw_headers = data[:headers_end].decode(errors="replace")

    cl = None
    for line in raw_headers.split("\r\n"):
        if line.lower().startswith("content-length:"):
            cl = int(line.split(":")[1].strip())
            break

    body = data[headers_end:]
    if cl is not None:
        while len(body) < cl:
            chunk = await asyncio.wait_for(reader.read(cl - len(body)), timeout=timeout)
            if not chunk:
                break
            body += chunk
        return raw_headers.encode() + body

    # chunked or no CL — read til close
    try:
        while True:
            chunk = await asyncio.wait_for(reader.read(4096), timeout=timeout)
            if not chunk:
                break
            body += chunk
    except (TimeoutError, ConnectionError):
        pass
    return raw_headers.encode() + body


async def _start_proxy(upstream_port, upstreams=None, retry_cfg=None, keepalive_cfg=None):
    """Start the proxy on a random port. Returns (server, proxy_port, pool, proxy_task)."""
    us = upstreams or [Upstream("127.0.0.1", upstream_port)]
    t = Timeouts(connect_ms=500, read_ms=2000, write_ms=2000, total_ms=10000)

    pool = UpstreamsPool(
        upstreams=us,
        timeouts=t,
        max_conns_per_upstream=10,
        cb_config=CircuitBreakerConfig(failure_threshold=5, cooldown_sec=5),
        upstream_keepalive=UpstreamKeepAliveConfig(max_idle=2, idle_timeout_sec=5),
    )

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("", 0))
    proxy_port = sock.getsockname()[1]
    sock.close()

    server = ReverseProxy(
        host="127.0.0.1",
        port=proxy_port,
        upstream_pool=pool,
        timeouts=t,
        max_conns=100,
        metrics=ProxyMetrics(),
        keepalive=keepalive_cfg
        or KeepAliveConfig(enabled=True, timeout_ms=10000, max_requests=200),
        retry_config=retry_cfg or RetryConfig(max_retries=0, base_delay_ms=10, max_delay_ms=50),
        config_path="",
    )

    _loop = asyncio.get_running_loop()
    with patch.object(_loop, "add_signal_handler", return_value=None):
        proxy_task = asyncio.create_task(server.start())
        await asyncio.sleep(0.05)

    return server, proxy_port, pool, proxy_task


pytestmark = pytest.mark.e2e


class TestE2E:
    async def test_get_request(self):
        async def upstream(reader, writer):
            await asyncio.wait_for(reader.read(65536), timeout=2.0)
            writer.write(b"HTTP/1.1 200 OK\r\nContent-Length: 12\r\n\r\nHello World!")
            await writer.drain()
            writer.close()

        us = await asyncio.start_server(upstream, "127.0.0.1", 0)
        us_port = us.sockets[0].getsockname()[1]

        async with us:
            server, proxy_port, _, ptask = await _start_proxy(us_port)
            try:
                r, w = await asyncio.open_connection("127.0.0.1", proxy_port)
                w.write(b"GET / HTTP/1.1\r\nHost: x\r\n\r\n")
                await w.drain()
                resp = await _read_http_response(r)
                assert b"200 OK" in resp
                assert b"Hello World!" in resp
                w.close()
            finally:
                await server._stop()
                await ptask

    async def test_post_echo(self):
        async def upstream(reader, writer):
            data = await asyncio.wait_for(reader.read(65536), timeout=2.0)
            body_start = data.find(b"\r\n\r\n") + 4
            body = data[body_start:]
            resp = (b"HTTP/1.1 200 OK\r\nContent-Length: %d\r\n\r\n" % (len(body),)) + body
            writer.write(resp)
            await writer.drain()
            writer.close()

        us = await asyncio.start_server(upstream, "127.0.0.1", 0)
        us_port = us.sockets[0].getsockname()[1]

        async with us:
            server, proxy_port, _, ptask = await _start_proxy(us_port)
            try:
                r, w = await asyncio.open_connection("127.0.0.1", proxy_port)
                payload = b"hello=world"
                req = b"POST /echo HTTP/1.1\r\nHost: x\r\nContent-Length: %d\r\n\r\n%s" % (
                    len(payload),
                    payload,
                )
                w.write(req)
                await w.drain()
                resp = await _read_http_response(r)
                assert b"200 OK" in resp
                assert payload in resp
                w.close()
            finally:
                await server._stop()
                await ptask

    async def test_bad_gateway_when_upstream_down(self):
        async def upstream(reader, writer):
            await asyncio.wait_for(reader.read(65536), timeout=2.0)
            writer.write(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nOK")
            await writer.drain()
            writer.close()

        us = await asyncio.start_server(upstream, "127.0.0.1", 0)
        us_port = us.sockets[0].getsockname()[1]

        async with us:
            server, proxy_port, _, ptask = await _start_proxy(us_port)
            # Stop the upstream before sending request
            us.close()

        # Now upstream is dead → proxy should get ConnectionRefused → 502
        try:
            r, w = await asyncio.open_connection("127.0.0.1", proxy_port)
            w.write(b"GET / HTTP/1.1\r\nHost: x\r\n\r\n")
            await w.drain()
            resp = await _read_http_response(r)
            assert b"502 Bad Gateway" in resp
            w.close()
        finally:
            await server._stop()
            await ptask

    async def test_retry_on_500_then_success(self):
        call_count = 0

        async def upstream(reader, writer):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                resp = b"HTTP/1.1 500\r\nContent-Length: 2\r\n\r\nE1"
            else:
                resp = b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhello"
            writer.write(resp)
            await writer.drain()
            writer.close()

        us = await asyncio.start_server(upstream, "127.0.0.1", 0)
        us_port = us.sockets[0].getsockname()[1]

        async with us:
            retry = RetryConfig(max_retries=1, base_delay_ms=10, max_delay_ms=100)
            server, proxy_port, _, ptask = await _start_proxy(us_port, retry_cfg=retry)
            try:
                r, w = await asyncio.open_connection("127.0.0.1", proxy_port)
                w.write(b"GET / HTTP/1.1\r\nHost: x\r\n\r\n")
                await w.drain()
                resp = await _read_http_response(r)
                assert b"200 OK" in resp
                assert b"hello" in resp
                assert call_count == 2
                w.close()
            finally:
                await server._stop()
                await ptask

    async def test_retry_all_fail_returns_502(self):
        async def upstream(reader, writer):
            await asyncio.wait_for(reader.read(65536), timeout=2.0)
            writer.write(b"HTTP/1.1 500\r\nContent-Length: 2\r\n\r\nE1")
            await writer.drain()
            writer.close()

        us = await asyncio.start_server(upstream, "127.0.0.1", 0)
        us_port = us.sockets[0].getsockname()[1]

        async with us:
            retry = RetryConfig(max_retries=1, base_delay_ms=10, max_delay_ms=100)
            server, proxy_port, _, ptask = await _start_proxy(us_port, retry_cfg=retry)
            try:
                r, w = await asyncio.open_connection("127.0.0.1", proxy_port)
                w.write(b"GET / HTTP/1.1\r\nHost: x\r\n\r\n")
                await w.drain()
                resp = await _read_http_response(r)
                assert b"502 Bad Gateway" in resp
                w.close()
            finally:
                await server._stop()
                await ptask

    async def test_retry_switches_to_another_upstream(self):
        bad_calls = {"n": 0}
        good_calls = {"n": 0}

        async def bad_upstream(reader, writer):
            await asyncio.wait_for(reader.read(65536), timeout=2.0)
            bad_calls["n"] += 1
            writer.write(b"HTTP/1.1 500\r\nContent-Length: 2\r\n\r\nE1")
            await writer.drain()
            writer.close()

        async def good_upstream(reader, writer):
            await asyncio.wait_for(reader.read(65536), timeout=2.0)
            good_calls["n"] += 1
            writer.write(b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhello")
            await writer.drain()
            writer.close()

        bad_srv = await asyncio.start_server(bad_upstream, "127.0.0.1", 0)
        good_srv = await asyncio.start_server(good_upstream, "127.0.0.1", 0)
        bad_port = bad_srv.sockets[0].getsockname()[1]
        good_port = good_srv.sockets[0].getsockname()[1]

        async with bad_srv, good_srv:
            retry = RetryConfig(max_retries=1, base_delay_ms=10, max_delay_ms=100)
            upstreams = [Upstream("127.0.0.1", bad_port), Upstream("127.0.0.1", good_port)]
            server, proxy_port, _, ptask = await _start_proxy(
                0, upstreams=upstreams, retry_cfg=retry
            )
            try:
                r, w = await asyncio.open_connection("127.0.0.1", proxy_port)
                w.write(b"GET / HTTP/1.1\r\nHost: x\r\n\r\n")
                await w.drain()
                resp = await _read_http_response(r)
                assert b"200 OK" in resp
                assert b"hello" in resp
                assert bad_calls["n"] == 1
                assert good_calls["n"] == 1
                w.close()
            finally:
                await server._stop()
                await ptask

    async def test_keepalive_multiple_requests(self):
        async def upstream(reader, writer):
            await asyncio.wait_for(reader.read(65536), timeout=2.0)

            writer.write(b"HTTP/1.1 200 OK\r\nContent-Length: 12\r\n\r\nHello World!")
            await writer.drain()
            writer.close()

        us = await asyncio.start_server(upstream, "127.0.0.1", 0)
        us_port = us.sockets[0].getsockname()[1]

        async with us:
            server, proxy_port, _, ptask = await _start_proxy(us_port)
            try:
                r, w = await asyncio.open_connection("127.0.0.1", proxy_port)

                for _ in range(3):
                    w.write(b"GET / HTTP/1.1\r\nHost: x\r\n\r\n")
                    await w.drain()
                    resp = await _read_http_response(r)
                    assert b"200 OK" in resp
                    assert b"Hello World!" in resp

                w.close()
            finally:
                await server._stop()
                await ptask

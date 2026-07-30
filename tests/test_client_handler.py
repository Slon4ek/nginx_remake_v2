# Standard Library
import asyncio
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock

# Project Modules
from proxy.client_handler import ClientHandler
from proxy.metrics import ProxyMetrics
from proxy.models import KeepAliveConfig, RetryConfig, Upstream
from proxy.timeouts import Timeouts


class MockPooledConnection:
    def __init__(self, reader, writer):
        self.reader = reader
        self.writer = writer
        self.last_used = 0.0
        self.request_count = 0

    @property
    def is_closed(self):
        return False

    async def close(self):
        pass


def _make_writer():
    w = AsyncMock(spec=asyncio.StreamWriter)
    w.is_closing.return_value = False
    w.write = MagicMock()
    w.drain = AsyncMock()
    w.close = MagicMock()
    w.wait_closed = AsyncMock()
    return w


def _make_pool(*upstream_responses: bytes):
    class MockPool:
        def __init__(self, responses):
            self._responses = list(responses)
            self.record_success = AsyncMock()
            self.record_failure = AsyncMock()
            self._call_index = 0

        @asynccontextmanager
        async def acquire_connection(self, _upstream):
            idx = self._call_index
            self._call_index += 1
            resp = self._responses[idx] if idx < len(self._responses) else b""
            reader = asyncio.StreamReader()
            reader.feed_data(resp)
            reader.feed_eof()
            writer = _make_writer()
            yield MockPooledConnection(reader, writer)

    return MockPool(list(upstream_responses))


def make_handler(
    client_request_data: bytes,
    *upstream_responses: bytes,
    timeouts=None,
    keepalive=None,
    retry_config=None,
    metrics=None,
):
    t = timeouts or Timeouts(connect_ms=5000, read_ms=5000, write_ms=5000, total_ms=10000)
    ka = keepalive or KeepAliveConfig(enabled=True, timeout_ms=60000, max_requests=200)
    rc = retry_config or RetryConfig(max_retries=2, base_delay_ms=10, max_delay_ms=50)
    m = metrics or ProxyMetrics()

    client_reader = asyncio.StreamReader()
    client_reader.feed_data(client_request_data)
    client_reader.feed_eof()

    client_writer = _make_writer()
    client_writer.get_extra_info.return_value = ("127.0.0.1", 12345)

    upstream = Upstream("127.0.0.1", 9001)
    pool = _make_pool(*upstream_responses)

    handler = ClientHandler(
        client_reader=client_reader,
        client_writer=client_writer,
        upstream=upstream,
        pool=pool,
        timeouts=t,
        keepalive=ka,
        metrics=m,
        retry_config=rc,
    )
    return handler, pool, client_writer


class TestClientHandler:
    """Tests for ClientHandler.handle() — full request/response cycle + retry + record."""

    # ─── Basic success ────────────────────────────────────────

    async def test_get_200_with_keepalive(self):
        handler, pool, _ = make_handler(
            b"GET / HTTP/1.1\r\nHost: x\r\n\r\n",
            b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhello",
        )
        ka = await handler.handle()
        assert ka is True
        pool.record_success.assert_awaited_once()
        pool.record_failure.assert_not_awaited()
        assert handler._status_code == 200

    async def test_get_200_with_close(self):
        handler, pool, cw = make_handler(
            b"GET / HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n",
            b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhello",
        )
        ka = await handler.handle()
        assert ka is False
        pool.record_success.assert_awaited_once()
        cw.close.assert_called_once()

    async def test_client_disconnect_return_false(self):
        handler, pool, _ = make_handler(b"")
        ka = await handler.handle()
        assert ka is False
        pool.record_success.assert_not_awaited()
        pool.record_failure.assert_not_awaited()

    async def test_post_200_with_body(self):
        body = b"payload"
        req = b"POST /echo HTTP/1.1\r\nHost: x\r\nContent-Length: %d\r\n\r\n%s" % (len(body), body)
        resp = b"HTTP/1.1 200 OK\r\nContent-Length: 7\r\n\r\npayload"
        handler, pool, _ = make_handler(req, resp)
        ka = await handler.handle()
        assert ka is True
        pool.record_success.assert_awaited_once()

    # ─── Retry: success after N-1 failures ────────────────────

    async def test_retry_500_then_200(self):
        handler, pool, _ = make_handler(
            b"GET / HTTP/1.1\r\nHost: x\r\n\r\n",
            b"HTTP/1.1 500\r\nContent-Length: 2\r\n\r\nE1",
            b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhello",
        )
        ka = await handler.handle()
        assert ka is True
        pool.record_success.assert_awaited_once()
        pool.record_failure.assert_not_awaited()
        assert handler._status_code == 200

    async def test_retry_500_500_then_200(self):
        handler, pool, _ = make_handler(
            b"GET / HTTP/1.1\r\nHost: x\r\n\r\n",
            b"HTTP/1.1 500\r\nContent-Length: 2\r\n\r\nE1",
            b"HTTP/1.1 500\r\nContent-Length: 2\r\n\r\nE2",
            b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhello",
        )
        ka = await handler.handle()
        assert ka is True
        pool.record_success.assert_awaited_once()
        assert handler._status_code == 200

    async def test_retry_500_then_502_then_200(self):
        handler, pool, _ = make_handler(
            b"GET / HTTP/1.1\r\nHost: x\r\n\r\n",
            b"HTTP/1.1 500\r\nContent-Length: 2\r\n\r\nE1",
            b"HTTP/1.1 502\r\nContent-Length: 2\r\n\r\nE2",
            b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhello",
        )
        ka = await handler.handle()
        assert ka is True
        pool.record_success.assert_awaited_once()

    # ─── Retry: all attempts fail ─────────────────────────────

    async def test_retry_all_500_sends_502(self):
        handler, pool, cw = make_handler(
            b"GET / HTTP/1.1\r\nHost: x\r\n\r\n",
            b"HTTP/1.1 500\r\nContent-Length: 2\r\n\r\nE1",
            b"HTTP/1.1 500\r\nContent-Length: 2\r\n\r\nE2",
            b"HTTP/1.1 500\r\nContent-Length: 2\r\n\r\nE3",
        )
        ka = await handler.handle()
        assert ka is False
        pool.record_failure.assert_awaited_once()
        pool.record_success.assert_not_awaited()
        assert handler._status_code == 502
        combined = b"".join(c[0][0] for c in cw.write.call_args_list)
        assert b"502 Bad Gateway" in combined

    async def test_retry_uses_at_most_max_retries_plus_one(self):
        rc = RetryConfig(max_retries=1, base_delay_ms=10, max_delay_ms=50)
        handler, pool, _ = make_handler(
            b"GET / HTTP/1.1\r\nHost: x\r\n\r\n",
            b"HTTP/1.1 500\r\nContent-Length: 2\r\n\r\nE1",
            b"HTTP/1.1 500\r\nContent-Length: 2\r\n\r\nE2",
            b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhello",
            retry_config=rc,
        )
        ka = await handler.handle()
        assert ka is False
        pool.record_failure.assert_awaited_once()
        pool.record_success.assert_not_awaited()

    async def test_retry_zero_max_retries_no_retry(self):
        rc = RetryConfig(max_retries=0, base_delay_ms=10, max_delay_ms=50)
        handler, pool, _ = make_handler(
            b"GET / HTTP/1.1\r\nHost: x\r\n\r\n",
            b"HTTP/1.1 500\r\nContent-Length: 2\r\n\r\nE1",
            b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhello",
            retry_config=rc,
        )
        ka = await handler.handle()
        assert ka is False
        pool.record_failure.assert_awaited_once()
        pool.record_success.assert_not_awaited()

    # ─── No retry for non-safe methods ────────────────────────

    async def test_post_500_no_retry_non_safe(self):
        handler, pool, cw = make_handler(
            b"POST / HTTP/1.1\r\nHost: x\r\nContent-Length: 0\r\n\r\n",
            b"HTTP/1.1 500\r\nContent-Length: 2\r\n\r\nE1",
            b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhello",
        )
        ka = await handler.handle()
        assert ka is False
        pool.record_failure.assert_awaited_once()
        pool.record_success.assert_not_awaited()
        combined = b"".join(c[0][0] for c in cw.write.call_args_list)
        assert b"502 Bad Gateway" in combined

    # ─── 4xx is not retried, not treated as upstream error ────

    async def test_get_400_is_not_retried(self):
        handler, pool, _ = make_handler(
            b"GET / HTTP/1.1\r\nHost: x\r\n\r\n",
            b"HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\n\r\n",
            b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhello",
        )
        ka = await handler.handle()
        assert ka is True
        pool.record_failure.assert_not_awaited()
        pool.record_success.assert_awaited_once()
        assert handler._status_code == 400

    # ─── Connection error (upstream unreachable) ──────────────

    async def test_connection_refused_sends_502(self):
        _upstream = Upstream("127.0.0.1", 9001)
        client_reader = asyncio.StreamReader()
        client_reader.feed_data(b"GET / HTTP/1.1\r\nHost: x\r\n\r\n")
        client_reader.feed_eof()

        client_writer = _make_writer()
        client_writer.get_extra_info.return_value = ("127.0.0.1", 12345)

        class _RaisingCtx:
            async def __aenter__(self):
                raise ConnectionRefusedError

            async def __aexit__(self, *args):
                pass

        class RaisingPool:
            record_success = AsyncMock()
            record_failure = AsyncMock()

            def acquire_connection(self, _upstream):
                return _RaisingCtx()

        pool = RaisingPool()
        handler = ClientHandler(
            client_reader=client_reader,
            client_writer=client_writer,
            upstream=_upstream,
            pool=pool,
            timeouts=Timeouts(500, 1000, 1000, 5000),
            keepalive=KeepAliveConfig(),
            metrics=ProxyMetrics(),
            retry_config=RetryConfig(),
        )
        ka = await handler.handle()
        assert ka is False
        pool.record_failure.assert_awaited_once()
        pool.record_success.assert_not_awaited()
        combined = b"".join(c[0][0] for c in client_writer.write.call_args_list)
        assert b"502 Bad Gateway" in combined

    # ─── Upstream read timeout ────────────────────────────────

    async def test_upstream_read_timeout_sends_504(self):
        _upstream = Upstream("127.0.0.1", 9001)
        client_reader = asyncio.StreamReader()
        client_reader.feed_data(b"GET / HTTP/1.1\r\nHost: x\r\n\r\n")
        client_reader.feed_eof()

        client_writer = _make_writer()
        client_writer.get_extra_info.return_value = ("127.0.0.1", 12345)

        class TimeoutPool:
            record_success = AsyncMock()
            record_failure = AsyncMock()

            @asynccontextmanager
            async def acquire_connection(self, _upstream):
                reader = asyncio.StreamReader()
                writer = _make_writer()
                yield MockPooledConnection(reader, writer)

        pool = TimeoutPool()
        handler = ClientHandler(
            client_reader=client_reader,
            client_writer=client_writer,
            upstream=_upstream,
            pool=pool,
            timeouts=Timeouts(500, 0.05, 500, 500),
            keepalive=KeepAliveConfig(),
            metrics=ProxyMetrics(),
            retry_config=RetryConfig(),
        )
        ka = await handler.handle()
        assert ka is False
        pool.record_failure.assert_awaited_once()
        pool.record_success.assert_not_awaited()
        combined = b"".join(c[0][0] for c in client_writer.write.call_args_list)
        assert b"504 Gateway Timeout" in combined

    # ─── Metrics tracking ─────────────────────────────────────

    async def test_metrics_recorded_for_success(self):
        m = ProxyMetrics()
        handler, _, _ = make_handler(
            b"GET / HTTP/1.1\r\nHost: x\r\n\r\n",
            b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhello",
            metrics=m,
        )
        await handler.handle()
        assert m.total_requests == 1
        assert m.status_counts[200] == 1
        assert m.active_connections == 0
        assert m.total_bytes_in > 0
        assert m.total_bytes_out > 0

    async def test_metrics_recorded_for_retry(self):
        m = ProxyMetrics()
        handler, _, _ = make_handler(
            b"GET / HTTP/1.1\r\nHost: x\r\n\r\n",
            b"HTTP/1.1 500\r\nContent-Length: 2\r\n\r\nE1",
            b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhello",
            metrics=m,
        )
        await handler.handle()
        assert m.total_requests == 2
        assert m.status_counts[200] == 1
        assert m.active_connections == 0

    async def test_metrics_recorded_for_all_fail(self):
        m = ProxyMetrics()
        handler, _, _ = make_handler(
            b"GET / HTTP/1.1\r\nHost: x\r\n\r\n",
            b"HTTP/1.1 500\r\nContent-Length: 2\r\n\r\nE1",
            b"HTTP/1.1 500\r\nContent-Length: 2\r\n\r\nE2",
            b"HTTP/1.1 500\r\nContent-Length: 2\r\n\r\nE3",
            metrics=m,
        )
        await handler.handle()
        assert m.total_requests == 3
        assert m.status_counts[502] == 1
        assert m.active_connections == 0

    # ─── record_success / record_failure per outcome ──────────

    async def test_record_success_on_200(self):
        handler, pool, _ = make_handler(
            b"GET / HTTP/1.1\r\nHost: x\r\n\r\n",
            b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n",
        )
        await handler.handle()
        pool.record_success.assert_awaited_once_with(Upstream("127.0.0.1", 9001))
        pool.record_failure.assert_not_awaited()

    async def test_record_success_on_retry_then_200(self):
        handler, pool, _ = make_handler(
            b"GET / HTTP/1.1\r\nHost: x\r\n\r\n",
            b"HTTP/1.1 500\r\nContent-Length: 2\r\n\r\nE1",
            b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n",
        )
        await handler.handle()
        pool.record_success.assert_awaited_once_with(Upstream("127.0.0.1", 9001))
        pool.record_failure.assert_not_awaited()

    async def test_record_failure_on_all_retry_exhausted(self):
        handler, pool, _ = make_handler(
            b"GET / HTTP/1.1\r\nHost: x\r\n\r\n",
            b"HTTP/1.1 500\r\nContent-Length: 2\r\n\r\nE1",
            b"HTTP/1.1 500\r\nContent-Length: 2\r\n\r\nE2",
            b"HTTP/1.1 500\r\nContent-Length: 2\r\n\r\nE3",
        )
        await handler.handle()
        pool.record_failure.assert_awaited_once_with(Upstream("127.0.0.1", 9001))
        pool.record_success.assert_not_awaited()

    async def test_record_failure_on_connection_refused(self):
        _upstream = Upstream("127.0.0.1", 9001)
        client_reader = asyncio.StreamReader()
        client_reader.feed_data(b"GET / HTTP/1.1\r\nHost: x\r\n\r\n")
        client_reader.feed_eof()
        client_writer = _make_writer()
        client_writer.get_extra_info.return_value = ("127.0.0.1", 12345)

        class _RaisingCtx:
            async def __aenter__(self):
                raise ConnectionRefusedError

            async def __aexit__(self, *args):
                pass

        class RaisingPool:
            record_success = AsyncMock()
            record_failure = AsyncMock()

            def acquire_connection(self, _upstream):
                return _RaisingCtx()

        pool = RaisingPool()
        handler = ClientHandler(
            client_reader=client_reader,
            client_writer=client_writer,
            upstream=_upstream,
            pool=pool,
            timeouts=Timeouts(500, 1000, 1000, 5000),
            keepalive=KeepAliveConfig(),
            metrics=ProxyMetrics(),
            retry_config=RetryConfig(),
        )
        await handler.handle()
        pool.record_failure.assert_awaited_once_with(Upstream("127.0.0.1", 9001))
        pool.record_success.assert_not_awaited()

    async def test_record_failure_on_timeout(self):
        _upstream = Upstream("127.0.0.1", 9001)
        client_reader = asyncio.StreamReader()
        client_reader.feed_data(b"GET / HTTP/1.1\r\nHost: x\r\n\r\n")
        client_reader.feed_eof()
        client_writer = _make_writer()
        client_writer.get_extra_info.return_value = ("127.0.0.1", 12345)

        class TimeoutPool:
            record_success = AsyncMock()
            record_failure = AsyncMock()

            @asynccontextmanager
            async def acquire_connection(self, _upstream):
                reader = asyncio.StreamReader()
                writer = _make_writer()
                yield MockPooledConnection(reader, writer)

        pool = TimeoutPool()
        handler = ClientHandler(
            client_reader=client_reader,
            client_writer=client_writer,
            upstream=_upstream,
            pool=pool,
            timeouts=Timeouts(500, 0.05, 500, 500),
            keepalive=KeepAliveConfig(),
            metrics=ProxyMetrics(),
            retry_config=RetryConfig(),
        )
        await handler.handle()
        pool.record_failure.assert_awaited_once_with(Upstream("127.0.0.1", 9001))
        pool.record_success.assert_not_awaited()

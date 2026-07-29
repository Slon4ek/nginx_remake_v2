import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from proxy.timeouts import Timeouts
from proxy.upstream_pool import (
    CircuitBreaker,
    CircuitBreakerConfig,
    CircuitState,
    Upstream,
    UpstreamKeepAliveConfig,
    UpstreamsPool,
)


@pytest.fixture
def upstreams():
    return [Upstream("127.0.0.1", 9001), Upstream("127.0.0.1", 9002)]


@pytest.fixture
def timeouts():
    return Timeouts(connect_ms=500, read_ms=1000, write_ms=1000, total_ms=5000)


@pytest.fixture
def pool(upstreams, timeouts):
    cb_cfg = CircuitBreakerConfig(failure_threshold=3, cooldown_sec=0.1)
    uk_cfg = UpstreamKeepAliveConfig(max_idle=2, idle_timeout_sec=5.0)
    return UpstreamsPool(
        upstreams,
        timeouts,
        max_conns_per_upstream=5,
        cb_config=cb_cfg,
        upstream_keepalive=uk_cfg,
    )


# ─── Round Robin ─────────────────────────────────────────────


class TestRoundRobin:
    def test_returns_upstream_in_order(self, pool):
        u1 = asyncio.run(pool.get_next_alive())
        u2 = asyncio.run(pool.get_next_alive())
        u3 = asyncio.run(pool.get_next_alive())
        assert u1 == Upstream("127.0.0.1", 9001)
        assert u2 == Upstream("127.0.0.1", 9002)
        assert u3 == Upstream("127.0.0.1", 9001)

    def test_skips_dead_upstream(self, pool):
        asyncio.run(pool.set_status(Upstream("127.0.0.1", 9001), False))
        u1 = asyncio.run(pool.get_next_alive())
        u2 = asyncio.run(pool.get_next_alive())
        assert u1 == Upstream("127.0.0.1", 9002)
        assert u2 == Upstream("127.0.0.1", 9002)

    def test_returns_none_when_all_dead(self, pool):
        asyncio.run(pool.set_status(Upstream("127.0.0.1", 9001), False))
        asyncio.run(pool.set_status(Upstream("127.0.0.1", 9002), False))
        assert asyncio.run(pool.get_next_alive()) is None

    def test_excluded_upstream(self, pool):
        excluded = {Upstream("127.0.0.1", 9001)}
        u = asyncio.run(pool.get_next_alive(excluded=excluded))
        assert u == Upstream("127.0.0.1", 9002)


# ─── Circuit Breaker ─────────────────────────────────────────


class TestCircuitBreaker:
    @pytest.mark.asyncio
    async def test_allows_requests_when_closed(self):
        cb = CircuitBreaker(CircuitBreakerConfig(failure_threshold=3, cooldown_sec=30))
        assert cb.allow_request() is True

    def test_opens_after_failures(self):
        cb = CircuitBreaker(CircuitBreakerConfig(failure_threshold=3, cooldown_sec=30))
        for _ in range(3):
            asyncio.run(cb.record_failure())
        assert cb.allow_request() is False

    @pytest.mark.asyncio
    async def test_half_open_after_cooldown(self):
        cb = CircuitBreaker(
            CircuitBreakerConfig(failure_threshold=1, cooldown_sec=0.05)
        )
        await cb.record_failure()
        assert cb.allow_request() is False
        await asyncio.sleep(0.06)
        assert cb.allow_request() is True

    @pytest.mark.asyncio
    async def test_closes_after_success_in_half_open(self):
        cb = CircuitBreaker(
            CircuitBreakerConfig(failure_threshold=1, cooldown_sec=0.05)
        )
        await cb.record_failure()
        await asyncio.sleep(0.06)
        cb.allow_request()
        await cb.record_success()
        assert cb.state == CircuitState.CLOSED
        assert cb.failure_count == 0

    def test_breaker_blocks_upstream_in_pool(self, pool):
        u = Upstream("127.0.0.1", 9001)
        for _ in range(3):
            asyncio.run(pool.record_failure(u))
        for _ in range(5):
            selected = asyncio.run(pool.get_next_alive())
            assert selected == Upstream("127.0.0.1", 9002)

    def test_breaker_recovers_after_cooldown(self, pool):
        u = Upstream("127.0.0.1", 9001)
        for _ in range(3):
            asyncio.run(pool.record_failure(u))
        asyncio.run(asyncio.sleep(0.11))
        selected = asyncio.run(pool.get_next_alive())
        assert selected == u


# ─── Acquire / Release (with mocked connections) ─────────────


class TestAcquireRelease:
    @pytest.fixture
    def mock_connection(self):
        """Создаёт мок соединения (reader, writer)."""
        reader = AsyncMock(spec=asyncio.StreamReader)
        reader.at_eof.return_value = False
        writer = AsyncMock(spec=asyncio.StreamWriter)
        writer.is_closing.return_value = False
        writer.close = MagicMock()
        writer.wait_closed = AsyncMock()
        return reader, writer

    @pytest.mark.asyncio
    async def test_acquire_returns_connection(self, pool, mock_connection):
        """acquire_connection возвращает рабочее соединение."""
        u = Upstream("127.0.0.1", 9001)

        with patch(
            "asyncio.open_connection", new=AsyncMock(return_value=mock_connection)
        ):
            async with pool.acquire_connection(u) as conn:
                assert conn is not None
                assert not conn.is_closed
                assert conn.request_count == 1

    @pytest.mark.asyncio
    async def test_reuse_idle_connection(self, pool, mock_connection):
        """Второе acquire возвращает то же idle-соединение."""
        u = Upstream("127.0.0.1", 9001)

        with patch(
            "asyncio.open_connection", new=AsyncMock(return_value=mock_connection)
        ) as mock_open:
            # Первый acquire — создаёт новое соединение
            async with pool.acquire_connection(u):
                pass

            # Второй acquire — должен переиспользовать idle
            async with pool.acquire_connection(u) as conn2:
                assert not conn2.is_closed

            # open_connection должен вызваться только 1 раз
            assert mock_open.call_count == 1

    @pytest.mark.asyncio
    async def test_semaphore_limits_connections(self, pool):
        """Semaphore ограничивает число одновременных соединений."""
        u = Upstream("127.0.0.1", 9001)
        # Уменьшаем лимит для теста
        pool._per_upstream_pools[u]._semaphore = asyncio.Semaphore(2)

        mock_reader = AsyncMock(spec=asyncio.StreamReader)
        mock_reader.at_eof.return_value = False
        mock_writer = AsyncMock(spec=asyncio.StreamWriter)
        mock_writer.is_closing.return_value = False
        mock_writer.close = MagicMock()
        mock_writer.wait_closed = AsyncMock()

        with patch(
            "asyncio.open_connection",
            new=AsyncMock(return_value=(mock_reader, mock_writer)),
        ):
            acquired = []
            async with pool.acquire_connection(u) as c1:
                acquired.append(c1)
                async with pool.acquire_connection(u) as c2:
                    acquired.append(c2)
                    # Третье — должно зависнуть/упасть по таймауту
                    with pytest.raises(asyncio.TimeoutError):
                        await asyncio.wait_for(
                            pool.acquire_connection(u).__aenter__(),
                            timeout=0.5,
                        )

    @pytest.mark.asyncio
    async def test_shutdown_closes_all(self, pool):
        """shutdown закрывает все соединения и блокирует новые."""
        await pool.shutdown()
        u = Upstream("127.0.0.1", 9001)
        with pytest.raises(RuntimeError, match="shutting down"):
            async with pool.acquire_connection(u):
                pass


# ─── Healthcheck (integration-style, требует реальных серверов) ───
# Помечаем как integration, чтобы можно было пропускать при unit-тестах


class TestHealthcheck:
    @pytest.mark.integration
    def test_mark_dead_on_refused(self, pool):
        """Healthcheck на закрытом порту помечает upstream как dead."""
        u = Upstream("127.0.0.1", 1)  # Порт 1 точно закрыт
        result = asyncio.run(pool.healthcheck(u))
        assert result is False
        assert u not in pool.alive_upstreams

    @pytest.mark.integration
    def test_alive_count_after_healthcheck(self, pool):
        """alive_upstreams обновляется после healthcheck."""
        assert len(pool.alive_upstreams) == 2
        asyncio.run(pool.set_status(Upstream("127.0.0.1", 9001), False))
        assert len(pool.alive_upstreams) == 1


# ─── Status ──────────────────────────────────────────────────


class TestStatus:
    def test_set_status_triggers_log(self, pool, caplog):
        import logging

        caplog.set_level(logging.INFO)
        u = Upstream("127.0.0.1", 9001)
        asyncio.run(pool.set_status(u, False))
        assert "status changed: alive -> dead" in caplog.text

    def test_set_same_status_no_log(self, pool, caplog):
        u = Upstream("127.0.0.1", 9001)
        asyncio.run(pool.set_status(u, True))
        assert "status changed" not in caplog.text

    def test_set_status_unknown_upstream(self, pool):
        u = Upstream("0.0.0.0", 1)
        asyncio.run(pool.set_status(u, False))
        assert u not in pool.alive_upstreams


# ─── Validation ──────────────────────────────────────────────


class TestValidation:
    def test_empty_upstreams_raises(self, timeouts):
        with pytest.raises(ValueError, match="At least one upstream"):
            UpstreamsPool([], timeouts, 5)

    def test_duplicate_upstream_raises(self, timeouts):
        ups = [Upstream("127.0.0.1", 9001), Upstream("127.0.0.1", 9001)]
        with pytest.raises(ValueError, match="Duplicate"):
            UpstreamsPool(ups, timeouts, 5)

    def test_zero_max_conns_raises(self, upstreams, timeouts):
        with pytest.raises(ValueError, match="max_conns_per_upstream must be > 0"):
            UpstreamsPool(upstreams, timeouts, 0)


# ─── Config Defaults ─────────────────────────────────────────


class TestConfigDefaults:
    def test_all_defaults_applied(self, timeouts):
        from proxy.config import ProxyConfig, Limits

        # Test that defaults are correctly applied
        config = ProxyConfig(
            listen="127.0.0.1:8080",
            upstreams=[{"host": "127.0.0.1", "port": 9001}],
        )
        assert config.listen == "127.0.0.1:8080"
        assert len(config.upstreams) == 1
        assert config.limits.max_client_conns == 1000
        assert config.limits.max_conns_per_upstream == 100
        assert config.timeouts.connect_ms == 1000
        assert config.timeouts.read_ms == 15000
        assert config.timeouts.write_ms == 15000
        assert config.timeouts.total_ms == 30000
        assert config.keep_alive.enabled is True
        assert config.keep_alive.timeout_ms == 60000
        assert config.keep_alive.max_requests == 200
        assert config.upstream_keepalive.max_idle == 50
        assert config.upstream_keepalive.idle_timeout_sec == 30.0
        assert config.upstream_keepalive.max_requests == 200
        assert config.circuit_breaker is not None
        assert config.circuit_breaker.failure_threshold == 10
        assert config.circuit_breaker.cooldown_sec == 30.0
        assert config.retry.max_retries == 2
        assert config.retry.base_delay_ms == 100
        assert config.retry.max_delay_ms == 1000
        assert config.rate_limit.enabled is False
        assert config.rate_limit.rate == 100.0
        assert config.rate_limit.burst == 200
        assert config.rate_limit.per_client is False
        assert config.rate_limiter is None
        assert config.logging.level == "INFO"
        assert config.logging.file_path is None
        assert config.rate_limiter is None

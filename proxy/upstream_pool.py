# Standard Library
import asyncio
import logging
import socket
import ssl
import time
from collections import deque
from contextlib import asynccontextmanager, suppress

# Project Modules
from proxy.models import CircuitBreakerConfig, Upstream, UpstreamKeepAliveConfig
from proxy.timeouts import Timeouts

logger = logging.getLogger(__name__)


class CircuitState:
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreaker:
    def __init__(self, config: CircuitBreakerConfig, upstream: Upstream | None = None):
        self.config = config
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.last_open_time: float = 0.0
        self._lock = asyncio.Lock()
        self._upstream = upstream

    def allow_request(self) -> bool:
        now = time.monotonic()
        if self.state == CircuitState.OPEN:
            if now - self.last_open_time >= self.config.cooldown_sec:
                self.state = CircuitState.HALF_OPEN
                return True
            return False

        if self.state == CircuitState.HALF_OPEN:
            return False

        return self.state == CircuitState.CLOSED

    async def record_success(self):
        async with self._lock:
            if self.state == CircuitState.HALF_OPEN:
                logger.info("Circuit breaker %s: HALF_OPEN → CLOSED (апстрим восстановился)", self._upstream)
                self.state = CircuitState.CLOSED
                self.failure_count = 0

    async def record_failure(self):
        async with self._lock:
            self.failure_count += 1
            if self.state == CircuitState.HALF_OPEN:
                logger.warning("Circuit breaker %s: HALF_OPEN → OPEN (апстрим не прошёл пробу)", self._upstream)
                self.state = CircuitState.OPEN
                self.last_open_time = time.monotonic()
                self.failure_count = 0
                return

            if self.failure_count >= self.config.failure_threshold:
                logger.warning("Circuit breaker %s: CLOSED → OPEN (порог ошибок %d/%d)", self._upstream, self.failure_count, self.config.failure_threshold)
                self.state = CircuitState.OPEN
                self.last_open_time = time.monotonic()


class PooledConnection:
    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        self.reader = reader
        self.writer = writer
        self.last_used = time.monotonic()
        self.request_count = 0

    @property
    def is_closed(self) -> bool:
        return self.reader.at_eof() or self.writer.is_closing()

    async def close(self):
        with suppress(OSError, ConnectionError):
            self.writer.close()
            await self.writer.wait_closed()


class PerUpstreamPool:
    def __init__(
        self,
        upstream: Upstream,
        timeouts: Timeouts,
        max_conns: int,
        keepalive_cfg: UpstreamKeepAliveConfig,
    ):
        self._upstream = upstream
        self._timeouts = timeouts
        self._max_conns = max_conns
        self._keepalive_cfg = keepalive_cfg
        self._semaphore = asyncio.Semaphore(max_conns)
        self._idle: deque[PooledConnection] = deque()
        self._lock = asyncio.Lock()

    async def acquire(self) -> PooledConnection:
        await self._timeouts.wait_for_total(self._semaphore.acquire())
        success = False
        try:
            now = time.monotonic()
            conn: PooledConnection | None = None
            to_close: list[PooledConnection] = []

            async with self._lock:
                while self._idle:
                    candidate = self._idle.popleft()

                    if (
                        candidate.is_closed
                        or candidate.request_count >= self._keepalive_cfg.max_requests
                    ):
                        to_close.append(candidate)
                        continue

                    if now - candidate.last_used > self._keepalive_cfg.idle_timeout_sec:
                        to_close.append(candidate)
                        continue

                    conn = candidate
                    break

            if to_close:
                await asyncio.gather(*(c.close() for c in to_close), return_exceptions=True)

            if conn is not None:
                conn.last_used = now
                success = True
                return conn

            ssl_context = ssl.create_default_context() if self._upstream.tls else None
            reader, writer = await self._timeouts.wait_for_connection(
                asyncio.open_connection(self._upstream.host, self._upstream.port, ssl=ssl_context)
            )
            conn = PooledConnection(reader, writer)
            conn.last_used = now
            conn.request_count = 1
            success = True
            return conn

        finally:
            if not success:
                self._semaphore.release()

    async def release(self, conn: PooledConnection):
        if conn.is_closed:
            self._semaphore.release()
            return

        conn.last_used = time.monotonic()
        conn.request_count += 1

        if conn.request_count >= self._keepalive_cfg.max_requests:
            self._semaphore.release()
            await conn.close()
            return

        async with self._lock:
            if len(self._idle) < self._keepalive_cfg.max_idle:
                self._idle.append(conn)
                self._semaphore.release()
                return

        self._semaphore.release()
        await conn.close()

    async def close_idle(self):
        async with self._lock:
            now = time.monotonic()
            keep = deque()
            while self._idle:
                conn = self._idle.popleft()
                if conn.is_closed or (now - conn.last_used > self._keepalive_cfg.idle_timeout_sec):
                    await conn.close()
                else:
                    keep.append(conn)
            self._idle = keep

    async def shutdown(self):
        to_close: list[PooledConnection] = []
        async with self._lock:
            to_close.extend(self._idle)
            self._idle.clear()
        if to_close:
            await asyncio.gather(*(conn.close() for conn in to_close), return_exceptions=True)
            logger.debug("Принудительно закрыто %d неактивных соединений", len(to_close))


class UpstreamsPool:
    def __init__(
        self,
        upstreams: list[Upstream],
        timeouts: Timeouts,
        max_conns_per_upstream: int,
        cb_config: CircuitBreakerConfig | None = None,
        upstream_keepalive: UpstreamKeepAliveConfig | None = None,
        max_concurrent_healthcheck: int = 5,
    ):
        self._validate_init_params(upstreams, max_conns_per_upstream)

        self._upstreams = list(upstreams)
        self._next_index = 0
        self._status: dict[Upstream, bool] = dict.fromkeys(self._upstreams, True)
        self._timeouts = timeouts
        self._lock = asyncio.Lock()
        self._healthcheck_semaphore = asyncio.Semaphore(max_concurrent_healthcheck)
        self._shutdown = False

        up_keepalive = upstream_keepalive or UpstreamKeepAliveConfig()

        self._per_upstream_pools: dict[Upstream, PerUpstreamPool] = {
            u: PerUpstreamPool(u, timeouts, max_conns_per_upstream, up_keepalive)
            for u in self._upstreams
        }

        self._breakers: dict[Upstream, CircuitBreaker] = {}
        if cb_config:
            self._breakers = {u: CircuitBreaker(cb_config, upstream=u) for u in self._upstreams}

    def _validate_init_params(self, upstreams: list[Upstream], max_conns_per_upstream: int) -> None:
        if not upstreams:
            raise ValueError("At least one upstream is required")
        if max_conns_per_upstream <= 0:
            raise ValueError("max_conns_per_upstream must be > 0")
        seen: set[Upstream] = set()
        for upstream in upstreams:
            if upstream in seen:
                raise ValueError(f"Duplicate upstream found: {upstream}")
            seen.add(upstream)

    @property
    def all_upstreams(self) -> list[Upstream]:
        return list(self._upstreams)

    @property
    def alive_upstreams(self) -> list[Upstream]:
        return [u for u, alive in self._status.items() if alive]

    @asynccontextmanager
    async def acquire_connection(self, upstream: Upstream):
        if self._shutdown:
            raise RuntimeError("Upstream pool is shutting down")

        pool = self._per_upstream_pools.get(upstream)
        if pool is None:
            raise RuntimeError(f"No pool for upstream {upstream}")

        conn = await pool.acquire()
        try:
            yield conn
        finally:
            await pool.release(conn)

    async def get_next_alive(self, excluded: set[Upstream] | None = None) -> Upstream | None:
        excluded = excluded or set()
        async with self._lock:
            count = len(self._upstreams)
            for _ in range(count):
                upstream = self._upstreams[self._next_index]
                self._next_index = (self._next_index + 1) % count

                if upstream in excluded:
                    continue

                if not self._status.get(upstream, False):
                    continue

                breaker = self._breakers.get(upstream)
                if breaker and not breaker.allow_request():
                    logger.debug("Circuit breaker заблокировал %s (состояние %s)", upstream, breaker.state)
                    continue

                return upstream
            return None

    async def record_success(self, upstream: Upstream):
        breaker = self._breakers.get(upstream)
        if breaker:
            await breaker.record_success()

    async def record_failure(self, upstream: Upstream):
        breaker = self._breakers.get(upstream)
        if breaker:
            await breaker.record_failure()

    async def set_status(self, upstream: Upstream, alive: bool) -> None:
        async with self._lock:
            if upstream in self._status:
                old_status = self._status[upstream]
                if old_status != alive:
                    self._status[upstream] = alive
                    status = "alive" if alive else "dead"
                    logger.info(
                        "Апстрим %s: статус изменился %s → %s",
                        upstream,
                        "alive" if old_status else "dead",
                        status,
                    )

    async def healthcheck(self, upstream: Upstream) -> bool:
        async with self._healthcheck_semaphore:
            addr = str(upstream)
            ssl_context = None
            if upstream.tls:
                ssl_context = ssl.create_default_context()

            try:
                _, writer = await asyncio.wait_for(
                    asyncio.open_connection(upstream.host, upstream.port, ssl=ssl_context),
                    timeout=self._timeouts.connect_sec,
                )
                writer.close()
                await writer.wait_closed()

                await self.set_status(upstream, True)
                logger.debug("Healthcheck OK для %s", addr)
                return True

            except TimeoutError:
                logger.warning(
                    "Healthcheck таймаут для %s (connect timeout %.2f с)",
                    addr,
                    self._timeouts.connect_sec,
                )
                await self.set_status(upstream, False)
                return False

            except ConnectionRefusedError:
                logger.warning("Healthcheck не удался для %s: соединение отклонено", addr)
                await self.set_status(upstream, False)
                return False

            except ssl.SSLError as e:
                logger.warning("Healthcheck не удался для %s: ошибка SSL: %s", addr, e)
                await self.set_status(upstream, False)
                return False

            except OSError as e:
                await self.set_status(upstream, False)
                if isinstance(e, socket.gaierror):
                    logger.error(
                        "Healthcheck не удался для %s: ошибка DNS: %s",
                        addr,
                        e,
                    )
                else:
                    logger.warning("Healthcheck не удался для %s: %s", addr, e)
                return False

            except Exception:
                await self.set_status(upstream, False)
                logger.exception("Healthcheck: непредвиденная ошибка для %s", addr)
                return False

    async def run_initial_healthcheck(self) -> None:
        tasks = [self.healthcheck(u) for u in self._upstreams]
        await asyncio.gather(*tasks, return_exceptions=True)

    async def shutdown(self) -> None:
        logger.info("Останавливаем пул апстримов...")
        self._shutdown = True

        for pool in self._per_upstream_pools.values():
            await pool.shutdown()

        async with self._lock:
            self._upstreams.clear()
            self._status.clear()

        logger.info("Пул апстримов остановлен")

    @property
    def per_upstream_pools(self):
        return self._per_upstream_pools

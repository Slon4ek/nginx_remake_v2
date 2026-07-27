import asyncio
import logging
import socket
import ssl
import time
from collections import deque
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass

from proxy.timeouts import Timeouts

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Upstream:
    host: str
    port: int
    tls: bool = False

    def address(self) -> tuple[str, int]:
        return self.host, self.port

    def __str__(self) -> str:
        return f"{self.host}:{self.port}"


@dataclass
class KeepAliveConfig:
    enabled: bool = True
    timeout_ms: int = 60000
    max_requests: int = 100


@dataclass
class UpstreamKeepAliveConfig:
    max_idle: int = 10
    idle_timeout_sec: float = 60.0


@dataclass
class RetryConfig:
    max_retries: int = 2
    base_delay_ms: int = 100
    max_delay_ms: int = 1000


@dataclass
class CircuitBreakerConfig:
    failure_threshold: int = 5
    cooldown_sec: float = 30.0


class CircuitState:
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreaker:
    def __init__(self, config: CircuitBreakerConfig):
        self.config = config
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.last_open_time: float = 0.0
        self._lock = asyncio.Lock()

    async def allow_request(self) -> bool:
        async with self._lock:
            if self.state == CircuitState.CLOSED:
                return True
            if self.state == CircuitState.OPEN:
                if time.monotonic() - self.last_open_time >= self.config.cooldown_sec:
                    self.state = CircuitState.HALF_OPEN
                    return True
                return False
            return True

    async def record_success(self):
        async with self._lock:
            if self.state == CircuitState.HALF_OPEN:
                self.state = CircuitState.CLOSED
                self.failure_count = 0

    async def record_failure(self):
        async with self._lock:
            self.failure_count += 1
            if self.failure_count >= self.config.failure_threshold:
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
        return self.writer.is_closing()

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
        await self._semaphore.acquire()

        async with self._lock:
            while self._idle:
                conn = self._idle.popleft()
                if not conn.is_closed:
                    return conn

        reader, writer = await self._timeouts.wait_for_connection(
            asyncio.open_connection(self._upstream.host, self._upstream.port)
        )
        return PooledConnection(reader, writer)

    async def release(self, conn: PooledConnection):
        if conn.is_closed:
            self._semaphore.release()
            return

        conn.last_used = time.monotonic()
        conn.request_count += 1

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
                if conn.is_closed or (
                    now - conn.last_used > self._keepalive_cfg.idle_timeout_sec
                ):
                    await conn.close()
                else:
                    keep.append(conn)
            self._idle = keep

    async def shutdown(self):
        async with self._lock:
            while self._idle:
                conn = self._idle.popleft()
                await conn.close()


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
        self._status: dict[Upstream, bool] = {u: True for u in self._upstreams}
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
            self._breakers = {u: CircuitBreaker(cb_config) for u in self._upstreams}

    def _validate_init_params(
        self, upstreams: list[Upstream], max_conns_per_upstream: int
    ) -> None:
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

    async def get_next_alive(
        self, excluded: set[Upstream] | None = None
    ) -> Upstream | None:
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
                if breaker and not await breaker.allow_request():
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
                        "Upstream %s status changed: %s -> %s",
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
                    asyncio.open_connection(
                        upstream.host, upstream.port, ssl=ssl_context
                    ),
                    timeout=self._timeouts.connect_sec,
                )
                writer.close()
                await writer.wait_closed()

                await self.set_status(upstream, True)
                logger.debug("Healthcheck OK for %s", addr)
                return True

            except asyncio.TimeoutError:
                logger.warning(
                    "Healthcheck timeout for %s (connect timeout %.2f s)",
                    addr,
                    self._timeouts.connect_sec,
                )
                await self.set_status(upstream, False)
                return False

            except ConnectionRefusedError:
                logger.warning("Healthcheck failed for %s: connection refused", addr)
                await self.set_status(upstream, False)
                return False

            except ssl.SSLError as e:
                logger.warning("Healthcheck failed for %s: SSL error: %s", addr, e)
                await self.set_status(upstream, False)
                return False

            except OSError as e:
                await self.set_status(upstream, False)
                if isinstance(e, socket.gaierror):
                    logger.error(
                        "Healthcheck failed for %s: DNS resolution error: %s",
                        addr,
                        e,
                    )
                else:
                    logger.warning("Healthcheck failed for %s: %s", addr, e)
                return False

            except Exception:
                await self.set_status(upstream, False)
                logger.exception("Healthcheck unexpected error for %s", addr)
                return False

    async def run_initial_healthcheck(self) -> None:
        tasks = [self.healthcheck(u) for u in self._upstreams]
        await asyncio.gather(*tasks, return_exceptions=True)
        logger.info(
            "Initial healthcheck completed. Alive: %d/%d",
            len(self.alive_upstreams),
            len(self._upstreams),
        )

    async def periodic_healthcheck(
        self, interval_sec: float, stop_event: asyncio.Event
    ) -> None:
        while not stop_event.is_set() and not self._shutdown:
            try:
                await self.run_initial_healthcheck()
                await asyncio.sleep(interval_sec)
            except asyncio.CancelledError:
                logger.info("Periodic healthcheck cancelled")
                break
            except Exception:
                logger.exception("Periodic healthcheck error")

    async def reap_idle_connections(
        self, interval_sec: float, stop_event: asyncio.Event
    ):
        while not stop_event.is_set() and not self._shutdown:
            try:
                for pool in self._per_upstream_pools.values():
                    await pool.close_idle()
                await asyncio.sleep(interval_sec)
            except asyncio.CancelledError:
                break

    async def shutdown(self) -> None:
        logger.info("Starting upstream pool shutdown...")
        self._shutdown = True

        for pool in self._per_upstream_pools.values():
            await pool.shutdown()

        async with self._lock:
            self._upstreams.clear()
            self._status.clear()

        logger.info("Upstream pool shutdown complete")

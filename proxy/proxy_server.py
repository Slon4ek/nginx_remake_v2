import asyncio
import logging
import signal
from contextlib import suppress

from proxy.client_handler import ClientHandler
from proxy.metrics import ProxyMetrics
from proxy.models import KeepAliveConfig
from proxy.rate_limiter import RateLimiter
from proxy.timeouts import Timeouts
from proxy.upstream_pool import UpstreamsPool

logger = logging.getLogger(__name__)


class ReverseProxy:
    def __init__(
        self,
        host: str,
        port: int,
        upstream_pool: UpstreamsPool,
        timeouts: Timeouts,
        max_conns: int,
        max_requests: int,
        metrics: ProxyMetrics,
        keepalive: KeepAliveConfig,
        rate_limiter: RateLimiter | None = None,
    ):
        self.host = host
        self.port = port
        self.pool = upstream_pool
        self.timeouts = timeouts
        self.max_conns = max_conns
        self.keepalive = keepalive
        self.rate_limiter = rate_limiter

        self._server: asyncio.AbstractServer | None = None
        self._is_shutting_down = False
        self._shutdown_event = asyncio.Event()
        self._metrics = metrics
        self._total_connections_semaphore: asyncio.Semaphore | None = None
        self._client_tasks: set[asyncio.Task] = set()
        self._request_semaphore: asyncio.Semaphore = asyncio.Semaphore(max_requests)

    @property
    def get_shutdown_event(self) -> asyncio.Event:
        return self._shutdown_event

    async def start(self):
        """Запускает TCP-сервер и ждёт сигнала остановки."""
        self._total_connections_semaphore = asyncio.Semaphore(self.max_conns)

        self._server = await asyncio.start_server(
            self.handle_connection, self.host, self.port
        )

        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            with suppress(NotImplementedError):
                loop.add_signal_handler(sig, lambda: asyncio.create_task(self.stop()))

        addr = self._server.sockets[0].getsockname()
        logger.info("Reverse Proxy запущен и слушает на %s:%d", *addr)

        async with self._server:
            await self._shutdown_event.wait()

    async def stop(self):
        """
        Graceful shutdown:
        1. Прекратить приём новых соединений
        2. Подождать завершения активных (с таймаутом)
        3. Остановить пул upstream'ов
        4. Сигналить остальным корутинам об остановке
        """
        if self._is_shutting_down:
            return

        self._is_shutting_down = True
        logger.info("Получен сигнал на остановку. Запуск Graceful Shutdown...")

        if self._server:
            self._server.close()
            await self._server.wait_closed()
            logger.info("Сервер прекратил приём новых клиентских соединений")

        shutdown_timeout = self.timeouts.total_sec
        logger.info(
            "Ожидание завершения %d активных соединений (таймаут %.2f сек)...",
            self._metrics.active_connections,
            shutdown_timeout,
        )

        steps = int(shutdown_timeout * 10)
        for _ in range(steps):
            if self._metrics.active_connections == 0:
                break
            await asyncio.sleep(0.1)

        if self._metrics.active_connections > 0:
            logger.warning(
                "Не все соединения закрылись плавно, принудительно завершаем работу"
            )
            for task in self._client_tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*self._client_tasks, return_exceptions=True)

        logger.info("Останавливаем пул апстримов...")
        await self.pool.shutdown()

        self._shutdown_event.set()
        logger.info("Reverse Proxy успешно остановлен")

    async def handle_connection(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ):
        """
        Принимает TCP-соединение от клиента.
        Если keep-alive включён — обрабатывает несколько
        запросов на одном соединении.

        Логика keep-alive:
          while keep_alive:
              handler = ClientHandler(...)
              keep_alive = await handler.handle()

        Если handler.handle() вернул False — выходим из цикла
        и закрываем клиентский сокет.
        """
        if self._is_shutting_down:
            await self._send_service_unavailable(writer)
            return

        if self.rate_limiter:
            peer = writer.get_extra_info("peername")
            client_ip = peer[0] if peer else None
            if not await self.rate_limiter.allow(client_ip):
                logger.warning("Rate limit exceeded for %s", client_ip)
                await self._send_429_too_many_requests(writer)
                return

        task = asyncio.current_task()
        try:
            self._client_tasks.add(task)
            async with self._total_connections_semaphore:
                await self._metrics.inc_active()
                try:
                    keep_alive = True
                    requests_handled = 0

                    while keep_alive and not self._is_shutting_down:
                        if requests_handled >= self.keepalive.max_requests:
                            logger.debug(
                                "Max requests per connection reached (%d)",
                                self.keepalive.max_requests,
                            )
                            break

                        upstream_target = await self.pool.get_next_alive()

                        if not upstream_target:
                            logger.error("Нет доступных живых апстримов в пуле")
                            await self._send_service_unavailable(writer)
                            break

                        handler = ClientHandler(
                            client_reader=reader,
                            client_writer=writer,
                            upstream=upstream_target,
                            pool=self.pool,
                            timeouts=self.timeouts,
                            keepalive=self.keepalive,
                            metrics=self._metrics,
                        )
                        async with self._request_semaphore:
                            try:
                                keep_alive = await asyncio.wait_for(
                                    handler.handle(), timeout=self.timeouts.total_sec
                                )
                            except asyncio.TimeoutError:
                                logger.warning(
                                    "Total timeout exceeded for %s",
                                    upstream_target,
                                )
                                break

                        if writer.is_closing():
                            break

                        requests_handled += 1

                        if keep_alive:
                            await asyncio.sleep(0)
                except (RuntimeError, ValueError) as e:
                    logger.error("Внутренний сбой инфраструктуры пула: %s", e)
                    await self._send_service_unavailable(writer)
                except (OSError, ConnectionError) as e:
                    logger.debug("Сетевой сбой при инициализации соединения: %s", e)
                finally:
                    await self._metrics.dec_active()
                    await self._close_writer(writer)
        finally:
            self._client_tasks.discard(task)

    # ─── Отправка ошибок клиенту ─────────────────────────────

    async def _send_service_unavailable(self, writer: asyncio.StreamWriter):
        """503 Service Unavailable + закрытие соединения."""
        with suppress(OSError, ConnectionError):
            response = (
                b"HTTP/1.1 503 Service Unavailable\r\n"
                b"Content-Type: text/plain; charset=utf-8\r\n"
                b"Content-Length: 23\r\n"
                b"Connection: close\r\n\r\n"
                b"503 Service Unavailable"
            )
            writer.write(response)
            await writer.drain()
        await self._close_writer(writer)

    async def _send_429_too_many_requests(self, writer: asyncio.StreamWriter):
        """429 Too Many Requests + закрытие соединения."""
        with suppress(OSError, ConnectionError):
            response = (
                b"HTTP/1.1 429 Too Many Requests\r\n"
                b"Content-Type: text/plain; charset=utf-8\r\n"
                b"Content-Length: 22\r\n"
                b"Connection: close\r\n\r\n"
                b"429 Too Many Requests"
            )
            writer.write(response)
            await writer.drain()
        await self._close_writer(writer)

    async def _close_writer(self, writer: asyncio.StreamWriter):
        """Безопасно закрывает writer."""
        with suppress(OSError, ConnectionError):
            writer.close()
            await writer.wait_closed()

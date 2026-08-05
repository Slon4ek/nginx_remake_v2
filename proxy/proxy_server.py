# Standard Library
import asyncio
import logging
import signal
from contextlib import suppress

# Project Modules
from proxy.client_handler import ClientHandler
from proxy.config import ProxyConfig
from proxy.http_parser import send_error_response
from proxy.metrics import ProxyMetrics
from proxy.models import KeepAliveConfig, RetryConfig
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
        metrics: ProxyMetrics,
        keepalive: KeepAliveConfig,
        retry_config: RetryConfig,
        config_path: str,
        rate_limiter: RateLimiter | None = None,
    ):
        self.host = host
        self.port = port
        self.pool = upstream_pool
        self.timeouts = timeouts
        self.max_conns = max_conns
        self.keepalive = keepalive
        self.rate_limiter = rate_limiter
        self.retry_config = retry_config
        self._server: asyncio.AbstractServer | None = None
        self._is_shutting_down = False
        self._shutdown_event = asyncio.Event()
        self._metrics = metrics
        self._total_connections_semaphore: asyncio.Semaphore | None = None
        self._client_tasks: set[asyncio.Task] = set()
        self.config_path = config_path

    @property
    def get_shutdown_event(self) -> asyncio.Event:
        return self._shutdown_event

    async def start(self):
        """Запускает TCP-сервер и ждёт сигнала остановки."""
        self._total_connections_semaphore = asyncio.Semaphore(self.max_conns)

        self._server = await asyncio.start_server(self.handle_connection, self.host, self.port)

        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            with suppress(NotImplementedError):
                loop.add_signal_handler(sig, lambda: asyncio.create_task(self._stop()))

        sighup = getattr(signal, "SIGHUP", None)
        if sighup is not None:
            with suppress(NotImplementedError):
                loop.add_signal_handler(
                    sighup, lambda: asyncio.create_task(self._reload_config(self.config_path))
                )

        addr = self._server.sockets[0].getsockname()
        logger.info("Reverse Proxy запущен и слушает на %s:%d", *addr)

        async with self._server:
            await self._shutdown_event.wait()

    async def _reload_config(self, config_path: str):
        """
        Перечитывает config.yaml на лету.

        Меняет:
          - timeouts
          - limits
          - upstream список (пересоздаёт под-пулы)
          - keepalive
          - rate limiter

        Не меняет (требуют рестарта):
          - listen (адрес/порт)
          - tls (сертификаты)
        """
        logger.info("SIGHUP получен. Перезагрузка конфигурации...")

        try:
            new_config = ProxyConfig.from_yaml(config_path)

            self.timeouts = new_config.timeouts
            self.max_conns = new_config.limits.max_client_conns
            self.keepalive = new_config.keep_alive

            await self.pool.shutdown()

            new_pool = UpstreamsPool(
                upstreams=new_config.upstreams,
                timeouts=new_config.timeouts,
                max_conns_per_upstream=new_config.limits.max_conns_per_upstream,
                cb_config=new_config.circuit_breaker,
                upstream_keepalive=new_config.upstream_keepalive,
            )
            self.pool = new_pool

            self.rate_limiter = new_config.rate_limiter

            logger.info(
                "Конфигурация перезагружена: %d upstream'ов, keep-alive=%s, timeouts=%s",
                len(new_config.upstreams),
                new_config.keep_alive.enabled,
                new_config.timeouts,
            )

        except Exception:
            logger.exception("Ошибка перезагрузки конфигурации")

    async def _stop(self):
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
            logger.warning("Не все соединения закрылись плавно, принудительно завершаем работу")
            for task in self._client_tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*self._client_tasks, return_exceptions=True)

        logger.info("Останавливаем пул апстримов...")
        await self.pool.shutdown()

        self._shutdown_event.set()
        logger.info("Reverse Proxy успешно остановлен")

    async def handle_connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
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
            await self._send_error_response(writer, 503)
            return

        if self.rate_limiter:
            peer = writer.get_extra_info("peername")
            client_ip = peer[0] if peer else None
            if not await self.rate_limiter.allow(client_ip):
                logger.warning("Превышен лимит запросов для клиента %s", client_ip)
                await self._send_error_response(writer, 429)
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
                                "Достигнут лимит запросов на соединение (%d)",
                                self.keepalive.max_requests,
                            )
                            break

                        upstream_target = await self.pool.get_next_alive()

                        if not upstream_target:
                            logger.error("Нет доступных живых апстримов в пуле")
                            await self._send_error_response(writer, 503)
                            break

                        handler = ClientHandler(
                            client_reader=reader,
                            client_writer=writer,
                            upstream=upstream_target,
                            pool=self.pool,
                            timeouts=self.timeouts,
                            keepalive=self.keepalive,
                            metrics=self._metrics,
                            retry_config=self.retry_config,
                        )
                        try:
                            keep_alive = await asyncio.wait_for(
                                handler.handle(), timeout=self.timeouts.total_sec
                            )
                        except TimeoutError:
                            logger.warning(
                                "Превышен общий таймаут для %s",
                                upstream_target,
                            )
                            await handler.record_metrics()
                            if not handler.response_committed:
                                await self._send_error_response(writer, 504)
                            break

                        if writer.is_closing():
                            break

                        requests_handled += 1

                        if keep_alive:
                            await asyncio.sleep(0)
                except (RuntimeError, ValueError) as e:
                    logger.error("Внутренний сбой инфраструктуры пула: %s", e)
                    await self._send_error_response(writer, 503)
                except (OSError, ConnectionError) as e:
                    logger.debug("Сетевой сбой при инициализации соединения: %s", e)
                finally:
                    await self._metrics.dec_active()
                    await self._close_writer(writer)
        finally:
            self._client_tasks.discard(task)

    async def periodic_healthcheck(self, interval_sec: float) -> None:
        while not self._shutdown_event.is_set():
            try:
                await self.pool.run_initial_healthcheck()
                await asyncio.sleep(interval_sec)
            except asyncio.CancelledError:
                logger.info("Периодическая проверка здоровья остановлена")
                break

    async def reap_idle_connections(self, interval_sec: float) -> None:
        while not self._shutdown_event.is_set():
            try:
                for pool in self.pool.per_upstream_pools.values():
                    await pool.close_idle()
                await asyncio.sleep(interval_sec)
            except asyncio.CancelledError:
                logger.info("Очистка неактивных соединений остановлена")
                break

    async def _send_error_response(self, writer: asyncio.StreamWriter, status_code: int) -> None:
        await send_error_response(writer, status_code)
        await self._close_writer(writer)

    async def _close_writer(self, writer: asyncio.StreamWriter):
        """Безопасно закрывает writer."""
        with suppress(OSError, ConnectionError):
            writer.close()
            await writer.wait_closed()

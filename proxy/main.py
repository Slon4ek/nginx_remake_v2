import asyncio
import logging
import signal

from aiohttp import web

from proxy.config import ProxyConfig
from proxy.logger import LoggingConfigurator
from proxy.metrics import ProxyMetrics, create_app
from proxy.proxy_server import ReverseProxy
from proxy.upstream_pool import UpstreamsPool

logger = logging.getLogger(__name__)
metrics = ProxyMetrics()


async def run_metrics_server(
    host: str,
    port: int,
    shutdown_event: asyncio.Event,
):
    """
    Отдельный HTTP-сервер для /metrics на aiohttp.
    Живёт пока жив proxy — shutdown_event завершает его при stop().
    """
    app = create_app(metrics)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host, port)
    await site.start()
    logger.info("Metrics server started on %s:%d", host, port)
    try:
        await shutdown_event.wait()
    except asyncio.CancelledError:
        pass
    finally:
        await runner.cleanup()


async def run_idle_reaper(
    pool: UpstreamsPool,
    interval_sec: float,
    shutdown_event: asyncio.Event,
):
    """
    Фоновая задача: каждые interval_sec закрывает
    просроченные idle-соединения к upstream'ам.
    """
    await pool.reap_idle_connections(interval_sec, shutdown_event)


async def main():

    config_path = "config.yaml"
    config = ProxyConfig.from_yaml(config_path)
    LoggingConfigurator(config).setup()

    logger.info(
        "Запуск Reverse Proxy. Listen: %s, Upstreams: %s",
        config.listen,
        [str(u) for u in config.upstreams],
    )

    pool = UpstreamsPool(
        upstreams=config.upstreams,
        timeouts=config.timeouts,
        max_conns_per_upstream=config.limits.max_conns_per_upstream,
        cb_config=config.circuit_breaker,
        upstream_keepalive=config.upstream_keepalive,
    )

    listen_host, listen_port_str = config.listen.split(":")
    listen_port = int(listen_port_str)

    server = ReverseProxy(
        host=listen_host,
        port=listen_port,
        upstream_pool=pool,
        timeouts=config.timeouts,
        max_conns=config.limits.max_client_conns,
        keepalive=config.keep_alive,
        rate_limiter=config.rate_limiter,
        metrics=metrics,
        max_requests=config.keep_alive.max_requests,
        retry_config=config.retry
    )

    try:
        loop = asyncio.get_running_loop()
        loop.add_signal_handler(
            signal.SIGHUP,
            lambda: asyncio.create_task(reload_config(config_path, server, pool)),
        )
        logger.info("SIGHUP handler registered for hot reload")
    except (NotImplementedError, AttributeError):
        logger.warning("SIGHUP not supported on this platform")

    await asyncio.gather(
        server.start(),
        run_metrics_server(
            "127.0.0.1",
            8081,
            server.get_shutdown_event,
        ),
        pool.periodic_healthcheck(
            30,
            server.get_shutdown_event,
        ),
        run_idle_reaper(
            pool,
            10,
            server.get_shutdown_event,
        ),
        return_exceptions=True,
    )

    logger.info("Reverse Proxy завершил работу")


async def reload_config(
    config_path: str,
    server: ReverseProxy,
    pool: UpstreamsPool,
):
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

        server.timeouts = new_config.timeouts
        server.max_conns = new_config.limits.max_client_conns
        server.keepalive = new_config.keep_alive

        await pool.shutdown()

        new_pool = UpstreamsPool(
            upstreams=new_config.upstreams,
            timeouts=new_config.timeouts,
            max_conns_per_upstream=new_config.limits.max_conns_per_upstream,
            cb_config=new_config.circuit_breaker,
            upstream_keepalive=new_config.upstream_keepalive,
        )
        server.pool = new_pool

        server.rate_limiter = new_config.rate_limit

        logger.info(
            "Конфигурация перезагружена: %d upstream'ов, keep-alive=%s, timeouts=%s",
            len(new_config.upstreams),
            new_config.keep_alive.enabled,
            new_config.timeouts,
        )

    except Exception as e:
        logger.error("Ошибка перезагрузки конфигурации: %s", e)
        logger.exception("Stack trace:")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass

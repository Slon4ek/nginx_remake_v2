# Standard Library
import asyncio
import logging
from contextlib import suppress

# Project Modules
from proxy.config import ProxyConfig
from proxy.logger import LoggingConfigurator
from proxy.metrics import ProxyMetrics, run_metrics_server
from proxy.proxy_server import ReverseProxy
from proxy.upstream_pool import UpstreamsPool

logger = logging.getLogger(__name__)
metrics = ProxyMetrics()


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
        retry_config=config.retry,
        config_path=config_path,
    )

    await asyncio.gather(
        server.start(),
        run_metrics_server(
            "127.0.0.1",
            8081,
            metrics,
            server.get_shutdown_event,
        ),
        server.periodic_healthcheck(30),
        server.reap_idle_connections(30),
        return_exceptions=True,
    )

    logger.info("Reverse Proxy завершил работу")


if __name__ == "__main__":
    with suppress(KeyboardInterrupt):
        asyncio.run(main())

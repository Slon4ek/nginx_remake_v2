from dataclasses import dataclass, field, fields
from pathlib import Path

import yaml

from proxy.rate_limiter import RateLimiter
from proxy.timeouts import Timeouts
from proxy.upstream_pool import (
    CircuitBreakerConfig,
    KeepAliveConfig,
    RetryConfig,
    Upstream,
    UpstreamKeepAliveConfig,
)


@dataclass
class Limits:
    """
    Лимиты ресурсов прокси.

    Attributes:
        max_client_conns: Максимальное количество одновременных клиентских подключений.
        max_conns_per_upstream: Максимальное количество одновременных соединений к одному upstream.
    """

    max_client_conns: int = 1000
    max_conns_per_upstream: int = 100


@dataclass
class RateLimitConfig:
    """Конфигурация ограничения скорости.

    Attributes:
        enabled: Флаг включения ограничения скорости.
        rate: Скорость ограничения в запросах в секунду.
        burst: Максимальное количество запросов в секунду.
        per_client: Флаг ограничения по клиенту.
    """

    enabled: bool = False
    rate: float = 100.0
    burst: int = 200
    per_client: bool = False


@dataclass
class LoggingConfig:
    """Конфигурация логирования.

    Attributes:
        level: Уровень логирования: DEBUG, INFO, WARNING, ERROR, CRITICAL.
        file_path: Путь к файлу лога (если None — вывод в stdout).
        format: Строка форматирования Python logging.
    """

    level: str = "INFO"
    file_path: str | None = None
    format: str = "[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s"


@dataclass
class ProxyConfig:
    listen: str = "127.0.0.1:8080"
    upstreams: list[Upstream] = field(default_factory=list[Upstream])
    timeouts: Timeouts = field(default_factory=Timeouts)
    limits: Limits = field(default_factory=Limits)
    logging: LoggingConfig = field(default_factory=LoggingConfig)
    keep_alive: KeepAliveConfig = field(default_factory=KeepAliveConfig)
    upstream_keepalive: UpstreamKeepAliveConfig = field(
        default_factory=UpstreamKeepAliveConfig
    )
    circuit_breaker: CircuitBreakerConfig | None = None
    retry: RetryConfig = field(default_factory=RetryConfig)
    rate_limit: RateLimitConfig = field(default_factory=RateLimitConfig)
    rate_limiter: RateLimiter | None = None

    @classmethod
    def from_yaml(cls, path: str | Path) -> "ProxyConfig":
        path = Path(path)
        if not path.exists():
            raise FileNotFoundError(f"Config file not found: {path}")

        with path.open("r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}

        listen = data.get("listen", "127.0.0.1:8080")
        if not isinstance(listen, str) or ":" not in listen:
            raise ValueError("'listen' must be a string in format 'host:port'")

        raw_upstreams = data.get("upstreams", [])
        if not isinstance(raw_upstreams, list):
            raise TypeError("'upstreams' must be a list")

        upstreams = []
        for i, u in enumerate(raw_upstreams):
            if not isinstance(u, dict):
                raise TypeError(f"Upstream at index {i} must be an object")
            host = u.get("host")
            port = u.get("port")
            if not host or not isinstance(host, str):
                raise ValueError(f"Upstream at index {i} missing valid 'host'")
            if port is None or not isinstance(port, int) or port < 1 or port > 65535:
                raise ValueError(
                    f"Upstream at index {i} missing valid 'port' (1–65535)"
                )
            tls = bool(u.get("tls", False))
            upstreams.append(Upstream(host=host, port=port, tls=tls))

        if not upstreams:
            raise ValueError("At least one upstream is required")

        raw_timeouts = data.get("timeouts", {})
        if not isinstance(raw_timeouts, dict):
            raise TypeError("'timeouts' must be an object")

        timeouts_kwargs = {}
        for f in fields(Timeouts):
            timeouts_kwargs[f.name] = int(
                raw_timeouts.get(f.name, getattr(Timeouts, f.name))
            )
        timeouts = Timeouts(**timeouts_kwargs)

        for name, val in [
            ("connect_ms", timeouts.connect_ms),
            ("read_ms", timeouts.read_ms),
            ("write_ms", timeouts.write_ms),
            ("total_ms", timeouts.total_ms),
        ]:
            if val <= 0:
                raise ValueError(f"'{name}' must be > 0, got {val}")

        raw_limits = data.get("limits", {})
        if not isinstance(raw_limits, dict):
            raise TypeError("'limits' must be an object")
        limits = Limits(
            max_client_conns=int(raw_limits.get("max_client_conns", 1000)),
            max_conns_per_upstream=int(raw_limits.get("max_conns_per_upstream", 100)),
        )
        if limits.max_client_conns <= 0 or limits.max_conns_per_upstream <= 0:
            raise ValueError(
                "'max_client_conns' and 'max_conns_per_upstream' must be > 0"
            )

        raw_ka = data.get("keep_alive", {})
        keep_alive = KeepAliveConfig(
            enabled=bool(raw_ka.get("enabled", True)),
            timeout_ms=int(raw_ka.get("timeout_ms", 60000)),
            max_requests=int(raw_ka.get("max_requests", 100)),
        )

        raw_uk = data.get("upstream_keepalive", {})
        upstream_keepalive = UpstreamKeepAliveConfig(
            max_idle=int(raw_uk.get("max_idle", 10)),
            idle_timeout_sec=float(raw_uk.get("idle_timeout_sec", 60.0)),
        )

        raw_cb = data.get("circuit_breaker", {})
        circuit_breaker = None
        if raw_cb:
            circuit_breaker = CircuitBreakerConfig(
                failure_threshold=int(raw_cb.get("failure_threshold", 5)),
                cooldown_sec=float(raw_cb.get("cooldown_sec", 30.0)),
            )

        raw_retry = data.get("retry", {})
        retry = RetryConfig(
            max_retries=int(raw_retry.get("max_retries", 2)),
            base_delay_ms=int(raw_retry.get("base_delay_ms", 100)),
            max_delay_ms=int(raw_retry.get("max_delay_ms", 1000)),
        )

        raw_rl = data.get("rate_limit", {})
        rate_limit = RateLimitConfig(
            enabled=bool(raw_rl.get("enabled", False)),
            rate=float(raw_rl.get("rate", 100.0)),
            burst=int(raw_rl.get("burst", 200)),
            per_client=bool(raw_rl.get("per_client", False)),
        )

        raw_logging = data.get("logging", {})
        if not isinstance(raw_logging, dict):
            raise TypeError("'logging' must be an object")
        log_level = str(raw_logging.get("level", "INFO")).upper()
        valid_levels = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
        if log_level not in valid_levels:
            if log_level == "WARN":
                log_level = "WARNING"
            else:
                raise ValueError(f"Invalid logging level: {raw_logging.get('level')}")
        logging_cfg = LoggingConfig(
            level=log_level,
            file_path=raw_logging.get("file_path"),
            format=raw_logging.get("format", LoggingConfig.format),
        )

        return cls(
            listen=listen,
            upstreams=upstreams,
            timeouts=timeouts,
            limits=limits,
            logging=logging_cfg,
            keep_alive=keep_alive,
            upstream_keepalive=upstream_keepalive,
            circuit_breaker=circuit_breaker,
            retry=retry,
            rate_limit=rate_limit,
            rate_limiter=RateLimiter(rate_limit) if rate_limit.enabled else None,
        )

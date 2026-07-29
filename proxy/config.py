from dataclasses import dataclass, field, fields
from pathlib import Path
from typing import TypeVar

import yaml

from proxy.models import (
    CircuitBreakerConfig,
    KeepAliveConfig,
    Limits,
    LoggingConfig,
    RateLimitConfig,
    RetryConfig,
    Upstream,
    UpstreamKeepAliveConfig,
)
from proxy.rate_limiter import RateLimiter
from proxy.timeouts import Timeouts

T = TypeVar("T")


def load_dataclass(cls: type[T], data: dict, prefix: str = "") -> T:
    if not isinstance(data, dict):
        raise TypeError(
            f"'{prefix}' must be an object" if prefix else "Data must be an object"
        )

    kwargs = {}
    for f in fields(cls):
        raw_val = data.get(f.name, getattr(cls, f.name))

        # Приведение типа
        if f.type is int:
            val = int(raw_val)
        elif f.type is float:
            val = float(raw_val)
        elif f.type is bool:
            val = bool(raw_val)
        else:
            val = raw_val

        meta = f.metadata
        if "gt" in meta and val <= meta["gt"]:
            raise ValueError(f"'{prefix}.{f.name}' must be > {meta['gt']}, got {val}")
        if "ge" in meta and val < meta["ge"]:
            raise ValueError(f"'{prefix}.{f.name}' must be >= {meta['ge']}, got {val}")
        if "lt" in meta and val >= meta["lt"]:
            raise ValueError(f"'{prefix}.{f.name}' must be < {meta['lt']}, got {val}")
        if "le" in meta and val > meta["le"]:
            raise ValueError(f"'{prefix}.{f.name}' must be <= {meta['le']}, got {val}")

        kwargs[f.name] = val

    return cls(**kwargs)


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
    circuit_breaker: CircuitBreakerConfig = field(default_factory=CircuitBreakerConfig)
    retry: RetryConfig = field(default_factory=RetryConfig)
    rate_limit: RateLimitConfig = field(default_factory=RateLimitConfig)
    rate_limiter: RateLimiter | None = field(default=None)

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
        timeouts = load_dataclass(Timeouts, raw_timeouts, "timeouts")

        raw_limits = data.get("limits", {})
        limits = load_dataclass(Limits, raw_limits, "limits")

        raw_ka = data.get("keep_alive", {})
        keep_alive = load_dataclass(KeepAliveConfig, raw_ka, "keep_alive")

        raw_uk = data.get("upstream_keepalive", {})
        upstream_keepalive = load_dataclass(
            UpstreamKeepAliveConfig, raw_uk, "upstream_keepalive"
        )

        raw_cb = data.get("circuit_breaker", {})
        circuit_breaker = load_dataclass(
            CircuitBreakerConfig, raw_cb, "circuit_breaker"
        )

        raw_retry = data.get("retry", {})
        retry = load_dataclass(RetryConfig, raw_retry, "retry")

        raw_rl = data.get("rate_limit", {})
        rate_limit = load_dataclass(RateLimitConfig, raw_rl, "rate_limit")

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

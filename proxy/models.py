from dataclasses import dataclass, field


@dataclass
class Limits:
    max_client_conns: int = field(default=1000, metadata={"gt": 0})
    max_conns_per_upstream: int = field(default=100, metadata={"gt": 0})


@dataclass
class RateLimitConfig:
    enabled: bool = False
    rate: float = field(default=100.0, metadata={"gt": 0})
    burst: int = field(default=200, metadata={"gt": 0})
    per_client: bool = False


@dataclass
class LoggingConfig:
    level: str = "INFO"
    file_path: str | None = None
    format: str = "[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s"


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
    timeout_ms: int = field(default=60000, metadata={"gt": 0})
    max_requests: int = field(default=200, metadata={"gt": 0})


@dataclass
class UpstreamKeepAliveConfig:
    max_idle: int = field(default=50, metadata={"gt": 0})
    idle_timeout_sec: float = field(default=30.0, metadata={"gt": 0})
    max_requests: int = field(default=200, metadata={"gt": 0})


@dataclass
class RetryConfig:
    max_retries: int = field(default=2, metadata={"ge": 0})
    base_delay_ms: int = field(default=100, metadata={"ge": 0})
    max_delay_ms: int = field(default=1000, metadata={"ge": 0})


@dataclass
class CircuitBreakerConfig:
    failure_threshold: int = field(default=10, metadata={"gt": 0})
    cooldown_sec: float = field(default=30.0, metadata={"gt": 0})

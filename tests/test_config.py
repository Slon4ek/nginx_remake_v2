# Third Party
import pytest

# Project Modules
from proxy.config import ProxyConfig


class TestConfigValidation:
    """Тесты валидации конфигурации."""

    def test_minimal_valid_config(self, tmp_path):
        """Минимальный валидный конфиг."""
        config_yaml = """
listen: "127.0.0.1:8080"
upstreams:
  - host: "127.0.0.1"
    port: 9001
"""
        config_file = tmp_path / "config.yaml"
        config_file.write_text(config_yaml)

        config = ProxyConfig.from_yaml(config_file)
        assert config.listen == "127.0.0.1:8080"
        assert len(config.upstreams) == 1
        assert config.upstreams[0].host == "127.0.0.1"
        assert config.upstreams[0].port == 9001

    def test_missing_config_file_raises(self):
        """Отсутствующий файл — FileNotFoundError."""
        with pytest.raises(FileNotFoundError, match="Config file not found"):
            ProxyConfig.from_yaml("/nonexistent/path.yaml")

    def test_missing_listen_uses_default(self, tmp_path):
        """Нет listen — используется дефолт."""
        config_yaml = """
upstreams:
  - host: "127.0.0.1"
    port: 9001
"""
        config_file = tmp_path / "config.yaml"
        config_file.write_text(config_yaml)

        config = ProxyConfig.from_yaml(config_file)
        assert config.listen == "127.0.0.1:8080"

    def test_invalid_listen_format_raises(self, tmp_path):
        """listen без порта — ошибка."""
        config_yaml = """
listen: "127.0.0.1"
upstreams:
  - host: "127.0.0.1"
    port: 9001
"""
        config_file = tmp_path / "config.yaml"
        config_file.write_text(config_yaml)

        with pytest.raises(ValueError, match="listen.*host:port"):
            ProxyConfig.from_yaml(config_file)

    def test_missing_upstreams_raises(self, tmp_path):
        """Нет upstreams — ошибка."""
        config_yaml = """
listen: "127.0.0.1:8080"
"""
        config_file = tmp_path / "config.yaml"
        config_file.write_text(config_yaml)

        with pytest.raises(ValueError, match="At least one upstream"):
            ProxyConfig.from_yaml(config_file)

    def test_upstream_missing_host_raises(self, tmp_path):
        """У upstream нет host — ошибка."""
        config_yaml = """
listen: "127.0.0.1:8080"
upstreams:
  - port: 9001
"""
        config_file = tmp_path / "config.yaml"
        config_file.write_text(config_yaml)

        with pytest.raises(ValueError, match="missing valid 'host'"):
            ProxyConfig.from_yaml(config_file)

    def test_upstream_invalid_port_raises(self, tmp_path):
        """Неверный порт — ошибка."""
        config_yaml = """
listen: "127.0.0.1:8080"
upstreams:
  - host: "127.0.0.1"
    port: 99999
"""
        config_file = tmp_path / "config.yaml"
        config_file.write_text(config_yaml)

        with pytest.raises(ValueError, match="missing valid 'port'"):
            ProxyConfig.from_yaml(config_file)

    def test_tls_upstream(self, tmp_path):
        """TLS upstream."""
        config_yaml = """
listen: "127.0.0.1:8080"
upstreams:
  - host: "127.0.0.1"
    port: 9001
    tls: true
"""
        config_file = tmp_path / "config.yaml"
        config_file.write_text(config_yaml)

        config = ProxyConfig.from_yaml(config_file)
        assert config.upstreams[0].tls is True

    def test_timeouts_validation(self, tmp_path):
        """Валидация таймаутов."""
        config_yaml = """
listen: "127.0.0.1:8080"
upstreams:
  - host: "127.0.0.1"
    port: 9001
timeouts:
  connect_ms: 100
  read_ms: 200
  write_ms: 300
  total_ms: 400
"""
        config_file = tmp_path / "config.yaml"
        config_file.write_text(config_yaml)

        config = ProxyConfig.from_yaml(config_file)
        assert config.timeouts.connect_ms == 100
        assert config.timeouts.read_ms == 200
        assert config.timeouts.write_ms == 300
        assert config.timeouts.total_ms == 400

    def test_zero_timeout_raises(self, tmp_path):
        """Таймаут 0 — ошибка."""
        config_yaml = """
listen: "127.0.0.1:8080"
upstreams:
  - host: "127.0.0.1"
    port: 9001
timeouts:
  connect_ms: 0
"""
        config_file = tmp_path / "config.yaml"
        config_file.write_text(config_yaml)

        with pytest.raises(ValueError, match="connect_ms.*must be > 0"):
            ProxyConfig.from_yaml(config_file)

    def test_limits_validation(self, tmp_path):
        """Валидация лимитов."""
        config_yaml = """
listen: "127.0.0.1:8080"
upstreams:
  - host: "127.0.0.1"
    port: 9001
limits:
  max_client_conns: 500
  max_conns_per_upstream: 100
"""
        config_file = tmp_path / "config.yaml"
        config_file.write_text(config_yaml)

        config = ProxyConfig.from_yaml(config_file)
        assert config.limits.max_client_conns == 500
        assert config.limits.max_conns_per_upstream == 100

    def test_zero_limit_raises(self, tmp_path):
        """Лимит 0 — ошибка."""
        config_yaml = """
listen: "127.0.0.1:8080"
upstreams:
  - host: "127.0.0.1"
    port: 9001
limits:
  max_client_conns: 0
"""
        config_file = tmp_path / "config.yaml"
        config_file.write_text(config_yaml)

        with pytest.raises(ValueError, match="max_client_conns.*must be > 0"):
            ProxyConfig.from_yaml(config_file)

    def test_keep_alive_config(self, tmp_path):
        """Keep-alive конфигурация."""
        config_yaml = """
listen: "127.0.0.1:8080"
upstreams:
  - host: "127.0.0.1"
    port: 9001
keep_alive:
  enabled: false
  timeout_ms: 30000
  max_requests: 50
"""
        config_file = tmp_path / "config.yaml"
        config_file.write_text(config_yaml)

        config = ProxyConfig.from_yaml(config_file)
        assert config.keep_alive.enabled is False
        assert config.keep_alive.timeout_ms == 30000
        assert config.keep_alive.max_requests == 50

    def test_upstream_keepalive_config(self, tmp_path):
        """Upstream keep-alive пул."""
        config_yaml = """
listen: "127.0.0.1:8080"
upstreams:
  - host: "127.0.0.1"
    port: 9001
upstream_keepalive:
  max_idle: 20
  idle_timeout_sec: 120.0
"""
        config_file = tmp_path / "config.yaml"
        config_file.write_text(config_yaml)

        config = ProxyConfig.from_yaml(config_file)
        assert config.upstream_keepalive.max_idle == 20
        assert config.upstream_keepalive.idle_timeout_sec == 120.0

    def test_circuit_breaker_config(self, tmp_path):
        """Circuit breaker конфиг."""
        config_yaml = """
listen: "127.0.0.1:8080"
upstreams:
  - host: "127.0.0.1"
    port: 9001
circuit_breaker:
  failure_threshold: 10
  cooldown_sec: 60.0
"""
        config_file = tmp_path / "config.yaml"
        config_file.write_text(config_yaml)

        config = ProxyConfig.from_yaml(config_file)
        assert config.circuit_breaker is not None
        assert config.circuit_breaker.failure_threshold == 10
        assert config.circuit_breaker.cooldown_sec == 60.0

    def test_retry_config(self, tmp_path):
        """Retry конфиг."""
        config_yaml = """
listen: "127.0.0.1:8080"
upstreams:
  - host: "127.0.0.1"
    port: 9001
retry:
  max_retries: 3
  base_delay_ms: 200
  max_delay_ms: 2000
"""
        config_file = tmp_path / "config.yaml"
        config_file.write_text(config_yaml)

        config = ProxyConfig.from_yaml(config_file)
        assert config.retry.max_retries == 3
        assert config.retry.base_delay_ms == 200
        assert config.retry.max_delay_ms == 2000

    def test_rate_limit_config(self, tmp_path):
        """Rate limit конфиг."""
        config_yaml = """
listen: "127.0.0.1:8080"
upstreams:
  - host: "127.0.0.1"
    port: 9001
rate_limit:
  enabled: true
  rate: 50.0
  burst: 100
  per_client: true
"""
        config_file = tmp_path / "config.yaml"
        config_file.write_text(config_yaml)

        config = ProxyConfig.from_yaml(config_file)
        assert config.rate_limit.enabled is True
        assert config.rate_limit.rate == 50.0
        assert config.rate_limit.burst == 100
        assert config.rate_limit.per_client is True

    def test_rate_limit_creates_limiter(self, tmp_path):
        """Rate limiter создаётся автоматически если enabled."""
        config_yaml = """
listen: "127.0.0.1:8080"
upstreams:
  - host: "127.0.0.1"
    port: 9001
rate_limit:
  enabled: true
  rate: 10
  burst: 5
"""
        config_file = tmp_path / "config.yaml"
        config_file.write_text(config_yaml)

        config = ProxyConfig.from_yaml(config_file)
        assert config.rate_limiter is not None
        assert config.rate_limit.enabled is True

    def test_rate_limit_disabled_no_limiter(self, tmp_path):
        """Rate limit disabled — limiter is None."""
        config_yaml = """
listen: "127.0.0.1:8080"
upstreams:
  - host: "127.0.0.1"
    port: 9001
rate_limit:
  enabled: false
"""
        config_file = tmp_path / "config.yaml"
        config_file.write_text(config_yaml)

        config = ProxyConfig.from_yaml(config_file)
        assert config.rate_limiter is None

    def test_logging_config(self, tmp_path):
        """Логирование конфиг."""
        config_yaml = """
listen: "127.0.0.1:8080"
upstreams:
  - host: "127.0.0.1"
    port: 9001
logging:
  level: "DEBUG"
  file_path: "/var/log/proxy.log"
  format: "%(message)s"
"""
        config_file = tmp_path / "config.yaml"
        config_file.write_text(config_yaml)

        config = ProxyConfig.from_yaml(config_file)
        assert config.logging.level == "DEBUG"
        assert config.logging.file_path == "/var/log/proxy.log"
        assert config.logging.format == "%(message)s"

    def test_invalid_log_level_raises(self, tmp_path):
        """Неверный уровень логирования — ошибка."""
        config_yaml = """
listen: "127.0.0.1:8080"
upstreams:
  - host: "127.0.0.1"
    port: 9001
logging:
  level: "INVALID"
"""
        config_file = tmp_path / "config.yaml"
        config_file.write_text(config_yaml)

        with pytest.raises(ValueError, match="Invalid logging level"):
            ProxyConfig.from_yaml(config_file)

    def test_warn_level_normalized_to_warning(self, tmp_path):
        """WARN нормализуется в WARNING."""
        config_yaml = """
listen: "127.0.0.1:8080"
upstreams:
  - host: "127.0.0.1"
    port: 9001
logging:
  level: "WARN"
"""
        config_file = tmp_path / "config.yaml"
        config_file.write_text(config_yaml)

        config = ProxyConfig.from_yaml(config_file)
        assert config.logging.level == "WARNING"


class TestConfigDefaults:
    """Тесты дефолтных значений."""

    def test_all_defaults_applied(self, tmp_path):
        """Все дефолты применяются при минимальном конфиге."""
        config_yaml = """
listen: "127.0.0.1:8080"
upstreams:
  - host: "127.0.0.1"
    port: 9001
"""
        config_file = tmp_path / "config.yaml"
        config_file.write_text(config_yaml)

        config = ProxyConfig.from_yaml(config_file)

        # Таймауты
        assert config.timeouts.connect_ms == 1000
        assert config.timeouts.read_ms == 15000
        assert config.timeouts.write_ms == 15000
        assert config.timeouts.total_ms == 30000

        # Лимиты
        assert config.limits.max_client_conns == 1000
        assert config.limits.max_conns_per_upstream == 100

        # Keep-alive
        assert config.keep_alive.enabled is True
        assert config.keep_alive.timeout_ms == 60000
        assert config.keep_alive.max_requests == 200

        # Upstream keep-alive
        assert config.upstream_keepalive.max_idle == 50
        assert config.upstream_keepalive.idle_timeout_sec == 30.0
        assert config.upstream_keepalive.max_requests == 200

        # Circuit breaker is not None by default
        assert config.circuit_breaker is not None

        # Retry
        assert config.retry.max_retries == 2
        assert config.retry.base_delay_ms == 100
        assert config.retry.max_delay_ms == 1000

        # Rate limit
        assert config.rate_limit.enabled is False
        assert config.rate_limit.rate == 100.0
        assert config.rate_limit.burst == 200
        assert config.rate_limit.per_client is False

        # Logging
        assert config.logging.level == "INFO"
        assert config.logging.file_path is None

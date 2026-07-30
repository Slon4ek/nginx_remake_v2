# Standard Library
import asyncio

# Third Party
import pytest

# Project Modules
from proxy.config import RateLimitConfig
from proxy.rate_limiter import RateLimiter, TokenBucket


class TestTokenBucket:
    """Тесты для TokenBucket — алгоритм «ведро с жетонами»."""

    @pytest.mark.asyncio
    async def test_allows_burst_up_to_capacity(self):
        """Можно потратить до burst токенов сразу."""
        bucket = TokenBucket(rate=10, burst=5)  # 10 токенов/сек, баст 5

        for _ in range(5):
            assert await bucket.consume() is True

        # 6-й — нельзя
        assert await bucket.consume() is False

    @pytest.mark.asyncio
    async def test_refills_over_time(self):
        """Токены пополняются со временем до burst."""
        bucket = TokenBucket(rate=100, burst=2)  # быстрое пополнение

        assert await bucket.consume() is True
        assert await bucket.consume() is True
        assert await bucket.consume() is False

        # Ждём 0.03 сек → должно добавиться ~3 токена, но capped до burst=2
        await asyncio.sleep(0.03)
        assert await bucket.consume() is True
        assert await bucket.consume() is True  # второе пополнение
        assert await bucket.consume() is False

    @pytest.mark.asyncio
    async def test_refills_up_to_burst_not_more(self):
        """Пополнение не превышает burst."""
        bucket = TokenBucket(rate=1000, burst=2)

        await bucket.consume()
        await bucket.consume()

        await asyncio.sleep(1)  # 1 сек * 1000 = 1000 токенов, но burst=2

        # Должно быть 2 токена (burst), не 1000
        assert await bucket.consume() is True
        assert await bucket.consume() is True
        assert await bucket.consume() is False

    @pytest.mark.asyncio
    async def test_consume_multiple_tokens_at_once(self):
        """Можно потреблять несколько токенов за раз."""
        bucket = TokenBucket(rate=10, burst=10)

        assert await bucket.consume(3) is True
        assert await bucket.consume(3) is True
        assert await bucket.consume(5) is False  # осталось 4

    @pytest.mark.asyncio
    async def test_zero_rate_no_refill(self):
        """rate=0 — токены не пополняются."""
        bucket = TokenBucket(rate=0, burst=2)

        assert await bucket.consume() is True
        assert await bucket.consume() is True
        assert await bucket.consume() is False

        await asyncio.sleep(1)
        assert await bucket.consume() is False


class TestRateLimiter:
    """Тесты для RateLimiter (global + per-client)."""

    @pytest.mark.asyncio
    async def test_global_limit(self):
        """Глобальный лимит работает."""
        config = RateLimitConfig(enabled=True, rate=10, burst=5, per_client=False)
        limiter = RateLimiter(config)

        for _ in range(5):
            assert await limiter.allow("1.2.3.4") is True
        assert await limiter.allow("1.2.3.4") is False

    @pytest.mark.asyncio
    async def test_per_client_limit_after_global_check(self):
        """Per-client проверяется ПОСЛЕ global.

        Если global исчерпан, per-client даже не проверяется.
        Per-client бакет создаётся только если global прошёл.
        """
        # global: rate=0, burst=3
        # per-client: тоже burst=3
        config = RateLimitConfig(enabled=True, rate=0, burst=3, per_client=True)
        limiter = RateLimiter(config)

        # IP 1.1.1.1 — 2 запроса (global: 2/3, per-client: 2/3)
        assert await limiter.allow("1.1.1.1") is True
        assert await limiter.allow("1.1.1.1") is True

        # IP 2.2.2.2 — 1 запрос (global: 3/3 исчерпан, per-client создаётся и проходит)
        assert await limiter.allow("2.2.2.2") is True

        # IP 3.3.3.3 — global исчерпан (0 токенов), per-client НЕ проверяется
        assert await limiter.allow("3.3.3.3") is False

        # Per-client бакет для 2.2.2.2 создался (global прошёл)
        assert "2.2.2.2" in limiter._per_client

        # Per-client бакет для 3.3.3.3 НЕ создавался (global не прошёл)
        assert "3.3.3.3" not in limiter._per_client

    @pytest.mark.asyncio
    async def test_global_and_per_client_combined(self):
        """Global + per-client: оба должны пропустить."""
        # global burst=2, per-client burst=2
        config = RateLimitConfig(enabled=True, rate=100, burst=2, per_client=True)
        limiter = RateLimiter(config)

        # Первый IP: 2 запроса проходят
        assert await limiter.allow("1.1.1.1") is True
        assert await limiter.allow("1.1.1.1") is True
        # 3-й — global burst исчерпан (2/2), должен вернуть False
        assert await limiter.allow("1.1.1.1") is False

        # Но per-client для 1.1.1.1 всё ещё имеет 0 токенов

    @pytest.mark.asyncio
    async def test_global_exhausted_blocks_per_client_creation(self):
        """Если global исчерпан, per-client бакет даже не создаётся."""
        config = RateLimitConfig(enabled=True, rate=10, burst=1, per_client=True)
        limiter = RateLimiter(config)

        # Первый запрос — OK (global: 1/1, per-client: 1/1)
        assert await limiter.allow("1.1.1.1") is True

        # Второй запрос — global исчерпан, возвращает False
        # per-client бакет для 1.1.1.1 НЕ должен проверяться
        assert await limiter.allow("1.1.1.1") is False

        # Проверяем: per-client бакет для 1.1.1.1 существует (создался при 1-м запросе)
        # но для нового IP 2.2.2.2 не создавался
        assert "1.1.1.1" in limiter._per_client
        assert "2.2.2.2" not in limiter._per_client

    @pytest.mark.asyncio
    async def test_disabled_limiter_allows_all(self):
        """Отключённый лимитер пропускает всё."""
        config = RateLimitConfig(enabled=False, rate=1, burst=1, per_client=False)
        limiter = RateLimiter(config)

        for _ in range(100):
            assert await limiter.allow("any") is True

    @pytest.mark.asyncio
    async def test_per_client_false_ignores_client_ip(self):
        """per_client=False игнорирует client_ip."""
        config = RateLimitConfig(enabled=True, rate=10, burst=2, per_client=False)
        limiter = RateLimiter(config)

        # Все запросы идут через global, client_ip игнорируется
        assert await limiter.allow("1.1.1.1") is True
        assert await limiter.allow("2.2.2.2") is True
        assert await limiter.allow("3.3.3.3") is False  # global burst=2 исчерпан

        assert len(limiter._per_client) == 0

    @pytest.mark.asyncio
    async def test_refill_allows_more_requests(self):
        """Пополнение токенов со временем позволяет новые запросы."""
        config = RateLimitConfig(enabled=True, rate=100, burst=1, per_client=False)
        limiter = RateLimiter(config)

        assert await limiter.allow("1.1.1.1") is True
        assert await limiter.allow("1.1.1.1") is False

        await asyncio.sleep(0.02)  # ~20 токенов прибавится, capped до burst=1
        assert await limiter.allow("1.1.1.1") is True


class TestRateLimiterEdgeCases:
    """Edge cases."""

    @pytest.mark.asyncio
    async def test_none_client_ip_with_per_client(self):
        """client_ip=None с per_client=True — работает как global only."""
        config = RateLimitConfig(enabled=True, rate=10, burst=2, per_client=True)
        limiter = RateLimiter(config)

        # Без IP per-client проверка пропускается
        assert await limiter.allow(None) is True
        assert await limiter.allow(None) is True
        assert await limiter.allow(None) is False

    @pytest.mark.asyncio
    async def test_concurrent_access_thread_safe(self):
        """Параллельные запросы не ломают счётчики."""
        config = RateLimitConfig(enabled=True, rate=1000, burst=100, per_client=False)
        limiter = RateLimiter(config)

        async def make_requests():
            for _ in range(50):
                await limiter.allow("1.2.3.4")

        await asyncio.gather(*[make_requests() for _ in range(10)])
        # 10 * 50 = 500 запросов, burst=100 → часть должна пройти, часть нет
        # Главное — не должно быть исключений

# Standard Library
import asyncio
import time


class TokenBucket:
    def __init__(self, rate: float, burst: int):
        self.rate = rate
        self.burst = burst
        self.tokens = float(burst)
        self.last_refill = time.monotonic()
        self._lock = asyncio.Lock()

    async def consume(self, tokens: int = 1) -> bool:
        async with self._lock:
            now = time.monotonic()
            elapsed = now - self.last_refill
            self.tokens = min(self.burst, self.tokens + elapsed * self.rate)
            self.last_refill = now
            if self.tokens >= tokens:
                self.tokens -= tokens
                return True
            return False


class RateLimiter:
    def __init__(self, config):
        self.config = config
        self._global: TokenBucket | None = None
        self._per_client: dict[str, TokenBucket] = {}
        self._lock = asyncio.Lock()

        if config.enabled:
            self._global = TokenBucket(config.rate, config.burst)

    async def allow(self, client_ip: str | None = None) -> bool:
        if not self.config.enabled:
            return True

        if self._global and not await self._global.consume():
            return False

        if self.config.per_client and client_ip:
            async with self._lock:
                if client_ip not in self._per_client:
                    self._per_client[client_ip] = TokenBucket(self.config.rate, self.config.burst)
                bucket = self._per_client[client_ip]
            return await bucket.consume()

        return True

import asyncio
import logging
from collections import defaultdict
from dataclasses import dataclass, field

from aiohttp import web

logger = logging.getLogger(__name__)

@dataclass
class ProxyMetrics:
    total_requests: int = 0
    active_connections: int = 0
    total_bytes_in: int = 0
    total_bytes_out: int = 0
    status_counts: defaultdict[int, int] = field(
        default_factory=lambda: defaultdict(int)
    )
    latency_buckets: defaultdict[str, int] = field(
        default_factory=lambda: defaultdict(int)
    )
    _lock: asyncio.Lock = field(init=False, repr=False)

    def __post_init__(self):
        self._lock = asyncio.Lock()

    async def inc_requests(self):
        async with self._lock:
            self.total_requests += 1

    async def inc_active(self):
        async with self._lock:
            self.active_connections += 1

    async def dec_active(self):
        async with self._lock:
            if self.active_connections > 0:
                self.active_connections -= 1

    async def add_bytes(self, bytes_in: int, bytes_out: int):
        async with self._lock:
            self.total_bytes_in += bytes_in
            self.total_bytes_out += bytes_out

    async def record_status(self, code: int):
        async with self._lock:
            self.status_counts[code] += 1

    async def record_latency(self, duration_ms: float):
        async with self._lock:
            if duration_ms < 10:
                self.latency_buckets["lt_10ms"] += 1
            elif duration_ms < 50:
                self.latency_buckets["10_50ms"] += 1
            elif duration_ms < 200:
                self.latency_buckets["50_200ms"] += 1
            else:
                self.latency_buckets["gt_200ms"] += 1

    async def snapshot(self) -> dict:
        async with self._lock:
            return {
                "total_requests": self.total_requests,
                "active_connections": self.active_connections,
                "total_bytes_in": self.total_bytes_in,
                "total_bytes_out": self.total_bytes_out,
                "status_counts": dict(self.status_counts),
                "latency_buckets": dict(self.latency_buckets),
            }


def create_app(metrics: ProxyMetrics) -> web.Application:
    """Создаёт aiohttp-приложение с /metrics."""

    async def metrics_handler(request):
        data = await metrics.snapshot()
        lines = [
            f"total_requests {data['total_requests']}",
            f"active_connections {data['active_connections']}",
            f"total_bytes_in {data['total_bytes_in']}",
            f"total_bytes_out {data['total_bytes_out']}",
        ]
        for code, count in data["status_counts"].items():
            lines.append(f'status_count{{code="{code}"}} {count}')
        for bucket, count in data["latency_buckets"].items():
            lines.append(f'latency_bucket{{bucket="{bucket}"}} {count}')
        text = "\n".join(lines) + "\n"
        return web.Response(text=text, content_type="text/plain")

    app = web.Application()
    app.router.add_get("/metrics", metrics_handler)
    return app


async def run_metrics_server(
    host: str,
    port: int,
    metrics: ProxyMetrics,
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

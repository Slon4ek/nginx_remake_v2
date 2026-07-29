import asyncio
from collections.abc import Coroutine
from dataclasses import dataclass, field
from typing import Any, TypeVar

T = TypeVar("T")


@dataclass(frozen=True)
class Timeouts:
    connect_ms: int = field(default=1000, metadata={"gt": 0})
    read_ms: int = field(default=15000, metadata={"gt": 0})
    write_ms: int = field(default=15000, metadata={"gt": 0})
    total_ms: int = field(default=30000, metadata={"gt": 0})

    @property
    def connect_sec(self) -> float:
        return self.connect_ms / 1000.0

    @property
    def read_sec(self) -> float:
        return self.read_ms / 1000.0

    @property
    def write_sec(self) -> float:
        return self.write_ms / 1000.0

    @property
    def total_sec(self) -> float:
        return self.total_ms / 1000.0

    async def wait_for_connection(self, coro: Coroutine[Any, Any, T]) -> T:
        try:
            return await asyncio.wait_for(coro, timeout=self.connect_sec)
        except asyncio.TimeoutError:
            raise asyncio.TimeoutError("Connection timed out") from None

    async def wait_for_read(self, coro: Coroutine[Any, Any, T]) -> T:
        try:
            return await asyncio.wait_for(coro, timeout=self.read_sec)
        except asyncio.TimeoutError:
            raise asyncio.TimeoutError("Read timed out") from None

    async def wait_for_write(self, coro: Coroutine[Any, Any, T]) -> T:
        try:
            return await asyncio.wait_for(coro, timeout=self.write_sec)
        except asyncio.TimeoutError:
            raise asyncio.TimeoutError("Write timed out") from None

    async def wait_for_total(self, coro: Coroutine[Any, Any, T]) -> T:
        try:
            return await asyncio.wait_for(coro, timeout=self.total_sec)
        except asyncio.TimeoutError:
            raise asyncio.TimeoutError("Total timed out") from None

import asyncio
from collections import deque


class UnreadableStreamReader:
    """Обёртка над asyncio.StreamReader с поддержкой unread()."""

    def __init__(self, reader: asyncio.StreamReader):
        self._reader = reader
        self._unread_buffer = deque()

    async def read(self, n: int = -1) -> bytes:
        if self._unread_buffer:
            data = b"".join(self._unread_buffer)
            self._unread_buffer.clear()
            if n == -1:
                return data + await self._reader.read(-1)
            if len(data) >= n:
                self._unread_buffer.append(data[n:])
                return data[:n]
            n -= len(data)
            return data + await self._reader.read(n)
        return await self._reader.read(n)

    async def readexactly(self, n: int) -> bytes:
        data = await self.read(n)
        if len(data) < n:
            raise asyncio.IncompleteReadError(data, n)
        return data

    async def readline(self) -> bytes:
        if self._unread_buffer:
            data = b"".join(self._unread_buffer)
            self._unread_buffer.clear()
            idx = data.find(b"\n")
            if idx != -1:
                self._unread_buffer.append(data[idx + 1 :])
                return data[: idx + 1]
            return data + await self._reader.readline()
        return await self._reader.readline()

    def unread(self, data: bytes):
        if data:
            self._unread_buffer.appendleft(data)

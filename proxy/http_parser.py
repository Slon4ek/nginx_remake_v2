# Standard Library
import asyncio
import re
from contextlib import suppress
from dataclasses import dataclass, field

# Project Modules
from proxy.buffered_reader import UnreadableStreamReader
from proxy.timeouts import Timeouts

_CHUNK_EXT = re.compile(rb"^([0-9a-fA-F]+)(?:;.*)?\r\n")


def _headers_to_bytes(start_line: str, headers: dict[str, str]) -> bytes:
    lines = [start_line]
    for k, v in headers.items():
        lines.append(f"{k}: {v}")
    lines.append("\r\n")
    return "\r\n".join(lines).encode()


@dataclass(frozen=True)
class HttpRequest:
    method: str
    path: str
    version: str
    headers: dict[str, str] = field(default_factory=dict)

    def to_bytes(self) -> bytes:
        return _headers_to_bytes(f"{self.method} {self.path} {self.version}", self.headers)


@dataclass(frozen=True)
class HttpResponse:
    version: str
    status_code: int
    reason: str
    headers: dict[str, str] = field(default_factory=dict)

    def to_bytes(self) -> bytes:
        return _headers_to_bytes(f"{self.version} {self.status_code} {self.reason}", self.headers)


class HttpMessageParser:
    def __init__(self):
        self._buffer: bytearray = bytearray()
        self.headers_done: bool = False

    def feed(self, chunk: bytes) -> tuple[object | None, bytes]:
        if self.headers_done:
            return None, chunk

        self._buffer.extend(chunk)
        delimiter = b"\r\n\r\n"
        idx = self._buffer.find(delimiter)

        if idx == -1:
            return None, b""

        headers_part = self._buffer[:idx]
        body_remainder = bytes(self._buffer[idx + len(delimiter) :])

        self._buffer = bytearray()
        self.headers_done = True

        lines = headers_part.decode().split("\r\n")
        metadata_object = self._build_metadata(lines)
        return metadata_object, body_remainder

    def _build_metadata(self, lines: list) -> object:
        raise NotImplementedError

    @staticmethod
    def _parse_headers(header_lines: list) -> dict[str, str]:
        headers = {}
        for line in header_lines:
            if ":" in line:
                key, val = line.split(":", 1)
                headers[key.strip().lower()] = val.strip()
        return headers


class HttpRequestParser(HttpMessageParser):
    def _build_metadata(self, lines: list) -> HttpRequest:
        start_line = lines[0].split(" ", 2)
        method = start_line[0] if len(start_line) > 0 else ""
        path = start_line[1] if len(start_line) > 1 else ""
        version = start_line[2] if len(start_line) > 2 else ""
        return HttpRequest(
            method=method,
            path=path,
            version=version,
            headers=self._parse_headers(lines[1:]),
        )


class HttpResponseParser(HttpMessageParser):
    def _build_metadata(self, lines: list) -> HttpResponse:
        start_line = lines[0].split(" ", 2)
        version = start_line[0] if len(start_line) > 0 else ""
        status_raw = start_line[1] if len(start_line) > 1 else "0"
        status_code = int(status_raw) if status_raw.isdigit() else 0
        reason = start_line[2] if len(start_line) > 2 else ""
        return HttpResponse(
            version=version,
            status_code=status_code,
            reason=reason,
            headers=self._parse_headers(lines[1:]),
        )


def safe_method(method: str) -> bool:
    return method.upper() in {"GET", "HEAD", "OPTIONS", "TRACE"}


def is_connection_close(headers: dict[str, str]) -> bool:
    return headers.get("connection", "").lower() == "close"


class BodyStreamerError(Exception):
    """Ошибка стриминга тела."""


class BodyStreamer:
    """
    Стримит тело HTTP-сообщения между reader и writer,
    не накапливая данные в памяти (O(1) буфер — chunk_size).

    После завершения стриминга reader готов читать следующий запрос.
    """

    def __init__(self, timeouts: Timeouts, chunk_size: int = 65536):
        self._timeouts = timeouts
        self._chunk_size = chunk_size

    async def stream_identity(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        content_length: int,
    ) -> int:
        """Стриминг по Content-Length: ровно N байт."""
        total = 0
        while total < content_length:
            remaining = content_length - total
            want = min(self._chunk_size, remaining)
            chunk = await self._timeouts.wait_for_read(reader.read(want))
            if not chunk:
                raise BodyStreamerError(f"Premature EOF: expected {remaining} more bytes")
            writer.write(chunk)
            await self._timeouts.wait_for_write(writer.drain())
            total += len(chunk)
        return total

    async def stream_chunked(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> int:
        """Стриминг Transfer-Encoding: chunked, парсинг «на лету»."""
        total = 0
        while True:
            line = await self._timeouts.wait_for_read(reader.readline())
            m = _CHUNK_EXT.match(line)
            if not m:
                raise BodyStreamerError(f"Invalid chunk size: {line!r}")
            size = int(m.group(1), 16)
            if size == 0:
                break
            writer.write(line)
            want = size + 2
            while want > 0:
                chunk = await self._timeouts.wait_for_read(reader.read(min(self._chunk_size, want)))
                if not chunk:
                    raise BodyStreamerError(f"Premature EOF in chunk body, {want} bytes remaining")
                writer.write(chunk)
                await self._timeouts.wait_for_write(writer.drain())
                total += len(chunk)
                want -= len(chunk)
        writer.write(b"0\r\n")
        await self._timeouts.wait_for_write(writer.drain())

        while True:
            line = await self._timeouts.wait_for_read(reader.readline())
            writer.write(line)
            await self._timeouts.wait_for_write(writer.drain())
            if line == b"\r\n":
                break
        return total

    async def stream_to_eof(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> int:
        """Стриминг до закрытия соединения (HTTP/1.0 fallback)."""
        total = 0
        while True:
            chunk = await self._timeouts.wait_for_read(reader.read(self._chunk_size))
            if not chunk:
                break
            writer.write(chunk)
            await self._timeouts.wait_for_write(writer.drain())
            total += len(chunk)
        return total

    async def stream_request(
        self,
        reader: asyncio.StreamReader | UnreadableStreamReader,
        writer: asyncio.StreamWriter,
        headers: dict[str, str],
    ) -> int:
        """
        Стримит тело ЗАПРОСА (client -> upstream).
        Если нет Content-Length и нет Transfer-Encoding — тело пустое (0 байт).
        """
        cl = self._parse_content_length(headers)
        chunked = self._is_chunked(headers)

        if cl is not None:
            return await self.stream_identity(reader, writer, cl)
        if chunked:
            return await self.stream_chunked(reader, writer)
        # Нет CL и TE — для запроса тело пустое
        return 0

    async def stream_response(
        self,
        reader: asyncio.StreamReader | UnreadableStreamReader,
        writer: asyncio.StreamWriter,
        headers: dict[str, str],
    ) -> int:
        """
        Стримит тело ОТВЕТА (upstream -> client).
        Если нет Content-Length и нет Transfer-Encoding — читаем до EOF (HTTP/1.0 fallback).
        """
        cl = self._parse_content_length(headers)
        chunked = self._is_chunked(headers)

        if cl is not None:
            return await self.stream_identity(reader, writer, cl)
        if chunked:
            return await self.stream_chunked(reader, writer)
        # Нет CL и TE — для ответа читаем до EOF
        return await self.stream_to_eof(reader, writer)

    @staticmethod
    def _parse_content_length(headers: dict[str, str]) -> int | None:
        raw = headers.get("content-length")
        if raw is None:
            return None
        try:
            return int(raw)
        except ValueError:
            return None

    @staticmethod
    def _is_chunked(headers: dict[str, str]) -> bool:
        return headers.get("transfer-encoding", "").lower() == "chunked"


async def send_error_response(writer: asyncio.StreamWriter, status_code: int) -> int:
    reasons = {
        404: "Not Found",
        429: "Too Many Requests",
        500: "Internal Server Error",
        502: "Bad Gateway",
        503: "Service Unavailable",
        504: "Gateway Timeout",
    }
    reason = reasons.get(status_code, str(status_code))
    body = f"{status_code} {reason}".encode()
    response = (
        b"HTTP/1.1 " + str(status_code).encode() + b" " + reason.encode() + b"\r\n"
        b"Content-Type: text/plain; charset=utf-8\r\n"
        b"Content-Length: " + str(len(body)).encode() + b"\r\n"
        b"Connection: close\r\n\r\n" + body
    )
    with suppress(OSError, ConnectionError):
        writer.write(response)
        await writer.drain()
    return len(response)

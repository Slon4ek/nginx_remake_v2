# Standard Library
import asyncio
import errno
import logging
import time
from contextlib import suppress

# Project Modules
from proxy.buffered_reader import UnreadableStreamReader
from proxy.http_parser import (
    BodyStreamer,
    BodyStreamerError,
    HttpRequest,
    HttpRequestParser,
    HttpResponse,
    HttpResponseParser,
    is_connection_close,
    safe_method,
)
from proxy.metrics import ProxyMetrics
from proxy.models import KeepAliveConfig, RetryConfig, Upstream
from proxy.timeouts import Timeouts
from proxy.upstream_pool import UpstreamsPool

logger = logging.getLogger(__name__)


class ClientHandler:
    def __init__(
        self,
        client_reader: asyncio.StreamReader,
        client_writer: asyncio.StreamWriter,
        upstream: Upstream,
        pool: UpstreamsPool,
        timeouts: Timeouts,
        keepalive: KeepAliveConfig,
        metrics: ProxyMetrics,
        retry_config: RetryConfig,
        chunk_size: int = 4096,
    ):
        self.client_reader = UnreadableStreamReader(client_reader)
        self.client_writer = client_writer
        self.upstream = upstream
        self.pool = pool
        self.timeouts = timeouts
        self.keepalive = keepalive
        self.chunk_size = chunk_size
        self._metrics = metrics
        self._bytes_in = 0
        self._bytes_out = 0
        self._status_code = 0
        self._successful_request = True
        self._retry_config = retry_config

    async def handle(self) -> bool:
        start_request_time = time.perf_counter()
        keep_alive = False
        base_delay = self._retry_config.base_delay_ms / 1000
        max_delay = self._retry_config.max_delay_ms / 1000
        max_retries = self._retry_config.max_retries
        req_meta = await self._read_request_headers()
        if not req_meta:
            return False

        keep_alive = self._should_keep_alive(req_meta)

        logger.debug(
            "[→] %s %s -> %s",
            req_meta.method,
            req_meta.path,
            self.upstream,
        )

        for attempt in range(max_retries + 1):
            try:
                async with self.pool.acquire_connection(self.upstream) as pooled_connection:
                    up_writer = pooled_connection.writer
                    up_reader = UnreadableStreamReader(pooled_connection.reader)

                    modified_req = self._modify_request_headers(req_meta, keep_alive)
                    up_writer.write(modified_req.to_bytes())
                    await self.timeouts.wait_for_write(up_writer.drain())

                    framer = BodyStreamer(self.timeouts.read_sec, self.chunk_size)
                    self._bytes_in += await framer.stream_request(
                        self.client_reader, up_writer, req_meta.headers
                    )

                    resp_meta = await self._read_response_headers(up_reader)
                    if not resp_meta:
                        await pooled_connection.close()
                        self._successful_request = False
                        return False

                    self._status_code = resp_meta.status_code
                    if self._should_retry(req_meta.method, resp_meta) and attempt < max_retries:
                        delay = min(base_delay * (2**attempt), max_delay)
                        logger.warning(
                            "Attempt %d/%d for %s, retry in %.1fs",
                            attempt + 1,
                            max_retries,
                            self.upstream,
                            delay,
                        )
                        await asyncio.sleep(delay)
                        continue
                    elif 500 <= self._status_code < 600:
                        await self._send_502_bad_gateway()
                        keep_alive = False
                        self._successful_request = False
                        break

                    logger.debug(
                        "[←] %s %s от %s",
                        resp_meta.status_code,
                        resp_meta.reason,
                        self.upstream,
                    )

                    modified_resp = self._modify_response_headers(resp_meta, keep_alive)
                    self.client_writer.write(modified_resp.to_bytes())
                    await self.timeouts.wait_for_write(self.client_writer.drain())

                    self._bytes_out += await framer.stream_response(
                        up_reader, self.client_writer, resp_meta.headers
                    )

            except TimeoutError as e:
                logger.warning("Сетевая операция с %s прервана по таймауту: %s", self.upstream, e)
                await self._send_504_gateway_timeout()
                keep_alive = False
                self._successful_request = False
            except BodyStreamerError as e:
                logger.error("Ошибка фрейминга ответа от %s: %s", self.upstream, e)
                await self._send_502_bad_gateway()
                keep_alive = False
                self._successful_request = False
            except (ConnectionRefusedError, ConnectionResetError) as e:
                logger.error("Апстрим %s отклонил или сбросил соединение: %s", self.upstream, e)
                await self._send_502_bad_gateway()
                keep_alive = False
                self._successful_request = False
            except (OSError, ConnectionError, BrokenPipeError) as e:
                if e.errno in (errno.ECONNRESET, errno.EPIPE, errno.ECONNABORTED):
                    logger.debug("Client closed connection during write: %s", e)
                logger.error("Сетевой сбой I/O при работе с %s: %s", self.upstream, e)
                await self._send_502_bad_gateway()
                keep_alive = False
                self._successful_request = False
            except RuntimeError as e:
                logger.error("Запрос отклонен: пул апстримов перегружен или закрывается: %s", e)
                await self._send_503_service_unavailable()
                keep_alive = False
                self._successful_request = False
            finally:
                if not keep_alive:
                    with suppress(OSError, ConnectionError):
                        self.client_writer.close()
                        await self.client_writer.wait_closed()
                end_request_time = time.perf_counter()
                duration = end_request_time - start_request_time
                await self._metrics.inc_requests()
                await self._metrics.add_bytes(self._bytes_in, self._bytes_out)
                await self._metrics.record_status(self._status_code)
                await self._metrics.record_latency(duration * 1000)

            if self._successful_request:
                break

            if not keep_alive:
                break

        if self._successful_request:
            await self.pool.record_success(self.upstream)
        else:
            await self.pool.record_failure(self.upstream)
        return keep_alive

    async def _read_request_headers(self) -> HttpRequest | None:
        req_parser = HttpRequestParser()
        request_meta: HttpRequest | None = None
        body_remainder = b""

        while not req_parser.headers_done:
            chunk = await self.timeouts.wait_for_read(self.client_reader.read(self.chunk_size))
            if not chunk:
                return None
            request_meta, body_remainder = req_parser.feed(chunk)
            self._bytes_in += len(chunk)

        if request_meta is None:
            return None

        if body_remainder:
            self.client_reader.unread(body_remainder)

        return request_meta

    async def _read_response_headers(
        self, reader: asyncio.StreamReader | UnreadableStreamReader
    ) -> HttpResponse | None:
        res_parser = HttpResponseParser()
        response_meta: HttpResponse | None = None
        body_remainder = b""

        while not res_parser.headers_done:
            chunk = await self.timeouts.wait_for_read(reader.read(self.chunk_size))
            if not chunk:
                return None
            response_meta, body_remainder = res_parser.feed(chunk)
            self._bytes_out += len(chunk)

        if response_meta is None:
            return None

        if body_remainder:
            if isinstance(reader, UnreadableStreamReader):
                reader.unread(body_remainder)
            else:
                reader = UnreadableStreamReader(reader)
                reader.unread(body_remainder)

        return response_meta

    def _should_retry(self, method: str, resp_meta: HttpResponse) -> bool:
        """Ретраим только безопасные методы И 5xx ошибки."""
        return safe_method(method) and 500 <= resp_meta.status_code < 600

    def _should_keep_alive(self, req: HttpRequest) -> bool:
        if not self.keepalive.enabled:
            return False
        if is_connection_close(req.headers):
            return False
        if req.version == "HTTP/1.1":
            return True
        if req.version == "HTTP/1.0":
            return req.headers.get("connection", "").lower() == "keep-alive"
        return False

    def _modify_request_headers(self, req: HttpRequest, keep_alive: bool) -> HttpRequest:
        h = dict(req.headers)
        peer = self.client_writer.get_extra_info("peername")
        if peer:
            existing = h.get("x-forwarded-for", "")
            h["x-forwarded-for"] = f"{existing}, {peer[0]}" if existing else peer[0]
        h["x-forwarded-proto"] = "http"
        h["x-forwarded-host"] = h.get("host", "")
        h["via"] = "1.1 nginx_remake"
        h["connection"] = "keep-alive" if keep_alive else "close"
        return HttpRequest(req.method, req.path, req.version, h)

    def _modify_response_headers(self, resp: HttpResponse, keep_alive: bool) -> HttpResponse:
        h = dict(resp.headers)
        h["via"] = "1.1 nginx_remake"
        h["connection"] = "keep-alive" if keep_alive else "close"
        return HttpResponse(resp.version, resp.status_code, resp.reason, h)

    async def _send_502_bad_gateway(self):
        await self._write_error_response(b"502", b"Bad Gateway", b"502 Bad Gateway")

    async def _send_503_service_unavailable(self):
        await self._write_error_response(b"503", b"Service Unavailable", b"503 Service Unavailable")

    async def _send_504_gateway_timeout(self):
        await self._write_error_response(b"504", b"Gateway Timeout", b"504 Gateway Timeout")

    async def _write_error_response(
        self, status_bytes: bytes, reason_bytes: bytes, body_bytes: bytes
    ):
        with suppress(OSError, ConnectionError):
            response = (
                b"HTTP/1.1 " + status_bytes + b" " + reason_bytes + b"\r\n"
                b"Content-Type: text/plain; charset=utf-8\r\n"
                b"Content-Length: " + str(len(body_bytes)).encode() + b"\r\n"
                b"Connection: close\r\n\r\n" + body_bytes
            )
            self._status_code = int(status_bytes)
            self._bytes_out += len(response)
            self.client_writer.write(response)
            await self.timeouts.wait_for_write(self.client_writer.drain())

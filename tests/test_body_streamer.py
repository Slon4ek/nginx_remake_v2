# Standard Library
import asyncio

# Third Party
import pytest

# Project Modules
from proxy.buffered_reader import UnreadableStreamReader
from proxy.http_parser import BodyStreamer, BodyStreamerError
from proxy.timeouts import Timeouts


class MockWriter:
    """Мок StreamWriter для захвата записанных данных."""

    def __init__(self):
        self.data = bytearray()
        self._drain_called = 0

    def write(self, data: bytes):
        self.data.extend(data)

    async def drain(self):
        self._drain_called += 1

    def close(self):
        pass

    async def wait_closed(self):
        pass


class MockReader:
    """Мок StreamReader с заданными данными."""

    def __init__(self, data: bytes):
        self._data = data
        self._pos = 0

    async def read(self, n: int) -> bytes:
        if self._pos >= len(self._data):
            return b""
        end = min(self._pos + n, len(self._data))
        result = self._data[self._pos : end]
        self._pos = end
        return result

    async def readline(self) -> bytes:
        idx = self._data.find(b"\n", self._pos)
        if idx == -1:
            result = self._data[self._pos :]
            self._pos = len(self._data)
        else:
            result = self._data[self._pos : idx + 1]
            self._pos = idx + 1
        return result


class TestBodyStreamerRequest:
    """Тесты для stream_request (client -> upstream)."""

    @pytest.fixture
    def streamer(self):
        return BodyStreamer(Timeouts(read_ms=1000, write_ms=1000), chunk_size=1024)

    # ── Content-Length (identity) ──

    async def test_stream_request_identity_exact(self, streamer):
        """Ровно Content-Length байт."""
        reader = MockReader(b"hello world")
        writer = MockWriter()
        headers = {"content-length": "11"}

        sent = await streamer.stream_request(reader, writer, headers)

        assert sent == 11
        assert writer.data == b"hello world"

    async def test_stream_request_identity_multiple_reads(self, streamer):
        """Чтение несколькими маленькими read() — чанки меньше тела."""
        # chunk_size=4, body=10 байт → 3 read() вызова
        streamer._chunk_size = 4
        reader = MockReader(b"0123456789")
        writer = MockWriter()
        headers = {"content-length": "10"}

        sent = await streamer.stream_request(reader, writer, headers)

        assert sent == 10
        assert writer.data == b"0123456789"

    async def test_stream_request_identity_premature_eof(self, streamer):
        """Content-Length: 100, а данных только 10 → ошибка."""
        reader = MockReader(b"short")
        writer = MockWriter()
        headers = {"content-length": "100"}

        with pytest.raises(BodyStreamerError, match="Premature EOF"):
            await streamer.stream_request(reader, writer, headers)

    # ── Transfer-Encoding: chunked ──

    async def test_stream_request_chunked_simple(self, streamer):
        """Простой chunked запрос."""
        # 5\r\nhello\r\n6\r\n world\r\n0\r\n\r\n
        chunked = b"5\r\nhello\r\n6\r\n world\r\n0\r\n\r\n"
        reader = MockReader(chunked)
        writer = MockWriter()
        headers = {"transfer-encoding": "chunked"}

        sent = await streamer.stream_request(reader, writer, headers)

        # Форвардим как есть (включая size lines, CRLF, trailers)
        assert writer.data == chunked
        assert sent == 15

    async def test_stream_request_chunked_multiple_chunks(self, streamer):
        """Много чанков."""
        chunked = b"3\r\nfoo\r\n3\r\nbar\r\n0\r\n\r\n"
        reader = MockReader(chunked)
        writer = MockWriter()
        headers = {"transfer-encoding": "chunked"}

        sent = await streamer.stream_request(reader, writer, headers)

        assert writer.data == chunked
        assert sent == 10

    async def test_stream_request_chunked_with_trailer(self, streamer):
        """Chunked с trailer headers."""
        # 4\r\nwiki\r\n0\r\nX-Trailer: value\r\n\r\n
        chunked = b"4\r\nwiki\r\n0\r\nX-Trailer: value\r\n\r\n"
        reader = MockReader(chunked)
        writer = MockWriter()
        headers = {"transfer-encoding": "chunked"}

        sent = await streamer.stream_request(reader, writer, headers)

        assert writer.data == chunked
        assert sent == 6

    async def test_stream_request_chunked_invalid_size_line(self, streamer):
        """Невалидная size line → ошибка."""
        reader = MockReader(b"GARBAGE\r\n")
        writer = MockWriter()
        headers = {"transfer-encoding": "chunked"}

        with pytest.raises(BodyStreamerError, match="Invalid chunk size"):
            await streamer.stream_request(reader, writer, headers)

    async def test_stream_request_chunked_premature_eof_in_body(self, streamer):
        """Обрыв связи внутри чанка."""
        # size=5, а данных только 3 байта
        reader = MockReader(b"5\r\nfoo")
        writer = MockWriter()
        headers = {"transfer-encoding": "chunked"}

        with pytest.raises(BodyStreamerError, match="Premature EOF in chunk body"):
            await streamer.stream_request(reader, writer, headers)

    # ── No Content-Length, No Transfer-Encoding (GET/HEAD) ──

    async def test_stream_request_no_cl_no_te_returns_zero(self, streamer):
        """GET без тела — сразу 0 байт, НЕ читаем до EOF."""
        reader = MockReader(b"this should not be read")
        writer = MockWriter()
        headers = {}  # нет CL, нет TE

        sent = await streamer.stream_request(reader, writer, headers)

        assert sent == 0
        assert writer.data == b""

    async def test_stream_request_no_cl_no_te_case_insensitive(self, streamer):
        """Заголовки case-insensitive."""
        reader = MockReader(b"")
        writer = MockWriter()
        headers = {"CONTENT-LENGTH": "0"}  # upper-case

        sent = await streamer.stream_request(reader, writer, headers)
        assert sent == 0


class TestBodyStreamerResponse:
    """Тесты для stream_response (upstream -> client)."""

    @pytest.fixture
    def streamer(self):
        return BodyStreamer(Timeouts(read_ms=1000, write_ms=1000), chunk_size=1024)

    # ── Content-Length (identity) ──

    async def test_stream_response_identity_exact(self, streamer):
        reader = MockReader(b"response body")
        writer = MockWriter()
        headers = {"content-length": "13"}

        sent = await streamer.stream_response(reader, writer, headers)

        assert sent == 13
        assert writer.data == b"response body"

    async def test_stream_response_identity_premature_eof(self, streamer):
        reader = MockReader(b"short")
        writer = MockWriter()
        headers = {"content-length": "100"}

        with pytest.raises(BodyStreamerError, match="Premature EOF"):
            await streamer.stream_response(reader, writer, headers)

    # ── Transfer-Encoding: chunked ──

    async def test_stream_response_chunked_simple(self, streamer):
        chunked = b"5\r\nhello\r\n6\r\n world\r\n0\r\n\r\n"
        reader = MockReader(chunked)
        writer = MockWriter()
        headers = {"transfer-encoding": "chunked"}

        sent = await streamer.stream_response(reader, writer, headers)

        assert writer.data == chunked
        assert sent == 15

    async def test_stream_response_chunked_premature_eof(self, streamer):
        reader = MockReader(b"5\r\nfoo")
        writer = MockWriter()
        headers = {"transfer-encoding": "chunked"}

        with pytest.raises(BodyStreamerError, match="Premature EOF in chunk body"):
            await streamer.stream_response(reader, writer, headers)

    # ── No Content-Length, No Transfer-Encoding → read to EOF (HTTP/1.0 fallback) ──

    async def test_stream_response_no_cl_no_te_reads_to_eof(self, streamer):
        """Главное отличие от request: читаем до EOF."""
        reader = MockReader(b"body without length header")
        writer = MockWriter()
        headers = {}  # нет CL, нет TE

        sent = await streamer.stream_response(reader, writer, headers)

        # "body without length header" = 26 bytes
        assert sent == 26
        assert writer.data == b"body without length header"

    async def test_stream_response_no_cl_no_te_empty_body(self, streamer):
        """Пустое тело без CL/TE."""
        reader = MockReader(b"")
        writer = MockWriter()
        headers = {}

        sent = await streamer.stream_response(reader, writer, headers)

        assert sent == 0
        assert writer.data == b""


class TestBodyStreamerIntegration:
    """Интеграционные тесты с UnreadableStreamReader (реальный сценарий)."""

    @pytest.fixture
    def streamer(self):
        return BodyStreamer(Timeouts(read_ms=1000, write_ms=1000), chunk_size=1024)

    async def test_stream_request_with_unread_buffer(self, streamer):
        """
        Симуляция: парсер заголовков прочитал лишнее и сделал unread().
        body_remainder = b"hello" — должен быть первым прочитан BodyStreamer'ом.
        """
        real_reader = asyncio.StreamReader()
        real_reader.feed_data(b" world")  # остаток потока
        real_reader.feed_eof()

        wrapper = UnreadableStreamReader(real_reader)
        wrapper.unread(b"hello")  # то, что парсер "вернул обратно"

        writer = MockWriter()
        headers = {"content-length": "11"}  # "hello world"

        sent = await streamer.stream_request(wrapper, writer, headers)

        assert sent == 11
        assert writer.data == b"hello world"

    async def test_stream_response_with_unread_buffer(self, streamer):
        """Аналогично для ответа от upstream."""
        real_reader = asyncio.StreamReader()
        real_reader.feed_data(b" world")
        real_reader.feed_eof()

        wrapper = UnreadableStreamReader(real_reader)
        wrapper.unread(b"hello")

        writer = MockWriter()
        headers = {"content-length": "11"}

        sent = await streamer.stream_response(wrapper, writer, headers)

        assert sent == 11
        assert writer.data == b"hello world"

    async def test_stream_request_chunked_with_unread(self, streamer):
        """Chunked с unread buffer."""
        real_reader = asyncio.StreamReader()
        real_reader.feed_data(b"6\r\n world\r\n0\r\n\r\n")
        real_reader.feed_eof()

        wrapper = UnreadableStreamReader(real_reader)
        wrapper.unread(b"5\r\nhello\r\n")  # парсер уже прочитал первый чанк

        writer = MockWriter()
        headers = {"transfer-encoding": "chunked"}

        sent = await streamer.stream_request(wrapper, writer, headers)

        expected = b"5\r\nhello\r\n6\r\n world\r\n0\r\n\r\n"
        assert writer.data == expected
        # chunk1: 5+2=7, chunk2: 6+2=8 = 15
        assert sent == 15

    async def test_stream_response_chunked_with_unread(self, streamer):
        """Chunked ответ с unread."""
        real_reader = asyncio.StreamReader()
        real_reader.feed_data(b"6\r\n world\r\n0\r\n\r\n")
        real_reader.feed_eof()

        wrapper = UnreadableStreamReader(real_reader)
        wrapper.unread(b"5\r\nhello\r\n")

        writer = MockWriter()
        headers = {"transfer-encoding": "chunked"}

        sent = await streamer.stream_response(wrapper, writer, headers)

        expected = b"5\r\nhello\r\n6\r\n world\r\n0\r\n\r\n"
        assert writer.data == expected
        assert sent == 15


class TestBodyStreamerEdgeCases:
    """Edge cases и граничные условия."""

    @pytest.fixture
    def streamer(self):
        return BodyStreamer(Timeouts(read_ms=500, write_ms=500), chunk_size=1024)

    async def test_chunked_zero_chunk_terminates(self, streamer):
        """0\r\n\r\n завершает чтение."""
        reader = MockReader(b"0\r\n\r\n")
        writer = MockWriter()
        headers = {"transfer-encoding": "chunked"}

        sent = await streamer.stream_request(reader, writer, headers)
        assert sent == 0
        assert writer.data == b"0\r\n\r\n"

    async def test_chunked_size_with_extensions_ignored(self, streamer):
        """Size line может иметь extensions: `5;ext=value\r\n` — игнорируем."""
        # 5;ignore=me\r\nhello\r\n0\r\n\r\n
        reader = MockReader(b"5;ext=value\r\nhello\r\n0\r\n\r\n")
        writer = MockWriter()
        headers = {"transfer-encoding": "chunked"}

        sent = await streamer.stream_request(reader, writer, headers)
        assert sent == 7
        assert writer.data == b"5;ext=value\r\nhello\r\n0\r\n\r\n"

    async def test_large_chunk_size_hex(self, streamer):
        """Большой hex размер (например, 10000 байт = 2710 hex)."""
        size_hex = "2710"  # 10000 в hex
        chunk_data = b"x" * 10000
        chunked = f"{size_hex}\r\n".encode() + chunk_data + b"\r\n0\r\n\r\n"

        reader = MockReader(chunked)
        writer = MockWriter()
        headers = {"transfer-encoding": "chunked"}

        sent = await streamer.stream_request(reader, writer, headers)
        assert sent == 10002

    async def test_content_length_zero(self, streamer):
        """Content-Length: 0 — тело пустое."""
        reader = MockReader(b"should not read")
        writer = MockWriter()
        headers = {"content-length": "0"}

        sent = await streamer.stream_request(reader, writer, headers)
        assert sent == 0
        assert writer.data == b""

    async def test_both_cl_and_te_cl_wins(self, streamer):
        """Если и CL, и TE — приоритет у Content-Length (RFC 7230)."""
        reader = MockReader(b"identity body")
        writer = MockWriter()
        headers = {"content-length": "13", "transfer-encoding": "chunked"}

        sent = await streamer.stream_request(reader, writer, headers)
        assert sent == 13
        assert writer.data == b"identity body"

    async def test_drain_called_after_each_write(self, streamer):
        """drain() вызывается после каждого write()."""
        reader = MockReader(b"hello")
        writer = MockWriter()
        headers = {"content-length": "5"}

        await streamer.stream_request(reader, writer, headers)
        assert writer._drain_called >= 1


class TestBodyStreamerTimeouts:
    """Проверка, что таймауты пробрасываются."""

    async def test_read_timeout_raises(self):
        """wait_for на read() кидает TimeoutError."""
        streamer = BodyStreamer(Timeouts(read_ms=10, write_ms=5000), chunk_size=1024)

        class SlowReader:
            async def read(self, _):
                await asyncio.sleep(1)
                return b"x"

        reader = SlowReader()
        writer = MockWriter()
        headers = {"content-length": "1"}

        with pytest.raises(asyncio.TimeoutError):
            await streamer.stream_request(reader, writer, headers)

    async def test_drain_timeout_raises(self):
        """wait_for на drain() кидает TimeoutError."""
        streamer = BodyStreamer(Timeouts(read_ms=5000, write_ms=10), chunk_size=1024)

        class SlowWriter:
            def write(self, data):
                pass

            async def drain(self):
                await asyncio.sleep(1)

        reader = MockReader(b"hello")
        writer = SlowWriter()
        headers = {"content-length": "5"}

        with pytest.raises(asyncio.TimeoutError):
            await streamer.stream_request(reader, writer, headers)

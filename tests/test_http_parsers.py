# Project Modules
from proxy.http_parser import HttpRequestParser, HttpResponseParser


class TestHttpRequestParser:
    """Тесты для HttpRequestParser — парсинг HTTP запросов."""

    def test_parse_simple_get(self):
        """Простой GET запрос без тела."""
        parser = HttpRequestParser()
        data = b"GET / HTTP/1.1\r\nHost: example.com\r\n\r\n"
        req, remainder = parser.feed(data)

        assert req is not None
        assert req.method == "GET"
        assert req.path == "/"
        assert req.version == "HTTP/1.1"
        assert req.headers["host"] == "example.com"
        assert remainder == b""

    def test_parse_post_with_content_length(self):
        """POST с Content-Length — тело пришло в том же пакете."""
        parser = HttpRequestParser()
        data = b"POST /echo HTTP/1.1\r\nContent-Length: 5\r\n\r\nhello"
        req, remainder = parser.feed(data)

        assert req is not None
        assert req.method == "POST"
        assert req.path == "/echo"
        assert req.headers["content-length"] == "5"
        assert remainder == b"hello"

    def test_parse_chunked_request(self):
        """Запрос с Transfer-Encoding: chunked."""
        parser = HttpRequestParser()
        data = b"POST / HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n"
        req, remainder = parser.feed(data)

        assert req is not None
        assert req.headers["transfer-encoding"] == "chunked"
        assert remainder == b""

    def test_parse_headers_lowercased(self):
        """Имена заголовков приводятся к lower-case."""
        parser = HttpRequestParser()
        data = b"GET / HTTP/1.1\r\nContent-Type: application/json\r\nX-Custom-Header: value\r\n\r\n"
        req, _ = parser.feed(data)

        assert "content-type" in req.headers
        assert "x-custom-header" in req.headers
        assert req.headers["content-type"] == "application/json"
        assert req.headers["x-custom-header"] == "value"

    def test_parse_multiple_chunks(self):
        """Парсинг заголовков порциями (streaming)."""
        parser = HttpRequestParser()

        # Первый чанк — только часть заголовков
        req1, rem1 = parser.feed(b"GET / HTTP/1.1\r\n")
        assert req1 is None
        assert rem1 == b""

        # Второй чанк — остальное
        req2, rem2 = parser.feed(b"Host: a\r\n\r\nbody")
        assert req2 is not None
        assert req2.method == "GET"
        assert req2.headers["host"] == "a"
        assert rem2 == b"body"

    def test_parse_with_query_string(self):
        """Путь с query string."""
        parser = HttpRequestParser()
        data = b"GET /path?foo=bar&baz=qux HTTP/1.1\r\n\r\n"
        req, _ = parser.feed(data)

        assert req.path == "/path?foo=bar&baz=qux"

    def test_parse_http_10(self):
        """HTTP/1.0 запрос."""
        parser = HttpRequestParser()
        data = b"GET / HTTP/1.0\r\n\r\n"
        req, _ = parser.feed(data)

        assert req.version == "HTTP/1.0"

    def test_empty_path_defaults_to_root(self):
        """Пустой путь (edge case)."""
        parser = HttpRequestParser()
        data = b"GET  HTTP/1.1\r\n\r\n"
        req, _ = parser.feed(data)

        assert req.path == ""

    def test_headers_done_after_complete(self):
        """headers_done становится True после полного парсинга."""
        parser = HttpRequestParser()
        assert parser.headers_done is False

        parser.feed(b"GET / HTTP/1.1\r\n")
        assert parser.headers_done is False

        parser.feed(b"\r\n")
        assert parser.headers_done is True

    def test_feed_after_done_returns_none(self):
        """После завершения парсинга feed возвращает (None, chunk)."""
        parser = HttpRequestParser()
        parser.feed(b"GET / HTTP/1.1\r\n\r\n")

        req, remainder = parser.feed(b"extra data")
        assert req is None
        assert remainder == b"extra data"


class TestHttpResponseParser:
    """Тесты для HttpResponseParser — парсинг HTTP ответов."""

    def test_parse_200_ok(self):
        """Стандартный 200 OK с Content-Length."""
        parser = HttpResponseParser()
        data = b"HTTP/1.1 200 OK\r\nContent-Length: 12\r\n\r\nHello World!"
        resp, rem = parser.feed(data)

        assert resp.version == "HTTP/1.1"
        assert resp.status_code == 200
        assert resp.reason == "OK"
        assert resp.headers["content-length"] == "12"
        assert rem == b"Hello World!"

    def test_parse_404_not_found(self):
        """404 без тела."""
        parser = HttpResponseParser()
        data = b"HTTP/1.1 404 Not Found\r\n\r\n"
        resp, _ = parser.feed(data)

        assert resp.status_code == 404
        assert resp.reason == "Not Found"

    def test_parse_301_redirect(self):
        """301 с Location заголовком."""
        parser = HttpResponseParser()
        data = b"HTTP/1.1 301 Moved Permanently\r\nLocation: /new\r\n\r\n"
        resp, _ = parser.feed(data)

        assert resp.status_code == 301
        assert resp.headers["location"] == "/new"

    def test_parse_chunked_response(self):
        """Ответ с Transfer-Encoding: chunked."""
        parser = HttpResponseParser()
        data = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n"
        resp, _ = parser.feed(data)

        assert resp.headers["transfer-encoding"] == "chunked"

    def test_parse_multiple_chunks(self):
        """Парсинг ответа порциями."""
        parser = HttpResponseParser()

        req1, _ = parser.feed(b"HTTP/1.1 200 OK\r\n")
        assert req1 is None

        req2, rem = parser.feed(b"Content-Length: 5\r\n\r\nhello")
        assert req2 is not None
        assert req2.status_code == 200
        assert rem == b"hello"

    def test_headers_lowercased(self):
        """Имена заголовков в lower-case."""
        parser = HttpResponseParser()
        data = b"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nX-Rate-Limit: 100\r\n\r\n"
        resp, _ = parser.feed(data)

        assert "content-type" in resp.headers
        assert "x-rate-limit" in resp.headers

    def test_reason_phrase_with_spaces(self):
        """Reason phrase может содержать пробелы."""
        parser = HttpResponseParser()
        data = b"HTTP/1.1 499 Client Closed Request\r\n\r\n"
        resp, _ = parser.feed(data)

        assert resp.status_code == 499
        assert resp.reason == "Client Closed Request"

    def test_headers_done_flag(self):
        """headers_done работает аналогично request parser."""
        parser = HttpResponseParser()
        assert parser.headers_done is False
        parser.feed(b"HTTP/1.1 200 OK\r\n\r\n")
        assert parser.headers_done is True

import asyncio

import pytest

from proxy.util.buffered_reader import UnreadableStreamReader


class TestUnreadableStreamReader:
    """Тесты для UnreadableStreamReader — обёртки над StreamReader с поддержкой unread()."""

    @pytest.fixture
    def make_reader(self):
        """Фабрика для создания StreamReader с данными."""

        def _make(data: bytes) -> asyncio.StreamReader:
            reader = asyncio.StreamReader()
            reader.feed_data(data)
            reader.feed_eof()
            return reader

        return _make

    async def test_read_returns_data(self, make_reader):
        """Базовое чтение данных."""
        reader = make_reader(b"hello world")
        wrapper = UnreadableStreamReader(reader)

        data = await wrapper.read(5)
        assert data == b"hello"

        data = await wrapper.read(100)
        assert data == b" world"

    async def test_read_exhausts_stream(self, make_reader):
        """Чтение до EOF возвращает пустые байты."""
        reader = make_reader(b"test")
        wrapper = UnreadableStreamReader(reader)

        assert await wrapper.read(2) == b"te"
        assert await wrapper.read(2) == b"st"
        assert await wrapper.read(10) == b""
        assert await wrapper.read(10) == b""

    async def test_read_all_with_minus_one(self, make_reader):
        """read(-1) читает всё до EOF."""
        reader = make_reader(b"hello")
        wrapper = UnreadableStreamReader(reader)

        data = await wrapper.read(-1)
        assert data == b"hello"

    async def test_unread_puts_data_back(self, make_reader):
        """unread() кладёт данные назад в буфер — они читаются следующими."""
        reader = make_reader(b"world")
        wrapper = UnreadableStreamReader(reader)

        wrapper.unread(b"hello ")
        data = await wrapper.read(100)
        assert data == b"hello world"

    async def test_unread_before_any_read(self, make_reader):
        """unread() до первого read() работает корректно."""
        reader = make_reader(b"original")
        wrapper = UnreadableStreamReader(reader)

        wrapper.unread(b"prepended ")
        data = await wrapper.read(100)
        assert data == b"prepended original"

    async def test_multiple_unreads_stack(self, make_reader):
        """Множественные unread() накапливаются (LIFO — последний unread читается первым)."""
        reader = make_reader(b"tail")
        wrapper = UnreadableStreamReader(reader)

        wrapper.unread(b"middle ")
        wrapper.unread(b"head ")
        data = await wrapper.read(100)
        assert data == b"head middle tail"

    async def test_unread_empty_bytes_noop(self, make_reader):
        """unread(b'') — no-op."""
        reader = make_reader(b"test")
        wrapper = UnreadableStreamReader(reader)

        wrapper.unread(b"")
        data = await wrapper.read(100)
        assert data == b"test"

    async def test_readline_without_unread(self, make_reader):
        """readline() работает как у обычного StreamReader."""
        reader = make_reader(b"line1\nline2\nline3")
        wrapper = UnreadableStreamReader(reader)

        assert await wrapper.readline() == b"line1\n"
        assert await wrapper.readline() == b"line2\n"
        assert await wrapper.readline() == b"line3"

    async def test_readline_with_unread(self, make_reader):
        """readline() учитывает unread-буфер."""
        reader = make_reader(b"line2\nline3")
        wrapper = UnreadableStreamReader(reader)

        wrapper.unread(b"line1\n")
        assert await wrapper.readline() == b"line1\n"
        assert await wrapper.readline() == b"line2\n"
        assert await wrapper.readline() == b"line3"

    async def test_readline_partial_unread(self, make_reader):
        """unread() с частичной строкой (без \n) + данные из reader — склеиваются."""
        reader = make_reader(b"rest\nmore")
        wrapper = UnreadableStreamReader(reader)

        wrapper.unread(b"fi")
        # unread("fi") + reader("rest\nmore") -> "firest\nmore"
        assert await wrapper.readline() == b"firest\n"

    async def test_readexactly(self, make_reader):
        """readexactly(n) читает ровно n байт или поднимает IncompleteReadError."""
        reader = make_reader(b"hello")
        wrapper = UnreadableStreamReader(reader)

        data = await wrapper.readexactly(3)
        assert data == b"hel"

        data = await wrapper.readexactly(2)
        assert data == b"lo"

        with pytest.raises(asyncio.IncompleteReadError):
            await wrapper.readexactly(1)

    async def test_readexactly_with_unread(self, make_reader):
        """readexactly() учитывает unread-буфер."""
        reader = make_reader(b"world")
        wrapper = UnreadableStreamReader(reader)

        wrapper.unread(b"hello ")
        data = await wrapper.readexactly(11)
        assert data == b"hello world"

    async def test_read_zero_bytes(self, make_reader):
        """read(0) возвращает пустые байты, не потребляя стрим."""
        reader = make_reader(b"test")
        wrapper = UnreadableStreamReader(reader)

        assert await wrapper.read(0) == b""
        assert await wrapper.read(4) == b"test"

    async def test_unread_large_data(self, make_reader):
        """unread() с большими данными работает корректно."""
        reader = make_reader(b"x")
        wrapper = UnreadableStreamReader(reader)

        large = b"a" * 10000
        wrapper.unread(large)
        data = await wrapper.read(10000)
        assert data == large
        assert await wrapper.read(1) == b"x"

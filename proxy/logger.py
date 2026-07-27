import logging
import queue
from logging.handlers import QueueHandler, QueueListener, RotatingFileHandler

from proxy.config import ProxyConfig


# noinspection SpellCheckingInspection
class LoggingConfigurator:
    """
    Настраивает корневой логгер с асинхронно-безопасным логированием через очередь.

    Использует пару ``QueueHandler`` + ``QueueListener``, чтобы вызовы
    логгера из асинхронного кода никогда не блокировались на I/O.
    Вывод производится как в ротируемый файл, так и в консоль.
    """

    def __init__(self, config: ProxyConfig):
        self.config = config
        self._log_queue: queue.Queue | None = None
        self._listener: QueueListener | None = None

    @staticmethod
    def _parse_level(level_str: str) -> int:
        level_map = {
            "debug": logging.DEBUG,
            "info": logging.INFO,
            "warning": logging.WARNING,
            "error": logging.ERROR,
            "critical": logging.CRITICAL,
        }
        level = level_map.get(level_str.lower())
        if level is None:
            raise ValueError(f"Invalid logging level: {level_str}")
        return level

    def setup(self) -> None:
        """Инициализирует инфраструктуру логирования.

        Вызывается однократно при старте. Создаёт ``RotatingFileHandler``
        и ``StreamHandler`` за ``QueueListener``, чтобы асинхронный код
        никогда не блокировался на дисковом/консольном I/O.
        """
        logging_cfg = self.config.logging

        level = self._parse_level(logging_cfg.level)

        formatter = logging.Formatter(logging_cfg.format)

        self._log_queue = queue.Queue()

        file_handler = RotatingFileHandler(
            filename=self.config.logging.file_path or "app.log",
            maxBytes=10 * 1024 * 1024,
            backupCount=5,
            encoding="utf-8",
        )
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)

        console_handler = logging.StreamHandler()
        console_handler.setLevel(level)
        console_handler.setFormatter(formatter)

        self._listener = QueueListener(
            self._log_queue,
            file_handler,
            console_handler,
            respect_handler_level=True,
        )
        self._listener.start()

        root_logger = logging.getLogger()
        root_logger.setLevel(level)
        root_logger.propagate = False
        root_logger.addHandler(QueueHandler(self._log_queue))

    def shutdown(self) -> None:
        """Сбрасывает и останавливает инфраструктуру логирования.

        Вызывается однократно при завершении. Гарантирует, что все
        буферизованные записи будут записаны перед выходом.
        """
        if self._listener is not None:
            self._listener.stop()
            self._listener = None
        if self._log_queue is not None:
            self._log_queue.join()
            self._log_queue = None

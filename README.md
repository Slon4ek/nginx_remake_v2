# nginx_remake — Reverse HTTP Proxy

Асинхронный реверсивный HTTP-прокси на Python, оптимизированный для низких задержек и высокой пропускной способности. Использует `asyncio` + `uvloop` для event loop'а и `aiohttp` для эндпоинта метрик.

## Архитектура

```
Клиент  ──►  ReverseProxy (:8080)  ──►  Upstream 1 (:9001)
                                        Upstream 2 (:9002)
                                      │
                                      └── Сервер метрик (:8081 /metrics)
```

## Структура проекта

```
├── proxy/
│   ├── main.py              # Точка входа, SIGHUP hot-reload
│   ├── proxy_server.py      # TCP-сервер + graceful shutdown
│   ├── client_handler.py    # Логика обработки одного клиента (request/response)
│   ├── upstream_pool.py     # Пул соединений, healthcheck'и, round-robin, circuit breaker
│   ├── config.py            # Модель конфигурации (YAML → dataclass)
│   ├── config.yaml          # Файл конфигурации
│   ├── timeouts.py          # Dataclass таймаутов
│   ├── metrics.py           # Метрики в стиле Prometheus
│   ├── logger.py            # Асинхронно-безопасное логирование через очередь
│   ├── rate_limiter.py      # Token Bucket rate limiter (global + per-client)
│   └── util/
│       ├── http.py          # Стриминговые HTTP-парсеры (headers + body)
│       └── buffered_reader.py  # StreamReader с поддержкой unread()
├── tests/
│   ├── echo_app.py              # FastAPI echo-сервер для тестирования
│   ├── test_upstream_pool.py    # Unit-тесты для пула upstream'ов
│   ├── test_buffered_reader.py  # Unit-тесты для UnreadableStreamReader
│   ├── test_http_parsers.py     # Unit-тесты для HttpRequestParser / HttpResponseParser
│   ├── test_body_streamer.py    # Unit-тесты для BodyStreamer (identity, chunked, EOF)
│   ├── test_rate_limiter.py     # Unit-тесты для TokenBucket и RateLimiter
│   └── test_config.py           # Unit-тесты для ProxyConfig.from_yaml()
├── requirements.txt
├── config.yaml              # Пример конфигурации (корневая копия)
├── pytest.ini              # Конфигурация pytest (asyncio_mode = auto)
├── CHEATSHEET.md           # Шпаргалка по архитектуре и коду
└── README.md
```

## Быстрый старт

### Установка зависимостей

```bash
pip install -r requirements.txt
# Или вручную:
pip install uvloop aiohttp pyyaml fastapi uvicorn
```

### Конфигурация

Скопируйте и отредактируйте `proxy/config.yaml`:

```yaml
listen: "127.0.0.1:8080"
upstreams:
  - host: "127.0.0.1"
    port: 9001
  - host: "127.0.0.1"
    port: 9002
timeouts:
  connect_ms: 1000
  read_ms: 15000
  write_ms: 15000
  total_ms: 30000
limits:
  max_client_conns: 1000
  max_conns_per_upstream: 500
logging:
  level: "debug"
```

### Запуск

**1. Запустите upstream-серверы (тестовые):**

```bash
uvicorn tests.echo_app:app --host 127.0.0.1 --port 9001 &
uvicorn tests.echo_app:app --host 127.0.0.1 --port 9002 &
```

**2. Запустите прокси:**

```bash
# Из корня проекта
PYTHONPATH=. python -m proxy.main
```

### Проверка работы

```bash
# GET запрос
curl http://127.0.0.1:8080/

# POST запрос с телом
curl -X POST -d "hello world" http://127.0.0.1:8080/echo

# Несколько запросов на одном keep-alive соединении
curl http://127.0.0.1:8080/ http://127.0.0.1:8080/

# Метрики
curl http://127.0.0.1:8081/metrics
```

## Конфигурация (полный справочник)

```yaml
# Адрес прослушивания прокси
listen: "127.0.0.1:8080"

# Список upstream-серверов (round-robin балансировка)
upstreams:
  - host: "127.0.0.1"
    port: 9001
    tls: false           # опционально, для HTTPS upstream'ов

# Таймауты (в миллисекундах)
timeouts:
  connect_ms: 1000       # соединение с upstream
  read_ms: 15000         # чтение ответа от upstream
  write_ms: 15000        # запись запроса к upstream / ответа клиенту
  total_ms: 30000        # общий таймаут всего цикла request-response

# Лимиты соединений
limits:
  max_client_conns: 1000           # макс. одновременных клиентских соединений
  max_conns_per_upstream: 500      # макс. соединений на один upstream

# Keep-alive для клиентских соединений
keep_alive:
  enabled: true
  timeout_ms: 60000       # таймаут простоя
  max_requests: 100       # макс. запросов на одно соединение

# Keep-alive для upstream-соединений (пул)
upstream_keepalive:
  max_idle: 10            # макс. idle-соединений в пуле на upstream
  idle_timeout_sec: 60.0  # TTL idle-соединения

# Circuit Breaker (опционально)
circuit_breaker:
  failure_threshold: 5    # ошибок до открытия цепи
  cooldown_sec: 30.0      # время в состоянии OPEN перед HALF_OPEN

# Retry политика (опционально)
retry:
  max_retries: 2
  base_delay_ms: 100
  max_delay_ms: 1000

# Rate Limiter (Token Bucket, опционально)
rate_limit:
  enabled: false
  rate: 100.0       # токенов в секунду (global)
  burst: 200        # размер ведра
  per_client: false # true = отдельное ведро на IP клиента

# Логирование
logging:
  level: "INFO"           # DEBUG, INFO, WARNING, ERROR, CRITICAL
  file_path: null         # путь к файлу (null = stdout)
  format: "[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s"
```

## Метрики (Prometheus-совместимый формат)

Эндпоинт: `GET http://127.0.0.1:8081/metrics`

```
total_requests 42
active_connections 3
total_bytes_in 1024
total_bytes_out 2048
status_count{code="200"} 40
status_count{code="502"} 2
latency_bucket{bucket="lt_10ms"} 30
latency_bucket{bucket="10_50ms"} 10
latency_bucket{bucket="50_200ms"} 2
latency_bucket{bucket="gt_200ms"} 0
```

Метрики:
- `total_requests` — всего обработанных запросов
- `active_connections` — текущих активных соединений
- `total_bytes_in/out` — суммарный трафик в/из upstream
- `status_count{code="XXX"}` — количество ответов по кодам
- `latency_bucket{bucket="..."}` — гистограмма латенси (мс): `<10`, `10-50`, `50-200`, `>200`

## Ключевые возможности

| Возможность | Описание |
|-------------|----------|
| **Пул upstream-соединений** | Keep-alive соединения возвращаются в пул; просроченные/битые — автоудаляются |
| **Healthcheck'и** | Периодические TCP-проверки (по умолчанию каждые 30с); мёртвые upstream'ы исключаются из балансировки |
| **Round-robin балансировка** | Запросы равномерно распределяются по живым upstream'ам с учётом circuit breaker |
| **Circuit Breaker** | Автоматическое отключение падающих upstream'ов (configurable threshold + cooldown) |
| **Retry** | Автоматические повторы на другом upstream при ошибках (настраиваемые задержки) |
| **Лимитирование** | `max_client_conns` (глобально) + `max_conns_per_upstream` (на бэкенд) |
| **Rate Limiting** | Token Bucket: global + опционально per-client (по IP) |
| **Graceful Shutdown** | Прекращает приём новых соединений → дожидается активных → очищает пулы |
| **Hot Reload (SIGHUP)** | Перечитывает `config.yaml` на лету: таймауты, лимиты, upstream-лист, keep-alive, rate limit |
| **Низкий оверхед** | Одна корутина на клиента (без тасок на I/O), `asyncio.timeout()`, минимальные аллокации |
| **Структурированное логирование** | QueueHandler + QueueListener → не блокирует event loop; ротация файлов + консоль |

## Тестирование

```bash
# Все тесты (119 unit-тестов)
python -m pytest tests/ -v

# Отдельные модули
python -m pytest tests/test_upstream_pool.py -v        # Пул, circuit breaker, healthcheck
python -m pytest tests/test_buffered_reader.py -v      # UnreadableStreamReader
python -m pytest tests/test_http_parsers.py -v         # HTTP парсеры заголовков
python -m pytest tests/test_body_streamer.py -v        # BodyStreamer (identity, chunked, EOF)
python -m pytest tests/test_rate_limiter.py -v         # TokenBucket + RateLimiter
python -m pytest tests/test_config.py -v               # Конфигурация и валидация

# Только integration-тесты (требуют запущенных upstream'ов)
python -m pytest tests/ -m integration -v

# Исключить integration-тесты
python -m pytest tests/ -m "not integration" -v
```

### Покрытие тестами

| Модуль | Тесты | Покрываемое |
|--------|-------|-------------|
| `test_upstream_pool.py` | 22 | UpstreamsPool, CircuitBreaker, PerUpstreamPool, round-robin, healthcheck, acquire/release |
| `test_buffered_reader.py` | 14 | UnreadableStreamReader — read, readline, readexactly, unread, LIFO stack |
| `test_http_parsers.py` | 18 | HttpRequestParser/ResponseParser — заголовки, body_remainder, streaming, edge cases |
| `test_body_streamer.py` | 28 | BodyStreamer — identity, chunked, stream_to_eof, request/response стратегии, unread integration, timeouts |
| `test_rate_limiter.py` | 14 | TokenBucket, RateLimiter — global/per-client, burst, refill, concurrency |
| `test_config.py` | 23 | ProxyConfig.from_yaml — валидация, дефолты, все секции конфига |
| **Итого** | **119** | Все ключевые компоненты |

### Ручное нагрузочное тестирование

Для нагрузочного тестирования используйте k6, wrk или vegeta:

```bash
# Пример k6 скрипта
k6 run docs/k6-load-test.js
```

## Запуск в продакшене

Рекомендуется использовать `uvloop` для ускорения event loop:

```bash
# Установка
pip install uvloop

# Запуск с uvloop (автоматически подхватывается, если доступен)
PYTHONPATH=. python -m proxy.main
```

## Требования

- Python 3.11+
- Зависимости: `uvloop`, `aiohttp`, `pyyaml`, `fastapi`, `uvicorn` (для тестов)


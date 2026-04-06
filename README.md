# investor-cache

Распределённая система кэширования и согласованной инвалидации профилей инвесторов.
Проект демонстрирует, как обеспечить низкую задержку чтения через Redis Cluster и при
этом сохранять согласованность кэша с источником истины (PostgreSQL) при помощи паттерна
**transactional outbox** и событийной инвалидации через Apache Kafka.

Репозиторий является практической частью дипломной работы и сопровождается дашбордами
Grafana и метриками Prometheus.

## Содержание

- [Архитектура](#архитектура)
- [Поток чтения и записи](#поток-чтения-и-записи)
- [Ключевые механизмы](#ключевые-механизмы)
- [Структура репозитория](#структура-репозитория)
- [Требования](#требования)
- [Быстрый старт](#быстрый-старт)
- [HTTP API](#http-api)
- [Конфигурация](#конфигурация)
- [Наблюдаемость](#наблюдаемость)
- [Тесты и бенчмарки](#тесты-и-бенчмарки)

## Архитектура

Система состоит из четырёх Go-сервисов и поддерживающей инфраструктуры. Каждый сервис
собирается из общего `Dockerfile` (аргумент сборки `SERVICE`) и публикует метрики
Prometheus.

| Сервис | Назначение | Порт приложения | Порт метрик |
| --- | --- | --- | --- |
| `middleware` | Путь чтения: cache-aside поверх Redis Cluster с фолбэком в PostgreSQL | 8080 | 9091 |
| `profile-service` | Путь записи: транзакционное обновление профиля + запись в outbox | 8081 | 9092 |
| `outbox-relay` | Чтение неопубликованных событий из outbox и их публикация в Kafka | — | 9093 |
| `invalidation-worker` | Потребление событий из Kafka и инвалидация ключей в Redis | — | 9094 |

Поддерживающая инфраструктура (поднимается через `docker compose`):

- **Redis Cluster** — 6 узлов (3 мастера + 3 реплики), порты 7001–7006.
- **Apache Kafka** — кластер из 3 брокеров в режиме KRaft (без ZooKeeper).
- **PostgreSQL 16** — источник истины, порт 5432.
- **Prometheus** — сбор метрик, порт 9090.
- **Grafana** — дашборды `cache-overview` и `invalidation`, порт 3000 (admin/admin).

```
            запись                                  чтение
   client ─────────────▶ profile-service       client ─────────────▶ middleware
                              │                                          │
                       (одна транзакция)                          cache-aside
                              │                                          │
                       ┌──────┴───────┐                       ┌──────────┴──────────┐
                       ▼              ▼                        ▼                     ▼
                  investors        outbox                Redis Cluster          PostgreSQL
                  (UPDATE)        (INSERT)              (HGETALL / hit)         (fallback / miss)
                                     │                        ▲
                                     ▼                        │ DEL
                               outbox-relay                   │
                                     │ publish                │
                                     ▼                        │
                                   Kafka ───────▶ invalidation-worker
                              (profile-updates)
```

## Поток чтения и записи

**Чтение (`middleware`):**

1. Запрос `GET /api/v1/investors/{id}` ищет ключ `investor:{id}` в Redis Cluster.
2. При попадании (cache hit) профиль возвращается сразу.
3. При промахе (cache miss) профиль читается из PostgreSQL и асинхронно
   докешируется; конкурентные промахи по одному ключу схлопываются через `singleflight`.
4. Обращение к Redis обёрнуто в circuit breaker: при недоступности кэша запрос
   деградирует к прямому чтению из PostgreSQL.

**Запись (`profile-service`):**

1. Запрос `PATCH /api/v1/investors/{id}` валидируется и применяется в одной транзакции.
2. В этой же транзакции триггер БД инкрементирует `cache_version`, а в таблицу `outbox`
   записывается событие `profile_updated` — это гарантирует атомарность изменения данных
   и факта публикации события.
3. `outbox-relay` периодически вычитывает неопубликованные записи и отправляет их в Kafka
   (топик `profile-updates`), помечая опубликованными в той же транзакции.
4. `invalidation-worker` потребляет события и удаляет соответствующие ключи из Redis,
   чтобы следующее чтение перезагрузило свежие данные.

## Ключевые механизмы

- **Transactional outbox.** Запись в `investors` и событие в `outbox` фиксируются одной
  транзакцией PostgreSQL — без потери и без дублирования семантики «изменили → известили».
- **Версионирование кэша.** Запись в Redis выполняется Lua-скриптом (compare-and-set по
  `cache_version`): более старая версия никогда не перезапишет более новую, что защищает
  от гонок между докешированием и инвалидацией.
- **Circuit breaker.** Горячий путь чтения защищён предохранителем (`sony/gobreaker`);
  при «открытии» трафик уходит в PostgreSQL. Для экспериментов отключается переменной
  `CACHE_DISABLE_CB=true`.
- **Singleflight.** Дедупликация одновременных промахов по одному ключу, чтобы не создавать
  «штормы» обращений к БД.
- **Retry + DLQ.** Воркер инвалидации повторяет обработку с экспоненциальной задержкой, а
  при исчерпании попыток отправляет сообщение в DLQ-топик `profile-updates-dlq`.
- **Reconciliation.** Фоновый сверщик периодически сэмплирует ключи Redis и сравнивает
  `cache_version` с PostgreSQL, вытесняя устаревшие записи (страховка от потерянных событий).

## Структура репозитория

```
.
├── cmd/                      # точки входа сервисов
│   ├── middleware/           # путь чтения (cache-aside + reconciler)
│   ├── profile-service/      # путь записи (transactional outbox)
│   ├── outbox-relay/         # outbox -> Kafka
│   └── invalidation-worker/  # Kafka -> Redis DEL
├── internal/
│   ├── cache/                # CacheManager, RedisStore (versioned set)
│   ├── circuitbreaker/       # обёртка над gobreaker
│   ├── domain/               # доменные модели и интерфейсы
│   ├── handler/              # HTTP-обработчики чтения
│   ├── kafka/                # consumer инвалидации + DLQ
│   ├── metrics/              # коллекторы Prometheus
│   ├── outbox/               # событие, репозиторий и relay
│   ├── profile/              # сервис, обработчики и валидация записи
│   ├── reconciler/           # фоновая сверка кэша с БД
│   └── repository/           # доступ к PostgreSQL
├── pkg/config/               # загрузка конфигурации из переменных окружения
├── deployments/              # конфиги Redis, Kafka, PostgreSQL, Prometheus, Grafana
├── docker-compose.yml        # вся инфраструктура и сервисы
├── Dockerfile                # общий многоступенчатый образ
└── Makefile                  # команды up/down/test/bench/seed/logs
```

## Требования

- Docker и Docker Compose (плагин `docker compose`).
- Go 1.25+ — для локального запуска тестов и бенчмарков.
- `curl` — для обращения к HTTP API.

## Быстрый старт

Поднять весь стек (инфраструктура + сервисы):

```bash
make up
```

Команда выполняет `docker compose up -d --build`. При первом запуске инициализируются
кластер Redis, топики Kafka, схема и сидовые данные PostgreSQL. Дождитесь, пока все
зависимости перейдут в состояние `healthy`.

Посмотреть логи пути чтения:

```bash
make logs
```

Остановить и удалить всё (включая тома данных):

```bash
make down
```

Доступные адреса после запуска:

- middleware: <http://localhost:8080>
- profile-service: <http://localhost:8081>
- Prometheus: <http://localhost:9090>
- Grafana: <http://localhost:3000> (admin / admin)

## HTTP API

### Чтение профиля (middleware, порт 8080)

```bash
curl http://localhost:8080/api/v1/investors/{id}
```

```bash
# health-check
curl http://localhost:8080/health
```

### Обновление профиля (profile-service, порт 8081)

```bash
curl -X PATCH http://localhost:8081/api/v1/investors/{id} \
  -H 'Content-Type: application/json' \
  -d '{"full_name": "Иван Петров"}'
```

```bash
# health-check
curl http://localhost:8081/health
```

Ответ профиля включает поле `cache_version`, которое монотонно возрастает при каждом
обновлении и используется для версионной записи в кэш.

## Конфигурация

Все параметры читаются из переменных окружения (`pkg/config/config.go`) и имеют разумные
значения по умолчанию. Наиболее важные:

| Переменная | По умолчанию | Описание |
| --- | --- | --- |
| `SERVER_PORT` | `8080` / `8081` | Порт HTTP-приложения |
| `METRICS_PORT` | `9091`–`9094` | Порт `/metrics` |
| `REDIS_ADDRS` | `redis-node-1:7001,…,redis-node-6:7006` | Узлы Redis Cluster |
| `DATABASE_DSN` | `postgres://investor:investor@postgres:5432/investordb?sslmode=disable` | Подключение к PostgreSQL |
| `KAFKA_BROKERS` | `kafka-1:9092,kafka-2:9092,kafka-3:9092` | Брокеры Kafka |
| `KAFKA_TOPIC` | `profile-updates` | Топик событий обновления |
| `KAFKA_DLQ_TOPIC` | `profile-updates-dlq` | Топик мёртвых сообщений |
| `CACHE_TTL` | `3600s` | TTL записей кэша |
| `RECONCILE_INTERVAL` | `5m` | Интервал фоновой сверки |
| `RECONCILE_SAMPLE_SIZE` | `100` | Размер выборки ключей для сверки |
| `OUTBOX_POLL_INTERVAL` | `100ms` | Период опроса таблицы outbox |
| `OUTBOX_BATCH_SIZE` | `100` | Размер батча публикации |
| `CACHE_DISABLE_CB` | `false` | Отключить circuit breaker на горячем пути (для экспериментов) |

## Наблюдаемость

- **Prometheus** собирает метрики со всех сервисов (cache hit/miss, латентность кэша,
  фолбэки в БД, состояние circuit breaker, лаг публикации outbox, успехи/ретраи/DLQ
  инвалидации, результаты сверки).
- **Grafana** автоматически провижинит дашборды `cache-overview` и `invalidation`
  из `deployments/grafana/dashboards`.

## Тесты и бенчмарки

```bash
# модульные тесты с детектором гонок
make test

# бенчмарки
make bench
```

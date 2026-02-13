# Data Format Benchmark

Инструмент для тестирования производительности форматов данных: **Parquet**, **Avro** и **Protobuf**.

## Особенности

- **Фазовая архитектура** для минимального использования памяти (~200MB для 10GB датасета)
- Генерация реалистичного датасета e-commerce транзакций
- Streaming запись и чтение данных
- Измерение всех метрик:
  - Размер файлов
  - Время записи
  - Время полного сканирования
  - Время фильтрованного чтения
  - Время агрегации
- Автоматическая генерация отчётов с анализом

## Установка

```bash
# Клонировать репозиторий
git clone <repository-url>
cd dataformat-bench

# Установить зависимости
uv sync

# Проверить установку
uv run dataformat-bench --help
```

## Быстрый старт

```bash
# Полный pipeline одной командой (рекомендуется для ДЗ)
uv run dataformat-bench run --size 10 --runs 3 --seed 42

# Результаты будут в data/:
# - benchmark_data.parquet, .avro, .pb (файлы данных)
# - write_results.json (метрики записи)
# - read_results.json (метрики чтения)
# - report.md (итоговый отчёт)
```

## Использование

### Способ 1: Полный pipeline (рекомендуется)

```bash
uv run dataformat-bench run --size 10 --runs 3 --seed 42
```

Выполняет все три фазы автоматически:
1. Генерация и запись данных (streaming)
2. Бенчмарк чтения из файлов
3. Генерация отчёта

### Способ 2: Пошаговое выполнение

Для большего контроля или повторного запуска отдельных фаз:

```bash
# Фаза 1: Генерация и запись (~200MB RAM)
uv run dataformat-bench write \
  --size 10 \
  --formats parquet,avro,protobuf \
  --seed 42

# Фаза 2: Бенчмарк чтения (~100MB RAM)
uv run dataformat-bench read \
  --input data/ \
  --runs 3

# Фаза 3: Отчёт
uv run dataformat-bench report \
  --write-results data/write_results.json \
  --read-results data/read_results.json \
  --output report.md
```

## Команды

### `write` - Генерация и запись

Генерирует данные streaming режиме и записывает в файлы.

**Параметры:**
- `--size, -s` - Размер датасета в GB (default: 10)
- `--output, -o` - Директория для файлов (default: data/)
- `--formats, -f` - Форматы через запятую (default: parquet,avro,protobuf)
- `--seed` - Random seed для воспроизводимости
- `--save-results` - Путь для write_results.json

**Использование памяти:** ~200MB

### `read` - Бенчмарк чтения

Выполняет тесты чтения на готовых файлах.

**Параметры:**
- `--input, -i` - Директория с файлами (default: data/)
- `--formats, -f` - Форматы через запятую
- `--runs, -r` - Количество повторов (default: 3)
- `--filter-category` - Категория для фильтрации (default: Electronics)
- `--save-results` - Путь для read_results.json

**Использование памяти:** ~100MB

### `report` - Генерация отчёта

Объединяет результаты и генерирует отчёт.

**Параметры:**
- `--write-results` - JSON с результатами записи
- `--read-results` - JSON с результатами чтения
- `--output, -o` - Путь для отчёта

### `run` - Полный pipeline

Выполняет write → read → report последовательно.

**Параметры:**
- `--size, -s` - Размер датасета в GB (default: 10)
- `--output, -o` - Директория (default: data/)
- `--formats, -f` - Форматы (default: все)
- `--runs, -r` - Повторы для read (default: 3)
- `--seed` - Random seed
- `--report-file` - Путь для отчёта

### `generate` - Тестовые данные

Генерирует небольшой файл для тестирования.

```bash
uv run dataformat-bench generate -n 1000 -f parquet -o test.parquet
```

## Структура проекта

```
dataformat-bench/
├── src/dataformat_bench/
│   ├── cli.py                 # CLI команды
│   ├── config.py              # Константы
│   ├── schema.py              # Схема Order + Avro schema
│   ├── generator.py           # Генератор данных
│   ├── write_benchmark.py     # Write бенчмарк
│   ├── read_benchmark.py      # Read бенчмарк
│   ├── benchmark.py           # Общие структуры (BenchmarkResult)
│   ├── report.py              # Генерация отчётов
│   └── formats/
│       ├── base.py            # FormatHandler (abstract)
│       ├── parquet.py         # Parquet handler
│       ├── avro.py            # Avro handler
│       └── protobuf.py        # Protobuf handler
├── proto/
│   └── order.proto            # Protobuf схема
└── data/                      # Результаты бенчмарка
```

## Схема данных (E-commerce Orders)

| Поле             | Тип       | Описание                       |
| ---------------- | --------- | ------------------------------ |
| order_id         | string    | UUID заказа                    |
| customer_id      | int64     | ID клиента                     |
| product_id       | int64     | ID товара                      |
| product_name     | string    | Название товара                |
| category         | string    | Категория (20 вариантов)       |
| quantity         | int32     | Количество                     |
| price            | float64   | Цена за единицу                |
| total_amount     | float64   | Итоговая сумма                 |
| order_date       | timestamp | Дата заказа                    |
| shipping_country | string    | Страна доставки (50 вариантов) |
| payment_method   | string    | Способ оплаты (5 вариантов)    |
| is_returned      | bool      | Флаг возврата                  |

## Измеряемые метрики

### Write Phase
- **File Size** - размер файла на диске
- **Write Time** - время записи всего датасета

### Read Phase
- **Full Scan** - время полного чтения всех записей
- **Filtered Read** - время чтения с фильтрацией по категории
- **Aggregation** - время агрегации суммы по странам

## Управление памятью

### Новая архитектура (фазовая)

```
Phase 1: Write
Generator → [batch 100K] → Write Parquet → file.parquet
Generator → [batch 100K] → Write Avro → file.avro  
Generator → [batch 100K] → Write Protobuf → file.pb

Phase 2: Read
file.parquet → Read Tests → metrics
file.avro → Read Tests → metrics
file.pb → Read Tests → metrics
```

**Использование памяти:**
- Write phase: ~200MB (один batch в памяти)
- Read phase: ~100MB (streaming итераторы)
- **Итого для 10GB датасета: ~300MB RAM**

### Оптимизации по форматам

**Parquet:**
- Write: `ParquetWriter` с инкрементальным append
- Read: Predicate pushdown, column pruning
- True streaming в обе стороны

**Avro:**
- Write: Накопление в памяти (ограничение fastavro)
- Read: Streaming через iterator
- Partial streaming (write собирает данные)

**Protobuf:**
- Write: Length-prefixed messages, полный streaming
- Read: Streaming парсинг
- True streaming в обе стороны

## Примеры использования

### Целевой кейс

```bash
# Полный бенчмарк с сохранением всех результатов
uv run dataformat-bench run \
  --size 10 \
  --runs 3 \
  --seed 42 \
  --report-file homework_report.md
```

### Быстрый тест на маленьком датасете

```bash
# 100MB датасет, один проход
uv run dataformat-bench run --size 0.1 --runs 1
```

### Тестирование только одного формата

```bash
# Только Parquet
uv run dataformat-bench write --size 1 --formats parquet
uv run dataformat-bench read --formats parquet --runs 3
uv run dataformat-bench report
```

### Повторный запуск read тестов

```bash
# Если файлы уже созданы, можно перезапустить только чтение
uv run dataformat-bench read --runs 5 --filter-category Books
```

## Результаты

После выполнения `run` команды получите:

```
data/
├── benchmark_data.parquet     # ~2-3 GB
├── benchmark_data.avro        # ~4-5 GB
├── benchmark_data.pb          # ~4-5 GB
├── write_results.json         # Метрики записи
├── read_results.json          # Метрики чтения
└── report.md                  # Финальный отчёт с таблицами
```

### Пример отчёта

```
# Data Format Benchmark Results

Total records: 52,428,800

## Summary

+----------+-----------+------------+-----------+---------------+-------------+
| Format   | File Size | Write Time | Full Scan | Filtered Read | Aggregation |
+==========+===========+============+===========+===============+=============+
| AVRO     | 4.21 GB   | 210.45 s   | 89.34 s   | 78.23 s       | 85.12 s     |
+----------+-----------+------------+-----------+---------------+-------------+
| PARQUET  | 2.15 GB   | 145.23 s   | 45.12 s   | 3.21 s        | 2.87 s      |
+----------+-----------+------------+-----------+---------------+-------------+
| PROTOBUF | 4.52 GB   | 180.67 s   | 120.45 s  | 110.23 s      | 115.34 s    |
+----------+-----------+------------+-----------+---------------+-------------+
```

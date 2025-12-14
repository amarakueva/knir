# Прототип витрины Retailrocket — запуск с нуля

## Что нужно
- Docker и Docker Compose.
- Репозиторий с данными в `archive/` - данный проект рассчитан на работу с датасетом Retailrocket - скачать его можно [здесь](https://www.kaggle.com/datasets/retailrocket/ecommerce-dataset/data). После этого в директории archive должны оказаться 4 `.csv`-файла

## Быстрый старт
1. Скопируйте переменные окружения (при необходимости поправьте пароли):
   ```bash
   cp .env.example .env
   ```
2.
   ```bash
   docker compose up -d
   ```

## Первичная загрузка данных
Есть два DAG:
- `retailrocket_full_load` — одноразовая полная загрузка из `archive/` (raw обновляется и грузится всё, включая category_tree). Запуск вручную:
  ```bash
  docker compose exec airflow-webserver \
    airflow dags trigger -e 2025-12-14 retailrocket_full_load
  ```
- `retailrocket_daily` — инкремент по 150k строк на запуск (файлы events, item_properties), без category_tree. Запланирован `@daily`, можно запустить вручную аналогично или включить (передвинуть кнопку включения в UI Airflow - тогда ежедневно в базу автоматически будет загружаться 150000 строк из набора данных)

## Доступы и UI
- Airflow: http://localhost:8080 (логин/пароль из `config/simple_auth_manager_passwords.json` (! этот пароль будет сгенерирован заново после перезапуска Airflow)).
- Grafana: http://localhost:3000 (без логина и пароля). Дашборд «Аналитическая панель».
- Postgres: `localhost:5432`, БД/пользователь/пароль — из `.env` (по умолчанию `airflow/airflow/airflow`).

## Полезные команды
- Логи Airflow scheduler:
  ```bash
  docker compose logs -f airflow-scheduler
  ```
- Логи конкретного таска DAG:
  ```bash
  docker compose exec airflow-webserver \
    airflow tasks logs retailrocket_full_load ingest_raw_full
  ```
- Проверка данных:
  ```bash
  docker compose exec postgres psql -U airflow -d airflow \
    -c "SELECT min(date_key), max(date_key), COUNT(*) FROM dm.fact_daily_events;"
  ```

## Структура данных
- RAW: `raw.events`, `raw.item_properties`, `raw.category_tree` (+ `raw.ingestion_state` для инкрементов).
- STG: `stg.events`, `stg.item_properties`, `stg.category_tree`.
- DM: `dm.dim_date`, `dm.dim_item`, `dm.fact_daily_events`, `dm.fact_item_daily`.

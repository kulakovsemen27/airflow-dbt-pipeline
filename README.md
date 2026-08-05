# Metrics Overview DWH

## Цель проекта

Построить хранилище данных и отчётный слой для анализа сумм депозитов, выводов и ставок:

- во времени;
- в разрезе стран регистрации игроков.

Исходные суммы пересчитываются в USD по курсу валюты на дату операции.

## Архитектура

### Pipeline
```text
CSV → Airflow → PostgreSQL raw → dbt staging → dbt data_mart
    → dbt data_mart_report → Metabase
```
### DWH layers
- `raw` хранит данные максимально близко к источнику.
- `staging` выполняет очистку, типизацию и дедупликацию.
- `data_mart` содержит измерения, факты и дневные метрики игроков.
- `data_mart_report` предоставляет готовые наборы данных для Metabase.

Слой `data_mart` построен как Kimball dimensional model и включает:
- измерения `dim_player`, `dim_provider`, `dim_game`;
- факты `fact_deposit`, `fact_withdrawal`, `fact_bet`;
- дневные показатели игроков `player_metrics`.

Слой `data_mart_report` содержит дневную таблицу `metrics_overview` и месячное представление `monthly_summary`.

## Структура репозитория

```text
airflow/          DAG и Python-код загрузки
dbt/              dbt-модели, тесты и конфигурация
docker/           Dockerfile и зависимости образов
docs/             документация и диаграммы
source_data/      исходные CSV-файлы
sql/ddl/          создание схем и таблиц PostgreSQL
docker-compose.yml
```

## Требования

- Git
- запущенный Docker Desktop с Docker Compose

## Запуск

```bash
git clone https://github.com/kulakovsemen27/nda_test_task.git
cd nda_test_task
cp .env.example .env
docker compose up --build -d
```

Локальный `.env` создаётся из `.env.example` и содержит настройки окружения. Настоящий `.env` исключён из Git и не передаётся вместе с проектом.

## Доступ к сервисам

| Сервис | Адрес | Credentials |
|---|---|---|
| Airflow | [http://localhost:8080](http://localhost:8080) | `AIRFLOW_ADMIN_USERNAME` / `AIRFLOW_ADMIN_PASSWORD` из `.env` |
| PostgreSQL | `localhost:5432` | `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD` из `.env` |
| Metabase | [http://localhost:3000](http://localhost:3000) | первоначальная настройка при первом открытии |

При использовании `.env.example` без изменений:

```text
Airflow
login: airflow
password: airflow

PostgreSQL
host: localhost
port: 5432
database: postgres_db
user: postgres_user
password: postgres_password
```

## Запуск пайплайна

После запуска контейнеров:

1. Открыть Airflow и войти с credentials из `.env`.
2. Найти DAG `metrics_overview` и включить его, если он находится на паузе.
3. Запустить DAG вручную и дождаться успешного завершения всех задач.
4. При необходимости подключиться к PostgreSQL и проверить схемы и таблицы.
5. Посмотреть [документацию DWH](docs/dwh/README.md) и [скриншоты Metabase](docs/metabase/README.md).

## Принятые упрощения

В рамках первой версии пайплайна, dbt-модели, кроме `monthly_summary`, материализуются как таблицы и полностью пересобираются при каждом запуске. Это сокращает сложность первой версии пайплайна. В production-сценарии факты и агрегаты можно перевести на инкрементальную загрузку после определения уникальных ключей, источника изменений и правил обработки опоздавших данных.

Готовая конфигурация дашборда Metabase не включена в репозиторий: вопросы, карточки и фильтры хранятся в локальном Docker volume. На чистом окружении Metabase запустится без настроенного дашборда, однако данные для него полностью воспроизводятся пайплайном.

## Остановка окружения

Остановить и удалить контейнеры, сохранив данные в Docker volumes:

```bash
docker compose down
```

Полностью удалить окружение вместе с данными PostgreSQL, Airflow и конфигурацией Metabase:

```bash
docker compose down -v
```

Команда с `-v` безвозвратно удаляет локальные данные проекта.

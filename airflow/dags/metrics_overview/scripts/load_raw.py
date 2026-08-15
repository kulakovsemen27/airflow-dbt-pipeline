import csv
import logging
import os
from datetime import date, timedelta
from pathlib import Path

import psycopg
from airflow.hooks.base import BaseHook
from psycopg import sql


LOGGER = logging.getLogger(__name__)
BATCH_SIZE = 1_000
SOURCE_DIR = Path(
    os.getenv(
        "SOURCE_DATA_DIR",
        Path(__file__).resolve().parents[4] / "source_data",
    )
)


def get_connection(conn_id: str) -> psycopg.Connection:
    airflow_connection = BaseHook.get_connection(conn_id)

    return psycopg.connect(
        host=airflow_connection.host,
        port=airflow_connection.port,
        dbname=airflow_connection.schema,
        user=airflow_connection.login,
        password=airflow_connection.password,
    )


def get_cutoff_date(
    connection: psycopg.Connection,
    table: str,
    date_column: str,
    lookback_days: int,
) -> date | None:
    # Calculate the start of the lookback window.
    query = sql.SQL("SELECT max({}) FROM raw.{}").format(
        sql.Identifier(date_column),
        sql.Identifier(table),
    )

    with connection.cursor() as cursor:
        cursor.execute(query)
        max_loaded_date = cursor.fetchone()[0]

    if max_loaded_date is None:
        return None

    return date.fromisoformat(max_loaded_date) - timedelta(days=lookback_days)


def get_latest_rows(
    connection: psycopg.Connection,
    table: str,
    columns: tuple[str, ...],
    key_columns: tuple[str, ...],
    date_column: str | None,
    cutoff_date: date | None,
) -> dict[tuple[str, ...], tuple[str, ...]]:
    # Read the latest stored version of each business key.
    where_clause = sql.SQL("")
    parameters = None

    if date_column and cutoff_date:
        where_clause = sql.SQL("WHERE {} >= %s").format(
            sql.Identifier(date_column)
        )
        parameters = (cutoff_date.isoformat(),)

    query = sql.SQL(
        """
        SELECT DISTINCT ON ({}) {}
        FROM raw.{}
        {}
        ORDER BY {}, loaded_at DESC
        """
    ).format(
        sql.SQL(", ").join(map(sql.Identifier, key_columns)),
        sql.SQL(", ").join(map(sql.Identifier, columns)),
        sql.Identifier(table),
        where_clause,
        sql.SQL(", ").join(map(sql.Identifier, key_columns)),
    )

    key_indexes = tuple(columns.index(column) for column in key_columns)

    with connection.cursor() as cursor:
        cursor.execute(query, parameters)
        rows = cursor.fetchall()

    return {
        tuple(row[index] for index in key_indexes): tuple(row)
        for row in rows
    }


def load_table(
    table: str,
    key_columns: tuple[str, ...],
    dwh_conn_id: str,
    date_column: str | None = None,
    lookback_days: int = 30,
) -> None:
    file_path = SOURCE_DIR / f"{table}.csv"
    LOGGER.info("Loading %s into raw.%s", file_path.name, table)

    with get_connection(dwh_conn_id) as connection:
        cutoff_date = None

        # Use business dates where change-tracking columns are unavailable.
        if date_column:
            cutoff_date = get_cutoff_date(
                connection,
                table,
                date_column,
                lookback_days,
            )

        with file_path.open("r", encoding="utf-8", newline="") as source_file:
            reader = csv.DictReader(source_file)
            columns = tuple(reader.fieldnames or ())

            if not columns:
                raise ValueError(f"CSV file has no header: {file_path}")

            # Prepare the current raw state and the append-only insert.
            latest_rows = get_latest_rows(
                connection,
                table,
                columns,
                key_columns,
                date_column,
                cutoff_date,
            )
            insert_query = sql.SQL(
                "INSERT INTO raw.{} ({}) VALUES ({})"
            ).format(
                sql.Identifier(table),
                sql.SQL(", ").join(map(sql.Identifier, columns)),
                sql.SQL(", ").join(sql.Placeholder() for _ in columns),
            )

            processed_rows = 0
            inserted_rows = 0
            batch = []

            with connection.cursor() as cursor:
                # Keep only new or changed versions inside the lookback window.
                for row in reader:
                    if (
                        cutoff_date
                        and date_column
                        and date.fromisoformat(row[date_column]) < cutoff_date
                    ):
                        continue

                    values = tuple(row[column] for column in columns)
                    key = tuple(row[column] for column in key_columns)
                    processed_rows += 1

                    if latest_rows.get(key) == values:
                        continue

                    batch.append(values)
                    latest_rows[key] = values

                    # Insert changes in batches.
                    if len(batch) == BATCH_SIZE:
                        cursor.executemany(insert_query, batch)
                        inserted_rows += len(batch)
                        batch.clear()

                if batch:
                    cursor.executemany(insert_query, batch)
                    inserted_rows += len(batch)

    LOGGER.info(
        "Processed %s CSV rows and inserted %s new raw versions into raw.%s",
        processed_rows,
        inserted_rows,
        table,
    )

import logging
import os
from pathlib import Path

import psycopg
from airflow.hooks.base import BaseHook
from psycopg import sql


LOGGER = logging.getLogger(__name__)

SOURCE_DIR = Path(
    os.getenv(
        "SOURCE_DATA_DIR",
        Path(__file__).resolve().parents[4] / "source_data",
    )
)


def get_connection(conn_id: str) -> psycopg.Connection:
    LOGGER.info("Connecting through Airflow connection: %s", conn_id)
    airflow_connection = BaseHook.get_connection(conn_id)

    return psycopg.connect(
        host=airflow_connection.host,
        port=airflow_connection.port,
        dbname=airflow_connection.schema,
        user=airflow_connection.login,
        password=airflow_connection.password,
    )


def load_table(table: str, conn_id: str) -> None:
    file_path = SOURCE_DIR / f"{table}.csv"
    LOGGER.info("Loading %s into raw.%s", file_path.name, table)

    columns = file_path.read_text().splitlines()[0].split(",")
    copy_query = sql.SQL(
        "COPY raw.{} ({}) FROM STDIN WITH (FORMAT CSV, HEADER TRUE)"
    ).format(
        sql.Identifier(table),
        sql.SQL(", ").join(map(sql.Identifier, columns)),
    )

    with get_connection(conn_id) as connection:
        with connection.cursor() as cursor:
            with cursor.copy(copy_query) as copy:
                copy.write(file_path.read_bytes())

    LOGGER.info("Loaded raw.%s", table)

import os
from pathlib import Path

import psycopg
from psycopg import sql


TABLES = (
    "players",
    "providers_map",
    "games_map",
    "currency_rates",
    "deposits",
    "withdrawals",
    "games",
)

# Find source files from the project root.
SOURCE_DIR = Path(__file__).resolve().parents[2] / "source_data"


def load_raw() -> None:
    # Load all files in one database transaction.
    with psycopg.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    ) as connection:
        with connection.cursor() as cursor:
            # Copy each CSV into the matching raw table.
            for table in TABLES:
                file_path = SOURCE_DIR / f"{table}.csv"
                columns = file_path.read_text().splitlines()[0].split(",")
                copy_query = sql.SQL(
                    "COPY raw.{} ({}) FROM STDIN WITH (FORMAT CSV, HEADER TRUE)"
                ).format(
                    sql.Identifier(table),
                    sql.SQL(", ").join(map(sql.Identifier, columns)),
                )

                with cursor.copy(copy_query) as copy:
                    copy.write(file_path.read_bytes())


if __name__ == "__main__":
    load_raw()

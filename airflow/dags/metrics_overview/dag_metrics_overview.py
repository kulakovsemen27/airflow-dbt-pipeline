from datetime import datetime, timezone

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from metrics_overview.scripts.load_raw import load_table


TABLES = (
    "players",
    "providers_map",
    "games_map",
    "currency_rates",
    "deposits",
    "withdrawals",
    "games",
)
POSTGRES_CONN_ID = "dwh_postgres"


with DAG(
    dag_id="metrics_overview",
    start_date=datetime(2026, 8, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
) as dag:

    start_task = EmptyOperator(
        task_id="start",
    )

    raw_loaded_task = EmptyOperator(
        task_id="raw_loaded",
    )

    for table in TABLES:
        truncate_task = SQLExecuteQueryOperator(
            task_id=f"truncate_{table}",
            conn_id=POSTGRES_CONN_ID,
            sql=f"TRUNCATE TABLE raw.{table}",
        )

        load_task = PythonOperator(
            task_id=f"load_{table}",
            python_callable=load_table,
            op_kwargs={
                "table": table,
                "conn_id": POSTGRES_CONN_ID,
            },
        )

        start_task >> truncate_task >> load_task >> raw_loaded_task

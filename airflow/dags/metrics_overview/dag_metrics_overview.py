from datetime import datetime, timezone

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.utils.task_group import TaskGroup

from metrics_overview.scripts.dbt_commands import DBT_COMMANDS, DBT_PROJECT_DIR
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
DIMENSION_MODELS = (
    "dim_player",
    "dim_provider",
    "dim_game",
)
FACT_MODELS = (
    "fact_deposit",
    "fact_withdrawal",
    "fact_bet",
)
PLAYER_METRICS_MODEL = "player_metrics"
DATA_MART_REPORT_MODEL = "metrics_overview"
MONTHLY_SUMMARY_MODEL = "monthly_summary"
STAGING_DBT_PATHS = {
    table: {
        "DBT_TARGET_PATH": f"/tmp/dbt/staging/{table}/target",
        "DBT_LOG_PATH": f"/tmp/dbt/staging/{table}/logs",
    }
    for table in TABLES
}
DATA_MART_DBT_PATHS = {
    model: {
        "DBT_TARGET_PATH": f"/tmp/dbt/data_mart/{model}/target",
        "DBT_LOG_PATH": f"/tmp/dbt/data_mart/{model}/logs",
    }
    for model in DIMENSION_MODELS + FACT_MODELS + (PLAYER_METRICS_MODEL,)
}
DATA_MART_REPORT_DBT_PATHS = {
    "DBT_TARGET_PATH": "/tmp/dbt/data_mart_report/target",
    "DBT_LOG_PATH": "/tmp/dbt/data_mart_report/logs",
}
POSTGRES_CONN_ID = "dwh_postgres"


with DAG(
    dag_id="metrics_overview",
    start_date=datetime(2026, 8, 1),
    schedule="@monthly",
    catchup=False,
    max_active_runs=1,
) as dag:
    start_task = EmptyOperator(
        task_id="start",
    )

    with TaskGroup(group_id="raw") as raw_group:
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

            truncate_task >> load_task >> raw_loaded_task

    with TaskGroup(group_id="staging") as staging_group:
        staging_models_loaded_task = EmptyOperator(
            task_id="staging_models_loaded",
        )

        staging_completed_task = EmptyOperator(
            task_id="staging_completed",
        )

        for table in TABLES:
            load_staging_task = BashOperator(
                task_id=f"load_staging_{table}",
                bash_command=DBT_COMMANDS["load_staging"].format(model=table),
                cwd=DBT_PROJECT_DIR,
                env=STAGING_DBT_PATHS[table],
                append_env=True,
            )

            test_staging_task = BashOperator(
                task_id=f"test_staging_{table}",
                bash_command=DBT_COMMANDS["test_staging"].format(model=table),
                cwd=DBT_PROJECT_DIR,
                env=STAGING_DBT_PATHS[table],
                append_env=True,
            )

            load_staging_task >> staging_models_loaded_task
            staging_models_loaded_task >> test_staging_task >> staging_completed_task

    with TaskGroup(group_id="data_mart") as data_mart_group:
        data_mart_completed_task = EmptyOperator(
            task_id="data_mart_completed",
        )

        fact_test_tasks = {}

        for model in DIMENSION_MODELS + FACT_MODELS:
            load_model_task = BashOperator(
                task_id=f"load_{model}",
                bash_command=DBT_COMMANDS["load_data_mart"].format(model=model),
                cwd=DBT_PROJECT_DIR,
                env=DATA_MART_DBT_PATHS[model],
                append_env=True,
            )

            test_model_task = BashOperator(
                task_id=f"test_{model}",
                bash_command=DBT_COMMANDS[
                    "test_data_mart_totals"
                    if model in FACT_MODELS
                    else "test_data_mart"
                ].format(model=model),
                cwd=DBT_PROJECT_DIR,
                env=DATA_MART_DBT_PATHS[model],
                append_env=True,
            )

            load_model_task >> test_model_task

            if model in FACT_MODELS:
                fact_test_tasks[model] = test_model_task
            else:
                test_model_task >> data_mart_completed_task

        load_player_metrics_task = BashOperator(
            task_id="load_player_metrics",
            bash_command=DBT_COMMANDS["load_data_mart"].format(
                model=PLAYER_METRICS_MODEL,
            ),
            cwd=DBT_PROJECT_DIR,
            env=DATA_MART_DBT_PATHS[PLAYER_METRICS_MODEL],
            append_env=True,
        )

        test_player_metrics_task = BashOperator(
            task_id="test_player_metrics",
            bash_command=DBT_COMMANDS["test_data_mart_totals"].format(
                model=PLAYER_METRICS_MODEL,
            ),
            cwd=DBT_PROJECT_DIR,
            env=DATA_MART_DBT_PATHS[PLAYER_METRICS_MODEL],
            append_env=True,
        )

        for fact_test_task in fact_test_tasks.values():
            fact_test_task >> load_player_metrics_task

        load_player_metrics_task >> test_player_metrics_task
        test_player_metrics_task >> data_mart_completed_task

    with TaskGroup(group_id="data_mart_report") as data_mart_report_group:
        load_metrics_overview_task = BashOperator(
            task_id="load_metrics_overview",
            bash_command=DBT_COMMANDS["load_data_mart_report"].format(
                model=DATA_MART_REPORT_MODEL,
            ),
            cwd=DBT_PROJECT_DIR,
            env=DATA_MART_REPORT_DBT_PATHS,
            append_env=True,
        )

        test_metrics_overview_task = BashOperator(
            task_id="test_metrics_overview",
            bash_command=DBT_COMMANDS["test_data_mart_report"].format(
                model=DATA_MART_REPORT_MODEL,
            ),
            cwd=DBT_PROJECT_DIR,
            env=DATA_MART_REPORT_DBT_PATHS,
            append_env=True,
        )

        build_monthly_summary_task = BashOperator(
            task_id="build_monthly_summary",
            bash_command=DBT_COMMANDS["build_data_mart_report"].format(
                model=MONTHLY_SUMMARY_MODEL,
            ),
            cwd=DBT_PROJECT_DIR,
            env=DATA_MART_REPORT_DBT_PATHS,
            append_env=True,
        )

        data_mart_report_completed_task = EmptyOperator(
            task_id="data_mart_report_completed",
        )

        (
            load_metrics_overview_task
            >> test_metrics_overview_task
            >> build_monthly_summary_task
            >> data_mart_report_completed_task
        )

    finish_task = EmptyOperator(
        task_id="finish",
    )

    (
        start_task
        >> raw_group
        >> staging_group
        >> data_mart_group
        >> data_mart_report_group
        >> finish_task
    )

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from metrics_overview.scripts.dbt_commands import DBT_COMMANDS, DBT_PROJECT_DIR
from metrics_overview.scripts.load_raw import load_table


TABLES = {
    "players": {"key_columns": ("id",)},
    "providers_map": {"key_columns": ("id",)},
    "games_map": {"key_columns": ("id",)},
    "currency_rates": {
        "key_columns": ("date", "currency"),
        "date_column": "date",
    },
    "deposits": {
        "key_columns": ("id",),
        "date_column": "deposit_date",
    },
    "withdrawals": {
        "key_columns": ("id",),
        "date_column": "withdrawal_date",
    },
    "games": {
        "key_columns": ("id",),
        "date_column": "game_date",
    },
}
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
DATA_MART_INPUTS = {
    "dim_player": ("players",),
    "dim_provider": ("providers_map",),
    "dim_game": ("games_map",),
    "fact_deposit": ("deposits", "currency_rates"),
    "fact_withdrawal": ("withdrawals", "currency_rates"),
    "fact_bet": ("games", "currency_rates"),
}
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
LOOKBACK_DAYS = 30


with DAG(
    dag_id="metrics_overview",
    start_date=datetime(2026, 8, 1),
    schedule="@monthly",
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
    },
) as dag:
    start_task = EmptyOperator(
        task_id="start",
    )

    # Incrementally load source data into raw tables.
    with TaskGroup(group_id="raw"):
        raw_load_tasks = {}

        for table, load_config in TABLES.items():
            raw_load_tasks[table] = PythonOperator(
                task_id=f"load_{table}",
                python_callable=load_table,
                op_kwargs={
                    "table": table,
                    **load_config,
                    "dwh_conn_id": POSTGRES_CONN_ID,
                    "lookback_days": LOOKBACK_DAYS,
                },
            )

            start_task >> raw_load_tasks[table]

    # Build and test cleaned staging models.
    with TaskGroup(group_id="staging"):
        staging_load_tasks = {}
        staging_test_tasks = {}

        for table in TABLES:
            staging_load_tasks[table] = BashOperator(
                task_id=f"load_staging_{table}",
                bash_command=DBT_COMMANDS["load_staging"].format(model=table),
                cwd=DBT_PROJECT_DIR,
                env=STAGING_DBT_PATHS[table],
                append_env=True,
            )

            staging_test_tasks[table] = BashOperator(
                task_id=f"test_staging_{table}",
                bash_command=DBT_COMMANDS["test_staging"].format(model=table),
                cwd=DBT_PROJECT_DIR,
                env=STAGING_DBT_PATHS[table],
                append_env=True,
            )

            (
                raw_load_tasks[table]
                >> staging_load_tasks[table]
                >> staging_test_tasks[table]
            )

    # Build dimensions and facts before player metrics.
    with TaskGroup(group_id="data_mart"):
        data_mart_test_tasks = {}

        for model in DIMENSION_MODELS + FACT_MODELS:
            load_model_task = BashOperator(
                task_id=f"load_{model}",
                bash_command=DBT_COMMANDS["load_data_mart"].format(model=model),
                cwd=DBT_PROJECT_DIR,
                env=DATA_MART_DBT_PATHS[model],
                append_env=True,
            )

            data_mart_test_tasks[model] = BashOperator(
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

            for table in DATA_MART_INPUTS[model]:
                staging_test_tasks[table] >> load_model_task

            load_model_task >> data_mart_test_tasks[model]

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

        for model in FACT_MODELS:
            data_mart_test_tasks[model] >> load_player_metrics_task

        load_player_metrics_task >> test_player_metrics_task

    # Build and test reporting models.
    with TaskGroup(group_id="data_mart_report"):
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

        (
            load_metrics_overview_task
            >> test_metrics_overview_task
            >> build_monthly_summary_task
        )

        data_mart_test_tasks["dim_player"] >> load_metrics_overview_task
        test_player_metrics_task >> load_metrics_overview_task

    finish_task = EmptyOperator(
        task_id="finish",
    )

    for model in ("dim_provider", "dim_game"):
        data_mart_test_tasks[model] >> finish_task

    build_monthly_summary_task >> finish_task

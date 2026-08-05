DBT_PROJECT_DIR = "/opt/airflow/dbt"

DBT_COMMANDS = {
    "load_staging": (
        "/home/airflow/.dbt-venv/bin/dbt run "
        "--select path:models/staging/{table}.sql"
    ),
    "test_staging": (
        "/home/airflow/.dbt-venv/bin/dbt test "
        "--select path:models/staging/{table}.sql --exclude test_type:singular"
    ),
    "load_data_mart": (
        "/home/airflow/.dbt-venv/bin/dbt run "
        "--select path:models/data_mart/{model}.sql"
    ),
    "test_data_mart": (
        "/home/airflow/.dbt-venv/bin/dbt test "
        "--select path:models/data_mart/{model}.sql "
        "--indirect-selection cautious"
    ),
    "test_data_mart_totals": (
        "/home/airflow/.dbt-venv/bin/dbt test "
        "--select path:models/data_mart/{model}.sql {model}_totals_match "
        "--indirect-selection cautious"
    ),
    "load_data_mart_report": (
        "/home/airflow/.dbt-venv/bin/dbt run "
        "--select path:models/data_mart_report/{model}.sql"
    ),
    "test_data_mart_report": (
        "/home/airflow/.dbt-venv/bin/dbt test "
        "--select path:models/data_mart_report/{model}.sql {model}_totals_match "
        "--indirect-selection cautious"
    ),
    "build_data_mart_report": (
        "/home/airflow/.dbt-venv/bin/dbt build "
        "--select path:models/data_mart_report/{model}.sql "
        "--indirect-selection cautious"
    ),
}

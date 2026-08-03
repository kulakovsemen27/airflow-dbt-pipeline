FROM apache/airflow:2.10.5-python3.12

USER root

RUN apt-get update \
    && apt-get install --yes --no-install-recommends git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

COPY requirements.txt /requirements.txt
COPY docker/airflow/dbt-requirements.txt /dbt-requirements.txt

RUN pip install --no-cache-dir \
    "apache-airflow==${AIRFLOW_VERSION}" \
    -r /requirements.txt

RUN python -m venv /home/airflow/.dbt-venv \
    && /home/airflow/.dbt-venv/bin/pip install --no-cache-dir \
        -r /dbt-requirements.txt

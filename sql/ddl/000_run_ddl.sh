#!/usr/bin/env bash

set -Eeuo pipefail

ddl_root="/docker-entrypoint-initdb.d"
schema_directories=(raw staging data_mart data_mart_report)
ddl_files=()

for schema_directory in "${schema_directories[@]}"; do
    while IFS= read -r ddl_file; do
        ddl_files+=("${ddl_file}")
    done < <(
        find "${ddl_root}/${schema_directory}" -maxdepth 1 -type f -name '*.sql' -print \
            | sort
    )
done

if ((${#ddl_files[@]} == 0)); then
    echo "No DDL files found"
    exit 0
fi

psql_arguments=(
    --set ON_ERROR_STOP=1
    --single-transaction
    --username "${POSTGRES_USER}"
    --dbname "${POSTGRES_DB}"
)

for ddl_file in "${ddl_files[@]}"; do
    echo "Adding DDL: ${ddl_file}"
    psql_arguments+=(--file "${ddl_file}")
done

psql "${psql_arguments[@]}"

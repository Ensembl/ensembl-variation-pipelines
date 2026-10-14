#!/bin/bash

# Apply sql/schema.sql using the [postgresql] settings in .env.
# Usage: nextflow/phenotypes/bin/apply_schema.sh [ /path/to/.env ]
# With no argument, read .env from the current working directory.

set -euo pipefail

PIPELINE_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="${1:-$PWD/.env}"
section=""
schema="phenotypes"

while IFS='=' read -r key value || [[ -n "$key" ]]; do
    if [[ "$key" == \[*\] ]]; then
        section="$key"
        continue
    fi
    [[ "$section" == "[postgresql]" ]] || continue
    case "$key" in
        host) export PGHOST="$value" ;;
        port) export PGPORT="$value" ;;
        user) export PGUSER="$value" ;;
        name) export PGDATABASE="$value" ;;
        schema) schema="$value" ;;
        password) if [[ -n "$value" ]]; then export PGPASSWORD="$value"; fi ;;
    esac
done < "$ENV_FILE"

exec psql -X -v ON_ERROR_STOP=1 \
    -v target_schema="$schema" -v schema_file="${PIPELINE_DIR}/sql/schema.sql" <<'SQL'
SET search_path TO :"target_schema";
\i :schema_file
SQL

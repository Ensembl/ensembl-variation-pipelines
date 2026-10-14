#!/bin/bash

# Create the phenotype tables in the configured PostgreSQL schema.
# Read connection details from an .env file and apply sql/schema.sql.

set -euo pipefail

PIPELINE_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="${1:-$PWD/.env}"
PYTHON_BIN="${PIPELINE_DIR}/.venv/bin/python"
SCHEMA_FILE="${PIPELINE_DIR}/sql/schema.sql"

if [[ ! -x "$PYTHON_BIN" ]]; then
    echo "Python environment not found. Run uv sync in $PIPELINE_DIR." >&2
    exit 1
fi

PYTHONPATH="${PIPELINE_DIR}/src" exec "$PYTHON_BIN" -m phenotypes.config \
    --env-file "$ENV_FILE" --apply-schema "$SCHEMA_FILE"

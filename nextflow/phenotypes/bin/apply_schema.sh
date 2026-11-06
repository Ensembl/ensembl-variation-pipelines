#!/bin/bash

# Apply sql/schema.sql using the [postgresql] settings in .env.
# Usage: nextflow/phenotypes/bin/apply_schema.sh [ /path/to/.env ]
# With no argument, read .env from the current working directory.

set -euo pipefail

PIPELINE_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="${1:-$PWD/.env}"

# Configuration parsing and PostgreSQL environment mapping have one shared owner.
# SQL and the selected target schema belong to this consumer.
exec "${PIPELINE_DIR}/.venv/bin/python" - "$ENV_FILE" "${PIPELINE_DIR}/sql/schema.sql" <<'PY'
import os
from pathlib import Path
import subprocess
import sys

from variation_sources.database import postgresql_environment

try:
    settings = postgresql_environment(Path(sys.argv[1]))
    environment = {**os.environ, **settings}
    command = [
        "psql", "-X", "-v", "ON_ERROR_STOP=1",
        "-v", f"target_schema={settings['PGSCHEMA']}",
        "-v", f"schema_file={sys.argv[2]}",
    ]
    result = subprocess.run(
        command,
        input='SET search_path TO :"target_schema";\n\\i :schema_file\n',
        text=True,
        env=environment,
    )
except (OSError, ValueError) as error:
    print(f"[Applying phenotype schema] {error}", file=sys.stderr)
    raise SystemExit(1)
raise SystemExit(result.returncode)
PY

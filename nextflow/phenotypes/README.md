# Phenotypes pipeline

Pipeline to download and parse phenotype data for PostgreSQL.

```bash
module load nextflow/26.04.0
```

## Project setup

Create the Python environment in the project directory:

```bash
cd ensembl-variation-pipelines/nextflow/phenotypes
PIPELINE_DIR="$PWD"
```

The Python project requires Python 3.14 and uses `uv`. Create or update the virtual environment with:

```bash
uv sync
```

## Database connection

Copy `.env` to the run directory:

```bash
cd /path/to/run-directory
cp -n "$PIPELINE_DIR/.env.example" .env
chmod 600 .env
```

Set the PostgreSQL connection details in `.env`:

```ini
[postgresql]
host=database_host
port=5432
user=database_user
name=database_name
schema=phenotypes
password=
```

Leave `password` empty to use `.pgpass` or the `psql` password prompt.

# First time only set-up **(first time only)**

## PostgreSQL setup **(first time only)**

The schema requires PostgreSQL 10 or later and was created using PostgreSQL 16.2. Load the `postgresql/16` module:

```bash
module load postgresql/16
```

Create the schema:

```sql
CREATE SCHEMA phenotypes AUTHORIZATION ensevp;
```

## Create the tables **(first time only)**

Apply `sql/schema.sql` to the configured empty schema using `.env` in the current directory:

```bash
"$PIPELINE_DIR/bin/apply_schema.sh"
```

Alternatively, pass an explicit `.env` path:

```bash
"$PIPELINE_DIR/bin/apply_schema.sh" /path/to/run-directory/.env
```

Check tables got generated correctly:

```bash
# Use the connection values from .env:
psql -h database_host -p database_port -U database_user -d database_name
```

At psql prompt: 

```SQL
-- list schemas
\dn

-- select phenotype schema
SET search_path TO phenotypes;

-- list tables
\dt
```

# Data sources

## ClinVar

- By default, the pipeline downloads the current [ClinVar RCV XML release](https://ftp.ncbi.nlm.nih.gov/pub/clinvar/xml/RCV_release/ClinVarRCVRelease_00-latest.xml.gz)
and [VCV XML release](https://ftp.ncbi.nlm.nih.gov/pub/clinvar/xml/ClinVarVCVRelease_00-latest.xml.gz).
Failed download attempts are retried three times.
- To use manually downloaded files, set `sources.clinvar.inputs.rcv` and/or `sources.clinvar.inputs.vcv` in the source configuration (`assets/source_config.json`). Inputs, species and assembly also come from `assets/source_config.json`. At present, these are hard-coded and can be edited by the user running the pipeline. 
- ClinVar requires a human context and an assembly for its location filter; `assembly_uuid` is optional.
- You can supply an alternative `source_config.json` file using:

```bash
nextflow run "$PIPELINE_DIR/main.nf" --source_config /path/to/source_config.json
```

- Outputs are published to `<outdir>/clinvar/<species_production_name>/<assembly>/`.
The first JSONL line contains shared `metadata`; subsequent lines contain associations.

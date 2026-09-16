# Phenotypes pipeline

Pipeline to download, parse, clean and load phenotype data into PostgreSQL.

## Project setup

Run commands in this README from the project directory:

```bash
cd ensembl-variation-pipelines/nextflow/phenotypes
```

The Python project requires Python 3.14 and uses `uv`. Create or update the virtual environment with:

```bash
uv sync
```

## Database connection

Copy the example connection file:

```bash
cp .env.example .env
```

Set these values in `.env`:

```bash
PGDATABASE=database_name
PGUSER=database_user
PGHOST=database_host
PGPORT=database_port
PGSCHEMA=phenotypes
```

Load `.env`:

```bash
set -a; source .env; set +a
```

## PostgreSQL setup **(first time only)**

The schema requires PostgreSQL 10 or later and was tested with PostgreSQL 16.2. Load the `postgresql/16` module:

```bash
module load postgresql/16
```

Create the schema:

```sql
CREATE SCHEMA phenotypes AUTHORIZATION ensevp;
```

## Create the tables **(first time only)**

Apply `sql/schema.sql` to the configured empty schema:

```bash
./bin/apply_schema.sh
```

Check tables got generated correctly:

```bash
psql
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
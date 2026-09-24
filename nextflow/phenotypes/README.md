# Phenotypes pipeline

Pipeline to download, parse, clean and load phenotype data into PostgreSQL.
This pipeline uses Nextflow 26.04.0.

```bash
module load nextflow/26.04.0
```

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

```.env
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

The schema requires PostgreSQL 10 or later and was created using PostgreSQL 16.2. Load the `postgresql/16` module:

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

## ClinVar

By default, the pipeline downloads the current [ClinVar RCV XML release](https://ftp.ncbi.nlm.nih.gov/pub/clinvar/xml/RCV_release/ClinVarRCVRelease_00-latest.xml.gz).
Failed download attempts are retried three times.

To use a manually downloaded ClinVar XML file instead:

```bash
nextflow run main.nf -profile standard --input /path/to/ClinVarRCVRelease_00-latest.xml.gz
```

Using `--input` skips the download.
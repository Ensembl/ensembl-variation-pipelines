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

The Python parser extracts the XML into an intermediate `clinvar_output.jsonl`
file ahead of database loading. Each line is a JSON object containing a supported
variant–phenotype association, its source information, assessments and variant
lookup inputs. An RCV with several traits can produce several output lines.

Evidence-only records are also retained as `no_classification` assessments in
an `unspecified` context; they do not represent a clinical classification.
Explicitly non-current RCVs are rejected; non-current SCV assessments are
omitted with a warning while the current RCV is retained.
References can be PubMed papers, DOI citations or HTTP/HTTPS evidence links.

The parser does not write to PostgreSQL. Resolution and loading will be separate
stages: resolution identifies optional Ensembl website links, and loading writes
the results into the database model. An unmatched supported association can still
be retained. The parser currently runs separately from Nextflow; those later
stages are not yet implemented.

The parser also writes:

- `clinvar_summary.json`: release and assembly metadata, accepted/rejected counts
  and percentages, emitted rows, unique phenotype names and warning counts.
- `warnings.txt`: tab-separated RCV, VCV, SCV, warning and details columns.
- `rejections.txt`: tab-separated RCV, VCV, rejection reason and details columns.

Only source placements on the requested assembly are retained. Source variant
values are preserved, including structural attributes and uncertain coordinates;
unsupported records are listed in the rejection file. Use the RCV accession to
find the original XML record when investigating an output or warning.

See the [ClinVar JSONL field guide](docs/clinvar_jsonl_fields.md) for field meanings,
XML origins and detailed parsing rules.

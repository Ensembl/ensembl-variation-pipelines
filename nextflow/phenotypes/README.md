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

### Notes

#### Condition groups

Certain condition groups stay together rather than being split into independent phenotype associations:

| ClinVar grouping | Output phenotype label |
| --- | --- |
| `TraitChoice` | `A OR B` |
| `multipleConditionExplanation="Uncertain"` | `A OR B (condition uncertain)` |
| `multipleConditionExplanation="Novel disease"` | `A AND B (novel disease)` |
| `multipleConditionExplanation="Co-occurring"` | `A AND B (co-occurring conditions)` |

#### `warnings.txt`

- `warnings.txt` is a tab-separated file, with columns `RCV`, `VCV`, `SCV`, `warning` and `details`
- `SCV` is blank when the warning is not tied to a particular submission
- A warning does not reject the whole RCV - its aggregate association remains, but a value may be missing or an SCV assessment may be left unattached
- Whole-record exclusions are listed separately in `rejections.txt`
- An RCV or SCV can appear more than once because it can have several warnings

| Warning | Meaning and effect on the output |
| --- | --- |
| `submission_trait_set_unmapped` | No submitted condition could be matched uniquely to an RCV condition, or the SCV has no conditions to match. Its assessment is not attached. |
| `submission_trait_set_partially_mapped` | Some submitted conditions matched, but others did not. An ordinary, ungrouped submission keeps its unique matches; an explicitly grouped submission is left unattached because its full combination has not matched. |
| `submission_trait_set_ambiguous` | A submitted condition matches more than one RCV condition, so the parser cannot choose one. Other unique matches from an ordinary submission may remain; an explicitly grouped submission is left unattached. |
| `submission_trait_set_subset_mapped` | Every submitted condition matched, but the SCV covers only part of the RCV condition set. Its assessment stays with that matched scope. A grouped subset gets its own combined row, without the broader aggregate assessment. |
| `submission_trait_set_choice_mismatch` | The conditions match, but the SCV does not report an OR grouping compatible with the RCV's TraitChoice or Uncertain group. The SCV keeps its own appropriately grouped or split rows rather than being attached to the aggregate OR row. |
| `vcv_rcv_version_unavailable` | The exact RCV accession/version is not listed under the relevant VCV in the supplied VCV file. VCV fallback matching is skipped; existing unique direct matches are retained. |
| `non_current_submission` | The SCV has a non-current record status, so its assessments are skipped. |
| `alternate_trait_name_used` | A preferred trait name is missing, so the single available alternate name is used instead. |
| `duplicate_trait_merged` | A repeated trait is merged using its ClinVar trait ID, or identical XML when no ID is present. Its mappings and references are combined rather than creating another trait. |
| `ambiguous_primary_location` | Retained placements for the variant or a haplotype component have different VCF reference/alternate allele pairs. Locations remain separate; the parent `reported_allele` is null if its own placements are ambiguous. |
| `conflicting_inheritance_context` | An assertion reports `Somatic mutation` alongside another inheritance mode. Its context is set to `unspecified`; the classification and reported inheritance values are preserved. |
| `invalid_numeric_attribute` | A supplied coordinate or other numeric location attribute is not a valid integer. That value becomes null; the rest of the record is retained. |
| `invalid_last_evaluated_date` | The assessment date cannot be read as a valid supported date. It becomes null; the assessment remains. |
| `invalid_doi` | The selected DOI cannot be recognised after cleaning. The publication remains with a null identifier; a URL is not substituted for the invalid DOI. |

For example, `RCV004801462.2 / SCV005051716.2` produces
`submission_trait_set_partially_mapped`: two features match, but the submitted
`HP:0000798` maps through VCV to `MedGen:C0868910 / Oligozoospermia`, whereas the
RCV uses `MedGen:C0028960 / Oligospermia`. The complete SCV assessment is therefore
left unattached, while the aggregate association is retained.

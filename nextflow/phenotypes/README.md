# Phenotypes pipeline

Pipeline to download, parse, load and harmonise phenotype data into PostgreSQL.

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

- By default, the pipeline downloads the current ClinVar XML releases:
  - RCV: https://ftp.ncbi.nlm.nih.gov/pub/clinvar/xml/RCV_release/ClinVarRCVRelease_00-latest.xml.gz
  - VCV: https://ftp.ncbi.nlm.nih.gov/pub/clinvar/xml/ClinVarVCVRelease_00-latest.xml.gz
- Failed download attempts are retried three times, separately for each file
- To use pre-downloaded files, set `sources.ClinVar.inputs.rcv` and
  `sources.ClinVar.inputs.vcv` in the source configuration (`assets/source_config.json`).
- The source key (`ClinVar`) supplies the source name in the JSONL metadata and database; no separate name setting is needed.
  A missing or null value downloads that file only; an incorrect supplied path is an error
- The parser reads VCV first to map haplotype component Allele IDs to versioned
  VCV accessions, then parses associations from RCV. VCV does not change phenotype
  grouping, classifications or which SCVs belong to an RCV
- Species and assembly also come from the `source_config.json` file
- ClinVar requires a human context and an assembly for its location filter, `assembly_uuid` is optional
- The default `source_config.json` used is the one in `/assets`, but an alternative can be provided by using:

```bash
nextflow run "$PIPELINE_DIR/main.nf" --source_config /path/to/source_config.json
```

- The intermediate JSONL is published to `<outdir>/clinvar/<species_production_name>/<assembly>/`
- The first JSONL line contains `metadata` shared across all records, subsequent lines are the records themselves
- Each record follows `PhenotypeAssociation`: `phenotype`, `entity`, `somatic_status` and `source_reports`. Assessments belong inside each source report; publications belong inside each assessment's evidence
- Shared source, import-run and genome-context objects remain in the header rather than being repeated in each record
- Parser outputs are `clinvar_output.jsonl`, `warnings.txt` and `rejections.txt`.

### Run the pipeline

The workflow runs download → parse → load. Create the database tables first,
then run from the directory containing `.env`:

```bash
module load nextflow/26.04.0
nextflow run "$PIPELINE_DIR/main.nf" \
    --source_config /path/to/source_config.json \
    --outdir output
```

- If ClinVar is already loaded, the loader refuses to overwrite it unless `--replace` is supplied. Replacement covers **all existing reports for that source**, not just the configured assembly

### Notes

#### Output: `warnings.txt`

- `warnings.txt` is a tab-separated file, with columns `RCV`, `VCV`, `SCV`, `warning` and `details`
- `SCV` is blank when the warning is not tied to a particular submission
- A warning does not reject the whole RCV - a value may be missing or a non-current SCV may be skipped
- Whole-record exclusions are listed separately in `rejections.txt`
- An RCV or SCV can appear more than once because it can have several warnings

| Warning | Meaning and effect on the output |
| --- | --- |
| `non_current_submission` | The SCV has a non-current record status, so its assessments are skipped. |
| `alternate_trait_name_used` | A preferred trait name is missing, so the single available alternate name is used instead. |
| `duplicate_trait_merged` | A repeated trait is merged using its ClinVar trait ID, or identical XML when no ID is present. Its mappings and references are combined rather than creating another trait. |
| `ambiguous_primary_location` | Retained placements for the variant or a haplotype component have different VCF reference/alternate allele pairs. Locations remain separate; the parent `reported_allele` is null if its own placements are ambiguous. |
| `conflicting_inheritance_context` | An assertion reports `Somatic mutation` alongside another inheritance mode. Its context is set to `unspecified`; the classification and reported inheritance values are preserved. |
| `ambiguous_somatic_context` | Legacy classification contexts conflict or contradict sample origin. All assessments remain together in one unspecified record rather than being split. |
| `invalid_numeric_attribute` | A supplied coordinate or other numeric location attribute is not a valid integer. That value becomes null; the rest of the record is retained. |
| `invalid_last_evaluated_date` | The assessment date cannot be read as a valid supported date. It becomes null; the assessment remains. |
| `invalid_doi` | The selected DOI cannot be recognised after cleaning. The publication remains with a null identifier; a URL is not substituted for the invalid DOI. |
| `missing_component_vcv` | No current individual VCV with a version was found for a haplotype component. Keep its original Allele ID. |
| `ambiguous_component_vcv` | Several different accession/version values were found for one component Allele ID. Keep its original Allele ID rather than choosing one. |
| `input_release_date_mismatch` | The RCV and VCV release dates differ. Processing continues, but identifiers may come from different snapshots. |

#### Output: `rejections.txt`

- This file has columns `RCV`, `VCV`, `rejection_reason` and `details`.
- A rejection excludes the whole RCV from `clinvar_output.jsonl`
- The reasons and meanings are as follows:

| Rejection reason | Meaning |
| --- | --- |
| `non_current_record` | The outer record or aggregate assertion is not current. |
| `missing_reference_assertion` | The RCV aggregate assertion is missing. |
| `missing_rcv_accession` | The aggregate has no RCV accession. |
| `missing_trait_set` | The aggregate has no condition set. |
| `unsupported_trait_set_type` | Its condition-set type is not supported. |
| `missing_traits` | The condition set contains no traits. |
| `missing_preferred_trait_name` | An aggregate trait has no preferred name and no single unambiguous alternate. |
| `unsupported_phenotype_instruction` | A PhenotypeInstruction contains text other than a recognised nonspecific label. |
| `missing_supported_traits` | No usable traits remain after trait processing; a defensive check. |
| `unsupported_genotype_set` | The record uses an unsupported GenotypeSet, such as a diplotype or compound heterozygote. |
| `missing_measure_set` | The aggregate has no MeasureSet describing the variant. |
| `missing_measure_set_type` | The MeasureSet has no type. |
| `unsupported_measure_set_type` | Its type is not Variant, Haplotype or Haplotype, single variant. |
| `unsupported_variant_representation` | There are no measures, or a non-Haplotype MeasureSet has more than one measure. |
| `missing_requested_assembly_location` | The variant or a haplotype component has no placement on the configured assembly. |
| `unsupported_classification` | No usable aggregate assessment is present in a supported classification category. |

- For example, `RCV000677106.1` has only a GRCh37 placement and is rejected with `missing_requested_assembly_location` when GRCh38 is requested

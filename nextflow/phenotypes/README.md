# Phenotypes pipeline

Download, parse and load phenotype data into PostgreSQL.

## Project setup

Requires Nextflow 26.04.0 and Python 3.14. Create the Python environment:

```bash
module load nextflow/26.04.0
cd ensembl-variation-pipelines/nextflow/phenotypes
PIPELINE_DIR="$PWD"
uv sync
```

Keep `../shared` alongside this pipeline: it supplies common parsing,
configuration and provenance code. Local and Slurm tasks use `.venv`; compute
nodes must be able to access it. Conda and container profiles are also available.

### Optional container execution

For `-profile docker`, build the image used by both parsers and the loader:

```bash
docker build -t ensembl-phenotypes:0.1.0 modules/local/load
```

For Singularity, convert this image to a SIF and set its absolute path as the
container for `PARSE_CLINVAR`, `PARSE_G2P` and `LOAD_PHENOTYPES` in a custom config.
A registry URL can also be supplied; this repository does not publish the image.
Containers need database network access and access to `.pgpass` if used.

## Database connection

Create `.env` in the run directory:

```bash
cd /path/to/run-directory
cp -n "$PIPELINE_DIR/../shared/assets/.env.example" .env
chmod 600 .env
```

Set the connection and target schema:

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
The loader and schema helper use these same settings. `.env` is staged in task
work directories but is not published; protect the work directory accordingly.
For native Psycopg library problems, see [the workaround below](#native-hpc-workaround).

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

## Run the pipeline

The workflow runs **download → parse → load** for every source in the selected
config. The default, `assets/phenotypes_config.json`, contains ClinVar and G2P.
Supply a config containing only one source to update that source alone.

For each input, a missing or null path triggers a download. Supplied paths must
exist and are resolved relative to the config file. Downloads retry failures
three times, wait ten seconds between retries, and rename `.part` files only
after success. Each source currently supports one processing context per run.

Create the database tables first, then run from the directory containing `.env`:

```bash
module load nextflow/26.04.0
nextflow run "$PIPELINE_DIR/main.nf" \
    --source_config /path/to/phenotypes_config.json \
    --outdir output
```

Omit `--source_config` to use the default. Use `--env_file /path/to/.env` to
select a different database config. The source key supplies its name in the
JSONL and database.

If a source is already loaded, loading stops unless `--replace` is supplied.
Replacement covers **all reports for each configured source**, across assemblies.
Loads run one at a time, each in its own transaction: a failed load rolls back that
source, while other committed sources remain loaded. The pipeline stops on
failure.

Use `-resume` with the original Nextflow cache and work directory to reuse
completed download and parser tasks. Parser code changes invalidate cached
parsing results. Database loading always runs again.

## Pipeline outputs

| Source | Output directory | Association file | Load summary |
| --- | --- | --- | --- |
| ClinVar | `<outdir>/clinvar/<species>/<assembly>/` | `clinvar_output.jsonl` | `clinvar_load_summary.json` |
| G2P | `<outdir>/g2p/<species>/` | `g2p_output.jsonl` | `g2p_load_summary.json` |

`<species>` is `species_production_name`. G2P adds an assembly directory when
one is configured. Each parser also writes `warnings.txt` and `rejections.txt`.
Downloaded inputs are copied to `<outdir>/clinvar/inputs/` or
`<outdir>/g2p/inputs/`.

The first JSONL line contains shared `metadata`: source, genome context and
import provenance, including both input filenames/checksums, pipeline date and
revision. Subsequent lines contain `phenotype`, `entity`, `somatic_status` and
`source_reports`. Assessments are nested within reports; evidence belongs to
its report or assessment.

# Data sources

## ClinVar

Set `sources.ClinVar.inputs.rcv` and `sources.ClinVar.inputs.vcv` to local files
or null:

| Input | Default download |
| --- | --- |
| `rcv` | [RCV XML](https://ftp.ncbi.nlm.nih.gov/pub/clinvar/xml/RCV_release/ClinVarRCVRelease_00-latest.xml.gz) |
| `vcv` | [VCV XML](https://ftp.ncbi.nlm.nih.gov/pub/clinvar/xml/ClinVarVCVRelease_00-latest.xml.gz) |

ClinVar requires a human context and an assembly to select locations;
`assembly_uuid` is optional. VCV is read first to map haplotype component Allele
IDs to versioned VCV accessions. Associations come from RCV; VCV does not change
phenotype grouping, classifications or SCV membership. The XML release date
becomes `import_run.source_version`.

### Notes

#### Multiple conditions

Multiple conditions reported together in an RCV are kept together. The parser
creates a combined `phenotype_name`, for example
`Multiple conditions (Muscle weakness ; Sensorimotor neuropathy)`.
Individual source names and their ontology mappings remain in
`source_report.reported_phenotypes`.

#### VCF end coordinates

Reported locations preserve ClinVar's original `start`, `stop`, VCF position
and alleles. The parser also calculates nullable `stop_vcf` as
`position_vcf + length(reference_allele_vcf) - 1`, including reference anchor
bases. For the ACKR1 deletion, position 159205718 and a 15-base reference
sequence give end 159205732; its source start remains 159205719.
Missing or unsupported VCF representations leave `stop_vcf` null. The loader
copies the value without calculating coordinates or filling shared EntityLocations.

#### Haplotype spans

The parent haplotype's reported `locations` can contain a calculated interval.
Its `start` is the earliest component VCF position and its `stop` is the latest
component `stop_vcf`. For NAT1*10 on GRCh38 chromosome 8, this is
18223135–18223142. Component locations and alleles remain unchanged; no parent
VCF position or allele sequence is constructed, and no origin field is added.

Calculation requires one complete VCF interval per component on the same
assembly, chromosome and reference sequence. Different chromosomes produce
`haplotype_span_different_chromosomes`; the parent span is unavailable,
represented by `locations: []`, and the record is retained. Missing or ambiguous
component intervals also leave parent locations empty. The loader copies any
calculated interval into reported locations, not shared EntityLocations.

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
| `ambiguous_primary_location` | Retained placements for the variant or a haplotype component have different VCF reference/alternate allele pairs. Locations remain separate; the affected reported entity's `focal_allele` is null. Haplotype components carry their own focal alleles; the parent focal allele is null. |
| `conflicting_inheritance_context` | An assertion reports `Somatic mutation` alongside another inheritance mode. Its context is set to `unspecified`; the classification and reported inheritance values are preserved. |
| `ambiguous_somatic_context` | Legacy classification contexts conflict or contradict sample origin. All assessments remain together in one unspecified record rather than being split. |
| `invalid_numeric_attribute` | A supplied coordinate or other numeric location attribute is not a valid integer. That value becomes null; the rest of the record is retained. |
| `invalid_last_evaluated_date` | The assessment date cannot be read as a valid supported date. It becomes null; the assessment remains. |
| `invalid_doi` | The selected DOI cannot be recognised after cleaning. The publication remains with a null identifier; a URL is not substituted for the invalid DOI. |
| `haplotype_span_different_chromosomes` | Component placements include different chromosomes. No parent span is calculated (`locations: []`); the haplotype and its components are retained. |
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

## G2P

Example G2P-only config; null inputs download both files:

```json
{
  "sources": {
    "G2P": {
      "inputs": {
        "data": null,
        "gene_id_mapping": null
      },
      "contexts": [{
        "species_taxonomy_id": 9606,
        "species_production_name": "homo_sapiens"
      }]
    }
  }
}
```

| Input | Default download |
| --- | --- |
| `data` | [Latest dated G2P CSV.GZ](https://ftp.ebi.ac.uk/pub/databases/gene2phenotype/G2P_data_downloads/) |
| `gene_id_mapping` | [HGNC complete-set TSV](https://storage.googleapis.com/public-download-files/hgnc/tsv/tsv/hgnc_complete_set.txt) |

Either input can use a local path while the other downloads. G2P requires a
human context; assembly and UUIDs are optional.

The downloader selects the newest dated FTP directory and keeps the filename,
for example `2026_10_28/allG2P_2026-10-28.csv.gz`. The parser accepts CSV and
CSV.GZ. Only FTP filenames matching `allG2P_YYYY-MM-DD.csv.gz` supply a release
version: `allG2P_2026-10-28.csv.gz` gives `import_run.source_version = "2026-10-28"`.
The parent directory is not used. Website downloads such as
`G2P_all_2026-11-11.csv` produce a log warning and a null version; their filename
date is not a release version. Other filenames also give a null version.
No manual version setting is needed. `-resume` reuses the previously downloaded
release; start a new run without it to select a newer release.

The parser keeps one report per G2P assertion, including disputed/refuted
confidence. It uses HGNC IDs to map genes and retains disease names/mappings,
allelic requirements, consequences, mechanisms and reviewed publications.
Gene coordinates and harmonisation fields remain unpopulated.

G2P rules live in `src/phenotypes/source_config.py`; shared ClinVar consumers
keep their own ClinVar-only config. Standalone Python parsing needs both local
files; downloading is handled by Nextflow.

## Loading parsed JSONL

Both sources use `phenotypes.loader`, which copies parser-prepared values into
the database without lookups, coordinate calculations or harmonisation. To
load an existing JSONL directly:

```bash
"$PIPELINE_DIR/.venv/bin/python" -m phenotypes.loader \
    --input /path/to/g2p_output.jsonl \
    --env-file /path/to/.env \
    --summary g2p_load_summary.json
```

The command also accepts ClinVar JSONL. Without `--summary`, it writes
`phenotypes_load_summary.json`. The same whole-source `--replace` policy and
transaction protection apply. A schema-wide lock prevents concurrent loads.

### Native HPC workaround

If Psycopg crashes because of native library conflicts, run this before Python
or Nextflow:

```bash
module load postgresql/16
export PSYCOPG_IMPL=python
export LD_LIBRARY_PATH="$(pg_config --libdir)${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
```

## Tests

From the repository root, run the phenotype Python tests in this consumer's
own environment:

```bash
uv sync --locked --project nextflow/phenotypes
nextflow/phenotypes/.venv/bin/python -m unittest discover -s nextflow/phenotypes/tests -v
```

These tests check phenotype associations, source identities, ontology identifiers
and parser-to-loader metadata compatibility. Shared source parsing and source
configuration have their own tests in `nextflow/shared/tests`.

The function tests in `tests/test_clinvar.py` select supplied RCV examples from
`shared/tests/data` for specific behaviours: FUT2 tests mixed disease/finding
TraitChoice grouping and OMIM filtering; CACNA1A and PTEN test assessments with
and without evaluation dates; PTEN also tests duplicate ontology references and
GeneReviews links. Expected phenotype results are loaded from
`tests/data/rcv_examples.expected.json`.

With Java 17+, Nextflow 26.04.0 and nf-test 0.9.5 available:

```bash
cd nextflow/phenotypes
nf-test test
```

The Nextflow suite checks the phenotype parser module, pipeline publishing and
the single-context loading guard. Parser integration uses a test configuration
that disables database loading; loader module tests check staging, output prefixes,
version reporting and protected-schema rejection without a database connection.
Full workflow stubs check loader outputs. These tests do not exercise a live
PostgreSQL server.

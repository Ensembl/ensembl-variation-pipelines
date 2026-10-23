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

This installs `variation-sources` from the adjacent `../shared` directory.
Keep both directories when copying the checkout. Downloads, variant evidence,
context validation and provenance are maintained in
[the shared components](../shared/README.md); phenotype transformations remain
in this pipeline.

The default `standard` profile runs tasks with this virtual environment on
`PATH`. For Slurm without a managed runtime, activate it before launching:

```bash
source .venv/bin/activate
nextflow run main.nf -profile slurm
```

Managed task runtimes are available through `-profile docker`,
`-profile singularity`, and `-profile conda`; combine with `slurm` for cluster
execution, for example `-profile slurm,singularity`. Configuration validation
still uses the pipeline's virtual environment, so run `uv sync` first with any
profile. Publishing and optional parser arguments are in `conf/modules.config`.

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

- By default, the pipeline downloads the current ClinVar RCV XML release from https://ftp.ncbi.nlm.nih.gov/pub/clinvar/xml/RCV_release/ClinVarRCVRelease_00-latest.xml.gz
Failed download attempts are retried three times
- To use a pre-downloaded file, set `sources.ClinVar.inputs.rcv` in the source configuration (`assets/source_config.json`) - a missing or null value triggers download
- Species and assembly also come from the `source_config.json` file
- ClinVar requires a human context and an assembly for its location filter, `assembly_uuid` is optional
- The default `source_config.json` used is the one in `/assets`, but an alternative can be provided by using:

```bash
nextflow run "$PIPELINE_DIR/main.nf" --source_config /path/to/source_config.json
```

- The intermediate JSONL is published to `<outdir>/clinvar/<species_production_name>/<assembly>/`
- The first JSONL line contains `metadata` shared across all records, subsequent lines are the records themselves

### Notes

The sections below contain some notes on how the parser handles the data

#### Multi-phenotype records

- The parser keeps each RCV's complete set of phenotypes together, without splitting multi-traits
- In some cases, a separator is used as it may help the user with interpretation later on, this is selected as follows:

| ClinVar grouping | Meaning | Example output label |
| --- | --- | --- |
| `TraitChoice` | Alternative conditions. | A OR B |
| `multipleConditionExplanation="Uncertain"` | One or several conditions may apply; OR does not mean exactly one. | A OR B |
| `multipleConditionExplanation="Novel disease"` | Features describe a proposed new disease; use a neutral separator. | Muscle weakness; Developmental delay |
| `multipleConditionExplanation="Co-occurring"` | Conditions are reported together; use a neutral separator. | A; B |
| `TraitChoice` with a non-empty explanation other than `Uncertain` | Set-level instructions differ; keep a neutral separator. | A; B |
| Missing, unknown or conflicting grouping | Keep the set without choosing AND or OR. | A; B |

The phenotype name uses the aggregate's grouping. For `RCV001834555.3`, the aggregate has TraitChoice, so its label becomes
`Hyperparathyroidism 4 OR Hypoparathyroidism, familial isolated, 2`.

#### Aggregate and submission assessments

- Submission assessment (associated with an SCV) is a submitter's classification of the pheno-variant relationship -- there can be multiple contributing to one pheno-variant relationship
- Aggregate assessment is ClinVar's summary of the >=1 submission assessments
- The parser keeps both the RCV aggregate and its current, supported SCV assessments

#### Germline, somatic and unspecified context

- The parser assigns a context to each assessment separately, including the aggregate and each SCV 
- It reads this from certain tags with the following rules:

| XML classification | Context |
| --- | --- |
| `GermlineClassification` | `germline` |
| `SomaticClinicalImpact` | `somatic` |
| `OncogenicityClassification` | `somatic` |
| `NoClassification` | `unspecified` |

- As per the ClinVar docs, historical somatic assessments can appear under the `GermlineClassification` tag, but it'd be wrong to classify these as germline, so the parser:
  - Checks that assertion's `ModeOfInheritance` values:
    - If it only contains `Somatic mutation`: use `somatic`
    - `Somatic mutation` plus another mode: use `unspecified` and output a warning
    - No `Somatic mutation`: keep `germline`
- Assessments with the same context are collected into one output record per RCV
- An `unspecified` assessment would go into a separate unspecified record
- For example, an RCV with a germline classification, somatic clinical-impact assessment and oncogenicity assessment produces two records:

```text
Same variant and complete condition set
├── germline record
│   └── germline assessment
└── somatic record
    ├── clinical-impact assessment
    └── oncogenicity assessment
```

#### Phenotype names and observed features

- The main phenotype name is the aggregate trait's preferred name, or its single available alternate name
- The whole RCV is rejected if the aggregate trait has neither
- Replace nonspecific labels such as `See cases`, `not provided` and `not specified` with `ClinVar: phenotype not specified`, keeping the original report label in `source_report.reported_phenotype_names`
- Treat `DrugResponse` names, such as `Ezetimibe response`, as phenotype names
- Use the assertion-level `TraitSet` for assessed conditions
- Clinical features under `ObservedIn/TraitSet` are currently not extracted or used as replacements. For example, `RCV000051340.4` is labelled `See cases` even though its SCV reports `Global developmental delay` as an observed feature. Its output uses the placeholder - it does not create a separate classified association for that feature
- Keep aggregate-trait six-digit OMIM condition references with `Type="MIM"` or no type; exclude allelic-variant references such as `613665.0004`

#### Assembly, locations and alleles

- The parser keep placements for the configured assembly only - it rejects the RCV if the variant, or any haplotype component, has no placement on that assembly
- Prefer sequence accessions beginning `NC_` when available
- Keep multiple qualifying placements separate, including X/Y placements - do not flatten them

#### Supported variant representations

- Retain structural variants and haplotypes
- Haplotype components keep their own identifiers and locations; the assessment applies to the complete haplotype, not independently to each component
- `Variant`, `Haplotype` and `Haplotype, single variant` MeasureSets are supported
- GenotypeSets, including diplotypes and compound heterozygotes, and other MeasureSet types are skipped.

#### Cleaning and provenance

- Write valid assessment dates as `YYYY-MM-DD`
- Remove recognised DOI prefixes and formatting spaces
- Invalid supplied dates, numbers and selected DOIs become null with a warning; the remaining record is kept
- Do not substitute a URL for an invalid DOI or look up missing values

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
| `invalid_numeric_attribute` | A supplied coordinate or other numeric location attribute is not a valid integer. That value becomes null; the rest of the record is retained. |
| `invalid_last_evaluated_date` | The assessment date cannot be read as a valid supported date. It becomes null; the assessment remains. |
| `invalid_doi` | The selected DOI cannot be recognised after cleaning. The publication remains with a null identifier; a URL is not substituted for the invalid DOI. |

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

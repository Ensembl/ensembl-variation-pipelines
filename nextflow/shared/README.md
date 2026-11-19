# Shared variation source components

Reusable Nextflow DSL2 modules and a dependency-free Python package for ClinVar
source extraction. Phenotypes consumes the common code; synonyms can use the
same extractor before applying its own mapping and staging rules.

These are repository-local modules following current
[nf-core component principles](https://nf-co.re/docs/specifications/components/modules/general):
tool/subtool directories, explicit inputs, metadata tuples, named outputs,
`ext.args`, `ext.prefix`, `ext.when`, resource labels, pinned runtime declarations,
version topics, module metadata, stubs, and nf-test coverage. They are not
published nf-core modules or a certified nf-core pipeline. The custom Python
source is staged explicitly, rather than relying on a published BioConda package.

## Components

| Component | Contract |
| --- | --- |
| `modules/local/clinvar/download` | Input `[meta, release_type, url]`; output `xml: [meta, release_type, xml_gz]` |
| `workflows/clinvar_inputs` | Input settings map with `inputs.rcv` and `inputs.vcv`; outputs `rcv_xml` and `vcv_xml` as reusable value channels |
| `modules/local/clinvar/extract` | Input `[meta, rcv_xml, genome_context]`, shared source directory, and source contents manifest; outputs `records`, `summary`, and `rejections`, each `[meta, file]` |
| `lib/source_files.nf` | Build a sorted Python filename/content manifest so nested source edits invalidate the task cache |

The single-module input resolver is kept under `workflows/`; nf-core
[subworkflows](https://nf-co.re/docs/specifications/components/subworkflows/general)
require at least two modules.

Modules use only `meta.id`; other caller metadata passes through unchanged.
Genome context is a separate required value in the same tuple as its files,
so contexts cannot become mismatched by independent queue consumption.
Output names default to `meta.id` and can be changed with `ext.prefix`.
Publishing and optional command arguments belong in the consumer's
`conf/modules.config`. Resource policies can include `conf/base.config` and
override its labels as appropriate. Versions are emitted as process/tool/version
tuples to the `versions` topic, following the current nf-core convention.

`CLINVAR_INPUTS` downloads each missing release once within a run. Call
`DOWNLOAD_CLINVAR` directly when only one release type is needed. Input settings
must first pass the shared configuration loader (`sources.ClinVar`, which also
sets `source_name` from that key): supplied paths are literal
files resolved against the configuration directory; wildcards are rejected.
The default URLs use `latest`; use local, versioned input files for reproducible
releases. Sharing code does not itself share completed work between independently
launched pipelines.

## Run download and extraction

The shared runner uses `assets/source_config.json` in this directory and the
shared Python environment. From the repository root:

```bash
uv sync --locked --project nextflow/shared
nextflow run nextflow/shared --outdir results/shared
```

With both ClinVar inputs null, the default run downloads the latest RCV
and VCV releases. Downloads are copied to `results/shared/clinvar/inputs/`.
The runner prints the resolved input paths and calls `EXTRACT_CLINVAR` once per
configured genome context. RCV evidence is written under
`results/shared/clinvar/evidence/<species_production_name>/<assembly>/`:

- `clinvar.jsonl`: metadata and source variant evidence.
- `clinvar.summary.json`: extraction counts and rejection reasons.
- `rejections.jsonl`: unsupported records and missing assembly placements.

The VCV release is prepared alongside RCV but is not parsed by this extractor.
Phenotype transformation and synonym mapping remain consumer steps.
The default local profile puts the shared virtual environment on task `PATH`.
Local execution also requires `curl` on `PATH`.

To use existing files, copy the shared asset, set `sources.ClinVar.inputs.rcv`
and `sources.ClinVar.inputs.vcv` to literal paths, and pass your configuration:

```bash
nextflow run nextflow/shared --source_config /path/to/source_config.json
```

Relative input paths resolve against that JSON file's directory. Existing input
files are used directly; extraction outputs and any downloaded files are
published. For a small local fixture run without downloading anything:

```bash
nextflow run nextflow/shared \
    --source_config nextflow/shared/tests/data/source_config.json \
    --outdir results/shared-test
```

The shared reader defaults to `assets/source_config.json` and supports ClinVar.
ClinVar requires human species and an assembly. Null inputs allow downloads;
supplied paths must exist. Duplicate species/assembly contexts are rejected.

Consumers pass their own `source_rules` to
`variation_sources.source_config.load_source_config`: a mapping of source names
to `(input names, context validator)`. Phenotypes owns G2P validation in
`phenotypes.source_config`; see its [README](../phenotypes/README.md).

Nextflow callers can import `read_source_config` from `lib/source_config.nf`,
passing the config path and Python executable. An optional third argument
selects the config module (default: `variation_sources.source_config`).
The reusable `CLINVAR_INPUTS` workflow receives validated settings explicitly
and does not read any pipeline's assets itself.

## Reuse from synonyms

From `nextflow/synonyms/workflows/clinvar.nf`, import:

```nextflow
include { CLINVAR_INPUTS } from '../../shared/workflows/clinvar_inputs/main'
include { EXTRACT_CLINVAR } from '../../shared/modules/local/clinvar/extract/main'
include { source_contents } from '../../shared/lib/source_files'
```

Inside the consumer workflow, assuming validated `source_settings`:

```nextflow
CLINVAR_INPUTS(source_settings)
shared_source = file("${projectDir}/../shared/src", checkIfExists: true)
extraction_inputs = channel.fromList(source_settings.contexts)
    .combine(CLINVAR_INPUTS.out.rcv_xml)
    .map { context, xml ->
        tuple([id: "clinvar_${context.assembly}".toString()], xml, context)
    }
EXTRACT_CLINVAR(extraction_inputs, shared_source, source_contents(shared_source))
```

Consume `EXTRACT_CLINVAR.out.records` through a synonym-specific adapter before
loading the synonyms datastore. The extraction JSONL is not a canonical synonym
table or a Parquet loader. Canonical variant matching, somatic-status policy,
reconciliation, Parquet staging and datastore loading remain consumer work.

## Python package and evidence

`variation_sources.clinvar.xml` owns bounded-memory XML streaming;
`variation_sources.clinvar.variants` owns common variant/location/allele parsing;
`variation_sources.clinvar.vcv` maps direct VCV alleles to their own component accessions;
`variation_sources.context` validates genome and ClinVar contexts;
`variation_sources.source_config` validates configs and file paths; defaults to ClinVar;
`variation_sources.provenance` owns input hashing and Git revision lookup.
Phenotypes depends on this local package through its `pyproject.toml` and lockfile.
Other Python consumers can install this directory or declare the same local dependency.

`python -m variation_sources.clinvar.extract` streams RCV XML/XML.GZ and writes a
JSONL metadata header followed by variant evidence. It retains release date,
input checksum, source statuses, RCV/VCV identifiers, allele IDs, reported HGVS,
dbSNP/dbVar references, requested-assembly placements, genes, haplotype components
and SCV accessions. It applies no phenotype or classification eligibility filter.
It does not generate HGVS/SPDI, lift coordinates, resolve dbSNP merge chains,
or establish biological equivalence. Non-current statuses are retained as
evidence for downstream policy. Unsupported representations and absent assembly
placements are reported in `rejections.jsonl`, with counts in the summary.

Each location includes nullable `stop_vcf`, calculated as
`position_vcf + length(reference_allele_vcf) - 1` for a positive VCF position
and complete literal REF/ALT sequences (A/C/G/T/N, case-insensitive).
Source start/stop, alleles and length remain unchanged. Missing or unsupported
VCF representations leave `stop_vcf` null; the record is retained.

A haplotype parent's `locations` may contain a calculated interval: minimum
component `position_vcf` to maximum component `stop_vcf`, stored as parent
`start`/`stop`. This requires one complete VCF interval per component on the
same assembly, chromosome and reference sequence. Parent VCF position and
alleles remain null. Components are unchanged; no origin field is added.
Different chromosomes produce `haplotype_span_different_chromosomes`. Missing,
ambiguous or incompatible intervals leave parent locations empty without
rejecting an otherwise supported record.

The supported MeasureSet types remain `Variant`, `Haplotype`, and
`Haplotype, single variant`; GenotypeSet is explicitly rejected. The inherited
placement policy prefers primary RefSeq `NC_` locations when present. Haplotype
component identifiers remain attached to their components and are never promoted
to parent synonyms. Assembly UUID may be null; ClinVar requires an assembly label.
RCV extraction alone does not cover VCV-only variants; add a VCV evidence adapter
before claiming complete ClinVar synonym coverage.

## Database configuration

Consumers import database configuration directly from
`variation_sources.database`:

- `database_config(section, env_file)` reads INI settings and validates the
  required connection fields and port.
- `postgresql_connection_args(settings)` maps validated settings to driver
  arguments without importing a database driver.
- `postgresql_environment(env_file)` builds string-valued PostgreSQL environment
  variables for tools such as `psql` and requires an explicit schema.

Copy `nextflow/shared/assets/.env.example` into the run directory and fill in
the connection details and consumer schema. Both the phenotype loader and its
schema helper use these shared functions. A blank password is omitted from
connection arguments and environment overrides, allowing libpq authentication.
SQL schemas, driver dependencies, transactions and loading policies remain with
each consumer. The shared download-and-extract runner does not read this file or
require database settings.

## Tests

Shared tests cover ClinVar streaming, variant identities, assembly selection,
source evidence and source configuration validation. They run in
the shared environment independently of any consumer:

```bash
uv sync --locked --project nextflow/shared
nextflow/shared/.venv/bin/python -m unittest discover -s nextflow/shared/tests -v
```

`tests/data` contains nine supplied RCV XML examples with release date
`2026-09-28`, preserved unchanged. Tests select fixtures for specific behaviours:
the insertion record checks GRCh37/GRCh38 placements, and a substitution plus
the insertion check VCF alleles. Existing synthetic fixtures cover streaming
and identifier edge cases. Expected variant results are loaded from
`tests/data/rcv_examples.expected.json`. No Downloads directory or network
access is needed to run these tests.

With Java 17+, Nextflow 26.04.0 and nf-test 0.9.5 available, run the shared
module, input-workflow and download/extraction runner tests:

```bash
cd nextflow/shared
nf-test test
```

The module tests check explicit inputs, outputs, version topics and stubs. The
runner checks published evidence for multiple assembly contexts. Synthetic XML
and local file URLs keep these tests independent of external ClinVar releases
and database services. Container and Conda execution are not exercised.

Phenotype behavior and loading policies are tested in the
[phenotype suite](../phenotypes/README.md#tests). That suite reuses the small raw
XML fixtures from `shared/tests/data`; its configurations and database fixtures
belong to `phenotypes/tests/data`.

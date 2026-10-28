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

## Run input preparation independently

The shared runner uses `assets/source_config.json` in this directory and the
shared Python environment. From the repository root:

```bash
uv sync --locked --project nextflow/shared
nextflow run nextflow/shared --outdir results/shared
```

The default asset sets both inputs to null, so this downloads the latest RCV
and VCV releases. Downloads are copied to `results/shared/clinvar/inputs/`.
The runner prints the resolved paths and prepares inputs only; it does not run
phenotype or synonym parsing. Local execution requires `curl` on `PATH`.

To use existing files, copy the shared asset, set `sources.ClinVar.inputs.rcv`
and `sources.ClinVar.inputs.vcv` to literal paths, and pass your configuration:

```bash
nextflow run nextflow/shared --source_config /path/to/source_config.json
```

Relative input paths resolve against that JSON file's directory. Existing input
files are used directly; only downloaded files are published. For a small local
fixture run without downloading anything:

```bash
nextflow run nextflow/shared \
    --source_config nextflow/shared/tests/data/source_config.json
```

Both the shared runner and phenotypes default to the single asset at
`nextflow/shared/assets/source_config.json`, selected by the common Nextflow
include `conf/source.config`. Standalone Python parsing uses
that same default. Consumers import the loader, default path and ClinVar context
validator directly from `variation_sources.source_config`; there is no phenotype
configuration wrapper. The common asset retains the phenotype
pipeline's GRCh38 assembly UUID. Run-specific settings can still be supplied
with `--source_config`; relative input paths resolve against that file.
All callers use the same shared Python validator;
Nextflow callers can import `read_source_config` from `lib/source_config.nf`,
passing a configuration path and their environment's Python executable.
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
`variation_sources.context` owns context validation;
`variation_sources.source_config` owns configuration and literal input validation;
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

The supported MeasureSet types remain `Variant`, `Haplotype`, and
`Haplotype, single variant`; GenotypeSet is explicitly rejected. The inherited
placement policy prefers primary RefSeq `NC_` locations when present. Haplotype
component identifiers remain attached to their components and are never promoted
to parent synonyms. Assembly UUID may be null; ClinVar requires an assembly label.
RCV extraction alone does not cover VCV-only variants; add a VCV evidence adapter
before claiming complete ClinVar synonym coverage.

## Validation

The shared package has its own environment and lockfile. From the repository
root, create `nextflow/shared/.venv` and run the shared ClinVar unit tests:

```bash
uv sync --locked --project nextflow/shared
nextflow/shared/.venv/bin/python -m unittest discover \
    -s nextflow/shared/tests -p 'test_clinvar*.py' -v
```

This installs only `variation-sources`; phenotypes is not needed. The package
requires Python 3.12 or later. Use Python 3.14 for the Nextflow module tests to
match their declared runtime.

`test_clinvar.py` covers extraction, configuration, statuses, assembly rejection
and VCV component identity. `test_clinvar_sources.py` covers XML streaming and
cleanup, identifier namespaces, assembly and scaffold selection, malformed
coordinates, allele ambiguity, rejection accounting, empty releases, and keeping
each haplotype member's HGVS evidence attached to its own allele. These contracts
are the starting point for a synonyms adapter. The existing extractor reads RCV
evidence; full VCV-only variant extraction remains future work.

With Java 17+, Nextflow 26.04.0 and nf-test 0.9.5 on `PATH`, run the nine shared
module and input-workflow tests independently of phenotypes:

```bash
export PATH="$PWD/nextflow/shared/.venv/bin:$PATH"
cd nextflow/shared
nf-test test modules/local/clinvar tests/clinvar_inputs.nf.test tests/shared_runner.nf.test
```

The phenotype integration tests use the consumer's separate environment.
The parser adopts the base branch's source-report/entity model and emits records,
warnings and rejections; the consumer loader now emits `clinvar_load_summary.json`
in place of a parser summary. A local adapter renders shared variant evidence
in that model without changing the shared extractor's variant/allele contract.
`genome_uuid` is now retained by shared context validation, alongside the existing
nullable `assembly_uuid`.

The database consumer accepts one configured context per source snapshot, as in
the base branch. Shared evidence extraction still supports multiple contexts.
Pipeline tests disable or stub database writes and use a dummy `.env` fixture;
the loader metadata preflight is also tested without a database connection.
From the repository root:

```bash
uv sync --locked --project nextflow/phenotypes
nextflow/phenotypes/.venv/bin/python -m unittest discover \
    -s nextflow/shared/tests -p 'test_phenotype_regression.py' -v
export PATH="$PWD/nextflow/phenotypes/.venv/bin:$PATH"
cd nextflow/shared
nf-test test tests/phenotype_module.nf.test tests/phenotypes.nf.test
```

The regression compares all three phenotype output files against the upstream
baseline. To run all Python tests together, use the phenotype environment with
`-p 'test_*.py'`. To run all fifteen Nextflow tests together, use `nf-test test`
after setting up the phenotype environment.

Tests use tiny synthetic XML files and no external ClinVar downloads. They cover
real and stub module execution, local/missing inputs, literal-path validation,
multiple assembly contexts, phenotype outputs matching the rebased upstream schema, gzip input, source
statuses, release headers, genotype rejection and haplotype relationships.
CI runs the same suites. Containers and Conda runtimes are declared but are not
exercised by the local-runtime test suite.

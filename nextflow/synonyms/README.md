# Variant and allele synonym evidence

This module extracts reported variant and allele synonyms from ClinVar, dbSNP
human ClinVar, dbSNP with separate internal variant and allele
identities. 

This consumer extracts reported ClinVar variant synonyms with their sources.
It reads RCV XML directly through the shared in-memory parser:

```text
CLINVAR_INPUTS → EXTRACT_CLINVAR_SYNONYMS
```

The RCV XML is streamed once per configured genome context. No intermediate
evidence JSONL is written or reused.
## Run

From the repository root:

```bash
uv sync --locked --project nextflow/synonyms
nextflow run nextflow/synonyms \
    -profile standard \
    --source_config /absolute/path/to/source_config.json \
    --outdir "$PWD/results/synonyms"
```

The default source configuration is `../shared/assets/source_config.json`.
The same `--source_config` format is used by shared extraction and phenotypes.
Set `sources.ClinVar.inputs.rcv` and `.vcv` to fixed local release files to keep
consumers on the same source snapshot.This
consumer currently extracts records from RCV, not VCV-only records.

Use `-profile slurm` for cluster execution. The project virtual environment
must be accessible to compute nodes. The synonym module also declares Conda
and container environments; the pipeline supports `conda`, `docker` and
`singularity` profiles. Modules stage Python sources explicitly and include
their filenames and contents in task cache keys.

The synonym extraction process has caching disabled. Every invocation reads
and regenerates synonyms from the complete RCV input, including when using
`-resume`; input download tasks can still be reused.

To run the Python extractor directly:

```bash
uv run --project nextflow/synonyms python -m synonyms.clinvar.extract \
    --rcv-input /absolute/path/to/ClinVarRCVRelease.xml.gz \
    --genome-context '{"species_taxonomy_id":9606,"species_production_name":"homo_sapiens","assembly":"GRCh38","assembly_uuid":null}' \
    --records "$PWD/synonyms/clinvar.synonyms.jsonl" \
    --summary "$PWD/synonyms/clinvar.summary.json"
```

## Output

Each context is published under:

```text
<outdir>/clinvar/<species>/<assembly>/
  synonyms/clinvar.synonyms.jsonl
  synonyms/clinvar.summary.json
  synonyms/rejections.jsonl
```



- `variant_identifier`: an external source identifier, such as a versioned VCV
  accession or shared component allele identifier; never an internal variant ID.
- `variant_type`, `allele_id`, `locations`: reported entity and requested-context
  placements.
- `parent_variant_identifier`, `component_ordinal`: component ownership; null
  for a top-level variant or haplotype.
- `synonyms`: objects with `value`, `type`, `source`, `reported_type` and
  `resolution`. Each resolution has `state: source_only`, `variant_id: null`
  and `variant_allele_id: null`. Matching has not run; do not interpret these
  records as failed matches or resolved variant/allele associations.
- `source_report`: reporting source, source release/version, versioned RCV
  accession, record/assertion statuses, SCV accessions/statuses and upstream warnings.

Reported synonyms include:

| Type | Source | Values |
|---|---|---|
| `HGVS` | ClinVar | Reported HGVS attributes, with their annotation types |
| `SPDI` | ClinVar | Reported `CanonicalSPDI` values |
| `dbSNP` | dbSNP | Reported rsIDs, such as `rs121908247` |
| `name` | ClinVar | Preferred and alternate variant names |
| `ClinVar` | ClinVar | Source variant and allele identifiers, plus the reference assertion's versioned RCV accession |
| `OMIM` | OMIM | IDs from direct `Measure/XRef` elements with `DB="OMIM"` |
| `ClinGen` | ClinGen | IDs from `MeasureSet/XRef`, `Measure/XRef`, `Measure/Name/XRef` or `Measure/AttributeSet/XRef` with `DB="ClinGen"` |
| `dbVar` | dbVar | Reported dbVar cross-references, when present |


## Tests

```bash
nextflow/synonyms/.venv/bin/python -m unittest discover -s nextflow/synonyms/tests -v
cd nextflow/synonyms
nf-test test
```



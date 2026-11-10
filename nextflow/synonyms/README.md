# Variant and allele synonym evidence

The target store follows the draft [feature brief](https://app.notion.com/p/769d85c5b7934e4ab15affb3aaa73d8d):
human ClinVar, dbSNP and PharmGKB, with separate internal variant and allele
identities. UniProt and structural-variant alias support are outside the initial
release. The current implementation is the ClinVar evidence adapter described
below; it does not implement the complete store. See the
[contract review and implementation gaps](docs/feature-contract.md).

This consumer extracts reported ClinVar variant synonyms with their sources.
It reuses shared XML extraction and reads the resulting evidence JSONL:

```text
CLINVAR_INPUTS → EXTRACT_CLINVAR → EXTRACT_CLINVAR_SYNONYMS
```

The RCV XML is streamed once per configured genome context. The synonym step
does not parse XML or depend on phenotype eligibility. Extraction produces
files only; canonical variant reconciliation and database loading are separate
consumer tasks.

## Run

From the repository root:

```bash
uv sync --locked --project nextflow/synonyms
nextflow run nextflow/synonyms \
    --source_config /absolute/path/to/source_config.json \
    --outdir "$PWD/results/synonyms"
```

The default source configuration is `../shared/assets/source_config.json`.
The same `--source_config` format is used by shared extraction and phenotypes.
Set `sources.ClinVar.inputs.rcv` and `.vcv` to fixed local release files to keep
consumers on the same source snapshot. Relative paths resolve against the
configuration directory. The shared input resolver resolves both files; this
consumer currently extracts records from RCV, not VCV-only records.

Use `-profile slurm` for cluster execution. The project virtual environment
must be accessible to compute nodes. The synonym module also declares Conda
and container environments; the pipeline supports `conda`, `docker` and
`singularity` profiles. Modules stage Python sources explicitly and include
their filenames and contents in task cache keys.

To transform previously extracted shared evidence directly:

```bash
uv run --project nextflow/synonyms python -m synonyms.clinvar.extract \
    --evidence /absolute/path/to/clinvar.jsonl \
    --records "$PWD/synonyms/clinvar.synonyms.jsonl" \
    --summary "$PWD/synonyms/clinvar.summary.json"
```

Re-extract older evidence files to obtain reported names and SPDI: the consumer
accepts their absence but cannot recover information omitted by an older
extractor.

## Output

Each context is published under:

```text
<outdir>/clinvar/<species>/<assembly>/
  evidence/clinvar.jsonl
  evidence/clinvar.summary.json
  evidence/rejections.jsonl
  synonyms/clinvar.synonyms.jsonl
  synonyms/clinvar.summary.json
  synonyms/rejections.jsonl
```

The synonym JSONL starts with a `metadata` header containing the source release,
genome context, original input filenames/checksums, evidence file checksum,
consumer version and pipeline revision. `artifact_kind` is `source_evidence`;
`synonym_schema_version` versions the consumer contract independently of the
inherited shared `schema_version`. Each subsequent record describes one
source variant or haplotype component:

- `variant_identifier`: an external source identifier, such as a versioned VCV
  accession or shared component allele identifier; never an internal variant ID.
- `variant_type`, `allele_id`, `locations`: reported entity and requested-context
  placements.
- `parent_variant_identifier`, `component_ordinal`: component ownership; null
  for a top-level variant or haplotype.
- `synonyms`: objects with `value`, `type`, `source`, `reported_types` and
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
| `ClinVar` | ClinVar | Source variant and allele identifiers |
| `dbVar` | dbVar | Reported dbVar cross-references, when present |

The enclosing `source_report.source` identifies ClinVar as the evidence
provider even for a dbSNP cross-reference. RCV and SCV accessions are provenance,
not variant synonyms. Equal value/type/source triples are deduplicated within
each entity, retaining distinct annotation types. Observations from different
RCVs remain separate, preserving their provenance.

The source namespace in a synonym and the provider in `source_report` serve
different purposes: an rsID reported by ClinVar retains the `dbSNP` namespace,
while its evidence release is ClinVar's release, not an inferred dbSNP build.
The summary counts emitted aliases by resolution state under
`synonym_resolution_states`.

HGVS and SPDI are retained as reported, including transcript or assembly
accessions that differ from the requested placement context. They are not
generated, normalised, lifted or asserted to be equivalent. Missing optional
representations produce no placeholder synonyms. Haplotype member HGVS, SPDI
and rsIDs stay on the member and are never assigned to the parent.

Non-current source statuses are retained for downstream policy. Unsupported
XML representations are reported by shared extraction; unusable identifiers or
component ownership are reported by the synonym step with the original evidence
record and release retained for review and reprocessing. This is source evidence,
not a canonical synonym table or a complete catalogue of VCV-only variants.

## Tests

```bash
nextflow/synonyms/.venv/bin/python -m unittest discover -s nextflow/synonyms/tests -v
cd nextflow/synonyms
nf-test test
```

Python tests target source attribution, optional representations, deduplication,
component ownership, phenotype-independent evidence and rejected-record
continuation. A supplied CACNA1A RCV record tests reported HGVS, SPDI and rsIDs.
Tests also check source-only resolution, null internal IDs, release provenance,
retention of historical/overlapping assertions, and recoverable rejected evidence.
Nextflow tests cover module output prefixes, metadata, versions, stubs and
end-to-end extraction for GRCh37 and GRCh38 using local fixtures. No database
or live ClinVar download is required.

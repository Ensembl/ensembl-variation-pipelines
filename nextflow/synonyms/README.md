# Variant and allele synonym evidence

The pipeline streams ClinVar VCV XML once per genome context and writes
variant and allele synonyms directly.

```text
CLINVAR_INPUTS → EXTRACT_CLINVAR_SYNONYMS
```

## Run

From the repository root:

```bash
uv sync --locked --project nextflow/synonyms
nextflow run nextflow/synonyms \
    -profile standard \
    --source_config /absolute/path/to/source_config.json \
    --outdir "$PWD/results/synonyms"
```

The default source configuration is `assets/source_config.json`.
The same `--source_config` format is used by shared extraction and phenotypes.
Set `sources.ClinVar.inputs.vcv` to a local VCV XML or XML.GZ file to pin the
source snapshot. Leave it null to download the latest VCV release. The synonyms
pipeline uses only the VCV input.

Use `-profile slurm` for cluster execution. The project virtual environment
must be accessible to compute nodes. The synonym module also declares Conda
and container environments; the pipeline supports `conda`, `docker` and
`singularity` profiles. Modules stage Python sources explicitly and include
their filenames and contents in task cache keys.

The synonym extraction process has caching disabled. Every invocation reads
and regenerates synonyms from the complete VCV input, including when using
`-resume`; input download tasks can still be reused.

To run the Python extractor directly:

```bash
uv run --project nextflow/synonyms python -m synonyms.clinvar.extract \
    --vcv-input /absolute/path/to/ClinVarVCVRelease.xml.gz \
    --genome-context '{"species_taxonomy_id":9606,"species_production_name":"homo_sapiens","assembly":"GRCh38","assembly_uuid":null}' \
    --variant-records "$PWD/synonyms/clinvar.variant_synonyms.jsonl" \
    --allele-records "$PWD/synonyms/clinvar.variant_allele_synonyms.jsonl" \
    --summary "$PWD/synonyms/clinvar.summary.json"
```

## Output

Each context is published under:

```text
<outdir>/clinvar/<species>/<assembly>/
  synonyms/clinvar.variant_synonyms.jsonl
  synonyms/clinvar.variant_allele_synonyms.jsonl
  synonyms/clinvar.summary.json
  synonyms/rejections.jsonl
```

The source report carries the VCV accession, numeric Variation ID, and all
versioned RCV accessions listed by that VCV record. The singular
`rcv_accession` field is populated when the list contains exactly one accession.
Included VCV records can contribute synonyms without an RCV accession.
Genotype aliases are attached to the genotype record, and member synonyms
link to their genotype or nested haplotype. When an allele has no placement
on the requested assembly, its reported aliases are retained with null REF
and ALT and a placement warning. The summary counts VCV archives, emitted
variant and allele evidence, and rejections.

## Tests

```bash
nextflow/synonyms/.venv/bin/python -m unittest discover -s nextflow/synonyms/tests -v
cd nextflow/synonyms
nf-test test
```



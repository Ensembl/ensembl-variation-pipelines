# Create MaveDB plugin data

This Nextflow pipeline prepares indexed variant scores for the VEP
[MaveDB plugin](https://github.com/Ensembl/VEP_plugins/blob/main/MaveDB.pm).
It lifts genomic mappings to GRCh38 and uses Variant Recoder for protein mappings.

## Requirements

- Nextflow with the legacy parser (`NXF_SYNTAX_PARSER=v1` for Nextflow 26.04).
- Singularity and the configured containers.
- Python, pandas, bgzip/tabix, Bash and GNU tools for host-side tasks.
- Ensembl VEP/Perl APIs and database access for Variant Recoder.

See [nextflow.config](nextflow.config) for execution profiles and
[pipeline_transfer.md](pipeline_transfer.md) for dependencies and test coverage.
Variant Recoder can require substantial memory and database connections.

## Run

Use a text file with one MaveDB URN per line. Prefer a local MaveDB data export
containing score CSVs, mapping JSONs and the combined metadata file.

Run from the repository root, replacing the input paths:

```bash
nextflow run nextflow/MaveDB/main.nf \
  -profile slurm \
  --urn urns.txt \
  --from_files true \
  --scores_path data/scores \
  --mappings_path data/mappings \
  --metadata_file data/main.json
```

Set `$ENSEMBL_ROOT_DIR` to the parent of the VEP installation. Supply `--registry`
if a specific Ensembl database registry is needed. Alternatively, use `--from_files false` to download inputs from the MaveDB API.
This mode may be less reliable.

## Options

| Option | Purpose |
| --- | --- |
| `--urn` | Required file listing the URNs to process |
| `--ensembl` | Parent of the VEP installation; defaults to `$ENSEMBL_ROOT_DIR` |
| `--registry` | Optional Variant Recoder registry |
| `--output` | Output filename; defaults to `output/MaveDB_variants.tsv.gz` |
| `--from_files` | Use local data; defaults to `true` |
| `--scores_path`, `--mappings_path`, `--metadata_file` | Local input locations |
| `--licences` | Accepted licences, comma-separated; defaults to `CC0` |
| `--round` | Decimal places for scores; defaults to `4` |
| `--previous_urn` | URNs to skip because they were processed before |
| `--previous_output` | Previous compressed results to retain for skipped URNs |

Provide both previous-input options to reuse results. Only requested URNs are
retained. Without `--previous_output`, skipped URNs are omitted from the output.

## Outputs

The pipeline filters by licence, maps scores to genomic variants, then merges,
sorts, compresses and indexes the results. It writes a `.tsv.gz` file and a `.tbi`
index. Reports include logs and lists of missing URNs; check these before using
the output.

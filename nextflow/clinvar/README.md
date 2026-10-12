# clinvar

Builds the enriched ClinVar VCFs that web VEP annotates with, one per assembly,
from each monthly ClinVar release.

- Adds submission-level detail from the VCV and RCV XML releases as `CLNSUBA`,
  `CLNPMID`, `CLNSUBN` and `CLNRCV`.
- Resolves each `GENEINFO` gene to the Ensembl gene IDs of the gene set VEP
  annotates with, written as `GENEENSG`.
- Web VEP reads the result from `<publish_dir>/<grch38|grch37>/clinvar.vcf.gz`.

## Flow

| Step | Process | Does |
|---|---|---|
| 1 | `RESOLVE_RELEASE` | Picks the newest release, or `month`, and stops if it is already published or not yet complete |
| 2 | `FETCH_FILE` | Downloads remote inputs and checks their md5 or Ensembl `CHECKSUMS` |
| 3 | `BUILD_GENE_MAP` | Maps GFF gene names and NCBI gene IDs to Ensembl gene IDs, cached per assembly and Ensembl release |
| 4 | `BUILD_DB` | Loads the VCV and RCV XML into `clinvar_<YYYYMMDD>.sqlite` and builds the annotation table |
| 5 | `ANNOTATE` | Writes the enriched VCF per assembly, bgzipped and tabix-indexed |
| 6 | `CHECK` | Runs the gates below |
| 7 | `PUBLISH` | Publishes the release and repoints the `clinvar.vcf.gz` links |

A run with nothing new to do logs the release status and exits 0, so a daily
cron is safe.

## GENEENSG

Each `GENEINFO` entry `NAME:NCBIID` resolves to Ensembl gene IDs in order.

1. If `NAME` is a gene name in the GFF, take those Ensembl IDs.
2. Otherwise take the GFF genes that Ensembl xrefs to `NCBIID`, skipping any
   gene that carries more than one NCBI ID.

- Unique IDs are joined with `+` in `GENEINFO` order.
- A record with no resolved gene has no `GENEENSG`.
- A record gets `GENEENSG` whether or not it has an XML row.

| Assembly | NCBI IDs from |
|---|---|
| GRCh38 | `Homo_sapiens.GRCh38.<release>.entrez.tsv.gz` |
| GRCh37 | `homo_sapiens_core_<release>_37` dump tables |

## Requirements

- Nextflow 24.04.3 or later
- Python 3.10+ with `requirements.txt` installed
- `bgzip` and `tabix` (htslib) on `PATH`

```bash
python3 -m pip install -r requirements.txt
```

## Local run

Copy `assets/local.params.example.yaml`, fill in the paths and pass it with
`-params-file`.

```bash
nextflow run main.nf -profile standard -params-file local.params.yaml
```

- Leave an input unset to fetch it from NCBI or Ensembl.
- A local ClinVar VCF's `##fileDate` must match the XML release date unless
  `allow_date_mismatch` is set.

## Codon run (not live yet)

Copy `assets/codon.params.example.yaml` outside the repo and fill it in. Web VEP
reads `$VEP_DATA_DIR/vep_plugins_data/grch3x/clinvar.vcf.gz`, so `publish_dir`
is `<VEP_DATA_DIR>/vep_plugins_data`.

1. `cron/crontab.example` runs `cron/run_clinvar.sh` daily as `ensvar`.
2. The script submits `cron/clinvar_head.sbatch` unless a head job is already
   queued or running.
3. The head job runs Nextflow with `-profile slurm`. It is itself a SLURM job
   because its launch and work directories are under `CLINVAR_RUN_DIR` on
   `/hps`, which only SLURM jobs can write.

| Variable | Holds |
|---|---|
| `CLINVAR_PARAMS` | Params file |
| `CLINVAR_RUN_DIR` | Launch and work directory on `/hps` |
| `CLINVAR_ENV` | Optional file sourced first, for modules, the Python env and htslib |

## Params

| Param | Default | Meaning |
|---|---|---|
| `publish_dir` | required | Output root |
| `gff_grch38`, `gff_grch37` | required per assembly | GFF VEP annotates with |
| `assemblies` | `GRCh38,GRCh37` | Assemblies to build |
| `ensembl_release` | `116` | Ensembl release of the GFF and NCBI mapping |
| `month` | newest | Release month, `YYYY-MM` |
| `clinvar_base_url` | NCBI FTP | ClinVar download root |
| `vcv_xml`, `rcv_xml` | fetched | Local XML releases, given together |
| `vcf_grch38`, `vcf_grch37` | fetched | Local ClinVar VCFs |
| `entrez_tsv_grch38` | fetched | Local Ensembl entrez TSV |
| `core_dump_dir_grch37` | fetched | Local directory holding `gene`, `object_xref`, `xref` and `external_db` `.txt.gz` |
| `allow_date_mismatch` | `false` | Accept a local VCF whose `##fileDate` differs from the XML date |
| `force` | `false` | Rebuild a published release, and replace regular files at the link paths |
| `cache_dir` | `<publish_dir>/cache` | Gene map cache |
| `work_dir` | `work` in the launch directory | Nextflow work directory for intermediate files, about 24 GB on a full run. `-work-dir` takes precedence |
| `keep_releases` | `3` | Releases kept per assembly |
| `max_problem_fraction` | `0.01` | Largest ratio of validation problems to records that `validate.py` accepts |
| `min_clnsuba_fraction` | `0.99` | Smallest share of records carrying `CLNSUBA` that `validate.py` accepts |
| `queue`, `fetch_queue` | `production`, `datamover` | SLURM partitions; downloads use `fetch_queue` |

The gene map cache is keyed by assembly and Ensembl release only. Delete the
cached map after changing a GFF within a release.

## Outputs

```
<publish_dir>/
  grch38/
    clinvar.vcf.gz -> releases/clinvar_<YYYYMMDD>.vcf.gz
    clinvar.vcf.gz.tbi -> releases/clinvar_<YYYYMMDD>.vcf.gz.tbi
    CURRENT
    releases/
      clinvar_<YYYYMMDD>.vcf.gz
      clinvar_<YYYYMMDD>.vcf.gz.tbi
      clinvar_<YYYYMMDD>.vcf.gz.provenance.json
  grch37/
    ...
  cache/gene_maps/<assembly>_<release>/gene_map.json
```

- `CURRENT` holds the published release date.
- The provenance file records the input sources and md5s, the gene map
  provenance, annotation stats, gate and validation results, pipeline commit,
  Nextflow version and start time.
- Links are relative.
- Each link is replaced atomically, the VCF link first and then the index link.
  Between the two swaps a reader can briefly see the new VCF with the old index.
- `PUBLISH` refuses to replace a regular file at a link path unless `force` is
  set.
- `PUBLISH` keeps the newest `keep_releases` releases and never deletes the one
  the links point to.

## Gates

A release is published only if every gate passes for that assembly.

| Script | Fails when |
|---|---|
| `check_vep_safe.py` | A field no longer parses to the same structure after VEP's value rewrite, or `##source=ClinVar` is missing |
| `validate.py` | Validation problems exceed `max_problem_fraction` of the records, or records with `CLNSUBA` fall below `min_clnsuba_fraction` |
| `check_outputs.py` | Record counts differ, a `GENEENSG` ID is not a GFF gene, or the tabix index is missing or fails a query |

## Resources

Times and sizes are from the 2026-10 release (dated 2026-09-28, 4.56M records),
run with the standard profile on a 10-core, 16 GB laptop.

| Step | Time | Memory requested |
|---|---|---|
| `BUILD_DB` | 43 min | 8 GB |
| `ANNOTATE` | 4.6 min per assembly | 4 GB |
| `CHECK` | 3.7 min per assembly | 4 GB |
| Whole run, both assemblies | 52 min | |

| Disk | Size |
|---|---|
| Database in the work directory | 11.8 GB |
| Work directory, with the XML releases fetched by the pipeline | about 24 GB |
| Each published assembly | about 470 MB |

Remove old work directories after a successful run.

## Tests

```bash
nf-test test . --profile local
```

nf-test 0.9.2 does not start on Java 26 (`Unsupported class file major
version 70`). Run it with Java 17 or 21, as CI does.

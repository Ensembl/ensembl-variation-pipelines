# clinvar

Builds the enriched ClinVar VCFs that web VEP annotates with, one per assembly.

Each monthly ClinVar release is enriched with submission-level detail from the
VCV and RCV XML releases (`CLNSUBA`, `CLNPMID`, `CLNSUBN`, `CLNRCV`). Each
`GENEINFO` gene is resolved to the Ensembl gene IDs of the gene set VEP
annotates with and written as `GENEENSG`. Web VEP reads the result from
`<publish_dir>/<grch38|grch37>/clinvar.vcf.gz`.

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

For each `GENEINFO` entry `NAME:NCBIID`:

- if `NAME` is a gene name in the GFF, take those Ensembl IDs
- otherwise take the Ensembl genes Ensembl xrefs to `NCBIID`, keeping only genes
  in the GFF that carry exactly one NCBI ID

IDs are joined with `+` in `GENEINFO` order. Records with no resolved gene have
no `GENEENSG`. The field does not depend on the record having an XML row.

NCBI IDs come from `Homo_sapiens.GRCh38.<release>.entrez.tsv.gz` for GRCh38
and from the `homo_sapiens_core_<release>_37` dump tables for GRCh37.

## Requirements

- Nextflow 24.04.3 or later
- Python 3.10+ with `requirements.txt` installed
- `bgzip` and `tabix` (htslib) on `PATH`

```bash
python3 -m pip install -r requirements.txt
```

## Local run

Copy `assets/local.params.example.yaml`, fill in the paths, then:

```bash
nextflow run main.nf -profile standard -params-file local.params.yaml
```

Leave an input unset to fetch it from NCBI or Ensembl. A local ClinVar VCF must
carry the release date in `##fileDate` unless `allow_date_mismatch` is set.

## Codon run (pencilled)

Copy `assets/codon.params.example.yaml` outside the repo and fill it in. Web VEP
reads `$VEP_DATA_DIR/vep_plugins_data/grch3x/clinvar.vcf.gz`, so `publish_dir`
is `<VEP_DATA_DIR>/vep_plugins_data`.

`cron/crontab.example` runs `cron/run_clinvar.sh` daily as `ensvar`. The script
submits `cron/clinvar_head.sbatch` unless a head job is already queued or
running. The head job runs Nextflow with `-profile slurm`, with its launch and
work directories under `CLINVAR_RUN_DIR` on `/hps`, because only SLURM jobs can
write `/hps`.

| Variable | Holds |
|---|---|
| `CLINVAR_PARAMS` | Params file |
| `CLINVAR_RUN_DIR` | Launch and work directory on `/hps` |
| `CLINVAR_ENV` | Optional file sourced first, for modules and the Python env |

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
| `keep_releases` | `3` | Releases kept per assembly |
| `max_problem_fraction` | `0.01` | Validation gate |
| `min_clnsuba_fraction` | `0.99` | Validation gate |
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

`CURRENT` holds the published release date. The provenance file records the
input sources and md5s, the gene map provenance, annotation stats, gate
results, pipeline commit, Nextflow version and start time.

Links are relative. Each link is replaced atomically, the data file first and
then the index. Between the two swaps a reader can briefly see the new VCF with
the old index. The publish step refuses to replace a regular file at a link
path unless `force` is set, and never deletes the release a link points to.

## Gates

A release is published only if every gate passes for that assembly.

| Script | Fails when |
|---|---|
| `check_vep_safe.py` | A field no longer parses to the same structure after VEP's value rewrite, or `##source=ClinVar` is missing |
| `validate.py` | Problem records exceed `max_problem_fraction`, or records with `CLNSUBA` fall below `min_clnsuba_fraction` |
| `check_outputs.py` | Record counts differ, a `GENEENSG` ID is not a GFF gene, or the tabix index does not query |

## Resources

Approximate times for a full release:

| Step | Time | Memory requested |
|---|---|---|
| `BUILD_DB` | about 20 min | 8 GB |
| `ANNOTATE` | about 2.5 min per assembly | 4 GB |

A run needs about 25 GB of scratch in the work directory. Remove old work
directories after a successful run.

## Tests

```bash
nf-test test . --profile local
```

nf-test 0.9.2 does not start on Java 26 (`Unsupported class file major
version 70`). Run it with Java 17 or 21, as CI does.

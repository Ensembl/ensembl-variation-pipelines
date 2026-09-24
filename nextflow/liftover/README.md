# Remap variants with bcftools +liftover

This pipeline is an alternative to the CrossMap-based Remapping pipeline.
It uses the freeseek/score liftover plugin.

## Inputs and requirements

Supply one target assembly ID, a VCF, a chain file and the source/target FASTAs.
Contig names must agree across these inputs. Each FASTA needs a `.fai` index;
BGZF-compressed FASTAs also need a `.gzi` index.

Use Nextflow with the legacy parser, Singularity, bgzip/tabix and `bc`.
Source and target FASTAs are staged separately, so their filenames may match.

## Run

From the repository root, replacing the input paths:

```bash
nextflow run nextflow/liftover/main.nf \
  -profile singularity \
  --ids ids.txt \
  --vcf variants.vcf.gz \
  --chain source_to_target.chain.gz \
  --src_fasta source.fa \
  --dst_fasta target.fa \
  --out_dir results
```

The IDs file must contain exactly one target ID. Run separately for each target
assembly. The pipeline publishes mapped and rejected records with their indexes,
plus a mapping report. Task failures stop the run; notifications are disabled by
default.

The Dockerfile specifies bcftools 1.21. The external `latest` image is not tied
to that build; pin a validated image before production use. See
[pipeline_transfer.md](pipeline_transfer.md) for the review and local fixes.

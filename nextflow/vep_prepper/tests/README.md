# VEP prepper tests

Run from the repository root:

```sh
python3 tests/run_pipeline_tests.py vep_prepper
```

Requires Nextflow, Java, `bgzip`, `tabix`, and `samtools`. The tests run the
production `VEP_PREPPER` workflow locally using tiny synthetic GFF3 and FASTA
files, with no network access or external datasets.

Coverage includes:

- Direct GFF/FASTA input and its precedence over a manifest or directory scan.
- CSV inputs with explicit output paths and fallback `outdir_suffix` values.
- Directory discovery, selecting the newest geneset, excluding incomplete
  GFF/FASTA pairs, and handling scans with no eligible genomes.
- Header preservation, chromosome/start/end sorting, BGZF compression, and
  successful indexed GFF and FASTA queries using generated CSI/FAI/GZI indexes.
- Sorting and indexing headerless GFF input, and reporting failed FASTA indexing
  for malformed input without publishing the invalid FASTA.
- Preserving the original input files and placing all outputs in the expected
  species/assembly/provider directory.

Payload generation remains outside this suite: it is disabled by default and
still marked `TBD` in the workflow, with an incomplete metadata script. These
tests establish coverage of the implemented preparation workflow; they do not
claim payload support or large dataset performance coverage.

# Remapping transfer review

## Legacy dependencies

No Ensembl Perl API or MySQL dependency was found. The workflow uses CrossMap,
local VCF/chain/FASTA files, htslib and shell reporting.

Optional publication in `modules/copy.nf` uses `become ensrapid`, rsync and a
Rapid Release filesystem path when `--rr_root` is set. Replace this with a
supported destination and credentials. Review host tools, scheduler settings and
notifications before deployment.

## PRs included

None change this directory. [#1165](https://github.com/Ensembl/ensembl-variation/pull/1165)
adds the separate `liftover` pipeline; both are retained.

## Local changes

`modules/crossmap.nf` previously sorted into the input filename when the output
name was unchanged, which could truncate staged input. It now writes a new
compressed file, handles header-only rejects and publishes both mapped and
rejected records with their indexes. Comments and help text were simplified.

## Validation

Nextflow tests using real bgzip/tabix confirm sorting, unchanged inputs and
rejected-file indexes. CrossMap and the publication service were not run.

Source: `release/116`, commit `db7394a02523a6e519cb0d89f84b2889bac12737`. See the [transfer summary](../pipeline_transfer.md) for scope and shared validation.

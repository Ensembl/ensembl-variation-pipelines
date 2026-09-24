# liftover transfer review

## Legacy dependencies

No Ensembl Perl API or MySQL dependency was found. The workflow uses
bcftools +liftover, local VCF/chain/FASTA files, htslib and shell reporting.

Pin the external container and freeseek plugin source before production use.
Host bgzip/tabix and `bc` are also required. The Dockerfile uses bcftools 1.21;
the external `latest` image is not tied to that build.

## PR included

[#1165](https://github.com/Ensembl/ensembl-variation/pull/1165), head
`c525f6a7c1f298ec68b4c7574a8c9fc6f07c8d0f`, adds this directory as a bcftools
alternative to Remapping. It includes the workflow, module, Dockerfile,
configuration and README.

## Local changes

| Issue | Change |
| --- | --- |
| Reference files were not staged | `main.nf` validates FASTAs and indexes. The module uses path inputs and separate staging directories for chain, source and target files. Commands quote paths. |
| Multiple target IDs reused one reference pair | `main.nf` now requires one filename-safe target ID per run. |
| Sorting could truncate input | The module writes new compressed output, handles header-only rejects and publishes mapped/rejected files with indexes. The same inherited bug was fixed in Remapping. |
| Task failures could be ignored | `nextflow.config` now terminates on ordinary errors while retaining resource-error retries. |
| Notifications used a fixed recipient | Notifications are disabled by default. |

The README and help now use the destination repository, describe required indexes
and show one general command. Removed site-specific module-loading instructions.
Dockerfile comments describe the configured version rather than calling it latest.

## Validation

Nextflow tests check reference/index staging with a fake bcftools command and
reject multiple target IDs before submission. Real bgzip/tabix tests confirm
sorting, unchanged inputs and header-only reject handling.

These checks validate task wiring. The liftover plugin, biological results and
container build remain untested.

Source: `release/116`, commit `db7394a02523a6e519cb0d89f84b2889bac12737`. See the [transfer summary](../pipeline_transfer.md) for scope and shared validation.

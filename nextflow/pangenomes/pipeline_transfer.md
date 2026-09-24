# pangenomes transfer review

## Legacy dependencies

GO and phenotype generation use Ensembl Perl APIs and MySQL, including when a
manifest supplies local FASTA/GFF3 inputs.

| Location | Dependency and work needed |
| --- | --- |
| `modules/annotation.nf:create_latest_annotation` | Runs VEP in database mode with GO/Phenotypes plugins. Replace generation with versioned files or supported services. |
| GO generation | Reads core transcript, cross-reference and ontology data. Choose a maintained gene-GO source and retain identifier/symbol mapping. |
| Phenotype generation | Reads variation phenotype associations and joins gene records through HGNC symbols. Replace this with the planned phenotype store or export. |
| `modules/test.nf` | Runs Perl VEP and plugins against local annotation files. Pin or replace this runtime and verify offline operation. |

The pipeline expects VEP and `VEP_plugins` under `$ENSEMBL_ROOT_DIR`; these are
not bundled or pinned. EBI geneset downloads, HGNC data, pandas and htslib are
separate external requirements. Local VEP/plugin source confirmed the indirect
API and database dependencies.

## PR included

[#1219](https://github.com/Ensembl/ensembl-variation/pull/1219), head
`3b4410f4d4f97bec632c8805cc601e4cda45d23f`, adds manifests, updated download URLs,
latest-geneset selection, GFF3 handling, accession-based filenames and tests at
annotated positions. It also sets Slurm resources and removes global
failure-ignore behaviour. It does not remove API or database dependencies.

## Local changes

Formatted `bin/create_pangenomes_annotation.py` with the repo's Ruff configuration
and removed two unnecessary f-string prefixes. Annotation logic is unchanged.
Comments and this review were shortened.

## Validation and remaining work

Ruff and Python syntax checks pass. Synthetic GFF3 tests cover GO and phenotype
projection, coding/noncoding features, identifiers and coordinates. No significant
new PR defect was found.

Downloads, MySQL generation and VEP integration remain untested. Review broad
suppression of VEP errors and the GO branch's placeholder path before production
use.

Source: `release/116`, commit `db7394a02523a6e519cb0d89f84b2889bac12737`. See the [transfer summary](../pipeline_transfer.md) for scope and shared validation.

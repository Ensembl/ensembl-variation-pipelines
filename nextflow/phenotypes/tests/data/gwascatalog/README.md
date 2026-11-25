# GWAS Catalog test data

`associations.tsv` contains 38 unchanged rows from
`gwas-catalog-download-associations-alt-full.tsv`, selected on 24 November 2026.
The subset retains all 38 source columns, including author-reported genes which
the parser deliberately ignores.

`study_builds.tsv` contains the corresponding `Study` and `Raw_genome_build`
values from `Harm_sumstats_status.db` (last updated 20 November 2026). Studies
absent from that database remain absent here. Tests create a small temporary
SQLite database from this file; the full database is not needed.

| Examples | Behaviour checked |
| --- | --- |
| GCST004604 | Ordinary associations, mapped genes, focal alleles, changed rsID, and an unmapped coordinate identifier with unknown assembly. |
| GCST007098; GCST004232 | Distinct analyses remain separate; rows differing only in author-reported genes collapse. |
| GCST90013473; GCST90000255 | Catalog GRCh38 mappings take precedence over original study build; confirmed GRCh38 identifiers can supply missing coordinates. |
| GCST004749; GCST003378; GCST003622 | Unmapped coordinate identifiers with GRCh37 or unknown assembly are rejected. |
| GCST004614; GCST003527 | Missing coordinates and non-rsID names are retained without inventing locations or shared identifiers. |
| GCST008413; GCST006495 | Haplotype and interaction component order, alleles, coordinates and joint statistics. |
| GCST004870; GCST010571; GCST001880 | Rejected compound records: incomplete coordinates, missing alleles and a haplotype spanning chromosomes. |
| GCST90662901 | Identical repeated members collapse. |
| GCST009596; GCST005812; GCST004610 | Commas within ontology labels, multiple accessions and an alphanumeric OBA accession. GCST004881 and GCST005810 provide single-term label examples. |
| GCST012219; GCST90002232 | Extreme logarithmic p-value and absent p-value. |
| GCST90132316; GCST008520; GCST002495; GCST003636 | Signed beta, infinity, age-dependent units and missing-value markers. |
| GCST000001; GCST002415; GCST010716 | Qualified, ranged and invalid effect-allele frequencies; an unclassified positive effect size. |
| GCST90693107; GCST90013452 | Missing phenotype name fallback and literal quotation marks in a study title. |

Additional boundary cases use explicitly modified copies of these rows inside
the tests: unknown study builds, conflicting partial coordinates, different
alleles for one rsID, invalid or missing logarithmic p-values, a missing
phenotype, and exact duplicates. These are not presented as real source rows.

Expected values are written explicitly in `test_gwascatalog.py`. Input fixtures
are committed; temporary databases, compressed inputs and parser outputs are
created during testing. Tests do not download data or connect to PostgreSQL.

Run from `nextflow/phenotypes`:

```bash
uv run pytest -q tests/test_gwascatalog.py
```

# Pipeline transfer review

Branch: `feature/pipeline_transfer`. Prepared on 2026-09-24.
The transfer includes the approved local fixes and documentation updates.
No source PR was merged in Ensembl/ensembl-variation.

## Scope

Source: [Ensembl/ensembl-variation release/116](https://github.com/Ensembl/ensembl-variation/tree/db7394a02523a6e519cb0d89f84b2889bac12737/nextflow),
commit `db7394a02523a6e519cb0d89f84b2889bac12737`.

The transfer includes five pipelines, shared utilities and the source `.gitignore`.
EVAImport is excluded at the owner's request because it is entirely legacy.
Existing `vcf_prepper` and `vep_prepper` files are unchanged.

| Pipeline | Ensembl Perl APIs | MySQL | PRs included |
| --- | --- | --- | --- |
| [MaveDB](MaveDB/pipeline_transfer.md) | Variant Recoder | Through Variant Recoder | #1200 and prerequisite #1188 |
| [ProteinFunction](ProteinFunction/pipeline_transfer.md) | Matrix creation in all modes | Online mode | #1152 |
| [Remapping](Remapping/pipeline_transfer.md) | None found | None found | None |
| [liftover](liftover/pipeline_transfer.md) | None found | None found | #1165 |
| [pangenomes](pangenomes/pipeline_transfer.md) | VEP and plugins | Annotation generation | #1219 |
| [utils](utils/pipeline_transfer.md) | None found | None found | Shared change from #1152 |

Each directory records its dependencies, PR commits, local changes and remaining
work. External scripts, APIs, databases and datasets are not bundled.

Only relevant PR diffs were applied. Open PRs #1059 and #454 affect files outside
`nextflow` and were excluded. MaveDB's prerequisite #1188 was already merged
upstream but absent from `release/116`; its conflict resolution is recorded in
the MaveDB note. The new liftover pipeline is retained alongside Remapping.

## Local changes

- ProteinFunction: fixed online arguments and flags, replaced shared SQLite
  writes with per-task databases and one final merge, made storage errors fatal,
  preserved binary matrices and corrected optional-output declarations.
- MaveDB: fixed result reuse, isolated planning files, separated trace and result
  paths, corrected output/datacheck ordering, handled empty results and fixed
  numeric rounding.
- liftover: staged indexed references, required one target per run, fixed input
  truncation, published rejected records and made task failures visible.
  Notifications are disabled by default.
- Remapping: applied the same input-truncation fix and published rejected records.
- Python: aligned standalone and embedded code with the unchanged `ruff.toml`.
- Text: shortened comments, help and documentation; retained PR and fix records;
  replaced outdated or overly specific examples with general commands.

## Validation

The initial copy preserved file contents and executable modes before local
changes. There are 55 original source files in the retained scope. MaveDB matched
#1200's head before fixes. The binary fixture and existing executable modes are
preserved.

All 10 [regression tests](../tests/pipeline_transfer/README.md) passed without
skips. They cover result reuse, rounding, SQLite storage, disabled output,
reference staging and safe VCF sorting. Ruff 0.12.7 lint/format checks, language
syntax checks and relative-import checks passed. Embedded Python was checked
separately because Ruff does not scan Nextflow files.

Nextflow checks used 26.04.4 with `NXF_SYNTAX_PARSER=v1`. The full MaveDB entry
point passed a synthetic reuse run through datachecks. ProteinFunction passed
runs with predictors disabled; online SQL was intercepted by a fake client.
Liftover staging used a fake bcftools command. SQLite, bgzip and tabix checks used
real tools. Pangenomes GO/phenotype projection passed synthetic GFF3 checks.

No real MySQL connection was made. Production prediction, MySQL FULL/UPDATE
behaviour, live recoding, full new-URN ingestion, biological liftover and container
builds still need integration testing. External data paths, API dependencies and
unpinned images remain migration work.

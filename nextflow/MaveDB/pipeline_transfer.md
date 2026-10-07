# MaveDB transfer review

## Legacy dependencies

Protein-HGVS mapping uses Ensembl Perl APIs and MySQL through Variant Recoder.
`nf_modules/variant_recoder.nf` runs the external `ensembl-vep/variant_recoder`
with an optional registry. Omitting the registry does not disable database access.
The VEP installation defaults to `$ENSEMBL_ROOT_DIR` and is not bundled or pinned.

Replace this step with a supported mapping service or file-based tool. Keep
transcript and reference versions explicit, and compare ambiguous and unmapped
variants before switching. Local MaveDB inputs do not remove this dependency.

Other steps use Python, MaveDB HTTP data, Ensembl REST lookups and UCSC chains.
They make no direct Perl API or MySQL calls. Host tools, containers, scheduler
settings and memory requirements also need a portable configuration.

## PRs included

- [#1200](https://github.com/Ensembl/ensembl-variation/pull/1200), head
  `335a9392c28304d25a587fe11bdb6e9e437eb20b`: skips previously processed URNs,
  merges previous results and adds a reuse-only path.
- Required [#1188](https://github.com/Ensembl/ensembl-variation/pull/1188), head
  `3467937c90a08145f94f9899681d9e55f8793190`, merge
  `4ad96af5317396ffeca518a38447b16b4b87c494`: adds logging, datachecks, metadata
  and mapping fixes, file matching, recoder retries, memory limits and tracing.
  It was already merged upstream but absent from `release/116`.

Only the relevant diffs were applied. The conflict in
`bin/map_scores_to_variants_fromfiles.py` was resolved by keeping #1188's guard
that logs and skips entries without mapping data. Before local fixes, the code
matched #1200's head.

## Local changes

| Issue | Change |
| --- | --- |
| Reuse failed on an invalid Path conversion and copied unrequested rows | `planning.nf`, `merge_previous_output.nf` and `main.nf` now send reused rows through filtering, sorting, indexing and datachecks. |
| Merge code was embedded in Nextflow | Moved it to `bin/merge_previous_output.py`. Both merge paths align headers, remove duplicate rows and reject missing requested URNs. |
| Concurrent runs shared planning files | `planning.nf` writes each run's lists to a separate directory, removes duplicate requests and checks previous results. |
| Trace output conflicted with the result filename | `nextflow.config` writes the trace to `reports/trace.txt`. |
| Datachecks could read output before publication finished | `output.nf` emits separate data/index paths and publishes copies. `main.nf` passes the task output directly to datachecks after log collation. |
| Indexing modified staged input | `output.nf` writes a new compressed file and handles header-only input. |
| Empty new results blocked reuse | Empty collections now reach concatenation and can be merged with previous results. Without any results, the workflow fails clearly. |
| Output failures could be ignored | Merge, concatenation and indexing now terminate on failure. |
| Rounding errors were hidden | Both mapping scripts now call Python's builtin `round` correctly and catch only conversion errors. |

Ruff formatting covers all standalone Python and the Python embedded in
`fetch.nf` and `output.nf`. Removed unused imports, split combined imports,
clarified variable names and expanded one-line statements. Concatenation now
catches empty-data errors specifically instead of hiding other read failures.
The initial check found 18 lint issues; all are resolved. The repo's Ruff
configuration is unchanged.

Documentation, help text, comments and docstrings were shortened. The README
now uses the destination path and the implemented `--from_files` option.

## Validation and remaining work

Tests cover filtering, header alignment, duplicate and missing URNs, empty input,
rounding, isolated planning files, and bgzip/tabix output. The full entry point
completed a synthetic reuse-only run through log collation and datachecks.
Ruff checks pass for standalone and extracted embedded Python.

Live recoding, API downloads, full new-URN ingestion and container execution
remain untested. The existing heap check still requires a large maximum heap;
small test inputs do not validate production memory needs.

Source: `release/116`, commit `db7394a02523a6e519cb0d89f84b2889bac12737`. See the [transfer summary](../pipeline_transfer.md) for scope and shared validation.

# ProteinFunction transfer review

## Legacy dependencies

Online mode uses MySQL. Both online and offline modes require Ensembl Perl APIs.

| Location | Dependency and work needed |
| --- | --- |
| `nf_modules/database.nf` | MySQL operations on translation mappings, metadata, attributes and prediction tables. Replace storage and incremental selection with the new store. |
| `nf_modules/translations.nf` | Reads existing translation hashes through the database module. Replace this lookup with a store query or input manifest. |
| `bin/store_sift_scores.pl`, `bin/store_polyphen_scores.pl` | Import Registry, the Variation DBAdaptor and ProteinFunctionPredictionMatrix. Replace API-based matrix creation and serialization while preserving format compatibility. |
| SIFT and PolyPhen storage processes | Use `ensemblorg/ensembl-vep:latest` for the Perl runtime. Pin the image and replace the API dependency when storage is migrated. |

SQLite is not a MySQL dependency, but its output still uses Ensembl matrices and
analysis IDs 267/268/269. The SIFT module also contains a fixed BLAST installation
path. BLAST/UniRef data, PolyPhen data, containers and scheduler settings remain
external requirements.

## PR included

[#1152](https://github.com/Ensembl/ensembl-variation/pull/1152), head
`a6e07a1810a3a2a0443967364b709c83962f888f`, adds offline/SQLite options, conditional
MySQL operations, SIFT memory/retry changes, OOM handling and a shared numeric
conversion fix. Its diff was applied to `release/116`, without importing its
older `postreleasefix/115` base.

## Local changes

| Issue | Change |
| --- | --- |
| Online storage lacked a required process argument | `main.nf` restores the wait dependency before storing translation mappings. |
| Perl misread the offline flag; disabled SQLite still received writes | SIFT/PolyPhen processes pass validated numeric flags and an empty SQLite path when disabled. All positional arguments are shell-quoted. |
| SQLite initialization and writes could race | Each score task now writes its own database. The subworkflows emit those files, and `main.nf` waits for all writers, including empty runs. Removed the unused shared initialization step. |
| Concurrent writes and errors could lose data | Both Perl writers use DBI error handling and a primary key with retry-safe inserts. New `bin/merge_prediction_databases.pl` combines task databases in one transaction and rejects duplicate keys. |
| Binary matrices could be stored as text | Writers and merger bind matrices as SQLite BLOBs. |
| Optional outputs failed to parse | Corrected the SIFT/PolyPhen output declarations. |

`database.nf` builds the final database in its task directory and publishes it to
`--sqlite_db`, creating the destination directory as needed. The schema remains
`predictions(md5, analysis, matrix)`, with primary key `(md5, analysis)` and an MD5
index. It contains predictions from the current run. Storage tasks rerun on
resume and rebuild this output.

Documentation and comments were shortened. The README now explains offline and
SQLite options and uses a valid SIFT-only example. Help uses a matching example.

## Validation and remaining work

Tests cover SIFT/PolyPhen score parsing, all analysis IDs, binary storage, empty
output, duplicate rejection, disabled SQLite and online writer selection with a
fake adaptor. Nextflow tests run concurrent local writers and the final merge.
The full entry point also passed offline and online runs with predictors
disabled; online SQL was intercepted by a fake client.

No real MySQL connection was made. Production prediction tools, MySQL FULL/UPDATE
operations, dual-output production runs, resume behaviour and containers still
need integration testing.

Source: `release/116`, commit `db7394a02523a6e519cb0d89f84b2889bac12737`. See the [transfer summary](../pipeline_transfer.md) for scope and shared validation.

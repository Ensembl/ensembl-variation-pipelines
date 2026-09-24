# Pipeline transfer tests

Run from the repository root:

```bash
python3 -m unittest discover -s tests/pipeline_transfer -v
ruff check --config ruff.toml nextflow/MaveDB/bin nextflow/pangenomes/bin tests/pipeline_transfer
ruff format --check --config ruff.toml nextflow/MaveDB/bin nextflow/pangenomes/bin tests/pipeline_transfer
```

Checks used Ruff 0.12.7 and Nextflow 26.04.4 with the legacy parser. Python tests
use the standard library. Integration checks also need Nextflow, bgzip/tabix,
Perl DBI/DBD::SQLite and the Ensembl Perl APIs. Missing dependencies cause skips;
inspect the test summary. All checks ran without skips during transfer validation.

Tests use temporary directories, disable containers and notifications, and set
`NXF_SYNTAX_PARSER=v1` and `NXF_OFFLINE=true`. Fake tools replace live liftover and
online storage. No production database is contacted.

Ruff does not scan `.nf` files. Extract and dedent the Python bodies in MaveDB's
`fetch.nf` and `output.nf` to check them separately, and preserve Nextflow escaping.

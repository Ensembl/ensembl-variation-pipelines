# Pipeline transfer tests

Run from the repository root:

```bash
python3 -m unittest discover -s tests/pipeline_transfer -v
ruff check --config ruff.toml nextflow/MaveDB/bin nextflow/pangenomes/bin tests/pipeline_transfer
ruff format --check --config ruff.toml nextflow/MaveDB/bin nextflow/pangenomes/bin tests/pipeline_transfer
```

Python 3.10+ tests use the standard library. Integration checks also need
Nextflow with the legacy parser, pandas, bgzip/tabix, Perl DBI/DBD::SQLite and
the Ensembl Perl APIs. Missing dependencies cause skips; inspect the test summary.
The restored-output checks were validated with Nextflow 24.04.3 on Java 17;
all 14 tests passed without skips.

Tests use temporary directories, disable containers and notifications, and set
`NXF_SYNTAX_PARSER=v1` and `NXF_OFFLINE=true`. Fake tools replace live liftover and
online storage. No production database is contacted.

The restored MaveDB pipeline has no previous-output reuse support. Its tests
cover rounding with explicit zero and omitted precision, empty and malformed
mapping failures, failed indexing even with a global `ignore` policy, and
publication only after validation succeeds. A stale published file checks that
validation consumes the generated task output directly. Successful output is
sorted, deduplicated, compressed and indexed without changing the input mapping.

Ruff does not scan `.nf` files. Extract and dedent the Python bodies in MaveDB's
`fetch.nf` and `output.nf` to check them separately, and preserve Nextflow escaping.

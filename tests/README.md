# Pipeline test selection

`pipeline_tests.yml` runs a separate job for each affected Python test suite.
A pull request uses its full merge-base diff, so a later commit cannot hide an
older pipeline change. Renames select both source and destination pipelines.
Pushes to `main`, merge queues and manual runs are also supported; manual runs
select every registered Python suite.

The CI infrastructure is independent of pipeline coverage. When a change
touches a pipeline directory under `nextflow/` with no registered suite, the job
summary reports that pipeline as a coverage gap. Manual runs and CI-only changes
run every registered suite and list no gaps. On the current `main` baseline, VEP
prepper is such a gap. Pipeline coverage can land in separate PRs in any order
without editing the selector, workflow or root manifest. The phenotypes scaffold
has no `main.nf`, so it cannot register a suite and its changes are always
listed as gaps.

VCF prepper remains owned by `.github/workflows/unit_test.yml`, which is unchanged.
Its existing nf-test discovery and dependencies also cover tests added by the
separate VCF coverage work. Keep its status check alongside the new
`pipeline-tests` aggregate check; this workflow does not certify VCF tests.
The existing VCF workflow also selects `.github/**` changes, so edits to this CI
infrastructure can still trigger that job.

The VCF `github` test profile pins the VEP container to the immutable VEP 116.2
image used by the existing RUN_VEP fixture. The previous untagged image moved to
116.3 and added a downstream transcript annotation, failing the exact expected
record without any repository change. Upgrade the image and its expected
annotations together in a VCF-specific change. This pins only that test runtime;
the VCF workflow still checks out its external Ensembl repositories at `main`.

## Register a pipeline suite

Add these files in the pipeline's own coverage PR:

- `nextflow/<pipeline>/tests/suite.json` containing `{"runner":"python"}`.
- `nextflow/<pipeline>/tests/test_*.py` containing discoverable `unittest` cases.
- Optional `nextflow/<pipeline>/tests/setup.sh` to install that suite's dependencies.

CI provisions Python 3.12 and Nextflow 26.04.4 on Ubuntu 24.04, then runs the
optional setup script with Bash from the repository root. Setup scripts may use
`sudo` for system packages and `$GITHUB_ENV` for environment variables needed by
subsequent steps. Pin external dependencies in each pipeline's setup script.
Scripts run only for selected suites. Keep Python test modules directly under
`tests/` so the registration check sees them.

Run a registered suite locally from the repository root, replacing `PIPELINE`:

```sh
python3 tests/run_pipeline_tests.py PIPELINE --strict
```

Install the suite's dependencies first. `--strict` makes skipped tests fail,
including missing dependencies; empty suites, failed imports and failed tests
also fail. Selection rejects `test_*.py` files directly under
`nextflow/<pipeline>/tests/` when that pipeline has no `suite.json`, so removing
a marker cannot silently disable its suite. Python tests elsewhere in the
repository are not checked. Omit `--strict`
for a local run that reports dependency skips. Shared helpers in
`tests/support/pipeline_test_support.py` provide isolated temporary
fixtures and local, offline Nextflow module execution.

## Selection and checks

Pipeline-local changes select that pipeline. Changes to `nextflow/utils/` select
the registered suites of its five known consumers: MaveDB, ProteinFunction,
liftover, Remapping and pangenomes; unregistered consumers that have a
`main.nf` are reported as gaps.
Shared Python helpers, the runner, selector and CI configuration select every
registered Python suite. Repository documentation alone selects none.

`.github/pipeline-tests.json` records shared dependencies and externally managed
suites. New Python coverage does not need a root manifest edit. Update the shared
consumer mapping if production pipeline dependencies change.

The aggregate check fails when selection fails, when a selected suite fails or
is cancelled, or when a selected job unexpectedly skips. A successful aggregate
with no selected suites means no registered suite was affected; it does not
claim coverage for pipelines listed as gaps. Repository branch protection is
not changed by this implementation. Neither this check nor the VCF check is
currently required on `main`; making them required is a separate repository
settings change.

Check the dependency-free selector, registration, runner and gate tests with:

```sh
python3 -m unittest discover -s tests/ci -v
```

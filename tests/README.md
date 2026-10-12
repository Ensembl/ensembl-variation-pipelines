# Pipeline tests

The VCF suite remains owned by the existing
[unit-test workflow](../.github/workflows/unit_test.yml), introduced in
[PR #87](https://github.com/Ensembl/ensembl-variation-pipelines/pull/87) and extended
by [PR #90](https://github.com/Ensembl/ensembl-variation-pipelines/pull/90).
This change adds the separate
[pipeline-tests workflow](../.github/workflows/pipeline_tests.yml) for the other
six suites. The jobs do not duplicate the VCF suite or change its runtime setup.

| Pipeline | Suite | Additional requirements |
| --- | --- | --- |
| MaveDB | 3 Python/Nextflow regression cases | bgzip, tabix |
| ProteinFunction | 4 Python/Perl/Nextflow regression cases | DBI, DBD::SQLite, Ensembl Core/Variation Perl APIs |
| liftover | 3 Python/Nextflow regression cases | bgzip, tabix |
| Remapping | 1 Python/Nextflow regression case | bgzip, tabix |
| pangenomes | 14 annotation CLI and Nextflow module cases | pandas 2.3.3, bgzip, tabix |
| vep_prepper | 6 local workflow cases for input modes, indexed output and failures | bgzip, tabix, samtools |
| vcf_prepper | Existing nf-test suite, expanded separately by PR #90 | Its existing workflow provisions its own dependencies |

The six Python suites use Python's standard library test runner and Nextflow/Java.
They include small integration checks, rather than being exclusively unit tests.
Each pipeline owns its tests, and common execution helpers live in `tests/support`.

Pangenomes cases check real GO/phenotype projection, coding/noncoding identifiers,
HGNC joins, coordinates, sorting, failure cases, wrapper argument staging and
indexed annotation outputs. VEP prepper cases run the real preparation workflow
for direct files, CSV manifests and directory discovery. They verify output
contents, indexes, input preservation, headerless GFFs and malformed FASTA failure.
No production database or annotation service is contacted by these six suites.

## Run locally

From the repository root:

```bash
python3 tests/run_pipeline_tests.py MaveDB --strict
python3 tests/run_pipeline_tests.py ProteinFunction --strict
python3 tests/run_pipeline_tests.py liftover --strict
python3 tests/run_pipeline_tests.py Remapping --strict
python3 tests/run_pipeline_tests.py pangenomes --strict
python3 tests/run_pipeline_tests.py vep_prepper --strict
```

The runner adds the repository root to Python's import path. `--strict` rejects
skipped tests and is always used in CI; without it, missing local tools produce
visible skips. An empty suite fails in either mode.

The new workflow pins Nextflow 26.04.4 with `NXF_SYNTAX_PARSER=v1`. The helpers
use temporary directories, disable containers/notifications and set
`NXF_OFFLINE=true`; install/cache Nextflow before running them. ProteinFunction
needs the Core and Variation API `modules` directories on `PERL5LIB`; CI uses
fixed commits and fake adaptors, without a MySQL server. Pangenomes also needs
`python3 -m pip install pandas==2.3.3`. VEP prepper needs samtools.

For VCF, follow its existing workflow and test configuration. Its CI discovery
command runs from `nextflow/vcf_prepper`:

```bash
nf-test test . --coverage --ci --profile github
```

That command requires the workflow's database fixtures, Ensembl checkouts, tools,
Perl/Python dependencies, environment variables and fixture path substitutions.
The `.` is intentional: PR #90 colocates tests under modules and subworkflows.
Restricting discovery to `tests/modules/local` misses that expanded coverage.
This change does not introduce a competing profile or integration-test exclusion.

## Selection and required checks

[`.github/pipeline-tests.json`](../.github/pipeline-tests.json) maps the six owned
suites and records VCF as externally managed by `unit_test.yml`.

| Changed paths | New pipeline-tests workflow | Existing VCF workflow |
| --- | --- | --- |
| One of the six owned pipeline directories | That suite only | Not selected |
| `nextflow/vcf_prepper/**` | No Python suite | VCF suite selected |
| `nextflow/utils/**` | MaveDB, ProteinFunction, liftover, Remapping, pangenomes | Not selected |
| `tests/support/**`, `tests/__init__.py`, `tests/run_pipeline_tests.py` | All six Python suites | Not selected |
| `scripts/ci/**`, `tests/ci/**` | All six Python suites | Not selected |
| New workflow or `.github/pipeline-tests.json` | All six Python suites | Also selected by its existing `.github/**` rule |
| Root documentation only | No pipeline suite; selection checks still run | Not selected |

Multiple pipeline changes select the union. Pipeline-specific documentation
conservatively selects that pipeline. The new workflow supports PRs, pushes to
`main`, merge queues and manual runs. Manual dispatch runs the six owned suites;
it does not invoke the separately maintained VCF workflow, whose current trigger
is `pull_request`. The VCF workflow's triggers and filters are unchanged.

The selector compares the complete PR against its merge base and push events
against their before/after commits. Disabling Git rename detection includes both
old and new paths. Invalid events or missing history fail selection. CI checks
that every `nextflow/<pipeline>/main.nf` has a registered suite, including the
external VCF suite; it searches recursively to support module-local tests.
The newly merged phenotypes directory is a schema/package scaffold without a
Nextflow entry point and is outside that inventory.

The new `pipeline-tests` aggregate requires selection and all selected Python
suites to succeed. Unexpected skips, failures and cancellations fail it. VCF
continues to report its existing check independently. If required checks are
configured, retain the existing VCF check and add the new `pipeline-tests`
aggregate; requiring the new aggregate alone does not gate VCF. Repository
settings have not been changed.

Validate the selector and aggregate logic with:

```bash
python3 -m unittest discover -s tests/ci -v
actionlint .github/workflows/pipeline_tests.yml
```

## Scope and extension

New runnable pipelines need a suite, a manifest entry, runner support and any
required dependency setup. Add selection tests demonstrating that unrelated
pipelines remain unselected. Externally managed suites must identify their owning
workflow; do not introduce duplicate CI jobs for them.

Pangenomes live downloads, database-generated source annotations, VEP plugins and
its full entry point (including the GO placeholder lookup path) still need
external integration tests. VEP prepper's default-disabled payload generator
remains unfinished. Root scripts and dataset-dependent `datachecks/` are separate
from this Nextflow pipeline coverage. The pull request's GitHub Actions run
validates the hosted Ubuntu dependency setup and execution.

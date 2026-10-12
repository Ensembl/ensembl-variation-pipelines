# Add pipeline-specific regression jobs alongside the VCF test work

Branch: `feature/pipeline_specific_testing`.

The branch contains `feature/pipeline_transfer` at `87ffa951` plus current main
at `7e488c06`, combined in merge commit `0d641094`. The pull request targets
`main` and depends on PR #88; its diff includes the transfer changes until that
dependency lands. The testing implementation is a separate commit for review.
The branch does not incorporate unmerged PR #90.

## Proposed PR description

Add pipeline-owned regression suites and selective CI for MaveDB, ProteinFunction,
liftover, Remapping, pangenomes and VEP prepper. Each PR runs only the suites for
its changed pipelines and shared dependencies. CI rejects empty/skipped suites
and new Nextflow entry points without registered tests.

The four transfer suites retain their regressions. New pangenomes tests exercise
real annotation projection and Nextflow modules. New VEP prepper tests exercise
input routing and real compression/indexing. They exposed a GFF ordering race;
the included one-line fix emits headers before sorted records.

VCF coverage remains owned by the workflow already merged in PR #87 and the
expansion in PR #90. This PR does not change the VCF source, tests, fixtures,
profiles, coverage reporting or workflow. It neither duplicates that suite nor
restricts its discovery to the old test directory.

The new workflow supports PRs, main-branch pushes, merge queues and manual runs.
Its aggregate result covers the six Python suites. The existing VCF check stays
independent and must remain required wherever repository rules currently use it.

## Compatibility audit

Inspected remote state on 2026-10-12:

| Work | Audited head | Relationship to this proposal |
| --- | --- | --- |
| [PR #87: CI status check fix](https://github.com/Ensembl/ensembl-variation-pipelines/pull/87) | `5f030905` (merged via `04fff8f3`) | Already on main; its VCF fixes and workflow supersede our initial duplicate edits |
| [PR #90: nf-test coverage expansion](https://github.com/Ensembl/ensembl-variation-pipelines/pull/90) | `9cb84f0a` | Moves/adds VCF tests and adds a database fixture setup step; preserve its ownership and discovery |
| [PR #73: unit test and refactoring](https://github.com/Ensembl/ensembl-variation-pipelines/pull/73) | `edfaeed4` | Larger precursor being split into smaller PRs; VEP prepper changes only default paths and README |
| [PR #88: pipeline transfer](https://github.com/Ensembl/ensembl-variation-pipelines/pull/88) | `87ffa951` | Dependency providing the transferred pipelines and original regression suite |
| [PR #84: payload format](https://github.com/Ensembl/ensembl-variation-pipelines/pull/84) | `b5f531de` | Changes `scripts/create_metadata_payload.py`, with no overlap here |

The initial proposal did clash: its legacy VCF fixture repairs overlapped landed
fixes, and `nf-test test tests/modules/local --profile ci` would run only six
remaining cases after PR #90. PR #90 has 17 test files/24 declared cases spread
across the pipeline, including module/subworkflow directories. All our duplicate
VCF changes, the extra nf-test job/profile/report guard and the root README edit
have been removed from this proposal.

The existing VCF workflow continues using:

```bash
cd nextflow/vcf_prepper
nf-test test . --coverage --ci --profile github
```

Its database fixtures, external repositories, dependency provisioning, coverage
commenting and filters are retained unchanged. We introduce only
`.github/workflows/pipeline_tests.yml`; `unit_test.yml` remains byte-identical to
current main. Changes to `.github/**` still select VCF through that existing broad
filter; ordinary changes to another pipeline do not.

No VEP prepper tests were found on the fetched remote refs. PR #73 changes
`params.base_dir` and `params.outdir` to null and updates its README; it does not
change the GFF module touched here. Our six VEP tests passed with those exact
PR #73 files overlaid in an isolated source copy, with zero skips. They supply
explicit paths, so the new defaults do not conflict. Omitted-input behavior is
not claimed by those tests.

Compatibility is checked against these commits, not guaranteed against future
branch updates. The large PR #73 is a precursor with superseded changes; this
proposal aligns with its current carve-outs rather than merging that entire PR.

## Implementation map

| Files | Change |
| --- | --- |
| `.github/workflows/pipeline_tests.yml` | New selective matrix for six Python suites and its aggregate result |
| `.github/pipeline-tests.json` | Pipeline/shared path ownership; identify VCF as externally managed |
| `scripts/ci/select_pipeline_tests.py` | Complete Git event diffs and explicit owned/external suite reporting |
| `nextflow/{MaveDB,ProteinFunction,liftover,Remapping}/tests/` | Split the existing transfer regressions |
| `nextflow/pangenomes/tests/` | 14 real CLI/Nextflow annotation and preparation cases |
| `nextflow/vep_prepper/tests/` | 6 local workflow cases across input modes, output checks and failures |
| `nextflow/vep_prepper/modules/process_gff.nf` | Serialize header output before sorted records |
| `tests/support/`, `tests/run_pipeline_tests.py` | Shared harness and strict per-pipeline runner |
| `tests/ci/` | Selection, suite inventory, strict-runner and aggregate-check regressions |
| `tests/README.md`, `tests/pipeline_transfer/README.md` | Ownership, commands and migration documentation |

## Acceptance examples

- A MaveDB-only change runs its three tests and no other pipeline suite.
- A VCF-only change selects no new Python jobs; its established workflow runs.
- Shared Nextflow utilities select their five consumers, excluding VEP prepper.
- Shared Python test support selects all six owned suites.
- PRs with multiple commits and cross-pipeline renames retain all affected suites.
- An unregistered Nextflow entry point fails the inventory check.
- Selection failures and selected-job failures, cancellations or skips fail the
  new aggregate. Documentation-only changes allow intentional job skips.
- Manual dispatch runs the six owned suites, without pretending to invoke VCF.

## Validation and remaining limits

All 31 Python/Perl/Nextflow pipeline cases have passed locally without skips:
MaveDB 3, ProteinFunction 4, liftover 3, Remapping 1, pangenomes 14, VEP prepper 6.
The six VEP cases also passed against PR #73's changed workflow defaults. The
new VCF job's earlier validation is superseded because that job has been removed;
the separately owned VCF suite has not been revalidated as part of this change.

All 31 selector, inventory, strict-runner and aggregate-check tests pass on the
aligned branch. Actionlint passes for the new workflow, Ruff passes for the new
Python code, and `git diff --check` reports no whitespace errors.

A merge rehearsal in an isolated clone combined the complete proposal with
PR #90 at `9cb84f0a`. Git merged it without conflicts, all 31 CI checks passed
again, and the resulting VCF tree, `unit_test.yml` and coverage-comment script
matched PR #90 byte for byte. This verifies merge compatibility and test
selection; it does not substitute for running that PR's hosted VCF suite.

Hosted Ubuntu dependency setup remains unverified. Pangenomes live downloads,
VEP/database generation and complete top-level orchestration still need external
integration tests. VEP prepper payload generation is an unfinished optional
feature. The newly merged phenotypes scaffold has no Nextflow entry point and
is outside this suite inventory. No coverage percentage or complete biological
validation is claimed.

## Review and rollout

1. Review against the combined transfer/current-main baseline. Keep all VCF
   changes in PR #90 and its follow-ups; coordinate any future shared workflow
   policy changes with that work.
2. Merge PR #88 before this pull request. If #88 is squash-merged, realign this
   branch afterward so the diff contains only the testing implementation.
   PR #90 need not be imported into this branch.
3. Run the new hosted workflow. Retain the existing VCF required check and add
   the new pipeline-tests aggregate; repository settings are not changed here.
4. Repeat the compatibility check if the relevant branch heads move before merge.

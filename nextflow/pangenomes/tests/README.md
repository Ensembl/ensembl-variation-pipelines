# Pangenomes regression tests

Run this pipeline independently from the repository root:

```bash
python3 tests/run_pipeline_tests.py pangenomes --strict
```

This coverage PR requires the pipeline transfer in PR #88 and the shared runner
and helpers from the `codex/pipeline-test-ci` CI PR. The suite declaration enables
that workflow to select these tests when this pipeline changes.

The 14 cases use tiny local fixtures: ten exercise the annotation CLI, covering GO
and phenotype projection, coding and noncoding transcripts, identifiers, ordering,
compressed inputs, output names and invalid or missing inputs. Four run real
Nextflow modules to check annotation input staging, GFF normalization and indexing,
FASTA decompression, and publication of queryable compressed plugin annotations.

Dependencies are Python 3.12, pandas 2.3.3, Nextflow 26.04.4 with its Java runtime,
and `bgzip`/`tabix`. CI installs the pipeline-specific packages using `setup.sh`;
local runs need equivalent tools on `PATH`. Strict mode rejects skipped tests.

These tests do not contact live services or run the top-level download workflow.
Production pipeline files are unchanged.

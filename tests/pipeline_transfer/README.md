# Pipeline transfer tests have moved

This change only relocates the four suites supplied by pipeline-transfer PR #88
and registers them with the pipeline-specific CI framework. It depends on #88
and the CI framework on `codex/pipeline-test-ci`. It adds no production behavior
or coverage for a previously untested pipeline. The original ten test methods
become eleven independently runnable cases because the shared VCF sorting check
now runs once for liftover and once for Remapping.

The transfer regressions are now owned by their pipelines:

- [MaveDB](../../nextflow/MaveDB/tests/test_mavedb.py)
- [ProteinFunction](../../nextflow/ProteinFunction/tests/test_protein_function.py)
- [liftover](../../nextflow/liftover/tests/test_liftover.py)
- [Remapping](../../nextflow/Remapping/tests/test_remapping.py)

Run a suite from the repository root, for example:

```bash
python3 tests/run_pipeline_tests.py MaveDB --strict
```

The shared harness is in [tests/support](../support). Each Nextflow case uses a
fresh temporary directory, disables containers and notifications, sets
`NXF_SYNTAX_PARSER=v1` and `NXF_OFFLINE=true`, and adds only the owning pipeline's
`bin` directory and its temporary fake tools to `PATH`. No production database
is contacted.

Python tests use the standard library. The local Nextflow regressions require
Nextflow and Java; MaveDB and the sorting tests also require `bgzip` and `tabix`.
ProteinFunction additionally requires Perl DBI, DBD::SQLite and the Ensembl Perl
APIs. Missing dependencies cause skips in direct `unittest` runs; inspect the
summary. CI provisions dependencies using each suite's `setup.sh` and rejects
unexpected skips. The `suite.json` marker registers each suite for independent
selection: a change confined to one pipeline selects only that pipeline, while
changes to the shared harness select every registered suite.

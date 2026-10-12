# Pipeline transfer tests have moved

The transfer regressions are now owned by their pipelines:

- [MaveDB](../../nextflow/MaveDB/tests/test_mavedb.py)
- [ProteinFunction](../../nextflow/ProteinFunction/tests/test_protein_function.py)
- [liftover](../../nextflow/liftover/tests/test_liftover.py)
- [Remapping](../../nextflow/Remapping/tests/test_remapping.py)

Run a suite from the repository root, for example:

```bash
python3 -m unittest discover -s nextflow/MaveDB/tests -v
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
summary. CI must provision dependencies and reject unexpected skips.

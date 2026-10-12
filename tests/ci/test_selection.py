"""Exercise path ownership and real Git event diff semantics without dependencies."""

import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import textwrap
import unittest


ROOT = Path(__file__).resolve().parents[2]
SELECTOR = ROOT / "scripts/ci/select_pipeline_tests.py"
SPEC = importlib.util.spec_from_file_location("select_pipeline_tests", SELECTOR)
selector = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(selector)
CONFIG = json.loads((ROOT / ".github/pipeline-tests.json").read_text())
PYTHON_PIPELINES = [
    "MaveDB",
    "ProteinFunction",
    "liftover",
    "Remapping",
    "pangenomes",
    "vep_prepper",
]


class PathSelectionTests(unittest.TestCase):
    def select(self, *paths, all_suites=False):
        return selector.select_paths(paths, CONFIG, all_suites)

    def test_each_pipeline_selects_only_its_suite(self):
        for pipeline in PYTHON_PIPELINES + ["vcf_prepper"]:
            with self.subTest(pipeline=pipeline):
                result = self.select(f"nextflow/{pipeline}/tests/test_case.py")
                self.assertEqual(
                    result["python_pipelines"],
                    [pipeline] if pipeline in PYTHON_PIPELINES else [],
                )
                self.assertEqual(
                    result["external_pipelines"],
                    [pipeline] if pipeline == "vcf_prepper" else [],
                )
                self.assertEqual(result["uncovered_pipelines"], [])

    def test_legacy_and_colocated_vcf_tests_remain_externally_managed(self):
        for path in [
            "nextflow/vcf_prepper/tests/modules/local/update_fields.nf.test",
            "nextflow/vcf_prepper/modules/local/vcf_to_bed/tests/main.nf.test",
            "nextflow/vcf_prepper/subworkflows/local/prepare_genome/tests/main.nf.test",
        ]:
            with self.subTest(path=path):
                result = self.select(path)
                self.assertEqual(result["python_pipelines"], [])
                self.assertEqual(result["external_pipelines"], ["vcf_prepper"])
                self.assertEqual(result["uncovered_pipelines"], [])
                report = selector.summary(result)
                self.assertIn("Selected test suites for this workflow: none.", report)
                self.assertIn(
                    "managed by their existing workflows: vcf_prepper", report
                )

    def test_existing_vcf_workflow_does_not_select_owned_suites(self):
        result = self.select(".github/workflows/unit_test.yml")
        self.assertEqual(result["python_pipelines"], [])
        self.assertEqual(result["external_pipelines"], [])

    def test_pipeline_changes_combine(self):
        result = self.select("nextflow/MaveDB/main.nf", "nextflow/Remapping/bin/run.py")
        self.assertEqual(result["python_pipelines"], ["MaveDB", "Remapping"])

    def test_repository_docs_select_nothing(self):
        result = self.select("README.md", "docs/testing.md")
        self.assertEqual(
            result,
            {
                "python_pipelines": [],
                "external_pipelines": [],
                "uncovered_pipelines": [],
            },
        )

    def test_new_pipeline_does_not_match_similar_name_and_is_reported(self):
        result = self.select("nextflow/MaveDB_extra/main.nf")
        self.assertEqual(
            result,
            {
                "python_pipelines": [],
                "external_pipelines": [],
                "uncovered_pipelines": ["MaveDB_extra"],
            },
        )

    def test_pipeline_docs_still_select_that_pipeline(self):
        self.assertEqual(
            self.select("nextflow/liftover/README.md")["python_pipelines"], ["liftover"]
        )

    def test_nextflow_utils_select_only_dependents(self):
        result = self.select("nextflow/utils/report.py")
        self.assertEqual(
            result["python_pipelines"],
            ["MaveDB", "ProteinFunction", "liftover", "Remapping", "pangenomes"],
        )
        self.assertEqual(result["external_pipelines"], [])
        self.assertEqual(result["uncovered_pipelines"], [])

    def test_shared_python_support_selects_python_suites(self):
        for path in [
            "tests/support/helper.py",
            "tests/__init__.py",
            "tests/run_pipeline_tests.py",
        ]:
            with self.subTest(path=path):
                result = self.select(path)
                self.assertEqual(result["python_pipelines"], PYTHON_PIPELINES)
                self.assertEqual(result["external_pipelines"], [])

    def test_ci_changes_select_all_owned_suites(self):
        for path in [
            ".github/workflows/pipeline_tests.yml",
            ".github/pipeline-tests.json",
            "scripts/ci/select_pipeline_tests.py",
            "tests/ci/test_selection.py",
        ]:
            with self.subTest(path=path):
                result = self.select(path)
                self.assertEqual(result["python_pipelines"], PYTHON_PIPELINES)
                self.assertEqual(result["external_pipelines"], [])
                self.assertEqual(result["uncovered_pipelines"], [])

    def test_newly_covered_pipelines_select_real_suites(self):
        result = self.select(
            "nextflow/pangenomes/main.nf", "nextflow/vep_prepper/main.nf"
        )
        self.assertEqual(result["python_pipelines"], ["pangenomes", "vep_prepper"])
        self.assertEqual(result["external_pipelines"], [])
        self.assertEqual(result["uncovered_pipelines"], [])

    def test_every_pipeline_entrypoint_has_a_registered_suite(self):
        pipeline_dirs = [path.parent for path in (ROOT / "nextflow").glob("*/main.nf")]
        self.assertTrue(pipeline_dirs)
        for directory in pipeline_dirs:
            with self.subTest(pipeline=directory.name):
                entry = CONFIG["pipelines"][directory.name]
                self.assertIn(entry["runner"], ("python", "external"))
                pattern = "test_*.py" if entry["runner"] == "python" else "*.nf.test"
                if entry["runner"] == "external":
                    self.assertTrue(
                        (ROOT / entry["workflow"]).is_file(),
                        f"{directory.name} needs its owning workflow",
                    )
                self.assertTrue(
                    list(directory.rglob(pattern)),
                    f"{directory.name} needs a pipeline-owned test suite",
                )

    def test_manual_selection_runs_only_owned_suites(self):
        self.assertIsNone(selector.read_changed_paths("workflow_dispatch", {}))
        result = self.select(all_suites=True)
        self.assertEqual(result["python_pipelines"], PYTHON_PIPELINES)
        self.assertEqual(result["external_pipelines"], [])
        self.assertEqual(result["uncovered_pipelines"], [])

    def test_all_suites_flag_preserves_external_changes_as_information(self):
        result = self.select("nextflow/vcf_prepper/main.nf", all_suites=True)
        self.assertEqual(result["python_pipelines"], PYTHON_PIPELINES)
        self.assertEqual(result["external_pipelines"], ["vcf_prepper"])
        self.assertEqual(result["uncovered_pipelines"], [])


class GitDiffTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.repo = Path(self.directory.name)
        self.git("init", "-q")
        self.git("config", "user.name", "Selector Test")
        self.git("config", "user.email", "selector@example.invalid")
        self.base = self.commit("README.md", "Initial commit\n")

    def git(self, *args):
        return selector.git(*args, cwd=self.repo).decode().strip()

    def commit(self, path, content):
        target = self.repo / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content)
        self.git("add", "--", path)
        self.git("commit", "-q", "-m", "Test change")
        return self.git("rev-parse", "HEAD")

    def changes(self, event_name, event):
        return selector.read_changed_paths(event_name, event, cwd=self.repo)

    def test_pr_uses_merge_base_and_all_feature_commits(self):
        base_head = self.commit("nextflow/vcf_prepper/main.nf", "Base branch only\n")
        self.git("checkout", "-q", "-b", "feature", self.base)
        self.commit("nextflow/MaveDB/main.nf", "First PR commit\n")
        head = self.commit("nextflow/ProteinFunction/main.nf", "Second PR commit\n")
        paths = self.changes(
            "pull_request",
            {"pull_request": {"base": {"sha": base_head}, "head": {"sha": head}}},
        )
        self.assertEqual(
            set(paths), {"nextflow/MaveDB/main.nf", "nextflow/ProteinFunction/main.nf"}
        )

    def test_push_handles_added_and_deleted_paths(self):
        before = self.commit("nextflow/MaveDB/old.py", "Old\n")
        (self.repo / "nextflow/MaveDB/old.py").unlink()
        self.git("add", "-u")
        after = self.commit("nextflow/liftover/new.py", "New\n")
        paths = self.changes("push", {"before": before, "after": after})
        self.assertEqual(
            set(paths), {"nextflow/MaveDB/old.py", "nextflow/liftover/new.py"}
        )

    def test_rename_across_pipelines_selects_both(self):
        before = self.commit("nextflow/MaveDB/old.py", "Identical content\n")
        target = self.repo / "nextflow/ProteinFunction/new.py"
        target.parent.mkdir(parents=True)
        (self.repo / "nextflow/MaveDB/old.py").rename(target)
        self.git("add", "-A")
        self.git("commit", "-q", "-m", "Rename")
        paths = self.changes(
            "push", {"before": before, "after": self.git("rev-parse", "HEAD")}
        )
        self.assertEqual(
            selector.select_paths(paths, CONFIG)["python_pipelines"],
            ["MaveDB", "ProteinFunction"],
        )

    def test_new_branch_push_runs_all(self):
        self.assertIsNone(
            self.changes("push", {"before": "0" * 40, "after": self.base})
        )

    def test_merge_queue_uses_its_base_and_head(self):
        head = self.commit("nextflow/Remapping/main.nf", "Queue change\n")
        paths = self.changes(
            "merge_group", {"merge_group": {"base_sha": self.base, "head_sha": head}}
        )
        self.assertEqual(paths, ["nextflow/Remapping/main.nf"])

    def test_invalid_or_missing_refs_fail_instead_of_selecting_nothing(self):
        with self.assertRaises(ValueError):
            self.changes("push", {"before": "--help", "after": self.base})
        with self.assertRaises(subprocess.CalledProcessError):
            self.changes("push", {"before": "f" * 40, "after": self.base})
        with self.assertRaises(ValueError):
            self.changes("unknown", {})

    def test_cli_writes_compact_outputs_and_summary(self):
        head = self.commit("nextflow/MaveDB/main.nf", "Change\n")
        event = self.repo / "event.json"
        output = self.repo / "output.txt"
        summary = self.repo / "summary.md"
        event.write_text(json.dumps({"before": self.base, "after": head}))
        process = subprocess.run(
            [
                sys.executable,
                str(SELECTOR),
                "--event-name",
                "push",
                "--event-path",
                str(event),
                "--output",
                str(output),
            ],
            cwd=self.repo,
            capture_output=True,
            text=True,
            env={**os.environ, "GITHUB_STEP_SUMMARY": str(summary)},
        )
        self.assertEqual(process.returncode, 0, process.stderr)
        self.assertEqual(
            output.read_text(),
            'python_pipelines=["MaveDB"]\nexternal_pipelines=[]\nuncovered_pipelines=[]\n',
        )
        self.assertIn(
            "Selected test suites for this workflow: MaveDB.", summary.read_text()
        )

    def test_cli_diff_failure_is_nonzero_and_does_not_write_outputs(self):
        event = self.repo / "event.json"
        output = self.repo / "output.txt"
        event.write_text(json.dumps({"before": "f" * 40, "after": self.base}))
        process = subprocess.run(
            [
                sys.executable,
                str(SELECTOR),
                "--event-name",
                "push",
                "--event-path",
                str(event),
                "--output",
                str(output),
            ],
            cwd=self.repo,
            capture_output=True,
            text=True,
        )
        self.assertNotEqual(process.returncode, 0)
        self.assertFalse(output.exists())
        self.assertIn("Unable to select pipeline tests", process.stderr)


class AggregateGateTests(unittest.TestCase):
    """Run the actual workflow gate under the runner's fail-fast Bash semantics."""

    @classmethod
    def setUpClass(cls):
        workflow = (ROOT / ".github/workflows/pipeline_tests.yml").read_text()
        gate = workflow.split("  pipeline-tests:\n", 1)[1]
        lines = gate.split("        run: |\n", 1)[1].splitlines()
        block = []
        for line in lines:
            if line.strip() and not line.startswith("          "):
                break
            block.append(line)
        cls.script = textwrap.dedent("\n".join(block))
        if not cls.script.strip():
            raise AssertionError("Aggregate gate script is missing")

    def gate(self, **overrides):
        environment = {
            "CHANGES_RESULT": "success",
            "PYTHON_PIPELINES": '["MaveDB"]',
            "PYTHON_RESULT": "success",
        }
        environment.update(overrides)
        result = subprocess.run(
            ["bash", "--noprofile", "--norc", "-e", "-c", self.script],
            env={**os.environ, **environment},
            capture_output=True,
            text=True,
        )
        return result.returncode == 0

    def test_all_selected_suites_pass(self):
        self.assertTrue(self.gate())

    def test_no_pipeline_changes_allow_skipped_suites(self):
        self.assertTrue(
            self.gate(
                PYTHON_PIPELINES="[]",
                PYTHON_RESULT="skipped",
            )
        )

    def test_failure_cancellation_and_unexpected_skips_fail(self):
        for variable in ["CHANGES_RESULT", "PYTHON_RESULT"]:
            for result in ["failure", "cancelled", "skipped"]:
                with self.subTest(variable=variable, result=result):
                    self.assertFalse(self.gate(**{variable: result}))

    def test_missing_selector_outputs_fail(self):
        for result in ("success", "skipped"):
            with self.subTest(result=result):
                self.assertFalse(self.gate(PYTHON_PIPELINES="", PYTHON_RESULT=result))
        self.assertFalse(
            self.gate(
                CHANGES_RESULT="failure",
                PYTHON_PIPELINES="",
                PYTHON_RESULT="skipped",
            )
        )
        self.assertFalse(self.gate(PYTHON_PIPELINES="", PYTHON_RESULT="skipped"))


if __name__ == "__main__":
    unittest.main()

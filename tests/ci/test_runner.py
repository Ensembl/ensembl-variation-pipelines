"""Check the shared runner's CI boundaries without external dependencies."""

from contextlib import redirect_stderr
import importlib.util
import io
from pathlib import Path
import sys
import unittest
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location(
    "run_pipeline_tests", ROOT / "tests/run_pipeline_tests.py"
)
runner = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(runner)


def passing_test():
    pass


def failing_test():
    raise AssertionError("A pipeline regression")


def unavailable_dependency():
    raise unittest.SkipTest("nextflow executable is unavailable")


class RunnerTests(unittest.TestCase):
    def run_suite(self, tests, *flags):
        suite = unittest.TestSuite(unittest.FunctionTestCase(test) for test in tests)
        self.output = io.StringIO()
        with (
            patch.object(sys, "argv", ["run_pipeline_tests.py", "MaveDB", *flags]),
            patch.object(sys, "path", list(sys.path)),
            patch.object(
                runner,
                "load_config",
                return_value={"pipelines": {"MaveDB": {"runner": "python"}}},
            ),
            patch.object(
                unittest.defaultTestLoader, "discover", return_value=suite
            ) as discover,
            redirect_stderr(self.output),
        ):
            result = runner.main()
        discover.assert_called_once_with(str(ROOT / "nextflow/MaveDB/tests"))
        return result

    def test_passing_suite_succeeds_in_strict_mode(self):
        self.assertEqual(self.run_suite([passing_test], "--strict"), 0)
        self.assertIn("Ran 1 test", self.output.getvalue())
        self.assertIn("OK", self.output.getvalue())

    def test_failed_suite_fails_locally_and_in_ci(self):
        for flags in [(), ("--strict",)]:
            with self.subTest(flags=flags):
                self.assertEqual(self.run_suite([failing_test], *flags), 1)
                self.assertIn("FAILED (failures=1)", self.output.getvalue())

    def test_empty_suite_is_an_error(self):
        with self.assertRaises(SystemExit) as error:
            self.run_suite([])
        self.assertEqual(error.exception.code, 2)
        self.assertIn("No tests found for MaveDB", self.output.getvalue())

    def test_unavailable_dependency_skip_fails_strict_mode(self):
        for tests in [[unavailable_dependency], [passing_test, unavailable_dependency]]:
            with self.subTest(test_count=len(tests)):
                self.assertEqual(self.run_suite(tests, "--strict"), 1)
                self.assertIn("skipped=1", self.output.getvalue())
                self.assertIn(
                    "nextflow executable is unavailable", self.output.getvalue()
                )
                self.assertIn(
                    "CI requires all selected tests to run", self.output.getvalue()
                )

    def test_local_skip_is_allowed_and_visible(self):
        self.assertEqual(self.run_suite([passing_test, unavailable_dependency]), 0)
        self.assertIn("OK (skipped=1)", self.output.getvalue())
        self.assertIn("nextflow executable is unavailable", self.output.getvalue())

    def test_uncovered_or_external_pipeline_cannot_run_as_owned(self):
        for kind in (None, "external"):
            with (
                self.subTest(runner=kind),
                patch.object(sys, "argv", ["run_pipeline_tests.py", "example"]),
                patch.object(
                    runner,
                    "load_config",
                    return_value={"pipelines": {"example": {"runner": kind}}},
                ),
                redirect_stderr(io.StringIO()),
                self.assertRaises(SystemExit) as error,
            ):
                runner.main()
            self.assertEqual(error.exception.code, 2)

    def test_broken_suite_registration_fails_before_running(self):
        with (
            patch.object(runner, "load_config", side_effect=ValueError("empty suite")),
            redirect_stderr(io.StringIO()) as output,
            self.assertRaises(SystemExit) as error,
        ):
            runner.main()
        self.assertEqual(error.exception.code, 2)
        self.assertIn("Unable to load pipeline suites: empty suite", output.getvalue())


if __name__ == "__main__":
    unittest.main()

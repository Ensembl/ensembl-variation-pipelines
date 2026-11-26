#!/usr/bin/env python3
"""Select pipeline test suites from the complete GitHub event diff.

Path rules ending in / match a directory recursively; other rules match one
file exactly. Pull requests use the merge base, including every commit in the
PR. Disabling rename detection includes both the old and new pipeline paths.
"""

import argparse
import json
import os
from pathlib import Path
import re
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[2]


def load_config(root=ROOT):
    """Discover pipeline-local suites, preserving existing workflow ownership.

    A pipeline without tests/suite.json stays visible as a coverage gap. Shared
    dependency rules may mention pipelines that have not landed in main yet.
    """
    root = Path(root)
    config = json.loads((root / ".github/pipeline-tests.json").read_text())
    pipelines = config["pipelines"]
    for name, entry in pipelines.items():
        if entry["runner"] != "external":
            raise ValueError(f"{name}: owned suites belong in tests/suite.json")
        if not (root / entry["workflow"]).is_file():
            raise ValueError(f"{name}: owning workflow is missing")
    for main in sorted((root / "nextflow").glob("*/main.nf")):
        name = main.parent.name
        marker = main.parent / "tests/suite.json"
        if name in pipelines:
            if marker.exists():
                raise ValueError(f"{name}: suite also belongs to an external workflow")
            continue
        entry = {"runner": None, "paths": [f"nextflow/{name}/"]}
        if marker.exists():
            suite = json.loads(marker.read_text())
            if suite != {"runner": "python"}:
                raise ValueError(f"{name}: suite.json must contain only runner: python")
            if not list(marker.parent.glob("test_*.py")):
                raise ValueError(f"{name}: registered suite has no test_*.py files")
            entry["runner"] = "python"
        elif list(marker.parent.glob("test_*.py")):
            raise ValueError(f"{name}: Python tests exist without tests/suite.json")
        pipelines[name] = entry
    for marker in (root / "nextflow").glob("*/tests/suite.json"):
        if not (marker.parent.parent / "main.nf").is_file():
            raise ValueError(f"{marker}: suite has no pipeline main.nf")
    config["pipelines"] = dict(sorted(pipelines.items()))
    return config


def matches(path, rules):
    return any(
        path.startswith(rule) if rule.endswith("/") else path == rule for rule in rules
    )


def select_paths(paths, config, all_suites=False):
    """Select owned suites and report pipelines managed by another workflow."""
    pipelines = config["pipelines"]
    selected = set()
    unconfigured = set()
    for path in paths:
        selected.update(
            name for name, entry in pipelines.items() if matches(path, entry["paths"])
        )
        for shared in config["shared_paths"]:
            if matches(path, shared["paths"]):
                selected.update(shared["pipelines"])
        parts = path.split("/", 2)
        if (
            len(parts) == 3
            and parts[0] == "nextflow"
            and parts[1] not in pipelines
            and not any(
                matches(path, shared["paths"]) for shared in config["shared_paths"]
            )
        ):
            unconfigured.add(parts[1])
        if matches(path, config["all_suite_paths"]):
            all_suites = True
    if all_suites:
        selected.update(
            name for name, entry in pipelines.items() if entry["runner"] == "python"
        )
    return {
        "python_pipelines": [
            name
            for name, entry in pipelines.items()
            if name in selected and entry["runner"] == "python"
        ],
        "external_pipelines": [
            name
            for name, entry in pipelines.items()
            if name in selected and entry["runner"] == "external"
        ],
        "uncovered_pipelines": [
            name
            for name, entry in pipelines.items()
            if name in selected and entry["runner"] is None
        ]
        + sorted(unconfigured),
    }


def git(*args, cwd=None):
    return subprocess.run(
        ["git", *args],
        cwd=cwd,
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    ).stdout


def sha(value):
    if not isinstance(value, str) or not re.fullmatch(
        r"[0-9a-fA-F]{40}|[0-9a-fA-F]{64}", value
    ):
        raise ValueError(f"Expected a full Git SHA, got {value!r}")
    return value


def read_changed_paths(event_name, event, cwd=None):
    """Return changed paths, or None when all suites should run."""
    if event_name == "workflow_dispatch":
        return None
    if event_name == "pull_request":
        base = sha(event["pull_request"]["base"]["sha"])
        head = sha(event["pull_request"]["head"]["sha"])
        base = git("merge-base", base, head, cwd=cwd).decode().strip()
    elif event_name == "push":
        base, head = sha(event["before"]), sha(event["after"])
        if set(base) == {"0"}:
            return None
    elif event_name == "merge_group":
        base = sha(event["merge_group"]["base_sha"])
        head = sha(event["merge_group"]["head_sha"])
    else:
        raise ValueError(f"Unsupported GitHub event: {event_name}")
    result = git("diff", "--name-only", "-z", "--no-renames", base, head, "--", cwd=cwd)
    return [os.fsdecode(path) for path in result.split(b"\0") if path]


def summary(selection):
    suites = selection["python_pipelines"]
    text = "Pipeline test selection\n\n"
    text += f"Selected test suites for this workflow: {', '.join(suites) or 'none'}.\n"
    if selection["external_pipelines"]:
        text += (
            "\nChanged pipelines managed by their existing workflows: "
            + ", ".join(selection["external_pipelines"])
            + ". This workflow does not run these suites.\n"
        )
    if selection["uncovered_pipelines"]:
        text += (
            "\nChanged pipelines without a standalone test suite: "
            + ", ".join(selection["uncovered_pipelines"])
            + ".\n"
        )
    return text


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--event-path", required=True, type=Path)
    parser.add_argument("--event-name", required=True)
    parser.add_argument("--output", type=Path, help="Append outputs to GITHUB_OUTPUT")
    parser.add_argument(
        "--all", action="store_true", help="Run all suites owned by this workflow"
    )
    args = parser.parse_args()
    try:
        config = load_config()
        event = json.loads(args.event_path.read_text())
        paths = read_changed_paths(args.event_name, event)
        selection = select_paths(
            paths or [], config, all_suites=args.all or paths is None
        )
        if args.output:
            with args.output.open("a") as handle:
                for name, value in selection.items():
                    handle.write(f"{name}={json.dumps(value, separators=(',', ':'))}\n")
        report = summary(selection)
        print(report)
        if os.environ.get("GITHUB_STEP_SUMMARY"):
            with open(os.environ["GITHUB_STEP_SUMMARY"], "a") as handle:
                handle.write(report)
    except (
        OSError,
        ValueError,
        KeyError,
        TypeError,
        subprocess.CalledProcessError,
    ) as error:
        print(f"Unable to select pipeline tests: {error}", file=sys.stderr)
        if isinstance(error, subprocess.CalledProcessError):
            print(os.fsdecode(error.stderr).strip(), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())

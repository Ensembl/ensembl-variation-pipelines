from hashlib import file_digest
from pathlib import Path
import subprocess
from typing import Iterable


def git_revision(directory: str | Path) -> str | None:
    try:
        result = subprocess.run(
            ["git", "-C", str(directory), "rev-parse", "HEAD"],
            capture_output=True, text=True, timeout=10, check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    return result.stdout.strip() if result.returncode == 0 else None


def input_file_provenance(paths: Iterable[str | Path]) -> dict[str, list[str]]:
    file_names: list[str] = []
    file_checksums: list[str] = []
    for path in paths:
        path = Path(path)
        with path.open("rb") as handle:
            checksum = file_digest(handle, "sha256").hexdigest()
        file_names.append(path.name)
        file_checksums.append(checksum)
    if not file_names:
        raise ValueError("An import run requires at least one input file")
    return {"file_names": file_names, "file_checksums": file_checksums}

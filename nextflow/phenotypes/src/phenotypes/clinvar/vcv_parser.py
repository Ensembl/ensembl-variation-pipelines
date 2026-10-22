"""Map ClinVar Allele IDs to their own versioned VCV accessions.

Only a direct SimpleAllele in ClassifiedRecord or IncludedRecord identifies
the archive's individual variant. Nested haplotype members and SCV alleles
must not inherit the enclosing archive's accession or version.
"""

from __future__ import annotations

import gzip
import re
import sys
import xml.etree.ElementTree as ET
from pathlib import Path
from time import monotonic


def _report_progress(n_records: int, started: float, stage: str) -> None:
    hours, remainder = divmod(int(monotonic() - started), 3600)
    minutes, seconds = divmod(remainder, 60)
    print(
        f"[Parsing ClinVar VCV] {stage}: {n_records:,} records processed; "
        f"elapsed {hours:02d}:{minutes:02d}:{seconds:02d}",
        file=sys.stderr, flush=True,
    )


def read_component_accessions(
    path: str | Path,
) -> tuple[dict[str, str | None], str | None]:
    """Return Allele ID -> accession.version, plus the VCV release date.

    None marks an ambiguous mapping rather than choosing a record by file
    order. Included-only variants are valid identifiers even when they have no
    independent classification. XML trees are discarded after each archive;
    only the identifier dictionary remains in memory.
    """
    path = Path(path)
    accessions: dict[str, str | None] = {}
    started = monotonic()
    n_records = 0
    root = None
    release_date = None
    print(f"[Parsing ClinVar VCV] Starting: {path}", file=sys.stderr, flush=True)
    with (gzip.open(path, "rb") if path.suffix.lower() == ".gz" else path.open("rb")) as handle:
        for event, element in ET.iterparse(handle, events=("start", "end")):
            if root is None:
                if element.tag not in {"ClinVarVariationRelease", "ClinVarResult-Set"}:
                    raise ValueError(f"Expected ClinVar VCV XML root, found {element.tag}")
                root = element
                release_date = root.get("ReleaseDate")
            if event != "end" or element.tag != "VariationArchive":
                continue
            n_records += 1
            status = element.findtext("RecordStatus", default="").strip().casefold()
            allele = element.find("ClassifiedRecord/SimpleAllele")
            if allele is None:
                allele = element.find("IncludedRecord/SimpleAllele")
            accession = element.get("Accession", "")
            version = element.get("Version", "")
            variation_id = element.get("VariationID", "")
            if (
                status in {"", "current"}
                and allele is not None
                and allele.get("AlleleID")
                and allele.get("VariationID") == variation_id
                and re.fullmatch(r"VCV[0-9]{9,}", accession)
                and re.fullmatch(r"[1-9][0-9]*", version)
                and re.fullmatch(r"[1-9][0-9]*", variation_id)
                and int(accession[3:]) == int(variation_id)
            ):
                allele_id = allele.get("AlleleID")
                versioned = f"{accession}.{version}"
                previous = accessions.get(allele_id, versioned)
                accessions[allele_id] = versioned if previous == versioned else None
            element.clear()
            root.clear()
            if n_records % 100_000 == 0:
                _report_progress(n_records, started, "Progress")
    if root is None:
        raise ValueError("No ClinVar VCV XML root found")
    _report_progress(n_records, started, "Complete")
    return accessions, release_date

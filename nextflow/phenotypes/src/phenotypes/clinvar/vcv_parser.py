"""Read ClinVar VCV XML
- Keep the record versions and mappings needed by the RCV parser to match
submitted condition names and identifiers to ClinVar's aggregate conditions
"""

import gzip
import sys
import xml.etree.ElementTree as ET
from pathlib import Path
from time import monotonic


PROGRESS_INTERVAL = 100_000


def format_elapsed(started):
    hours, remainder = divmod(int(monotonic() - started), 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"


def normalise_name(value):
    return " ".join(value.split()).casefold()


def normalise_accession(database, value):
    """Build comparison keys only; preserve source identifiers in exported data."""
    if database.casefold() == "medgen":
        return value.upper().removeprefix("MEDGEN:")
    return value.removeprefix(database + ":")


def is_current(status):
    status = (status or "").strip()
    return not status or status == "current"


def mapping_key(kind, reference, value):
    if kind == "Name":
        value = normalise_name(value)
    else:
        value = normalise_accession(reference, value)
    return kind, reference.casefold(), value


def trait_keys(trait):
    result = set()
    for name in trait.findall("Name/ElementValue"):
        if name.text and name.text.strip():
            result.add(mapping_key("Name", name.get("Type", ""), name.text.strip()))
    for path in ("XRef", "Name/XRef", "Symbol/XRef", "AttributeSet/XRef"):
        for xref in trait.findall(path):
            database, identifier = xref.get("DB"), xref.get("ID")
            if database and identifier:
                result.add(mapping_key("XRef", database, identifier))
    return result


class VcvMappings:
    def __init__(self, path):
        self.path = Path(path)
        self.release_date = None
        self.n_records = 0
        self._rcv_versions = set()
        self._mappings = {}

    def __enter__(self):
        self._build()
        return self

    def __exit__(self, *args):
        self._rcv_versions.clear()
        self._mappings.clear()

    def _progress(self, stage, started):
        print(f"[Parsing ClinVar VCV] {stage}: {self.n_records:,} records processed; "
              f"elapsed {format_elapsed(started)}",
              file=sys.stderr, flush=True)

    def _build(self):
        started = monotonic()
        print(f"[Parsing ClinVar VCV] Starting: {self.path}", file=sys.stderr, flush=True)
        opener = gzip.open if self.path.suffix.lower() == ".gz" else open
        root = None
        with opener(self.path, "rb") as stream:
            for event, element in ET.iterparse(stream, events=("start", "end")):
                if root is None and event == "start":
                    if element.tag not in {"ClinVarVariationRelease", "ClinVarResult-Set"}:
                        raise ValueError(f"Expected VCV XML root, found {element.tag}")
                    root = element
                    self.release_date = root.get("ReleaseDate")
                    continue
                if event != "end" or element.tag != "VariationArchive":
                    continue
                self.n_records += 1
                if is_current(element.findtext("RecordStatus")):
                    self._add_record(element)
                element.clear()
                root.clear()
                if self.n_records % PROGRESS_INTERVAL == 0:
                    self._progress("Progress", started)
            if root is None:
                raise ValueError("No VCV XML root found")
        self._progress("Complete", started)

    def _add_record(self, archive):
        vcv = archive.get("Accession")
        classified = archive.find("ClassifiedRecord")
        if not vcv or classified is None:
            return
        for accession in classified.findall("RCVList/RCVAccession"):
            identifier, version = accession.get("Accession"), accession.get("Version")
            if identifier and version:
                self._rcv_versions.add((vcv, f"{identifier}.{version}"))

        scvs = {}
        for assertion in classified.findall("ClinicalAssertionList/ClinicalAssertion"):
            if not is_current(assertion.findtext("RecordStatus")):
                continue
            accession = assertion.find("ClinVarAccession")
            if (accession is not None and accession.get("Accession")
                    and accession.get("Version") and assertion.get("ID")):
                identifier, version = accession.get("Accession"), accession.get("Version")
                scvs[assertion.get("ID")] = f"{identifier}.{version}"
        for mapping in classified.findall("TraitMappingList/TraitMapping"):
            assertion_id = mapping.get("ClinicalAssertionID")
            scv = scvs.get(assertion_id)
            medgen = mapping.find("MedGen")
            kind, reference, value = (mapping.get(attr, "") for attr in
                                      ("MappingType", "MappingRef", "MappingValue"))
            if not scv or medgen is None or kind not in {"Name", "XRef"} or not value:
                continue
            kind, reference, value = mapping_key(kind, reference, value)
            key = (vcv, scv, kind, reference, value)
            self._mappings.setdefault(key, []).append(
                (mapping.get("TraitType", ""), medgen.get("CUI"), medgen.get("Name"))
            )

    def has_rcv_version(self, vcv, rcv):
        if not vcv:
            return False
        return (vcv.split(".")[0], rcv) in self._rcv_versions

    def find(self, vcv, scv, trait):
        matches = set()
        for kind, reference, value in trait_keys(trait):
            rows = self._mappings.get((vcv.split(".")[0], scv, kind, reference, value), ())
            for trait_type, cui, name in rows:
                if trait_type and trait.get("Type") and trait_type != trait.get("Type"):
                    continue
                matches.add((cui, name))
        return sorted(matches, key=lambda row: tuple(value or "" for value in row))

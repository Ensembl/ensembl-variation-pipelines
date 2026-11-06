"""Stream ClinVar release headers and records with bounded XML memory."""

import xml.etree.ElementTree as ET
from collections.abc import Iterator
from typing import BinaryIO


def iter_release(
    stream: BinaryIO, release_type: str
) -> Iterator[tuple[str, ET.Element]]:
    """Yield a validated release header, then records in source order.

    Events are ``release`` and ``record``. Elements are borrowed: consume or
    copy their evidence before advancing, because records are cleared afterward.
    The caller owns the stream and may open compressed input with ``open_xml``.
    """
    if release_type == "RCV":
        root_tags = {"ReleaseSet"}
        record_tag = "ClinVarSet"
        expected = "ReleaseSet"
    elif release_type == "VCV":
        root_tags = {"ClinVarVariationRelease", "ClinVarResult-Set"}
        record_tag = "VariationArchive"
        expected = "VCV XML"
    else:
        raise ValueError("ClinVar release type must be RCV or VCV")
    root = None
    for event, element in ET.iterparse(stream, events=("start", "end")):
        if root is None and event == "start":
            if element.tag not in root_tags:
                raise ValueError(f"Expected {expected} root, found {element.tag}")
            if release_type == "RCV" and not element.get("Dated"):
                raise ValueError("ReleaseSet is missing required Dated attribute")
            root = element
            yield "release", element
            continue
        if event == "end" and element.tag == record_tag:
            try:
                yield "record", element
            finally:
                element.clear()
                root.clear()
    if root is None:
        raise ValueError(f"No {expected} root found")

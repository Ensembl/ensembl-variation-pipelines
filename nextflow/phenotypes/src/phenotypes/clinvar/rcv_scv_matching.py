"""Match submitted conditions to the conditions in a ClinVar RCV record
- Keep a unique direct match when available
- Otherwise use the supplied VCV mappings to find a single supported match, 
leaving uncertain matches unresolved
"""

from phenotypes.clinvar.vcv_parser import normalise_name


class SubmissionMatching:
    def __init__(self, vcv=None):
        self.vcv = vcv
        self.rcv = None
        self.vcv_accession = None
        self.warnings = []
        self._has_rcv = False

    def start_rcv(self, accession, n_traits, vcv_accession=None, *, match_single_trait=False):
        self.rcv = accession
        self.vcv_accession = vcv_accession
        self.warnings = []
        self._has_rcv = bool(self.vcv is not None and (n_traits > 1 or match_single_trait)
                             and self.vcv.has_rcv_version(vcv_accession, accession))

    def match(self, direct_matches, aggregate_traits, scv, *, submission_trait):
        if len(direct_matches) == 1 or self.vcv is None:
            return direct_matches
        if not self._has_rcv:
            warning = ("vcv_rcv_version_unavailable", scv,
                       f"{self.rcv} not present under {self.vcv_accession} in supplied VCV XML")
            if warning not in self.warnings:
                self.warnings.append(warning)
            return direct_matches

        mappings = self.vcv.find(self.vcv_accession, scv, submission_trait)
        if not mappings:
            return direct_matches
        matches = set()
        for cui, name in mappings:
            identifier = f"MedGen:{cui}" if cui else None
            targets = {i for i, trait in enumerate(aggregate_traits)
                       if identifier and identifier in trait["match_accessions"]}
            if not targets and name:
                targets = {i for i, trait in enumerate(aggregate_traits)
                           if normalise_name(name) in trait["match_names"]}
            matches.update(targets)
        if len(matches) == 1 and (not direct_matches or matches <= direct_matches):
            return matches
        return direct_matches

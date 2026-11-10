# Feature contract review

Reviewed against the draft, unverified [Variant and allele synonyms data store
feature brief](https://app.notion.com/p/769d85c5b7934e4ab15affb3aaa73d8d)
and its [parent design page](https://app.notion.com/p/407e86b0260880c8b09deeb1915bcd0b)
on 3 November 2026. The brief leaves exact schema fields, cardinalities, matching
rules, production format and validation thresholds undecided. The following
distinguishes current behavior from proposed implementation work.

## Corrections to the earlier single-table design

- Initial source scope is human ClinVar, dbSNP and PharmGKB. UniProt is a later
  source. A ClinVar dbSNP cross-reference is not a replacement for dbSNP ingestion.
- An rsID is an external identifier, not the internal primary key for every
  variant or allele. VCV accessions and ClinVar allele IDs are also external IDs.
- Variant-level and allele-level synonym associations need distinct targets.
  A reported rsID does not establish allele equivalence; retain allele-specific
  HGVS/SPDI and component ownership as matching evidence.
- Source-only, unmatched and ambiguous assertions must survive consolidation.
  An unresolved identifier is not intrinsically an invalid source record, and
  ambiguity is not an instruction to select the first candidate.
- Historical status and resolution outcome describe different things. Preserve
  reported statuses and merge history separately from whether an internal match
  exists. ClinVar record status alone cannot supply a dbSNP merge chain.
- Source loaders produce independent artifacts. A consolidation stage owns
  writes to a build datastore; independent loaders do not concurrently mutate
  a shared DuckDB file.

The earlier nine-column schema remains useful as a possible consumer-facing
projection, but it cannot be the authoritative store: it has no separate
internal variant/allele relationships, resolution state or source assertion key.
In particular, `variant_name` is a display identifier, not a universal rsID key.
Projection rules for unresolved records and allele targets still need agreement.
`source_name` must distinguish identifier namespace from evidence provider;
the existing adapter retains both. Mapping type and somatic status must come
from evidence or agreed policy, not defaults inferred from an identifier.

## Current source-evidence contract

The consumer reads shared ClinVar JSONL and preserves the shared release,
assembly context and raw-file checksums in the output metadata. It does not
parse XML again. Shared streaming, configuration and provenance remain in
`nextflow/shared`; synonym semantics remain in this consumer.

Each alias includes its reported value, namespace, annotation types and an
explicit resolution envelope:

```json
{
  "value": "rs121908247",
  "type": "dbSNP",
  "source": "dbSNP",
  "reported_types": [],
  "resolution": {
    "state": "source_only",
    "variant_id": null,
    "variant_allele_id": null
  }
}
```

The enclosing `source_report` identifies ClinVar, its source version and RCV/SCV
evidence. The example asserts that ClinVar reported an rsID; it does not assert
a match to an internal variant or allele, nor claim the ClinVar release is a
dbSNP release. `source_only` means matching has not been attempted, rather than
that a matching attempt found zero candidates. Internal ID fields are reserved
and remain null in this adapter. The final datastore's key types are undecided.

The inherited `schema_version` identifies shared evidence compatibility.
`synonym_schema_version: 1` separately identifies this consumer's evidence
contract. `artifact_kind: source_evidence` makes its build-stage role explicit.
These fields are additive to the existing output fields and channel contracts.

The adapter preserves separate observations across RCV reports and keeps
haplotype member aliases on their member. It does not assert that every alias
on a source entity has the same eventual internal target or target level.
HGVS/SPDI are reported evidence; they are not generated or normalized here.
No internal ID is derived from an rsID, source accession or coordinate string.

Domain-invalid source entities are written to `rejections.jsonl` with reason,
source release and the complete original evidence record, allowing later
review/reprocessing. Valid source-only entities remain in the main output.
Malformed JSON or incompatible file headers still fail the artifact conversion;
a partially written output must not be treated as a completed release.

The shared source parser may retain structural evidence for other consumers.
Its presence in an extraction artifact does not make structural-variant aliases
eligible for this datastore's initial release. Release-scope classification and
counts must be explicit in the future consolidation step; do not silently drop
these observations or narrow the shared parser for this consumer.

## Acceptance criteria and gaps

| Brief requirement | Current implementation | Remaining work |
| --- | --- | --- |
| Separate Variant and VariantAllele identities and associations | External variant IDs, allele evidence and component ownership retained; internal IDs stay null | Agree internal entities, schema keys, association cardinalities and integrity constraints |
| Source, identifier, release/report and resolution provenance | ClinVar evidence adapter supplies these, with source-only states | Define the cross-source assertion/report model and implement dbSNP and PharmGKB adapters |
| Matched, unmatched, source-only, historical and ambiguous cases retained | Source-only and reported historical/overlapping assertions retained | Implement matching outcomes, candidate evidence and dbSNP merge/archive history without guessing matches |
| Automated tests across all three sources and matching rules | ClinVar Python and Nextflow extraction tests | dbSNP/PharmGKB fixtures and the agreed variant/allele resolution cases |
| Release candidate completeness, uniqueness, mapping and integrity validation | Extraction counts, resolution-state counts and rejections | Candidate-wide validation, scope accounting and agreed blocking thresholds |
| Immutable releases, current selection and rollback | Not implemented in this checkout | Choose delivery format and implement build/promotion/selection/rollback |
| Lookup returns entities, aliases and evidence | Not implemented | Agree and implement a lookup contract for VEP and the feature browser |

RCV extraction alone does not supply VCV-only ClinVar coverage. Downloads may
resolve both RCV and VCV release files, but only RCV evidence is currently parsed
by this workflow. Completeness checks must account for this limitation.

## Consolidation and release plan

1. Agree Source/SourceReport, Variant/VariantAllele and their separate synonym
   association schemas. Preserve each provider assertion, identifier namespace,
   source release/report, genome context and source entity/component ownership.
2. Add dbSNP and PharmGKB acquisition/adapters. Pin source releases and record
   checksums. Keep raw evidence and parsed artifacts independently reproducible.
3. Consolidate source artifacts in one build stage. The user requested DuckDB
   for staging; retain that build direction while the brief's engine decision
   and production delivery format await agreement. Do not couple consumers to
   DuckDB as the production format.
4. Resolve against supplied internal identities using agreed normalization and
   matching rules. Retain candidate sets and evidence for ambiguous matches;
   distinguish no attempted match from an attempted match with no candidates.
   Keep variant and allele matches separate, and do not equate coordinate-only
   observations across assemblies or infer missing allele sequence.
5. Validate input completeness, accounted source/rejection/scope outcomes,
   assertion uniqueness, mapping outcomes and internal references. Check allele
   ownership against its parent variant. Unresolved states should be reported
   and governed by agreed release thresholds, rather than always rejected.
6. Publish a new immutable version with input manifests, tool/configuration
   versions and validation results. After validation, select that completed
   version as current through a separately controlled pointer. Rollback should
   select a previous completed version, retaining both artifacts and audit
   history. Pointer storage, atomicity and the operational procedure depend on
   the chosen delivery format and are not implemented yet.
7. Define lookup inputs, including identifier namespace, release, assembly and
   variant-versus-allele filters. Return the resolution outcome, all applicable
   candidates and source/release evidence; never silently return one ambiguous
   match. For a resolved entity, return its appropriate variant- or allele-level
   aliases with the evidence behind each link. Keep unresolved evidence queryable.

None of the open choices above is an implemented production API or an agreed
database schema. Record the team's decisions before freezing those interfaces.

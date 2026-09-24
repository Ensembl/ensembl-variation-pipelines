-- Phenotype datastore schema
-- Model: variation-datastore-models/phenotypes (ee376ea683b8c46fc33e532cbd7e720a6d3e7c75)

BEGIN;

CREATE TABLE entity (
    entity_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    identifier TEXT NOT NULL UNIQUE,
    type TEXT NOT NULL,
    label TEXT NOT NULL
);

CREATE TABLE ontology_term (
    ontology_term_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    accession TEXT NOT NULL UNIQUE,
    label TEXT NOT NULL,
    source TEXT NOT NULL,
    url TEXT
);

CREATE TABLE source (
    source_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    description TEXT,
    url TEXT
);

CREATE TABLE import_run (
    import_run_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_version TEXT,
    download_url TEXT,
    download_date DATE,
    ingestion_date DATE NOT NULL,
    pipeline_version TEXT
);

CREATE TABLE publication (
    publication_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    identifier TEXT NOT NULL,
    source TEXT NOT NULL,
    url TEXT,
    UNIQUE (source, identifier)
);

CREATE TABLE phenotype (
    phenotype_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name TEXT NOT NULL,
    primary_ontology_term_id BIGINT REFERENCES ontology_term (ontology_term_id) ON DELETE RESTRICT
);

CREATE TABLE phenotype_measurement (
    phenotype_id BIGINT PRIMARY KEY REFERENCES phenotype (phenotype_id) ON DELETE CASCADE,
    measurement_type TEXT NOT NULL,
    measured_entity_id BIGINT NOT NULL REFERENCES entity (entity_id) ON DELETE RESTRICT
);

CREATE TABLE entity_component (
    entity_component_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    parent_entity_id BIGINT NOT NULL REFERENCES entity (entity_id) ON DELETE CASCADE,
    component_entity_id BIGINT NOT NULL REFERENCES entity (entity_id) ON DELETE RESTRICT,
    allele TEXT,
    ordinal INTEGER CHECK (ordinal > 0),
    CHECK (parent_entity_id <> component_entity_id),
    UNIQUE (parent_entity_id, ordinal)
);

CREATE TABLE genome_context (
    genome_context_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    entity_id BIGINT NOT NULL REFERENCES entity (entity_id) ON DELETE CASCADE,
    species_taxon_id INTEGER NOT NULL,
    species_scientific_name TEXT NOT NULL,
    organism_id TEXT,
    organism_display_name TEXT,
    genome_uuid TEXT
);

CREATE TABLE entity_location (
    entity_location_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    genome_context_id BIGINT NOT NULL REFERENCES genome_context (genome_context_id) ON DELETE CASCADE,
    seq_region_name TEXT NOT NULL,
    seq_region_start BIGINT NOT NULL,
    seq_region_end BIGINT NOT NULL,
    seq_region_strand INTEGER CHECK (seq_region_strand IN (-1, 1)),
    reference_allele TEXT,
    alternate_alleles TEXT[] NOT NULL DEFAULT '{}',
    CHECK (
        seq_region_start >= 1
        AND seq_region_end >= 0
        AND seq_region_end >= seq_region_start - 1
    )
);

CREATE TABLE phenotype_association (
    phenotype_association_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    phenotype_id BIGINT NOT NULL REFERENCES phenotype (phenotype_id) ON DELETE RESTRICT,
    entity_id BIGINT NOT NULL REFERENCES entity (entity_id) ON DELETE RESTRICT,
    somatic_status TEXT NOT NULL CHECK (somatic_status IN ('germline', 'somatic')),
    UNIQUE (entity_id, phenotype_id, somatic_status)
);

CREATE TABLE source_report (
    source_report_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    phenotype_association_id BIGINT NOT NULL REFERENCES phenotype_association (phenotype_association_id) ON DELETE CASCADE,
    source_id BIGINT NOT NULL REFERENCES source (source_id) ON DELETE RESTRICT,
    import_run_id BIGINT NOT NULL REFERENCES import_run (import_run_id) ON DELETE RESTRICT,
    source_accession TEXT,
    reported_phenotype_name TEXT NOT NULL,
    reported_genes TEXT[] NOT NULL DEFAULT '{}',
    reported_allele TEXT,
    comparison_allele TEXT,
    allele_role TEXT,
    reported_relationship_type TEXT,
    reported_biological_context TEXT,
    biological_context_id BIGINT REFERENCES ontology_term (ontology_term_id) ON DELETE RESTRICT
);

CREATE TABLE reported_variant (
    source_report_id BIGINT PRIMARY KEY REFERENCES source_report (source_report_id) ON DELETE CASCADE,
    identifier TEXT,
    assembly TEXT,
    reference_allele TEXT,
    alternate_alleles TEXT[] NOT NULL DEFAULT '{}'
);

CREATE TABLE phenotype_ontology_mapping (
    phenotype_ontology_mapping_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_report_id BIGINT NOT NULL REFERENCES source_report (source_report_id) ON DELETE CASCADE,
    ontology_term_id BIGINT NOT NULL REFERENCES ontology_term (ontology_term_id) ON DELETE RESTRICT,
    mapping_method TEXT
);

CREATE TABLE external_reference (
    external_reference_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_report_id BIGINT NOT NULL REFERENCES source_report (source_report_id) ON DELETE CASCADE,
    accession TEXT NOT NULL,
    database TEXT NOT NULL,
    reference_subject TEXT NOT NULL CHECK (reference_subject IN ('entity', 'phenotype')),
    url TEXT
);

CREATE TABLE phenotype_assessment (
    phenotype_assessment_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_report_id BIGINT NOT NULL REFERENCES source_report (source_report_id) ON DELETE CASCADE,
    assessment_type TEXT NOT NULL,
    assessment_value TEXT NOT NULL,
    assessment_level TEXT CHECK (assessment_level IN ('aggregate', 'submission')),
    review_status TEXT,
    last_evaluated_date DATE,
    source_accession TEXT,
    submitters TEXT[] NOT NULL DEFAULT '{}'
);

CREATE TABLE phenotype_evidence (
    phenotype_evidence_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_report_id BIGINT UNIQUE REFERENCES source_report (source_report_id) ON DELETE CASCADE,
    phenotype_assessment_id BIGINT UNIQUE REFERENCES phenotype_assessment (phenotype_assessment_id) ON DELETE CASCADE,
    ega_accessions TEXT[] NOT NULL DEFAULT '{}',
    CHECK (
        (source_report_id IS NOT NULL) <> (phenotype_assessment_id IS NOT NULL)
    )
);

CREATE TABLE phenotype_statistic (
    phenotype_statistic_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    phenotype_evidence_id BIGINT NOT NULL REFERENCES phenotype_evidence (phenotype_evidence_id) ON DELETE CASCADE,
    statistic_type TEXT NOT NULL,
    value NUMERIC,
    unit TEXT,
    direction TEXT,
    reported_value TEXT
);

CREATE TABLE evidence_publication (
    phenotype_evidence_id BIGINT NOT NULL REFERENCES phenotype_evidence (phenotype_evidence_id) ON DELETE CASCADE,
    publication_id BIGINT NOT NULL REFERENCES publication (publication_id) ON DELETE RESTRICT,
    PRIMARY KEY (phenotype_evidence_id, publication_id)
);

CREATE TABLE phenotype_annotation (
    phenotype_annotation_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_report_id BIGINT UNIQUE REFERENCES source_report (source_report_id) ON DELETE CASCADE,
    phenotype_assessment_id BIGINT UNIQUE REFERENCES phenotype_assessment (phenotype_assessment_id) ON DELETE CASCADE,
    inheritance_types TEXT[] NOT NULL DEFAULT '{}',
    disease_mechanism TEXT,
    allelic_requirement TEXT,
    variation_consequence TEXT,
    CHECK (
        (source_report_id IS NOT NULL) <> (phenotype_assessment_id IS NOT NULL)
    )
);

COMMIT;
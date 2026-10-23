-- Create the tables and relationships for the phenotype datastore.
-- Run in the target schema after it has been created.
-- Model: variation-datastore-models/phenotypes

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
    label TEXT,
    source TEXT NOT NULL
);

CREATE TABLE source (
    source_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    description TEXT,
    url TEXT
);

CREATE TABLE import_run (
    import_run_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_name TEXT NOT NULL,
    source_version TEXT,
    file_names TEXT[] NOT NULL,
    file_checksums TEXT[] NOT NULL,
    pipeline_run_date DATE NOT NULL,
    pipeline_revision TEXT
);

CREATE TABLE publication (
    publication_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    identifier TEXT,
    source TEXT NOT NULL,
    url TEXT,
    UNIQUE (source, identifier)
);

CREATE TABLE phenotype (
    phenotype_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    phenotype_name TEXT NOT NULL
);

-- Harmonisation-selected terms only, not an automatic union of source terms.
-- Derive mapped_terms (labels) and ontology_accessions in the same accession order.
CREATE TABLE phenotype_ontology_term (
    phenotype_id BIGINT NOT NULL REFERENCES phenotype (phenotype_id) ON DELETE CASCADE,
    ontology_term_id BIGINT NOT NULL REFERENCES ontology_term (ontology_term_id) ON DELETE RESTRICT,
    PRIMARY KEY (phenotype_id, ontology_term_id)
);

CREATE TABLE phenotype_measurement (
    phenotype_id BIGINT PRIMARY KEY REFERENCES phenotype (phenotype_id) ON DELETE CASCADE,
    measurement_type TEXT NOT NULL,
    measured_entity_id BIGINT NOT NULL REFERENCES entity (entity_id) ON DELETE RESTRICT
);

CREATE TABLE genome_context (
    genome_context_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    species_taxonomy_id INTEGER NOT NULL,
    species_production_name TEXT NOT NULL,
    assembly TEXT,
    assembly_uuid TEXT,
    genome_uuid TEXT
);

CREATE TABLE entity_genome_context (
    entity_genome_context_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    entity_id BIGINT NOT NULL REFERENCES entity (entity_id) ON DELETE CASCADE,
    genome_context_id BIGINT NOT NULL REFERENCES genome_context (genome_context_id) ON DELETE RESTRICT,
    UNIQUE (entity_id, genome_context_id)
);

CREATE TABLE entity_location (
    entity_location_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    entity_genome_context_id BIGINT NOT NULL REFERENCES entity_genome_context (entity_genome_context_id) ON DELETE CASCADE,
    seq_region_name TEXT NOT NULL,
    seq_region_start BIGINT NOT NULL,
    seq_region_end BIGINT NOT NULL,
    seq_region_strand INTEGER CHECK (seq_region_strand IN (-1, 1)),
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
    somatic_status TEXT NOT NULL CHECK (somatic_status IN ('germline', 'somatic', 'unspecified')),
    UNIQUE (entity_id, phenotype_id, somatic_status)
);

CREATE TABLE source_report (
    source_report_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    phenotype_association_id BIGINT NOT NULL REFERENCES phenotype_association (phenotype_association_id) ON DELETE CASCADE,
    source_id BIGINT NOT NULL REFERENCES source (source_id) ON DELETE RESTRICT,
    import_run_id BIGINT NOT NULL REFERENCES import_run (import_run_id) ON DELETE RESTRICT,
    source_accession TEXT,
    reported_genes TEXT[] NOT NULL DEFAULT '{}',
    reported_allele TEXT,
    reported_comparison_allele TEXT,
    reported_allele_role TEXT,
    reported_relationship_type TEXT,
    reported_biological_context TEXT
);

CREATE TABLE reported_entity (
    reported_entity_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_report_id BIGINT UNIQUE REFERENCES source_report (source_report_id) ON DELETE CASCADE,
    parent_reported_entity_id BIGINT REFERENCES reported_entity (reported_entity_id) ON DELETE CASCADE,
    component_ordinal INTEGER,
    identifier TEXT,
    alternative_identifiers TEXT[] NOT NULL DEFAULT '{}',
    entity_type TEXT NOT NULL,
    label TEXT,
    reported_type TEXT,
    structural_attributes JSONB NOT NULL DEFAULT '{}',
    UNIQUE (parent_reported_entity_id, component_ordinal),
    CHECK (
        (source_report_id IS NOT NULL) <> (parent_reported_entity_id IS NOT NULL)
    )
);

CREATE TABLE reported_entity_location (
    reported_entity_location_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    reported_entity_id BIGINT NOT NULL REFERENCES reported_entity (reported_entity_id) ON DELETE CASCADE,
    assembly TEXT,
    assembly_accession TEXT,
    assembly_status TEXT,
    chromosome TEXT,
    sequence_accession TEXT,
    start BIGINT,
    stop BIGINT,
    display_start BIGINT,
    display_stop BIGINT,
    outer_start BIGINT,
    inner_start BIGINT,
    inner_stop BIGINT,
    outer_stop BIGINT,
    position_vcf BIGINT,
    reference_allele_vcf TEXT,
    alternate_allele_vcf TEXT,
    variant_length BIGINT,
    strand TEXT
);

CREATE TABLE reported_phenotype (
    reported_phenotype_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_report_id BIGINT NOT NULL REFERENCES source_report (source_report_id) ON DELETE CASCADE,
    phenotype_ordinal INTEGER NOT NULL,
    name TEXT NOT NULL,
    UNIQUE (source_report_id, phenotype_ordinal)
);

CREATE TABLE reported_phenotype_ontology_term (
    reported_phenotype_id BIGINT NOT NULL REFERENCES reported_phenotype (reported_phenotype_id) ON DELETE CASCADE,
    ontology_term_id BIGINT NOT NULL REFERENCES ontology_term (ontology_term_id) ON DELETE RESTRICT,
    PRIMARY KEY (reported_phenotype_id, ontology_term_id)
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

// Read and validate source settings, then run the source workflows.

include { CLINVAR } from './clinvar'
include { read_source_config } from '../../shared/lib/source_config'

workflow PHENOTYPES {
    main:
    source_config = read_source_config(params.source_config, "${projectDir}/.venv/bin/python")
    if (source_config.sources.ClinVar.contexts.size() != 1) {
        error('ClinVar loading currently requires one context per source snapshot; multiple contexts must be loaded together before whole-source replacement can support them.')
    }
    database_env = file(params.env_file, checkIfExists: true).toAbsolutePath().toString()
    CLINVAR(source_config.sources.ClinVar, database_env, params.replace)

    emit:
    records = CLINVAR.out.records
    load_summary = CLINVAR.out.load_summary
    warnings = CLINVAR.out.warnings
    rejections = CLINVAR.out.rejections
}

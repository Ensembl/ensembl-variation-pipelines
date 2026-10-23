// Read and validate source settings, then run the source workflows.

include { CLINVAR } from './clinvar'
include { read_source_config } from '../../shared/lib/source_config'

workflow PHENOTYPES {
    main:
    source_config = read_source_config(params.source_config, "${projectDir}/.venv/bin/python")
    CLINVAR(source_config.sources.ClinVar)

    emit:
    records = CLINVAR.out.records
    summary = CLINVAR.out.summary
    warnings = CLINVAR.out.warnings
    rejections = CLINVAR.out.rejections
}

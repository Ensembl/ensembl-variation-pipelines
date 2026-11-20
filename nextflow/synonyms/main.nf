include { CLINVAR_SYNONYMS } from './workflows/clinvar'
include { read_source_config } from '../shared/lib/source_config'

workflow {
    source_config = read_source_config(params.source_config, "${projectDir}/.venv/bin/python")
    CLINVAR_SYNONYMS(source_config.sources.ClinVar)
}

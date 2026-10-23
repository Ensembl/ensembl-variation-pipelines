include { CLINVAR_INPUTS } from './workflows/clinvar_inputs/main'
include { read_source_config } from './lib/source_config'

workflow {
    source_config = read_source_config(params.source_config, "${projectDir}/.venv/bin/python")
    CLINVAR_INPUTS(source_config.sources.ClinVar)

    CLINVAR_INPUTS.out.rcv_xml.view { "RCV: ${it}" }
    CLINVAR_INPUTS.out.vcv_xml.view { "VCV: ${it}" }
}

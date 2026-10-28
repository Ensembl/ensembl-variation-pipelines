include { CLINVAR_INPUTS } from './workflows/clinvar_inputs/main'
include { EXTRACT_CLINVAR } from './modules/local/clinvar/extract/main'
include { read_source_config } from './lib/source_config'
include { source_contents } from './lib/source_files'

workflow {
    source_config = read_source_config(params.source_config, "${projectDir}/.venv/bin/python")
    source_settings = source_config.sources.ClinVar
    CLINVAR_INPUTS(source_settings)

    shared_source = file("${projectDir}/src", checkIfExists: true)
    extraction_inputs = channel.fromList(source_settings.contexts)
        .combine(CLINVAR_INPUTS.out.rcv_xml)
        .map { context, rcv ->
            tuple([id: "clinvar_${context.species_production_name}_${context.assembly}".toString()] + context, rcv, context)
        }
    EXTRACT_CLINVAR(extraction_inputs, shared_source, source_contents(shared_source))

    CLINVAR_INPUTS.out.rcv_xml.view { "RCV: ${it}" }
    CLINVAR_INPUTS.out.vcv_xml.view { "VCV: ${it}" }
}

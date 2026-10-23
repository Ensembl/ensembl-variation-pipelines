include { CLINVAR_INPUTS } from '../../shared/workflows/clinvar_inputs/main'
include { source_contents } from '../../shared/lib/source_files'
include { PARSE_CLINVAR } from '../modules/local/clinvar/phenotypes/main'

workflow CLINVAR {
    take:
    source_settings

    main:
    CLINVAR_INPUTS(source_settings)
    python_source = file("${projectDir}/src", checkIfExists: true)
    shared_source = file("${projectDir}/../shared/src", checkIfExists: true)
    processing_inputs = channel.fromList(source_settings.contexts)
        .combine(CLINVAR_INPUTS.out.rcv_xml)
        .combine(CLINVAR_INPUTS.out.vcv_xml)
        .map { context, rcv, vcv ->
            tuple([id: "clinvar_${context.species_production_name}_${context.assembly}".toString()] + context, rcv, vcv, context)
        }
    // Keep each context and its files together in one task tuple.
    PARSE_CLINVAR(processing_inputs, channel.value(source_settings.source_name), python_source, source_contents(python_source), shared_source, source_contents(shared_source))

    emit:
    rcv_xml = CLINVAR_INPUTS.out.rcv_xml
    vcv_xml = CLINVAR_INPUTS.out.vcv_xml
    // Preserve the established public [processing_context, file] contracts.
    records = PARSE_CLINVAR.out.records.map { meta, records -> tuple(meta.findAll { key, value -> key != 'id' }, records) }
    summary = PARSE_CLINVAR.out.summary.map { meta, summary -> tuple(meta.findAll { key, value -> key != 'id' }, summary) }
    warnings = PARSE_CLINVAR.out.warnings.map { meta, warnings -> tuple(meta.findAll { key, value -> key != 'id' }, warnings) }
    rejections = PARSE_CLINVAR.out.rejections.map { meta, rejections -> tuple(meta.findAll { key, value -> key != 'id' }, rejections) }
}

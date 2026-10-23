// Use local ClinVar RCV/VCV XML files or download the omitted releases.
// Run the parser once for each configured species and assembly context.

include { DOWNLOAD_CLINVAR } from '../modules/local/download_clinvar'
include { PARSE_CLINVAR } from '../modules/local/parse_clinvar'

workflow CLINVAR {
    take:
    source_settings

    main:
    missing_inputs = ['rcv', 'vcv'].findAll { release_type -> !source_settings.inputs[release_type] }
    DOWNLOAD_CLINVAR(channel.fromList(missing_inputs))

    clinvar_rcv = source_settings.inputs.rcv ? channel.value(file(source_settings.inputs.rcv, checkIfExists: true)) :
        DOWNLOAD_CLINVAR.out.xml.filter { release_type, xml -> release_type == 'rcv' }
            .map { release_type, xml -> xml }.first()
    clinvar_vcv = source_settings.inputs.vcv ? channel.value(file(source_settings.inputs.vcv, checkIfExists: true)) :
        DOWNLOAD_CLINVAR.out.xml.filter { release_type, xml -> release_type == 'vcv' }
            .map { release_type, xml -> xml }.first()

    processing_contexts = channel.fromList(source_settings.contexts)
    PARSE_CLINVAR(clinvar_rcv, clinvar_vcv, channel.value(source_settings.source_name), processing_contexts)

    emit:
    rcv_xml = clinvar_rcv
    vcv_xml = clinvar_vcv
    records = PARSE_CLINVAR.out.records
    warnings = PARSE_CLINVAR.out.warnings
    rejections = PARSE_CLINVAR.out.rejections
}

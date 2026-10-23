// Use local ClinVar RCV/VCV XML files or download the omitted releases.
// Parse the configured context, then load the snapshot into PostgreSQL.

include { DOWNLOAD_CLINVAR } from '../modules/local/download_clinvar'
include { PARSE_CLINVAR } from '../modules/local/parse_clinvar'
include { LOAD_CLINVAR } from '../modules/local/load_clinvar'

workflow CLINVAR {
    take:
    source_settings
    env_file
    replace_existing

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
    LOAD_CLINVAR(PARSE_CLINVAR.out.records, channel.value(env_file), channel.value(replace_existing))

    emit:
    rcv_xml = clinvar_rcv
    vcv_xml = clinvar_vcv
    records = PARSE_CLINVAR.out.records
    warnings = PARSE_CLINVAR.out.warnings
    rejections = PARSE_CLINVAR.out.rejections
    load_summary = LOAD_CLINVAR.out.summary
}

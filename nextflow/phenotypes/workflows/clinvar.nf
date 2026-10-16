// Use local ClinVar XML files or download the missing RCV and VCV releases.
// Run the parser once for each configured species and assembly context.

include { DOWNLOAD_CLINVAR } from '../modules/local/download_clinvar'
include { PARSE_CLINVAR } from '../modules/local/parse_clinvar'

workflow CLINVAR {
    take:
    source_settings

    main:
    releases_to_download = []
    if (!source_settings.inputs.rcv) {
        releases_to_download.add('RCV')
    }
    if (!source_settings.inputs.vcv) {
        releases_to_download.add('VCV')
    }
    if (releases_to_download) {
        DOWNLOAD_CLINVAR(channel.fromList(releases_to_download))
    }

    clinvar_rcv = source_settings.inputs.rcv ? channel.value(file(source_settings.inputs.rcv, checkIfExists: true)) :
        DOWNLOAD_CLINVAR.out.xml.filter { release_type, xml -> release_type == 'RCV' }.map { release_type, xml -> xml }.first()
    clinvar_vcv = source_settings.inputs.vcv ? channel.value(file(source_settings.inputs.vcv, checkIfExists: true)) :
        DOWNLOAD_CLINVAR.out.xml.filter { release_type, xml -> release_type == 'VCV' }.map { release_type, xml -> xml }.first()

    processing_contexts = channel.fromList(source_settings.contexts)
    PARSE_CLINVAR(clinvar_rcv, clinvar_vcv, processing_contexts)

    emit:
    rcv_xml = clinvar_rcv
    vcv_xml = clinvar_vcv
    records = PARSE_CLINVAR.out.records
    summary = PARSE_CLINVAR.out.summary
    warnings = PARSE_CLINVAR.out.warnings
    rejections = PARSE_CLINVAR.out.rejections
}

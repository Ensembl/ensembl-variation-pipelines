// Use a local ClinVar RCV XML file or download the release.
// Run the parser once for each configured species and assembly context.

include { DOWNLOAD_CLINVAR } from '../modules/local/download_clinvar'
include { PARSE_CLINVAR } from '../modules/local/parse_clinvar'

workflow CLINVAR {
    take:
    source_settings

    main:
    if (!source_settings.inputs.rcv) {
        DOWNLOAD_CLINVAR()
    }
    clinvar_rcv = source_settings.inputs.rcv ? channel.value(file(source_settings.inputs.rcv, checkIfExists: true)) :
        DOWNLOAD_CLINVAR.out.xml.first()

    processing_contexts = channel.fromList(source_settings.contexts)
    PARSE_CLINVAR(clinvar_rcv, processing_contexts)

    emit:
    rcv_xml = clinvar_rcv
    records = PARSE_CLINVAR.out.records
    summary = PARSE_CLINVAR.out.summary
    warnings = PARSE_CLINVAR.out.warnings
    rejections = PARSE_CLINVAR.out.rejections
}

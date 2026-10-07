include { DOWNLOAD_CLINVAR } from '../modules/local/download_clinvar'
include { PARSE_CLINVAR } from '../modules/local/parse_clinvar'

workflow CLINVAR {
    main:
    if (params.input) {
        clinvar_xml = channel.fromPath(params.input, checkIfExists: true)
    }
    else {
        DOWNLOAD_CLINVAR()
        clinvar_xml = DOWNLOAD_CLINVAR.out.xml
    }

    // Stage the script as an input so parser edits invalidate its cached task.
    parser_script = file("${projectDir}/src/phenotypes/clinvar/parser.py", checkIfExists: true)
    PARSE_CLINVAR(clinvar_xml, parser_script, params.assembly)

    emit:
    xml = clinvar_xml
    records = PARSE_CLINVAR.out.records
    summary = PARSE_CLINVAR.out.summary
    warnings = PARSE_CLINVAR.out.warnings
    rejections = PARSE_CLINVAR.out.rejections
}

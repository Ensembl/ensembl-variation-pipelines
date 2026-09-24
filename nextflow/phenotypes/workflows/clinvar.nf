include { DOWNLOAD_CLINVAR } from '../modules/local/download_clinvar'

workflow CLINVAR {
    main:
    if (params.input) {
        clinvar_xml = channel.fromPath(params.input, checkIfExists: true)
    }
    else {
        DOWNLOAD_CLINVAR()
        clinvar_xml = DOWNLOAD_CLINVAR.out.xml
    }

    emit:
    xml = clinvar_xml
}

include { DOWNLOAD_CLINVAR } from '../../modules/local/clinvar/download/main'

workflow CLINVAR_INPUTS {
    take:
    source_settings // Validated Map with input paths or null and an optional releases list.

    main:
    releases_to_download = []
    releases = source_settings.get('releases', ['RCV', 'VCV'])
    releases.each { release ->
        if (!source_settings.inputs[release.toLowerCase()]) {
            def filename = "ClinVar${release}Release_00-latest"
            def directory = release == 'RCV' ? 'RCV_release/' : ''
            def url = "https://ftp.ncbi.nlm.nih.gov/pub/clinvar/xml/${directory}${filename}.xml.gz"
            releases_to_download.add(tuple([id: filename.toString()], release, url.toString()))
        }
    }
    if (releases_to_download) {
        DOWNLOAD_CLINVAR(channel.fromList(releases_to_download))
    }
    clinvar_rcv = channel.empty()
    if ('RCV' in releases) {
        clinvar_rcv = source_settings.inputs.rcv ? channel.value(file(source_settings.inputs.rcv, checkIfExists: true)) :
            DOWNLOAD_CLINVAR.out.xml.filter { meta, release, xml -> release == 'RCV' }.map { meta, release, xml -> xml }.first()
    }
    clinvar_vcv = channel.empty()
    if ('VCV' in releases) {
        clinvar_vcv = source_settings.inputs.vcv ? channel.value(file(source_settings.inputs.vcv, checkIfExists: true)) :
            DOWNLOAD_CLINVAR.out.xml.filter { meta, release, xml -> release == 'VCV' }.map { meta, release, xml -> xml }.first()
    }

    emit:
    rcv_xml = clinvar_rcv // Broadcast the requested release to each assembly context.
    vcv_xml = clinvar_vcv // Broadcast the requested release to each assembly context.
}

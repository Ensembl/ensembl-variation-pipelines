// Download one ClinVar XML release (RCV or VCV).
// Retry failed downloads 3 times

process DOWNLOAD_CLINVAR {
    tag "ClinVar ${release_type.toUpperCase()} XML"

    input:
    val release_type

    output:
    tuple val(release_type), path('ClinVar*Release_00-latest.xml.gz'), emit: xml

    script:
    def filename = "ClinVar${release_type.toUpperCase()}Release_00-latest.xml.gz"
    def directory = release_type == 'rcv' ? 'RCV_release/' : ''
    def url = "https://ftp.ncbi.nlm.nih.gov/pub/clinvar/xml/${directory}${filename}"
    """
    curl --fail --location --silent --show-error \
        --retry 3 \
        --retry-delay 10 \
        --retry-all-errors \
        --output '${filename}.part' \
        '${url}'
    mv '${filename}.part' '${filename}'
    """
}

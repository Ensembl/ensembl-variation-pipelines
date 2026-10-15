// Download either the RCV or VCV XML release from ClinVar
// Retry failed downloads 3 times

process DOWNLOAD_CLINVAR {
    tag "ClinVar ${release_type} XML"

    input:
    val release_type

    output:
    tuple val(release_type), path("ClinVar${release_type}Release_00-latest.xml.gz"), emit: xml

    script:
    def filename = "ClinVar${release_type}Release_00-latest.xml.gz"
    def url = release_type == 'RCV' ?
        "https://ftp.ncbi.nlm.nih.gov/pub/clinvar/xml/RCV_release/${filename}" :
        "https://ftp.ncbi.nlm.nih.gov/pub/clinvar/xml/${filename}"
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

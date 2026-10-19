// Download the ClinVar RCV XML release
// Retry failed downloads 3 times

process DOWNLOAD_CLINVAR {
    tag 'ClinVar RCV XML'

    output:
    path('ClinVarRCVRelease_00-latest.xml.gz'), emit: xml

    script:
    def filename = 'ClinVarRCVRelease_00-latest.xml.gz'
    def url = "https://ftp.ncbi.nlm.nih.gov/pub/clinvar/xml/RCV_release/${filename}"
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

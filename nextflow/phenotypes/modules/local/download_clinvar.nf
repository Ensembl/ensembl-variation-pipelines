process DOWNLOAD_CLINVAR {
    tag 'ClinVar RCV XML'

    output:
    path 'ClinVarRCVRelease_00-latest.xml.gz', emit: xml

    script:
    """
    curl --fail --location --silent --show-error \
        --retry 3 \
        --retry-delay 10 \
        --retry-all-errors \
        --output ClinVarRCVRelease_00-latest.xml.gz.part \
        'https://ftp.ncbi.nlm.nih.gov/pub/clinvar/xml/RCV_release/ClinVarRCVRelease_00-latest.xml.gz'
    mv ClinVarRCVRelease_00-latest.xml.gz.part ClinVarRCVRelease_00-latest.xml.gz
    """
}

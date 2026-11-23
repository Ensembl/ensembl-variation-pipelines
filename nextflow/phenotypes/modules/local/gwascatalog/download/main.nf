process DOWNLOAD_GWASCATALOG {
    tag "${meta.id}"
    label 'process_single'

    conda "${moduleDir}/environment.yml"
    container 'curlimages/curl:8.12.1'

    input:
    tuple val(meta), val(input_name), val(url)

    output:
    tuple val(meta), val(input_name), path("${task.ext.prefix ?: meta.id}*.${meta.extension}"), emit: files
    tuple val("${task.process}"), val('curl'), eval("curl --version | sed -n '1s/^curl \\([^ ]*\\).*/\\1/p'"), emit: versions_curl, topic: versions

    when:
    task.ext.when == null || task.ext.when

    script:
    def prefix = task.ext.prefix ?: meta.id
    def url_argument = "'" + url.replace("'", "'\"'\"'") + "'"
    def curl_args = "${task.ext.args ?: ''} --fail --location --silent --show-error --retry 3 --retry-delay 10 --retry-all-errors"
    def resolve_input = input_name == 'data' ? """
    download_url=${url_argument}
    release=''
    for level in year month day; do
        curl ${curl_args} --output releases.html "\$download_url"
        if [ "\$level" = year ]; then
            pattern='[0-9][0-9][0-9][0-9]'
        else
            pattern='[0-9][0-9]'
        fi
        directory=\$(sed -n "s|.*href=\\"\\(\$pattern\\)/\\".*|\\1|p" releases.html | sort -u | tail -n 1)
        if [ -z "\$directory" ]; then
            echo "No dated GWAS Catalog \$level directories found at \$download_url" >&2
            exit 1
        fi
        release="\${release:+\${release}/}\${directory}"
        download_url="\${download_url}\${directory}/"
    done
    release_date=\$(printf '%s' "\$release" | tr '/' '-')
    filename="${prefix}-r\${release_date}.${meta.extension}"
    download_url="\${download_url}gwas-catalog-associations_ontology-annotated-full.zip"
    echo "[Downloading GWAS_Catalog] Release: \$release_date"
    """ : """
    filename='${prefix}.${meta.extension}'
    download_url=${url_argument}
    """
    """
    ${resolve_input}
    curl ${curl_args} --output "\${filename}.part" "\$download_url"
    mv "\${filename}.part" "\$filename"
    """

    stub:
    def prefix = task.ext.prefix ?: meta.id
    def filename = input_name == 'data' ? "${prefix}-r2000-01-01.${meta.extension}" : "${prefix}.${meta.extension}"
    """
    touch '${filename}'
    """
}

process DOWNLOAD_G2P {
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
    curl ${curl_args} --output releases.html ${url_argument}
    release=\$(sed -n 's|.*href="\\([0-9]\\{4\\}_[0-9]\\{2\\}_[0-9]\\{2\\}\\)/".*|\\1|p' releases.html | sort -u | tail -n 1)
    if [ -z "\$release" ]; then
        echo "No dated G2P release directories found" >&2
        exit 1
    fi
    release_date=\$(printf '%s' "\$release" | tr '_' '-')
    filename="${prefix}_\${release_date}.${meta.extension}"
    download_url=${url_argument}"\${release}/allG2P_\${release_date}.csv.gz"
    echo "[Downloading G2P] Release: \$release"
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
    def filename = input_name == 'data' ? "${prefix}_2000-01-01.${meta.extension}" : "${prefix}.${meta.extension}"
    """
    touch '${filename}'
    """
}

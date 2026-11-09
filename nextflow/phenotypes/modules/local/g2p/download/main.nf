process DOWNLOAD_G2P {
    tag "${meta.id}"
    label 'process_single'

    conda "${moduleDir}/environment.yml"
    container 'curlimages/curl:8.12.1'

    input:
    tuple val(meta), val(input_name), val(url)

    output:
    tuple val(meta), val(input_name), path("${task.ext.prefix ?: meta.id}.${meta.extension}"), emit: files
    tuple val("${task.process}"), val('curl'), eval("curl --version | sed -n '1s/^curl \\([^ ]*\\).*/\\1/p'"), emit: versions_curl, topic: versions

    when:
    task.ext.when == null || task.ext.when

    script:
    def args = task.ext.args ?: ''
    def prefix = task.ext.prefix ?: meta.id
    def url_argument = "'" + url.replace("'", "'\"'\"'") + "'"
    """
    curl ${args} \\
        --fail --location --silent --show-error \\
        --retry 3 --retry-delay 10 --retry-all-errors \\
        --output '${prefix}.${meta.extension}.part' \\
        ${url_argument}
    mv '${prefix}.${meta.extension}.part' '${prefix}.${meta.extension}'
    """

    stub:
    def prefix = task.ext.prefix ?: meta.id
    """
    touch '${prefix}.${meta.extension}'
    """
}

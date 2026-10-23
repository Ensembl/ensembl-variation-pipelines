process DOWNLOAD_CLINVAR {
    tag "${meta.id}"
    label 'process_single'

    conda "${moduleDir}/environment.yml"
    container 'curlimages/curl:8.12.1'

    input:
    tuple val(meta), val(release_type), val(url)

    output:
    tuple val(meta), val(release_type), path("${task.ext.prefix ?: meta.id}.xml.gz"), emit: xml
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
        --output '${prefix}.xml.gz.part' \\
        ${url_argument}
    mv '${prefix}.xml.gz.part' '${prefix}.xml.gz'
    """

    stub:
    def prefix = task.ext.prefix ?: meta.id
    """
    printf '<ReleaseSet Dated="2000-01-01"/>\\n' | gzip -c > '${prefix}.xml.gz'
    """
}

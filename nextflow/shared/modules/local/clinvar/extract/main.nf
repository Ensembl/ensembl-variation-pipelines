process EXTRACT_CLINVAR {
    tag "${meta.id}"
    label 'process_medium'

    conda "${moduleDir}/environment.yml"
    container 'python:3.14.0-slim'

    input:
    tuple val(meta), path(rcv_xml), val(genome_context)
    path shared_source, stageAs: 'shared_src'
    val shared_source_contents

    output:
    tuple val(meta), path("${task.ext.prefix ?: meta.id}.jsonl"), emit: records
    tuple val(meta), path("${task.ext.prefix ?: meta.id}.summary.json"), emit: summary
    tuple val(meta), path('rejections.jsonl'), emit: rejections
    tuple val("${task.process}"), val('python'), eval("python --version | sed 's/Python //'"), emit: versions_python, topic: versions
    tuple val("${task.process}"), val('variation_sources'), eval("PYTHONPATH=shared_src python -c 'from variation_sources import __version__; print(__version__)'"), emit: versions_parser, topic: versions

    when:
    task.ext.when == null || task.ext.when

    script:
    def args = task.ext.args ?: ''
    def prefix = task.ext.prefix ?: meta.id
    def context_argument = "'" + groovy.json.JsonOutput.toJson(genome_context).replace("'", "'\"'\"'") + "'"
    """
    PYTHONDONTWRITEBYTECODE=1 PYTHONPATH="${shared_source}" \\
    python -m variation_sources.clinvar.extract \\
        ${args} \\
        --rcv-input "${rcv_xml}" \\
        --genome-context ${context_argument} \\
        --records '${prefix}.jsonl' \\
        --summary '${prefix}.summary.json'
    """

    stub:
    def prefix = task.ext.prefix ?: meta.id
    """
    touch '${prefix}.jsonl' rejections.jsonl
    printf '{}\\n' > '${prefix}.summary.json'
    """
}

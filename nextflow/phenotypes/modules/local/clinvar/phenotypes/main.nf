process PARSE_CLINVAR {
    tag "${meta.id}"
    label 'process_medium'

    conda "${moduleDir}/environment.yml"
    container 'python:3.14.0-slim'

    input:
    tuple val(meta), path(rcv_xml), path(vcv_xml), val(genome_context)
    val source_name
    path python_source, stageAs: 'phenotype_src'
    val python_source_contents
    path shared_source, stageAs: 'shared_src'
    val shared_source_contents

    output:
    tuple val(meta), path("${task.ext.prefix ?: meta.id}_output.jsonl"), emit: records
    tuple val(meta), path('warnings.txt'), emit: warnings
    tuple val(meta), path('rejections.txt'), emit: rejections
    tuple val("${task.process}"), val('python'), eval("python --version | sed 's/Python //'"), emit: versions_python, topic: versions
    // The staged parser has no --version command; keep aligned with pyproject.toml.
    tuple val("${task.process}"), val('phenotypes'), val('0.1.0'), emit: versions_parser, topic: versions
    tuple val("${task.process}"), val('variation_sources'), eval("PYTHONPATH=shared_src python -c 'from variation_sources import __version__; print(__version__)'"), emit: versions_shared, topic: versions

    when:
    task.ext.when == null || task.ext.when

    script:
    def args = task.ext.args ?: ''
    def prefix = task.ext.prefix ?: meta.id
    def source_argument = "'" + source_name.replace("'", "'\"'\"'") + "'"
    def context_argument = "'" + groovy.json.JsonOutput.toJson(genome_context).replace("'", "'\"'\"'") + "'"
    """
    PYTHONDONTWRITEBYTECODE=1 PYTHONPATH="${python_source}:${shared_source}" \\
    python -m phenotypes.clinvar.rcv_parser \\
        ${args} \\
        --rcv-input "${rcv_xml}" \\
        --vcv-input "${vcv_xml}" \\
        --source-name ${source_argument} \\
        --genome-context ${context_argument} \\
        --records '${prefix}_output.jsonl'
    """

    stub:
    def prefix = task.ext.prefix ?: meta.id
    """
    touch '${prefix}_output.jsonl' warnings.txt rejections.txt
    """
}

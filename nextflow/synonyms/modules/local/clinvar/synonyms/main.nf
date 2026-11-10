process EXTRACT_CLINVAR_SYNONYMS {
    tag "${meta.id}"
    label 'process_medium'

    conda "${moduleDir}/environment.yml"
    container 'python:3.14.0'

    input:
    tuple val(meta), path(evidence)
    path python_source, stageAs: 'synonym_src'
    val python_source_contents
    path shared_source, stageAs: 'shared_src'
    val shared_source_contents

    output:
    tuple val(meta), path("${task.ext.prefix ?: meta.id}.synonyms.jsonl"), emit: records
    tuple val(meta), path("${task.ext.prefix ?: meta.id}.summary.json"), emit: summary
    tuple val(meta), path('rejections.jsonl'), emit: rejections
    tuple val("${task.process}"), val('python'), eval("python --version | sed 's/Python //'"), emit: versions_python, topic: versions
    tuple val("${task.process}"), val('synonyms'), eval("PYTHONPATH=synonym_src python -c 'from synonyms import __version__; print(__version__)'"), emit: versions_consumer, topic: versions
    tuple val("${task.process}"), val('variation_sources'), eval("PYTHONPATH=shared_src python -c 'from variation_sources import __version__; print(__version__)'"), emit: versions_shared, topic: versions

    when:
    task.ext.when == null || task.ext.when

    script:
    def args = task.ext.args ?: ''
    def prefix = task.ext.prefix ?: meta.id
    """
    PYTHONDONTWRITEBYTECODE=1 PYTHONPATH="${python_source}:${shared_source}" \\
    python -m synonyms.clinvar.extract \\
        ${args} \\
        --evidence "${evidence}" \\
        --records '${prefix}.synonyms.jsonl' \\
        --summary '${prefix}.summary.json'
    """

    stub:
    def prefix = task.ext.prefix ?: meta.id
    """
    touch '${prefix}.synonyms.jsonl' rejections.jsonl
    printf '{}\\n' > '${prefix}.summary.json'
    """
}

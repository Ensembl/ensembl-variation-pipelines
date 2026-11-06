process LOAD_CLINVAR {
    tag "${meta.id}"

    label 'process_medium'
    // Database state cannot be restored from a cached task result.
    cache false

    conda "${moduleDir}/environment.yml"
    container 'ensembl-phenotypes-clinvar-loader:0.1.0'

    input:
    tuple val(meta), path(records)
    path env_file, stageAs: 'database.env'
    val replace_existing
    path python_source, stageAs: 'phenotype_src'
    val python_source_contents
    path shared_source, stageAs: 'shared_src'
    val shared_source_contents

    output:
    tuple val(meta), path("${task.ext.prefix ?: meta.id}_load_summary.json"), emit: summary

    tuple val("${task.process}"), val('python'), eval("python --version | sed 's/Python //'"), emit: versions_python, topic: versions
    // Keep the staged consumer version aligned with pyproject.toml.
    tuple val("${task.process}"), val('phenotypes'), val('0.1.0'), emit: versions_loader, topic: versions
    tuple val("${task.process}"), val('variation_sources'), eval("PYTHONPATH=shared_src python -c 'from variation_sources import __version__; print(__version__)'"), emit: versions_shared, topic: versions
    tuple val("${task.process}"), val('psycopg'), eval("python -c 'import psycopg; print(psycopg.__version__)'"), emit: versions_psycopg, topic: versions
    tuple val("${task.process}"), val('simplejson'), eval("python -c 'import simplejson; print(simplejson.__version__)'"), emit: versions_simplejson, topic: versions
    tuple val("${task.process}"), val('libpq'), eval("python -c 'from psycopg import pq; print(pq.version())'"), emit: versions_libpq, topic: versions

    when:
    task.ext.when == null || task.ext.when

    script:
    def args = task.ext.args ?: ''
    def prefix = task.ext.prefix ?: meta.id
    def replace_argument = replace_existing ? '--replace' : ''
    """
    PYTHONDONTWRITEBYTECODE=1 PYTHONPATH="${python_source}:${shared_source}" \\
    python -m phenotypes.clinvar.loader \\
        ${args} \\
        --input "${records}" \\
        --env-file "${env_file}" \\
        ${replace_argument} \\
        --summary '${prefix}_load_summary.json'
    """
    stub:
    def prefix = task.ext.prefix ?: meta.id
    """
    printf '{}\\n' > '${prefix}_load_summary.json'
    """
}

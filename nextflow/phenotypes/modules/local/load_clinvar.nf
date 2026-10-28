process LOAD_CLINVAR {
    tag "ClinVar ${processing_context.species_production_name} ${processing_context.assembly}"

    label 'process_medium'
    cache false

    input:
    tuple val(processing_context), path(records)
    val env_file
    val replace_existing
    path python_source, stageAs: 'phenotype_src'
    val python_source_contents
    path shared_source, stageAs: 'shared_src'
    val shared_source_contents

    output:
    tuple val(processing_context), path('clinvar_load_summary.json'), emit: summary

    when:
    task.ext.when == null || task.ext.when

    script:
    def args = task.ext.args ?: ''
    def env_argument = "'" + env_file.replace("'", "'\"'\"'") + "'"
    def replace_argument = replace_existing ? '--replace' : ''
    """
    postgresql_libdir=\$(pg_config --libdir)
    export LD_LIBRARY_PATH="\$postgresql_libdir\${LD_LIBRARY_PATH:+:\$LD_LIBRARY_PATH}"
    export PSYCOPG_IMPL=python

    PYTHONDONTWRITEBYTECODE=1 PYTHONPATH="${python_source}:${shared_source}" \\
    "${projectDir}/.venv/bin/python" -m phenotypes.clinvar.loader \\
        ${args} \\
        --input "${records}" \\
        --env-file ${env_argument} \\
        ${replace_argument} \\
        --summary clinvar_load_summary.json
    """
    stub:
    """
    printf '{}\\n' > clinvar_load_summary.json
    """
}

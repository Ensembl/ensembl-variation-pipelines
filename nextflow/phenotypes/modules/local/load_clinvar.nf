// Load the parsed snapshot into PostgreSQL; publish the committed load summary.

process LOAD_CLINVAR {
    tag "ClinVar ${processing_context.species_production_name} ${processing_context.assembly}"

    module 'postgresql/16'
    cache false

    publishDir path: { "${params.outdir}/clinvar/${processing_context.species_production_name}/${processing_context.assembly}" },
        mode: 'copy', overwrite: true

    input:
    tuple val(processing_context), path(records)
    val env_file
    val replace_existing

    output:
    tuple val(processing_context), path('clinvar_load_summary.json'), emit: summary

    script:
    def env_argument = "'" + env_file.replace("'", "'\"'\"'") + "'"
    def replace_argument = replace_existing ? '--replace' : ''
    """
    postgresql_libdir=\$(pg_config --libdir)
    export LD_LIBRARY_PATH="\$postgresql_libdir\${LD_LIBRARY_PATH:+:\$LD_LIBRARY_PATH}"
    export PSYCOPG_IMPL=python

    "${projectDir}/.venv/bin/python" -m phenotypes.clinvar.loader \\
        --input "${records}" \\
        --env-file ${env_argument} \\
        ${replace_argument} \\
        --summary clinvar_load_summary.json
    """
}

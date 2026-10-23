// Parse RCV associations and use VCV records for component identifiers.
// Publish the association records, warnings and rejections.

process PARSE_CLINVAR {
    tag "${source_name} ${processing_context.species_production_name} ${processing_context.assembly}"

    publishDir path: { "${params.outdir}/clinvar/${processing_context.species_production_name}/${processing_context.assembly}" },
        mode: 'copy', overwrite: true

    input:
    path rcv_xml
    path vcv_xml
    val source_name
    val processing_context

    output:
    tuple val(processing_context), path('clinvar_output.jsonl'), emit: records
    tuple val(processing_context), path('warnings.txt'), emit: warnings
    tuple val(processing_context), path('rejections.txt'), emit: rejections

    script:
    def context_argument = "'" + groovy.json.JsonOutput.toJson(processing_context).replace("'", "'\"'\"'") + "'"
    def source_argument = "'" + source_name.replace("'", "'\"'\"'") + "'"
    """
    # Emit model-shaped associations with source_reports, nested assessments and evidence.
    PYTHONDONTWRITEBYTECODE=1 \
    "${projectDir}/.venv/bin/python" -m phenotypes.clinvar.rcv_parser \
        --rcv-input "${rcv_xml}" \
        --vcv-input "${vcv_xml}" \
        --source-name ${source_argument} \
        --genome-context ${context_argument} \
        --pipeline-directory "${projectDir}" \
        --records clinvar_output.jsonl
    """
}

process PARSE_CLINVAR {
    tag "${clinvar_xml.name} (${assembly})"

    input:
    path clinvar_xml
    path parser_script
    val assembly

    output:
    path 'clinvar_output.jsonl', emit: records
    path 'clinvar_summary.json', emit: summary
    path 'warnings.txt', emit: warnings
    path 'rejections.txt', emit: rejections

    script:
    """
    "${projectDir}/.venv/bin/python" "${parser_script}" \
        --input "${clinvar_xml}" \
        --assembly "${assembly}" \
        --records clinvar_output.jsonl \
        --summary clinvar_summary.json
    """
}

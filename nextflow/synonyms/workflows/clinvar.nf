include { CLINVAR_INPUTS } from '../../shared/workflows/clinvar_inputs/main'
include { EXTRACT_CLINVAR } from '../../shared/modules/local/clinvar/extract/main'
include { EXTRACT_CLINVAR_SYNONYMS } from '../modules/local/clinvar/synonyms/main'
include { source_contents } from '../../shared/lib/source_files'

workflow CLINVAR_SYNONYMS {
    take:
    source_settings

    main:
    CLINVAR_INPUTS(source_settings)
    shared_source = file("${projectDir}/../shared/src", checkIfExists: true)
    python_source = file("${projectDir}/src", checkIfExists: true)
    extraction_inputs = channel.fromList(source_settings.contexts)
        .combine(CLINVAR_INPUTS.out.rcv_xml)
        .map { context, rcv ->
            tuple([id: "clinvar_${context.species_production_name}_${context.assembly}".toString()] + context, rcv, context)
        }
    EXTRACT_CLINVAR(extraction_inputs, shared_source, source_contents(shared_source))
    EXTRACT_CLINVAR_SYNONYMS(EXTRACT_CLINVAR.out.records, python_source, source_contents(python_source), shared_source, source_contents(shared_source))

    emit:
    evidence = EXTRACT_CLINVAR.out.records
    evidence_summary = EXTRACT_CLINVAR.out.summary
    evidence_rejections = EXTRACT_CLINVAR.out.rejections
    records = EXTRACT_CLINVAR_SYNONYMS.out.records
    summary = EXTRACT_CLINVAR_SYNONYMS.out.summary
    rejections = EXTRACT_CLINVAR_SYNONYMS.out.rejections
}

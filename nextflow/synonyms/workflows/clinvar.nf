include { CLINVAR_INPUTS } from '../../shared/workflows/clinvar_inputs/main'
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
    EXTRACT_CLINVAR_SYNONYMS(extraction_inputs, python_source, source_contents(python_source), shared_source, source_contents(shared_source))

    emit:
    variant_records = EXTRACT_CLINVAR_SYNONYMS.out.variant_records
    allele_records = EXTRACT_CLINVAR_SYNONYMS.out.allele_records
    summary = EXTRACT_CLINVAR_SYNONYMS.out.summary
    rejections = EXTRACT_CLINVAR_SYNONYMS.out.rejections
}

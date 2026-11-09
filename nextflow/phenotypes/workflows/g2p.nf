include { PARSE_G2P } from '../modules/local/g2p/parse/main'
include { source_contents } from '../../shared/lib/source_files'

workflow G2P {
    take:
    source_settings

    main:
    python_source = file("${projectDir}/src", checkIfExists: true)
    shared_source = file("${projectDir}/../shared/src", checkIfExists: true)
    data_file = file(source_settings.inputs.data, checkIfExists: true)
    gene_id_mapping = file(source_settings.inputs.gene_id_mapping, checkIfExists: true)
    processing_inputs = channel.fromList(source_settings.contexts)
        .map { context ->
            def assembly_suffix = context.assembly ? "_${context.assembly}" : ''
            def meta = [id: "g2p_${context.species_production_name}${assembly_suffix}".toString(), source_name: source_settings.source_name] + context
            tuple(meta, data_file, gene_id_mapping, context)
        }
    source_metadata = [source_name: source_settings.source_name, source_version: source_settings.get('source_version')]
    PARSE_G2P(processing_inputs, channel.value(source_metadata), python_source, source_contents(python_source), shared_source, source_contents(shared_source))

    emit:
    records = PARSE_G2P.out.records
    warnings = PARSE_G2P.out.warnings
    rejections = PARSE_G2P.out.rejections
}

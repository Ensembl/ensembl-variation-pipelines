include { DOWNLOAD_G2P } from '../modules/local/g2p/download/main'
include { PARSE_G2P } from '../modules/local/g2p/parse/main'
include { source_contents } from '../../shared/lib/source_files'

workflow G2P {
    take:
    source_settings

    main:
    python_source = file("${projectDir}/src", checkIfExists: true)
    shared_source = file("${projectDir}/../shared/src", checkIfExists: true)
    downloads = [
        tuple([id: 'allG2P', extension: 'csv.gz'], 'data', 'https://ftp.ebi.ac.uk/pub/databases/gene2phenotype/G2P_data_downloads/'),
        tuple([id: 'hgnc_complete_set', extension: 'txt'], 'gene_id_mapping', 'https://storage.googleapis.com/public-download-files/hgnc/tsv/tsv/hgnc_complete_set.txt'),
    ].findAll { meta, input_name, url -> source_settings.inputs[input_name] == null }
    if (downloads) {
        DOWNLOAD_G2P(channel.fromList(downloads))
    }
    g2p_data = source_settings.inputs.data ? channel.value(file(source_settings.inputs.data, checkIfExists: true)) :
        DOWNLOAD_G2P.out.files.filter { meta, input_name, input_file -> input_name == 'data' }.map { meta, input_name, input_file -> input_file }.first()
    hgnc_mapping = source_settings.inputs.gene_id_mapping ? channel.value(file(source_settings.inputs.gene_id_mapping, checkIfExists: true)) :
        DOWNLOAD_G2P.out.files.filter { meta, input_name, input_file -> input_name == 'gene_id_mapping' }.map { meta, input_name, input_file -> input_file }.first()
    processing_inputs = channel.fromList(source_settings.contexts)
        .combine(g2p_data)
        .combine(hgnc_mapping)
        .map { context, data_file, gene_id_mapping ->
            def assembly_suffix = context.assembly ? "_${context.assembly}" : ''
            def meta = [id: "g2p_${context.species_production_name}${assembly_suffix}".toString(), source_name: source_settings.source_name] + context
            tuple(meta, data_file, gene_id_mapping, context)
        }
    PARSE_G2P(processing_inputs, channel.value(source_settings.source_name), python_source, source_contents(python_source), shared_source, source_contents(shared_source))

    emit:
    records = PARSE_G2P.out.records
    warnings = PARSE_G2P.out.warnings
    rejections = PARSE_G2P.out.rejections
}

include { DOWNLOAD_GWASCATALOG } from '../modules/local/gwascatalog/download/main'
include { PARSE_GWASCATALOG } from '../modules/local/gwascatalog/parse/main'
include { source_contents } from '../../shared/lib/source_files'

workflow GWASCATALOG {
    take:
    source_settings

    main:
    python_source = file("${projectDir}/src", checkIfExists: true)
    shared_source = file("${projectDir}/../shared/src", checkIfExists: true)
    downloads = [
        tuple([id: 'gwascatalog', extension: 'zip'], 'data', 'https://ftp.ebi.ac.uk/pub/databases/gwas/releases/'),
        tuple([id: 'Harm_sumstats_status', extension: 'db'], 'study_builds', 'https://ftp.ebi.ac.uk/pub/databases/gwas/summary_statistics/sumstats_harm_status/Harm_sumstats_status.db'),
    ].findAll { meta, input_name, url -> source_settings.inputs[input_name] == null }
    if (downloads) {
        DOWNLOAD_GWASCATALOG(channel.fromList(downloads))
    }
    gwascatalog_data = source_settings.inputs.data ? channel.value(file(source_settings.inputs.data, checkIfExists: true)) :
        DOWNLOAD_GWASCATALOG.out.files.filter { meta, input_name, input_file -> input_name == 'data' }.map { meta, input_name, input_file -> input_file }.first()
    study_build_reference = source_settings.inputs.study_builds ? channel.value(file(source_settings.inputs.study_builds, checkIfExists: true)) :
        DOWNLOAD_GWASCATALOG.out.files.filter { meta, input_name, input_file -> input_name == 'study_builds' }.map { meta, input_name, input_file -> input_file }.first()
    processing_inputs = channel.fromList(source_settings.contexts)
        .combine(gwascatalog_data)
        .combine(study_build_reference)
        .map { context, data_file, study_builds ->
            def assembly_suffix = context.assembly ? "_${context.assembly}" : ''
            def meta = [id: "gwascatalog_${context.species_production_name}${assembly_suffix}".toString(), source_name: source_settings.source_name] + context
            tuple(meta, data_file, study_builds, context)
        }
    PARSE_GWASCATALOG(processing_inputs, channel.value(source_settings.source_name), python_source, source_contents(python_source), shared_source, source_contents(shared_source))

    emit:
    records = PARSE_GWASCATALOG.out.records
    warnings = PARSE_GWASCATALOG.out.warnings
    rejections = PARSE_GWASCATALOG.out.rejections
}

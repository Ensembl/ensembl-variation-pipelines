include { CLINVAR } from './clinvar'
include { G2P } from './g2p'
include { LOAD_PHENOTYPES } from '../modules/local/load/main'
include { read_source_config } from '../../shared/lib/source_config'

workflow PHENOTYPES {
    main:
    source_config = read_source_config(params.source_config, "${projectDir}/.venv/bin/python", 'phenotypes.source_config')
    source_config.sources.each { source_name, settings ->
        if (settings.contexts.size() != 1) {
            error("${source_name} loading requires one context per source snapshot; multiple contexts must be loaded together before whole-source replacement can support them.")
        }
    }
    database_env = file(params.env_file, checkIfExists: true)
    python_source = file("${projectDir}/src", checkIfExists: true)
    shared_source = file("${projectDir}/../shared/src", checkIfExists: true)
    parsed_records = channel.empty()
    parsed_warnings = channel.empty()
    parsed_rejections = channel.empty()

    if (source_config.sources.containsKey('ClinVar')) {
        CLINVAR(source_config.sources.ClinVar)
        parsed_records = parsed_records.mix(CLINVAR.out.records)
        parsed_warnings = parsed_warnings.mix(CLINVAR.out.warnings)
        parsed_rejections = parsed_rejections.mix(CLINVAR.out.rejections)
    }
    if (source_config.sources.containsKey('G2P')) {
        G2P(source_config.sources.G2P)
        parsed_records = parsed_records.mix(G2P.out.records)
        parsed_warnings = parsed_warnings.mix(G2P.out.warnings)
        parsed_rejections = parsed_rejections.mix(G2P.out.rejections)
    }

    LOAD_PHENOTYPES(parsed_records, channel.value(database_env), channel.value(params.replace), python_source, shared_source)

    emit:
    records = parsed_records.map { meta, records -> tuple(meta.findAll { key, value -> key != 'id' && key != 'source_name' }, records) }
    load_summary = LOAD_PHENOTYPES.out.summary.map { meta, summary -> tuple(meta.findAll { key, value -> key != 'id' && key != 'source_name' }, summary) }
    warnings = parsed_warnings.map { meta, warnings -> tuple(meta.findAll { key, value -> key != 'id' && key != 'source_name' }, warnings) }
    rejections = parsed_rejections.map { meta, rejections -> tuple(meta.findAll { key, value -> key != 'id' && key != 'source_name' }, rejections) }
}

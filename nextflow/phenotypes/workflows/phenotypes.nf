include { CLINVAR } from './clinvar'

def read_source_config(config_path) {
    def config_file = file(config_path, checkIfExists: true).toAbsolutePath()
    def command = ["${projectDir}/.venv/bin/python",
                   "${projectDir}/src/phenotypes/source_config.py", config_file.toString()]
    def output = new StringBuilder()
    def errors = new StringBuilder()
    def validation = command.execute()
    validation.waitForProcessOutput(output, errors)
    if (validation.exitValue() != 0) {
        error(errors.toString().trim())
    }
    return new groovy.json.JsonSlurper().parseText(output.toString())
}

workflow PHENOTYPES {
    main:
    source_config = read_source_config(params.source_config)
    if (source_config.sources.ClinVar.contexts.size() != 1) {
        error('ClinVar loading currently requires one context per source snapshot; multiple contexts must be loaded together before whole-source replacement can support them.')
    }
    database_env = file(params.env_file, checkIfExists: true).toAbsolutePath().toString()
    CLINVAR(source_config.sources.ClinVar, database_env, params.replace)

    emit:
    records = CLINVAR.out.records
    warnings = CLINVAR.out.warnings
    rejections = CLINVAR.out.rejections
    load_summary = CLINVAR.out.load_summary
}

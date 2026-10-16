// Read and validate source settings, then run the source workflows.

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
    CLINVAR(source_config.sources.clinvar)

    emit:
    records = CLINVAR.out.records
    summary = CLINVAR.out.summary
    warnings = CLINVAR.out.warnings
    rejections = CLINVAR.out.rejections
}

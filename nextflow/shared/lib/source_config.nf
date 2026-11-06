// Use the shared Python validator from the caller's environment.
def read_source_config(config_path, python_executable) {
    def config_file = file(config_path, checkIfExists: true).toAbsolutePath()
    def command = [python_executable.toString(), '-c',
                   'import json, sys; from variation_sources.source_config import load_source_config; print(json.dumps(load_source_config(sys.argv[1])))',
                   config_file.toString()]
    def output = new StringBuilder()
    def errors = new StringBuilder()
    def validation = command.execute()
    validation.waitForProcessOutput(output, errors)
    if (validation.exitValue() != 0) {
        error(errors.toString().trim())
    }
    return new groovy.json.JsonSlurper().parseText(output.toString())
}

// Use the shared Python validator from the caller's environment.
def read_source_config(config_path, python_executable, config_module = 'variation_sources.source_config') {
    def config_file = file(config_path, checkIfExists: true).toAbsolutePath()
    def command = [python_executable.toString(), '-c',
                   'import importlib, json, sys; print(json.dumps(importlib.import_module(sys.argv[2]).load_source_config(sys.argv[1])))',
                   config_file.toString(), config_module.toString()]
    def output = new StringBuilder()
    def errors = new StringBuilder()
    def validation = command.execute()
    validation.waitForProcessOutput(output, errors)
    if (validation.exitValue() != 0) {
        error(errors.toString().trim())
    }
    return new groovy.json.JsonSlurper().parseText(output.toString())
}

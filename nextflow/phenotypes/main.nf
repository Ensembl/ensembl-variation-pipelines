// Start the phenotype pipeline using the chosen source configuration.

include { PHENOTYPES } from './workflows/phenotypes'

workflow {
    PHENOTYPES()
}

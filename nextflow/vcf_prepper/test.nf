include { PROCESS_INPUT }  from "./subworkflows/local/process_input"

workflow {
	meta = [:]
	meta["file_type"] = "local"
	PROCESS_INPUT(Channel.of([meta, "/Users/snhossain/ensembl-repos/ensembl-variation-pipelines/nextflow/vcf_prepper/assets/examples/test.vcf.gz"]))
}

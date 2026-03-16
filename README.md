# ensembl-variation-pipelines

scripts
## Nextflow Pipelines

- [vcf_prepper](nextflow/vcf_prepper/README.md): Main pipeline for preparing VCF and track data for Ensembl.

## Scripts

The `scripts/` directory contains various utility scripts:

- [auto_create_input_config.py](scripts/auto_create_input_config.py): Automatically generate input config JSON files for vcf_prepper.
- [calculate_frequency_from_gt.py](scripts/calculate_frequency_from_gt.py): Calculate allele frequency from genotype data. The generated file is used in the population_data.json config file for vcf_prepper.
- [create_input_config.py](scripts/create_input_config.py): Create input config JSON file as required by vcf_prepper pipeline.
- [create_metadata_payload.py](scripts/create_metadata_payload.py): Create payloads with statistics and variation examples for submission to metadata database.
- [create_track_api_metadata.py](scripts/create_track_api_metadata.py): Create JSON with metadata about tracks needed for track API.
- [preprocess_hgsvc3.py](scripts/preprocess_hgsvc3.py): Convert population data to haplotype format.
- [test_track_api_endpoint.py](scripts/test_track_api_endpoint.py): Test the track API endpoint after a handover.

## Python Utilities

- [ensembl-variation-utils](src/python/README.md): Python package for pipeline logic, file location, database access, and VEP config generation. See the [ensembl-variation-utils README](src/python/README.md) for details.

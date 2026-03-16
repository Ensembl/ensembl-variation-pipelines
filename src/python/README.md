# ensembl-variation-utils

Python package for Ensembl Variation pipelines. Provides utilities for:

- File location and management (FASTA, GFF, VEP cache, etc.)
- Database access (metadata/core)
- VEP config generation (JSON-based)
- Specialized logic for VEP plugins and custom annotation data

## Features

- `clients`: Handles connections to metadata/core databases (future support for gRPC/REST)
- `file_locator`: Discovers files and manages file operations
- `vep_config`: Generates VEP configuration from JSON input

## Usage

Import the relevant modules in your pipeline scripts or Nextflow processes. Example:

```python
from ensembl.variation_utils.file_locator import FileLocator
from ensembl.variation_utils.clients import MetadataClient
from ensembl.variation_utils.vep_config import VEPConfig
```

See the [vcf_prepper pipeline](../../nextflow/vcf_prepper) for integration details.

## Testing

Unit tests are available in `tests/variation_utils/`.

### Running tests locally

#### 1. Install the package and test dependencies

```bash
pip install .
pip install pytest coverage
```

#### 2. Set up test databases (optional)

Some tests (e.g., `clients/`) require a local MySQL instance with test databases:

```bash
mysql -h 127.0.0.1 -u root -e 'CREATE DATABASE ensembl_genome_metadata;'
mysql -h 127.0.0.1 -u root ensembl_genome_metadata < tests/variation_utils/data/ensembl_genome_metadata.dump

mysql -h 127.0.0.1 -u root -e 'CREATE DATABASE homo_sapiens_core_110_38;'
mysql -h 127.0.0.1 -u root homo_sapiens_core_110_38 < tests/variation_utils/data/homo_sapiens_core_110_38.dump
```

#### 3. Run pytest

```bash
# Run all tests
pytest .

# Run with coverage
coverage run -m pytest .
coverage report
```

## License

See LICENSE in this repository.

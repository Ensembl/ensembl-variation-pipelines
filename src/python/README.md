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

Unit tests are available in `src/python/tests/variation_utils/`.

## License

See LICENSE in this repository.

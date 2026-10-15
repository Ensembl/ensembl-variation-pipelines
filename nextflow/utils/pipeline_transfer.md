# Shared utilities transfer review

The pipelines import `../utils/utils.nf` for parameter logging, workflow summaries
and JVM memory checks. No Ensembl Perl API or MySQL dependency was found.
Callers should exclude credentials from parameter logging.

[#1152](https://github.com/Ensembl/ensembl-variation/pull/1152), head
`a6e07a1810a3a2a0443967364b709c83962f888f`, adds numeric conversion before rounding
in the memory check. Local edits simplify comments only.

The module was reviewed and all relative imports resolve.

Source: `release/116`, commit `db7394a02523a6e519cb0d89f84b2889bac12737`. See the [transfer summary](../pipeline_transfer.md) for scope and shared validation.

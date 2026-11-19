# G2P test data

`g2p_test.csv` contains 12 unchanged rows from the G2P download;
`hgnc_test.txt` contains their 10 matching rows from the HGNC complete set.
Both retain the original columns and values.

The cases include regular HMX1/ATR associations, HDAC8 assertions with different
allelic requirements, KCNA2 assertions with different mechanisms, disputed EMX2,
refuted MYH8, missing optional data, multiple consequences and publications.

The mapping-failure test deliberately supplies missing/conflicting HGNC mappings
in memory. These scenarios are not claimed to occur in the unchanged fixtures.

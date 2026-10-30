process concatenate_files {
  errorStrategy 'terminate'
  // Concatente variants associated with MaveDB scores into a single file

  input:  path(mapped_variants)
  output: path("combined.tsv")

  memory '20GB'

  """
  #!/usr/bin/env python3
  import glob, pandas, os
  from os.path import exists
  import subprocess

  output        = "combined.tsv"
  output_sorted = "combined_sorted.tsv"
  output_bgzip  = "combined.tsv.gz"
  files         = glob.glob("*map_*.tsv", recursive=True)

  print(f"Found {len(files)} files matching the pattern")

  def standardise_columns(df):
    df = df.rename(columns={'p-value':'pvalue'})
    df.columns = df.columns.str.lower()
    return df

  # concatenate header of all files
  print("Creating header...")
  header = None
  for f in files:
    print("Processing file:", f)

    if os.path.getsize(f) == 0:
      print("Expected empty mapping:", f)
      continue
    content = pandas.read_csv(f, delimiter="\t", nrows=0)

    content = standardise_columns(content)
    required = {'chr', 'start', 'end', 'ref', 'alt', 'urn', 'score'}
    if not required.issubset(content.columns):
      raise ValueError(f"Invalid mapping header in {f}: missing {required - set(content.columns)}")

    if header is not None:
      header = pandas.concat([header, content], axis=0, ignore_index=True)
    else:
      header = content

  if header is None:
    raise ValueError("No mapped variants available; no output will be published")
  print("Header columns:", header.columns.values)

  # merge data and append to file (one file at a time)
  rows_written = 0
  print("\\nMerging and writing content...")
  for f in files:
    print("Processing file:", f)
    if os.path.getsize(f) == 0:
      continue
    content = pandas.read_csv(f, delimiter="\t")

    content = standardise_columns(content)
    rows_written += len(content)
    out = pandas.concat([header, content], axis=0, ignore_index=True)
    out.to_csv(output, sep="\t", mode="a", index=False, header=not exists(output))

  if rows_written == 0:
    raise ValueError("No mapped variants available; no output will be published")
  """
}

process tabix {
  errorStrategy 'terminate'

  input:  path out
  output:
    path "${file(params.output).name}", emit: data
    path "${file(params.output).name}.tbi", emit: index

  script:
  def name = file(params.output).baseName
  def gzip = file(params.output).name
  """
  set -euo pipefail
  test -s "${out}"

  # remove LRG and chromosome patches
  awk 'NR > 1 && !/^#/ && !/^LRG/ && !/^CHR_/' "${out}" > tmp.tsv

  # sort file by position
  (head -n1 "${out}" | sed 's/^/#/'; sort -k1,1 -k2,2n -k3,3n tmp.tsv | uniq) > ${name}

  bgzip "${name}"
  tabix -s1 -b2 -e3 "${gzip}"
  rm tmp.tsv
  """
}

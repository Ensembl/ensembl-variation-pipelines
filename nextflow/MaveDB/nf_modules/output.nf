process concatenate_files {
  errorStrategy 'terminate'
  // Combine mapped variant scores.

  input:  path(mapped_variants)
  output: path("combined.tsv")

  memory '20GB'

  """
  #!/usr/bin/env python3
  import glob
  import pandas
  from os.path import exists

  output = "combined.tsv"
  output_sorted = "combined_sorted.tsv"
  output_bgzip = "combined.tsv.gz"
  files = glob.glob("*map_*.tsv", recursive=True)

  print(f"Found {len(files)} files matching the pattern")


  def standardise_columns(df):
      df = df.rename(columns={"p-value": "pvalue"})
      df.columns = df.columns.str.lower()
      return df


  # concatenate header of all files
  print("Creating header...")
  header = None
  for f in files:
      print("Processing file:", f)

      try:
          content = pandas.read_csv(f, delimiter="\t", nrows=0)
      except pandas.errors.EmptyDataError:
          print("File empty")
          continue

      content = standardise_columns(content)

      if header is not None:
          header = pandas.concat([header, content], axis=0, ignore_index=True)
      else:
          header = content

  if header is None:
      # No new records survived. A previous-output merge can still supply rows.
      open(output, "w").close()
      raise SystemExit(0)

  print("Header columns:", header.columns.values)

  # merge data and append to file (one file at a time)
  print("\\nMerging and writing content...")
  for f in files:
      print("Processing file:", f)
      try:
          content = pandas.read_csv(f, delimiter="\t")
      except pandas.errors.EmptyDataError:
          print("File empty")
          continue

      content = standardise_columns(content)
      out = pandas.concat([header, content], axis=0, ignore_index=True)
      out.to_csv(output, sep="\t", mode="a", index=False, header=not exists(output))
  """
}

process tabix {
  errorStrategy 'terminate'
  publishDir file(params.output).parent, mode: 'copy', overwrite: true

  input:  path out
  output:
    path "${file(params.output).name}", emit: data
    path "${file(params.output).name}.tbi", emit: index

  script:
  def gzip = file(params.output).name
  """
  test -s ${out} || { echo "No mapped variants available" >&2; exit 1; }

  # Preserve staged input for upstream tasks.
  awk 'NR > 1 && !/^#/ && !/^LRG/ && !/^CHR_/' "${out}" > tmp.tsv
  (head -n1 "${out}" | sed 's/^#*/#/'; LC_ALL=C sort -k1,1 -k2,2n -k3,3n tmp.tsv | uniq) \
    | bgzip -c > "${gzip}"
  tabix -s1 -b2 -e3 "${gzip}"
  rm tmp.tsv
  """
}

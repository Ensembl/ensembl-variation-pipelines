process merge_previous_output {
  tag "merge_previous_output"
  errorStrategy 'terminate'

  input:
    tuple path(current_combined), path(prev_output), path(skipped_urns)

  output:
    path 'merged_combined.tsv'

  """
  merge_previous_output.py --current "${current_combined}" \
    --previous "${prev_output}" --urns "${skipped_urns}" \
    --output merged_combined.tsv
  """
}

// Filter and index reused results like new results.
process reuse_previous_output {
  tag "reuse_previous_output"
  errorStrategy 'terminate'

  input:
    path prev_output
    path skipped_urns

  output:
    path 'merged_combined.tsv'

  """
  merge_previous_output.py --previous "${prev_output}" \
    --urns "${skipped_urns}" --output merged_combined.tsv
  """
}

def source_contents(source_directory) {
    // Include nested Python filenames and bytes in task cache keys.
    return channel.fromPath("${source_directory}/**/*.py", checkIfExists: true)
        .map { source_file -> tuple(source_directory.relativize(source_file).toString(), source_file.text) }
        .toSortedList { left, right -> left[0] <=> right[0] }
}

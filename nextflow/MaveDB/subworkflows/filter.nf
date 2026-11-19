include { download_MaveDB_metadata } from '../nf_modules/fetch.nf'

workflow filter_by_licence {
  take:
    urn
    licences
  main:
    download_MaveDB_metadata( urn )
    // Warn about discarded files
    data = download_MaveDB_metadata.out
             .map { [ urn: it[0], metadata: it[1], licence: it[2] ] }

    excluded = data
      .filter { !licences.contains(it.licence.text) }
      .map {
        println "NOTE: Discarded ${it.urn} based on licence ${it.licence.text}"
        [it.urn, 'licence_excluded']
      }

    // Return files with matching licences
    filtered = data.filter { licences.contains(it.licence.text) }
  emit:
    filtered = filtered
    excluded = excluded
}

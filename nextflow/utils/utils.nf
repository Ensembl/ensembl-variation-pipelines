#!/usr/bin/env nextflow

def print_params(description, nullable=[], sort=false, skip=['help'], 
                 params=params, separator="-"*description.length(), indent=4) {
  /*
    Print script parameters

    @param description Short pipeline description
    @param nullable    List of params allowed to be null; raises an error for
                       null params that are not in this list (default: [])
    @param sort        Boolean to sort params alphabetically (default: false)
    @param skip        List of params not to print; if null, all params are
                       printed (default: ['help'])
    @param params      Map of params (default: script params)
    @param separator   Separator used between description and params
                       (default: '-' based on description length)
    @param indent      Number of spaces used to indent the message (default: 4)
  */
  indent = " " * indent
  log.info "\n${indent}${description}\n${indent}${separator}"

  // ignore specific parameters
  p = params.findAll { it.key !in skip }

  // sort parameters (by default, they are ordered as defined in the script)
  if (sort) { p = p.sort() }

  // get max length of keys
  max = p.collect { it.key.length() }.max()
  
  for (i in p) {
    // print parameter
    log.info "${indent}${i.key.padRight(max)} : ${i.value}"

    if (i.key !in nullable && i.value == null) {
      // raise error if param is null
      exit 1, "ERROR: parameter --${i.key} not defined"
    }
  }
  log.info ""
}

def print_summary() {
  /*
    Print workflow summary when it finishes
  */
  workflow.onComplete {
    println ( workflow.success ? """
    Workflow summary
    ----------------
    Completed at: ${workflow.complete}
    Duration    : ${workflow.duration}
    Success     : ${workflow.success}
    workDir     : ${workflow.workDir}
    Exit status : ${workflow.exitStatus}
    """ : """
    Failed      : ${workflow.errorReport}
    Exit status : ${workflow.exitStatus}
    """
    )
  }
}

def jvm_max_heap () {
  /*
    Max heap size of the running JVM, in GiB

    Reads the MaxHeapSize flag that HotSpot is honouring rather than
    Runtime.getRuntime().maxMemory(), which reports the heap minus one survivor
    space. That shortfall depends on the garbage collector, so maxMemory()
    under-reports a correctly sized heap: at -Xmx4g it gives 3.87 GiB under
    SerialGC and 3.56 GiB under ParallelGC, against the full 4.00 GiB under
    G1GC. SerialGC is what the JVM picks when only one CPU is available, which
    is the common case for a head node in a single-core scheduler allocation.
  */
  try {
    server = java.lang.management.ManagementFactory.getPlatformMBeanServer()
    bean   = new javax.management.ObjectName('com.sun.management:type=HotSpotDiagnostic')
    option = server.invoke(bean, 'getVMOption',
                           ['MaxHeapSize'] as Object[],
                           ['java.lang.String'] as String[])
    bytes  = option instanceof javax.management.openmbean.CompositeData ?
               option.get('value') : option.value
    return (bytes as Long) / (1024 ** 3)
  }
  catch (Exception e) {
    // Not a HotSpot JVM, or the diagnostic bean is unavailable: fall back and
    // accept the survivor-space shortfall.
    return Runtime.getRuntime().maxMemory() / (1024 ** 3)
  }
}

def check_JVM_mem (min=0.4) {
  /*
    Check memory available to the Java Virtual Machine (JVM) instance running
    the Nextflow head node

    Throws error if memory is lower than parameter min

    @param min Min memory in GiB (default: 0.4)
  */

  // Use a numeric type that supports rounding.
  min = min / 10.0 * 10.0
  mem = jvm_max_heap() // in GiB
  if (mem < min) {
    // Round the suggested heap up, so that following the advice satisfies this
    // check even when min is not a whole number of GiB.
    suggested = Math.ceil(min as double) as int
    log.error """
    ERROR: The memory of the Nextflow head node (${mem.round(2)} GiB) is below the recommended (${min} GiB); please try the following actions:
      - Increase the JVM's max RAM percentage: export NXF_JVM_ARGS="-XX:InitialRAMPercentage=25 -XX:MaxRAMPercentage=75"
      - Increase the JVM heap size memory: export NXF_OPTS="-Xms50m -Xmx${suggested}g"
      - Increase the memory of the job used to run the Nextflow pipeline: salloc --time 6:00:00 --mem ${(min * 4).round(0)}GB
    """.stripIndent()
    exit 1
  }
}

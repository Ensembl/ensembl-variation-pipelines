# VCF Prepper

VCF prepper pipeline is an orchestrated way to run VEP. It does the following -
- process input VCF and format it according to specification
- generate VEP INI config file
- run VEP

## Meta-map

Because we extensively use meta-map instead of using more channels to allow extensibility we need to make sure the process that creates/process/generate file and do not set it in the output are set cache to false.

This should be done on file processing that does not take much time so that a pipeline restart is not too castly.

I have tried to not make a complex meta-map and simplify it.
genome meta will only contain meta regarding genome.
cache/fasta/gff/plugin/custom_anno each will also contain their own meta.

meta-map will be generated only at the beginning and would not be updated in any of the process. So it will be easier to join/combine channel using genome meta. 
If we need to use same meta in multiple process and then we need to join/combine it will create problem as the meta would not be considered different.

Before we relied on gff/fasta file name that is fixed and used the meta-map to get their directories. So it was not communicated via the exact process handling the specfic file. Instead of doing that we would rely on file that contain vep config file parts.

## Process name

use PROCESS/PREPARE prefix if the process/workflow update meta-map 

## Project layout

Input config
- list of genome
- each genome can have input from multiple source

VEP config
- generic config; does not specify genome
- we can create separate config for sv, sv-hgvs3 etc.
- we can overwrite config for a genome
- we can not overwrite config for a source in a genome

- the files and params under plugin needs to be list because if there is unkeyed plugin we will need them in order

fasta/gff/cache

if file is provided check if the file exist+valid, if so write that file in output file, otherwise try to process or error
if dir is provided locate file name using factory, check if the file exists+valid, if so write that file in output file, otherwise try to process or error
if di/file not provided locate dir+file using factory, process file if needed, if so write that file in output file, otherwise try to process or error

Note:
if you want to run different vep with different config you better run separate runs of the pipeline

other updates
- update pipeline so that we can create subworkflow from output of vep run
    - we can have separate sub-workflow


file locator
file locators locate a file in a particular common storage area - typically FTP or our internal data directory. It can also be another teams data directory or in a hypothetical scenario remote s3 and so on.
A locator can use a client's help to determine the exact location of a file in that common storage area.
A storage area can often use different storage media - FTP can use disk and server. We can use a retry mechanism.

Ideally we would like to use fixed locator for a particular file type. For example, FTP loator for GFF, FASTA, etc. So the default will always be 'CURRENT'. We can support a legacy factory until some time and the availability can be checked via a enum type.
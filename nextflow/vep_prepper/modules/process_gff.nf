process PROCESS_GFF {	
	input:
	tuple path(gff), val(outdir_suffix)

	output:

	shell:
	out_dir = params.outdir + "/" + outdir_suffix
	out_filename = gff.getName().replace(".gz", ".bgz")

	'''
	gzip -dc !{gff} > test.gff
	(grep "^#" test.gff & grep -v "^#" test.gff | sort -k1,1 -k4,4n -k5,5n -t$'\t') | bgzip -c > !{out_filename}
	tabix -p gff -C !{out_filename}

	# move
	mkdir -p !{out_dir}
	mv *.bgz* !{out_dir}/
	'''
}

ENS_REPO_PATH=../../../ensembl-vep/

clean:
	rm -rf external-imports/

external-imports/:
	mkdir external-imports/

external-imports/ensembl-vep: external-imports/
	ln -s ${ENS_REPO_PATH} external-imports/ensembl-vep

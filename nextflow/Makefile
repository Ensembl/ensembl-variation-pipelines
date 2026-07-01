ENS_REPO_PATH=../../../ensembl-vep/

clean:
	rm -rf external-imports/

external-imports/:
	mkdir -p external-imports/

external-imports/ensembl-vep: external-imports/
	ln -fns ${ENS_REPO_PATH} external-imports/ensembl-vep

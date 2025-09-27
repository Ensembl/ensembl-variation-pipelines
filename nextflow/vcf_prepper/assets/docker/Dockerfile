FROM ubuntu:22.04 

LABEL maintainer="ensembl-variation@ebi.ac.uk"

# For nextflow matrics collection
RUN apt-get update && apt-get -y install \
    awk date grep ps sed tail tee

RUN pip3 install -e ../../../../src/python

ENV PYTHONPATH=\$PYTHONPATH:/app

FROM ubuntu:22.04 

LABEL maintainer="ensembl-variation@ebi.ac.uk"

ARG BRANCH_NAME=unit_test

# basic tools
RUN apt-get update && apt-get -y install \
    gawk \
    grep \
    sed \
    wget \
    software-properties-common

# install specific python version from deadsnakes
RUN add-apt-repository ppa:deadsnakes/ppa
RUN apt-get update && apt-get -y install python3.7

# install pip and python requirements
RUN wget https://bootstrap.pypa.io/get-pip.py && python3 get-pip.py
RUN wget https://raw.githubusercontent.com/nakib103/ensembl-variation-pipelines/$BRANCH_NAME/nextflow/vcf_prepper/requirements.py
RUN sed -i -e "s/ensembl-variation-pipelines.git@main/ensembl-variation-pipelines.git@$BRANCH_NAME/" requirements.py
RUN pip install -r requirements.txt
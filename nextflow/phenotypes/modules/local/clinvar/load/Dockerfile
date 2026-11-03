# Build from this directory; Python sources are staged by Nextflow.
FROM python:3.14.0-slim
# Nextflow task metrics require ps inside the container.
RUN apt-get update \
    && apt-get install -y --no-install-recommends procps \
    && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir 'psycopg[binary]==3.3.6' 'simplejson==3.20.2'

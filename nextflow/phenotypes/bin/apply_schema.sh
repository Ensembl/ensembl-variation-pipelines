#!/bin/bash

# assumes PGSCHEMA has been initiated in db, and then set in .env 
set -a; source .env; set +a
psql -X -v ON_ERROR_STOP=1 -c "SET search_path TO ${PGSCHEMA}" -f sql/schema.sql
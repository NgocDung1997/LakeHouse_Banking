#!/bin/sh
# ============================================================
# One database per layer for the Iceberg REST JDBC catalogs (runs only on the first start of an empty volume).
#
# Each layer's catalog registry (iceberg_tables, iceberg_namespace_properties) lives in its own database,
# apart from the banking source data in ${POSTGRES_DB}. The iceberg-rest-<layer>-dungafk service creates the
# two tables itself on first start; this script only creates the empty databases.
# ICEBERG_<LAYER>_CATALOG_DB come from .env (env_file in docker-compose.yml).
# ============================================================
set -e

# Every ICEBERG_<LAYER>_CATALOG_DB variable from .env gets its own database (e.g. iceberg_catalog_bronze).
dbs=$(env | sed -n 's/^ICEBERG_[A-Z]*_CATALOG_DB=//p')
if [ -z "${dbs}" ]; then
    echo "No ICEBERG_<LAYER>_CATALOG_DB variable set — no Iceberg catalog database created" >&2
    exit 1
fi

for db in ${dbs}; do
    psql -v ON_ERROR_STOP=1 --username "${POSTGRES_USER}" --dbname "${POSTGRES_DB}" <<EOSQL
CREATE DATABASE "${db}" OWNER "${POSTGRES_USER}";
COMMENT ON DATABASE "${db}" IS 'Iceberg REST JDBC catalog registry of one layer (lakehouse metadata, not business data)';
EOSQL
done

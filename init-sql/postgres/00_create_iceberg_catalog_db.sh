#!/bin/sh
# ============================================================
# Separate database for the Iceberg REST JDBC catalog (runs only on the first start of an empty volume).
#
# The catalog registry (iceberg_tables, iceberg_namespace_properties) lives in its own database
# ${ICEBERG_CATALOG_DB}, apart from the banking source data in ${POSTGRES_DB}. iceberg-rest-dungafk
# creates the two tables itself on first start; this script only creates the empty database.
# ICEBERG_CATALOG_DB comes from .env (env_file in docker-compose.yml).
# ============================================================
set -e

if [ -z "${ICEBERG_CATALOG_DB}" ]; then
    echo "ICEBERG_CATALOG_DB is not set — skipping creation of the Iceberg catalog database" >&2
    exit 1
fi

psql -v ON_ERROR_STOP=1 --username "${POSTGRES_USER}" --dbname "${POSTGRES_DB}" <<EOSQL
CREATE DATABASE "${ICEBERG_CATALOG_DB}" OWNER "${POSTGRES_USER}";
COMMENT ON DATABASE "${ICEBERG_CATALOG_DB}" IS 'Iceberg REST JDBC catalog registry (lakehouse metadata, not business data)';
EOSQL

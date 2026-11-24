-- ============================================================
-- Source schemas, one per business domain (runs first: files execute in name order)
--   crm          customer master data
--   core_banking accounts and account transactions (retail + core banking)
--   payment      card / wallet payments at merchants
-- The public schema stays empty: the Iceberg REST JDBC catalog lives in its own database (ICEBERG_CATALOG_DB,
-- see 00_create_iceberg_catalog_db.sh).
-- ============================================================

CREATE SCHEMA IF NOT EXISTS crm;
CREATE SCHEMA IF NOT EXISTS core_banking;
CREATE SCHEMA IF NOT EXISTS payment;

COMMENT ON SCHEMA crm          IS 'Customer domain: customer master data';
COMMENT ON SCHEMA core_banking IS 'Core banking domain: accounts and account transactions';
COMMENT ON SCHEMA payment      IS 'Payment domain: merchant card / wallet payments';

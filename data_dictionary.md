# Data Dictionary — LakeHouse_Banking

Describes every dataset the lakehouse uses: source tables in Postgres, how they land in the bronze layer,
known data quirks, and (later) silver / gold models. This is the single reference for "what does this column mean".

- **Maintenance:** any change to the data (new table/column, changed allowed values, new seed logic, new layer)
  is reflected here in the same change, and a line is added to the [Change log](#8-change-log).
- **Source of truth for schemas:** `init-sql/postgres/*.sql` (source) and `jobs/` (lakehouse layers).
  If this document and the code disagree, the code wins — fix the document.

---

## 1. Overview

| Layer | Location | Status |
|---|---|---|
| Source | Postgres database `${POSTGRES_DB}` (container `postgres-dungafk`), one schema per business domain: `crm`, `core_banking`, `payment` | Done |
| Bronze | Iceberg `bronze.<domain>.<table>` (catalog `bronze`), one partition per business date `cob_dt` | Loaded (transaction 2026-11-16..20, merchant_transaction 2026-11-21, snapshots 2026-11-20 / 23) |
| Silver | Iceberg `silver.<namespace>.<table>` (catalog `silver`): namespace `base` (dims / facts) and `pii` (encrypted personal data) | Loaded (dims from snapshots 2026-11-20 / 23, transactions 2026-11-16..20, merchant 2026-11-21) |
| Gold | Not designed yet (transaction analytics marts) | Planned |

**Source schemas (business domains).** Each Postgres schema is one domain, and the bronze layer keeps the same split
(Iceberg catalog `bronze`, namespace `<domain>`: `bronze.<domain>.<table>`). The `public` schema is empty.
The Iceberg REST catalog tables (`iceberg_tables`, `iceberg_namespace_properties`) live in a **separate database**
`ICEBERG_BRONZE_CATALOG_DB` (default `iceberg_catalog_bronze`) on the same Postgres server — lakehouse metadata, not business data.

| Schema (domain) | Tables | Business meaning |
|---|---|---|
| `crm` | `customer` | Customer master data |
| `core_banking` | `account`, `transaction`, `core_account` | Accounts and account transactions (retail + core banking) |
| `payment` | `merchant_transaction` | Card / wallet payments at merchants |

**Data lineage groups.** Independently of the schemas, the tables form **two groups that are *not* linked by any key**:

| Group | Tables | Purpose |
|---|---|---|
| **A. Retail banking (batch)** | `crm.customer` → `core_banking.account` → `core_banking.transaction` | Main batch pipeline: bronze → silver → gold |
| **B. Core / merchant (CDC)** | `core_banking.core_account`, `payment.merchant_transaction` | Future Debezium CDC sources: upsert (`core_account`) and aggregate (`merchant_transaction`) cases |

### 1.1 Data flow through the layers

Solid lines = built, dashed lines / dashed boxes = planned (design not decided yet).

```mermaid
flowchart LR
    subgraph SRC["Source — Postgres 15 (schemas crm / core_banking / payment)"]
        direction TB
        s_cust[crm.customer]
        s_acc[core_banking.account]
        s_txn[core_banking.transaction]
        s_core[core_banking.core_account]
        s_mt[payment.merchant_transaction]
    end

    subgraph BRZ["Bronze (Iceberg) — raw copy + cob_dt + technical columns"]
        direction TB
        b_cust["bronze.crm.customer<br/>daily snapshot"]
        b_acc["bronze.core_banking.account<br/>daily snapshot"]
        b_txn["bronze.core_banking.transaction<br/>incremental by transaction_date"]
        b_core["bronze.core_banking.core_account<br/>daily snapshot"]
        b_mt["bronze.payment.merchant_transaction<br/>incremental by txn_ts"]
    end

    subgraph SLV["Silver — cleansed, typed, history (SCD), PII encrypted"]
        direction TB
        sv_a["silver.base<br/>dim_party, dim_account (SCD2), dim_date<br/>fact_account_balance, fact_transaction"]
        sv_p["silver.pii<br/>dim_party_identity (SCD1), dim_contact (SCD3)"]
        sv_b["silver.base.fact_merchant_transaction<br/>(group B, simple)"]
    end

    subgraph GLD["Gold — analytics marts (planned)"]
        direction TB
        g_txn["Transaction analytics marts"]
        g_mer["Merchant revenue aggregates"]
    end

    s_cust -->|"Spark JDBC<br/>per cob_dt"| b_cust
    s_acc --> b_acc
    s_txn --> b_txn
    s_core --> b_core
    s_mt --> b_mt

    b_cust -->|"Spark SQL<br/>per cob_dt"| sv_a
    b_cust --> sv_p
    b_acc --> sv_a
    b_txn --> sv_a
    b_mt --> sv_b
    sv_a -.-> g_txn
    sv_b -.-> g_mer

    s_core -.->|"Debezium CDC<br/>(later)"| b_core

    classDef planned stroke-dasharray: 5 5
    class GLD,g_txn,g_mer planned
```

### 1.2 Table relationships (source)

`||--o{` = one-to-many with a real foreign key; `||..o{` = logical link only (no FK constraint in Postgres).
Schemas: `customer` is in `crm`; `account`, `transaction`, `core_account` in `core_banking`; `merchant_transaction` in `payment`.
The `account → customer` FK crosses schemas (`core_banking.account.customer_id` → `crm.customer.customer_id`).

```mermaid
erDiagram
    customer ||--o{ account : "owns (customer_id)"
    account  ||--o{ transaction : "has (account_num)"
    core_account ||..o{ merchant_transaction : "pays from (account_id, no FK)"

    customer {
        int customer_id PK
        varchar full_name
        char gender "M | F"
        date date_of_birth
        varchar phone
        varchar email
        varchar city
        varchar customer_segment "RETAIL | PRIORITY | VIP"
        smallint is_active "1 | 0"
        date register_date
        timestamp last_updated
    }

    account {
        char account_num PK "1900 + 8 digits"
        int customer_id FK
        varchar account_type "CHECKING | SAVINGS | CREDIT"
        numeric balance
        date open_date
        varchar status "ACTIVE | CLOSED | FROZEN"
        timestamp last_updated
    }

    transaction {
        int transaction_id PK
        char account_num FK
        timestamp transaction_date
        numeric amount "always positive"
        varchar transaction_type "DEPOSIT | WITHDRAWAL | TRANSFER | PAYMENT"
        varchar status "SUCCESS | FAILED | PENDING"
        varchar channel "ATM | ONLINE | MOBILE | BRANCH"
        varchar description
    }

    core_account {
        varchar account_id PK "ACC001..."
        varchar cif
        varchar customer_name
        varchar account_type "CHECKING | SAVINGS | BUSINESS"
        numeric balance
        varchar status "ACTIVE | FROZEN | CLOSED"
        date open_date
        timestamp updated_at
    }

    merchant_transaction {
        varchar txn_id PK "UUID"
        varchar merchant_id
        varchar merchant_name
        varchar category "FOOD_DRINK | TRANSPORT | SHOPPING | UTILITY | ENTERTAINMENT"
        numeric amount
        varchar txn_status "SUCCESS | FAILED | PENDING"
        varchar account_id "logical FK"
        timestamp txn_ts
        timestamp created_at
    }
```

There is **no link between domain A and domain B** (`core_account.account_id` ≠ `account.account_num`).

---

## 2. Group A — Retail banking (`crm`, `core_banking`)

### 2.1 `crm.customer`

One row per bank customer. Script: `init-sql/postgres/01_seed_customer.sql`.

| Column | Type | Null | Key | Allowed values / rule | Description |
|---|---|---|---|---|---|
| `customer_id` | SERIAL (int) | NO | PK | 1..20 | Surrogate customer ID |
| `full_name` | VARCHAR(200) | NO | | Vietnamese, with diacritics | Customer full name (PII) |
| `gender` | CHAR(1) | NO | | `M`, `F` | Gender |
| `date_of_birth` | DATE | NO | | 1970–1998 in seed | Date of birth (PII) |
| `phone` | VARCHAR(15) | YES | | 10-digit VN mobile | Phone number (PII) |
| `email` | VARCHAR(200) | YES | | | Email (PII) |
| `city` | VARCHAR(100) | YES | | TP.HCM, Hà Nội, Đà Nẵng, Hải Phòng, Cần Thơ | City of residence |
| `customer_segment` | VARCHAR(20) | NO | | `RETAIL`, `PRIORITY`, `VIP` | Customer tier |
| `is_active` | SMALLINT | NO | | `1` = active, `0` = inactive (default 1) | Active flag |
| `register_date` | DATE | NO | | 2016–2023 in seed | Date the customer joined the bank |
| `last_updated` | TIMESTAMP | NO | | default `NOW()` | Last change time (see quirks) |

**Seed:** 20 fixed rows (hand-written). 1 inactive customer (`Mai Thị Thu`).

### 2.2 `core_banking.account`

One row per bank account; a customer has many accounts. Script: `02_seed_account.sql`.

| Column | Type | Null | Key | Allowed values / rule | Description |
|---|---|---|---|---|---|
| `account_num` | CHAR(12) | NO | PK | `'1900'` + 8 digits | Account number |
| `customer_id` | INTEGER | NO | FK → `crm.customer.customer_id` | | Account owner |
| `account_type` | VARCHAR(20) | NO | | `CHECKING`, `SAVINGS`, `CREDIT` | Product type |
| `balance` | NUMERIC(15,2) | NO | | 0 – 500,000,000 (VND), default 0 | Current balance |
| `open_date` | DATE | NO | | within the last 5 years before seed date | Account open date |
| `status` | VARCHAR(10) | NO | | `ACTIVE`, `CLOSED`, `FROZEN` | Account status |
| `last_updated` | TIMESTAMP | NO | | default `NOW()` | Last change time |

**Seed:** 200 rows, round-robin over customers → **10 accounts per customer**.
`account_type`, `balance`, `open_date` are random; `status` is random with weights ACTIVE ≈ 4/6, CLOSED ≈ 1/6, FROZEN ≈ 1/6.

### 2.3 `core_banking.transaction`

One row per account transaction. Script: `03_seed_transaction.sql`. Index on `transaction_date`.

| Column | Type | Null | Key | Allowed values / rule | Description |
|---|---|---|---|---|---|
| `transaction_id` | SERIAL (int) | NO | PK | 1..10000 | Transaction ID |
| `account_num` | CHAR(12) | NO | FK → `core_banking.account.account_num` | | Account the transaction belongs to |
| `transaction_date` | TIMESTAMP | NO | | T-1 … T-5 (relative to seed date), random time of day | Transaction time |
| `amount` | NUMERIC(15,2) | NO | | 10,000 – 50,000,000 (VND), always positive | Transaction amount |
| `transaction_type` | VARCHAR(20) | NO | | `DEPOSIT`, `WITHDRAWAL`, `TRANSFER`, `PAYMENT` | Transaction type |
| `status` | VARCHAR(10) | NO | | `SUCCESS`, `FAILED`, `PENDING` | Result (weights SUCCESS ≈ 4/6, FAILED ≈ 1/6, PENDING ≈ 1/6) |
| `channel` | VARCHAR(10) | NO | | `ATM`, `ONLINE`, `MOBILE`, `BRANCH` | Channel used |
| `description` | VARCHAR(255) | YES | | `GD-D<day_offset>-<nnnn>` | Free text (seed: synthetic code) |

**Seed:** 2,000 rows/day × 5 days = **10,000 rows**, spread evenly → **50 transactions per account**.
There is no `last_updated` column — the table is append-only by design.

---

## 3. Group B — Core account & merchant, CDC sources (`core_banking`, `payment`)

### 3.1 `core_banking.core_account`

Core-banking account snapshot; future CDC **upsert** source (Debezium → Iceberg MERGE). Script: `04_core_account.sql`.

| Column | Type | Null | Key | Allowed values / rule | Description |
|---|---|---|---|---|---|
| `account_id` | VARCHAR(20) | NO | PK | `ACC001`… | Core account ID |
| `cif` | VARCHAR(15) | NO | | `CIF001`… | Customer Information File number |
| `customer_name` | VARCHAR(100) | NO | | unaccented Vietnamese | Customer name (PII) |
| `account_type` | VARCHAR(20) | NO | | `CHECKING`, `SAVINGS`, `BUSINESS` (no CHECK constraint) | Product type |
| `balance` | NUMERIC(18,2) | NO | | default 0 | Current balance (VND) |
| `status` | VARCHAR(10) | NO | | `ACTIVE`, `FROZEN`, `CLOSED` (no CHECK constraint), default `ACTIVE` | Account status |
| `open_date` | DATE | NO | | default `CURRENT_DATE` | Open date |
| `updated_at` | TIMESTAMP | NO | | default `NOW()` | Last change time |

**Seed:** 7 fixed rows (`ACC001`–`ACC007`), 1 `FROZEN` (`ACC005`).

### 3.2 `payment.merchant_transaction`

Card / wallet payments at merchants; future CDC **aggregate** source (e.g. revenue per merchant/category). Script: `05_merchant_transaction.sql`.

| Column | Type | Null | Key | Allowed values / rule | Description |
|---|---|---|---|---|---|
| `txn_id` | VARCHAR(36) | NO | PK | UUID, default `gen_random_uuid()` | Transaction ID |
| `merchant_id` | VARCHAR(20) | NO | | `MER001`… | Merchant ID |
| `merchant_name` | VARCHAR(100) | NO | | | Merchant name (denormalized) |
| `category` | VARCHAR(30) | NO | | `FOOD_DRINK`, `TRANSPORT`, `SHOPPING`, `UTILITY`, `ENTERTAINMENT` (no CHECK) | Merchant category |
| `amount` | NUMERIC(18,2) | NO | | positive (VND) | Payment amount |
| `txn_status` | VARCHAR(10) | NO | | `SUCCESS`, `FAILED`, `PENDING` (no CHECK) | Result |
| `account_id` | VARCHAR(20) | NO | logical → `core_banking.core_account.account_id` (no FK, cross-schema) | | Paying account |
| `txn_ts` | TIMESTAMP | NO | | default `NOW()` | Business time of the payment |
| `created_at` | TIMESTAMP | NO | | default `NOW()` | Row insert time |

**Seed:** 10 rows, `txn_ts` within the last 3 hours before seed time; 6 merchants (`MER001`–`MER006`); 1 `FAILED`.

---

## 4. Bronze layer

Produced by **one self-contained job per table**, `jobs/bronze/<domain>/bronze__<table>.py`. The only shared code is
`add_technical_columns()` in `jobs/bronze/bronze_columns.py` (section 4.3) and the argument parsers in `jobs/common/parsers.py`. The CSV-driven jobs in
`jobs/auto/` (`bronze_tables.csv` with `source_domain` / `target_domain`, + `bronze_columns.csv`; validated by
`jobs/auto/validate/`): the job files above are **generated** from them
(`auto/generate/generate_bronze_job/generate_jobs.py`), and the parameterised `auto/param/ingest_table.py` writes the
same tables with the same columns and load strategy.
Storage: **Iceberg only** (format v2, Snappy Parquet), catalog `bronze` (warehouse `s3://lakehouse/bronze/`), queryable
from Trino. The 5 tables created before 2026-11-25 keep their files under `s3://lakehouse/bronze_<domain>/<table>/`
(registered into the new catalog with `register_table`); new tables go to `s3://lakehouse/bronze/<domain>/<table>/`.

### 4.1 Load strategy

Every run loads **one business date `cob_dt`** (close-of-business date) and replaces exactly that partition
(Iceberg overwrite by filter `cob_dt = <date>`): re-running a date is idempotent, other dates are never touched,
and an empty load clears the partition. Default `cob_dt` = **yesterday in Vietnam time (UTC+7)**; any date can be
passed with `--cob-dt`. Incremental jobs also accept a range `--from-dt/--to-dt` (backfill, one partition per day);
snapshot jobs do not, because a snapshot can only capture the table as it is at run time.

| Load type | Meaning | Source query |
|---|---|---|
| `snapshot` | Copy of the source table as it is **at run time**, tagged with `cob_dt` | `SELECT <columns> FROM <schema>.<table> [WHERE <source_filter>]` |
| `incremental` | Only the rows of one business date, selected by the **required** `source_filter` with placeholders `{cob_dt}` / `{next_dt}` (= cob_dt + 1 day). Default `cob_dt` = T-1 | e.g. `… WHERE transaction_date >= DATE '{cob_dt}' AND transaction_date < DATE '{next_dt}'` (filtered in Postgres) |
| `init` | **First run of an incremental job** (the bronze table does not exist yet): the whole source table, no filter; `cob_dt` = date of `event_col` of each row, so every day gets its own partition | `SELECT <columns> FROM <schema>.<table>` |

**Subsets of columns and rows** (all job kinds: hand-written constants, CSV columns for the auto jobs):

| Setting | Effect |
|---|---|
| `SCHEMA` / `bronze_columns.csv` | StructType declaring the columns to load and their bronze types (CSV: one row per column). Only these columns are selected in Postgres and each is cast to its declared type; bronze then has exactly these columns (+ technical columns). `None` / empty = all source columns with their source types. The current jobs declare all columns with the types bronze already had (section 4.4), so nothing changed for existing tables |
| `SOURCE_FILTER` / CSV `source_filter` | Snapshot: optional **fixed filter** (case 1), applied on every run. Incremental: **required** window of one date, must use `{cob_dt}` on `event_col` (validated, known_issues KI-031); not applied in the init load |
| `--where "<p>"` | **Partial reload** (case 2, ad hoc): only the source rows matching `<p>` (and the fixed filter) are read; in the `cob_dt` partition the rows matching `<p>` are replaced by them, all other rows are kept. One atomic Iceberg commit: the kept rows and the new rows overwrite the partition together. Rows matching `<p>` that no longer exist in the source disappear from the partition |

The filters used are not stored in bronze (the `_source_filter` column was removed 2026-11-25); `_batch_id` tells which
run wrote each row. Predicates must be valid in both Postgres and Spark SQL (`--where` is also evaluated on the bronze
table, so its columns must be loaded columns).
Target table names may differ from the source table (auto jobs: CSV `source_table` / `target_table`).

### 4.2 Jobs (one job = one table)

One Iceberg **catalog per layer** (`bronze`), one namespace per target domain: tables are `bronze.<domain>.<table>`,
the same name in Spark and Trino. Job files are under `jobs/bronze/<domain>/bronze__<table>.py`.

| Domain | Table | Source | Bronze table | Load type | Event column | Primary key |
|---|---|---|---|---|---|---|
| `crm` | `customer` | `crm.customer` | `bronze.crm.customer` | snapshot | — | `customer_id` |
| `core_banking` | `account` | `core_banking.account` | `bronze.core_banking.account` | snapshot | — | `account_num` |
| `core_banking` | `core_account` | `core_banking.core_account` | `bronze.core_banking.core_account` | snapshot | — | `account_id` |
| `core_banking` | `transaction` | `core_banking.transaction` | `bronze.core_banking.transaction` | incremental | `transaction_date` | `transaction_id` |
| `payment` | `merchant_transaction` | `payment.merchant_transaction` | `bronze.payment.merchant_transaction` | incremental | `txn_ts` | `txn_id` |

Why: master data (`customer`, `account`, `core_account`) is small, updated in place and has no reliable change
timestamp (no trigger on `last_updated` / `updated_at`), so a daily snapshot is the only safe way to capture changes.
Event data (`transaction`, `merchant_transaction`) is append-only and large, so only the day's events are loaded.
Bronze does **no data validation**: each job reads the source once and writes it (no cache, no row-count comparison,
no duplicate-key check). Duplicated keys, if any, are kept as is; validation and deduplication come later (silver / a
separate data-quality step, not designed yet).

### 4.3 Columns added to every bronze table

All source columns are kept 1:1 and come first; these columns are appended:

| Column | Type | Description |
|---|---|---|
| `cob_dt` | date | **Business date of the load** (close of business). Partition column (identity). Snapshot: the date the snapshot represents; incremental: the event date. |
| `_load_type` | string | `snapshot`, `incremental`, or `init` (first full load of an incremental table) |
| `_source` | string | `postgres.<schema>.<table>`, e.g. `postgres.core_banking.account` |
| `_batch_id` | string | `<table>_<cob_dt yyyymmdd or init>_<run start UTC yyyymmddThhmmss>`, e.g. `customer_20261120_20261124T055502` — identifies the run that wrote the row |
| `_ingested_at` | timestamp with time zone | Time the row was written |
| `_row_hash` | string | SHA-256 (hex) of all loaded source columns (`sha2(to_json(struct(*)))`). Same row content → same hash on every `cob_dt`, so silver can detect changes (e.g. SCD2) without comparing every column. |

### 4.4 Type mapping (Postgres → Iceberg / Trino)

`SERIAL/INTEGER` → integer, `VARCHAR/CHAR` → varchar, `NUMERIC(p,s)` → decimal(p,s), `DATE` → date,
`TIMESTAMP` → **timestamp(6) without time zone** (read with JDBC `preferTimestampNTZ=true`, so values are kept exactly
as in Postgres and are not shifted by the Spark session time zone). Check with `DESCRIBE bronze.<domain>.<table>` in Trino.

### 4.5 Schema changes

New source columns are added to the bronze table automatically (`write.spark.accept-any-schema` + `mergeSchema`).
Renamed / dropped / retyped source columns are not handled automatically — update the table by hand and note it here.

---

## 5. Silver layer

Catalog `silver` (own Iceberg REST service `iceberg-rest-silver-dungafk`, registry database `ICEBERG_SILVER_CATALOG_DB`
= `iceberg_catalog_silver`, warehouse `s3://lakehouse/silver/`). Namespaces are **by model**, not by source domain:

| Namespace | Holds |
|---|---|
| `base` | Dimensions and facts without personal data (join keys: surrogate keys `*_sk`, `customer_id`) |
| `pii` | Personal data of a customer, **encrypted** (AES-256-GCM), plus keyed hash and masked value. Joined to `base` on `customer_id` |

Input = bronze Iceberg tables, one `cob_dt` partition per run. Jobs: one independent file per table, each with its own
`run_silver_job()` that calls the load pattern directly (dates / options helpers from `jobs/common/`),
`jobs/silver/<namespace>/silver__<table>.py`; the transformation is written in **Spark SQL** (`SOURCE_SQL`), the write
is done by a shared load pattern in `jobs/common/load_patterns.py`, PII expressions come from `jobs/common/pii.py`.
Options: `--cob-dt` (default T-1, UTC+7) or `--from-dt/--to-dt` (dates loaded in order), `--test`
(→ `silver.test_<namespace>`). A date without rows in the bronze source is skipped.
**No data-quality checks yet** (user decision: later).

### 5.1 Load patterns (`jobs/common/load_patterns.py`)

| Function | Used for | What it does |
|---|---|---|
| `merge_upsert` | Facts / events (incremental update) | `MERGE` by key on one `cob_dt` partition: new keys inserted, rows whose `_row_hash` changed updated, others untouched |
| `overwrite_where` | Daily snapshots | Replaces the rows matching a predicate (`cob_dt = X`) in one commit |
| `scd1` | Dimension without history | One row per key, changed attributes overwritten |
| `scd2` | Dimension with history | One row per version: `valid_from`, `valid_to` (9999-12-31 = open), `is_current`. Change → old version closed on `cob_dt - 1`, new version from `cob_dt`. First load of a table: `valid_from = 1900-01-01` (so older facts still find a version). Same-date re-run updates the version in place. A `cob_dt` older than the latest version is refused. Keys missing from the snapshot stay open (deletes not tracked) |
| `scd3` | Dimension with "previous value" | One row per key; for each tracked column `c` also `prev_c`; `prev_changed_dt` = date of the last change |

Common rules:
- Surrogate keys are **BIGINT auto-increment**: `MAX(sk) + ROW_NUMBER()` over the new rows (one writer per table).
- Every table gets `_row_hash` (sha256 of the compared columns), `_batch_id` (`<table>_<cob_dt yyyymmdd>_<run UTC>`)
  and `_updated_at`.
- Only new / changed rows enter a MERGE, so a re-run with the same data writes nothing.
- Duplicate keys in the source: the latest `_ingested_at` wins (`dedup_order`).
- Tables are created on the first run, without `write.spark.accept-any-schema` (known_issues KI-033): add new columns
  with `ALTER TABLE ... ADD COLUMN`.
- `*_sk` lookups in facts use the dimension version valid on the fact date; `-1` = not found.

### 5.2 PII protection (`jobs/common/pii.py`)

| Function | Result | Use |
|---|---|---|
| `encrypt(col)` | base64 AES-256-GCM (new random IV each time) | Stored value, reversible with `decrypt(col [, type])` |
| `keyed_hash(col)` | sha256 of `PII_HASH_KEY` + value | Compare / join / count without decrypting; change detection |
| `mask_phone`, `mask_email`, `mask_name`, `mask` | `090****567`, `a********@gmail.com`, `N***** V** A*` | Display |

Keys: `.env` `PII_ENCRYPTION_KEY` (16 / 24 / 32 characters) and `PII_HASH_KEY`. Losing or changing the encryption key
makes stored values unreadable; changing the hash key marks every row as changed on the next run.

### 5.3 Tables

| Table | Source | Pattern | Key | Partition | Rows (2026-11-26) |
|---|---|---|---|---|---|
| `silver.base.dim_date` | generated 2015-01-01..2035-12-31 | full rebuild | `date_sk` = yyyymmdd | — | 7,670 |
| `silver.base.dim_party` | `bronze.crm.customer` | SCD2 | `party_sk` (NK `customer_id`) | — | 20 |
| `silver.base.dim_account` | `bronze.core_banking.account` | SCD2 | `account_sk` (NK `account_num`) | — | 200 |
| `silver.base.fact_account_balance` | `bronze.core_banking.account` | overwrite `cob_dt` | `cob_dt` + `account_num` | `cob_dt` | 400 (2 dates) |
| `silver.base.fact_transaction` | `bronze.core_banking.transaction` | merge_upsert on `cob_dt` | `transaction_id` | `cob_dt` | 10,000 |
| `silver.base.fact_merchant_transaction` | `bronze.payment.merchant_transaction` | merge_upsert on `cob_dt` | `txn_id` | `cob_dt` | 10 |
| `silver.pii.dim_party_identity` | `bronze.crm.customer` | SCD1 | `customer_id` | — | 20 |
| `silver.pii.dim_contact` | `bronze.crm.customer` | SCD3 | `customer_id` | — | 20 |

Run order (facts look up the dims): `dim_date` (once) → `dim_party`, `dim_account`, `pii.*` → facts.

**Columns** (technical columns `_row_hash`, `_batch_id`, `_updated_at` on every table except `dim_date`):

| Table | Columns |
|---|---|
| `dim_party` | `party_sk` bigint, `customer_id` int, `party_type` (`CUSTOMER`), `gender` (`M`/`F`), `age_band` (`<25`, `25-34`, `35-44`, `45-54`, `55+`, `UNKNOWN`; age on `cob_dt`), `city`, `customer_segment` (`RETAIL`/`PRIORITY`/`VIP`), `is_active` boolean (from 0/1), `register_date`, `valid_from`, `valid_to`, `is_current` |
| `dim_account` | `account_sk` bigint, `account_num` (trimmed CHAR(12)), `customer_id`, `account_type`, `account_status`, `open_date`, `valid_from`, `valid_to`, `is_current`. No balance (it changes daily → fact) |
| `fact_account_balance` | `cob_dt`, `account_sk`, `party_sk`, `account_num`, `customer_id`, `account_status`, `balance` decimal(15,2) |
| `fact_transaction` | `transaction_id`, `cob_dt`, `transaction_ts` timestamp (no zone), `account_sk`, `party_sk`, `account_num`, `transaction_type`, `direction` (`IN` = DEPOSIT, else `OUT`), `amount` (positive), `signed_amount` (+ in / − out), `transaction_status`, `channel`, `description` |
| `fact_merchant_transaction` | `txn_id`, `cob_dt`, `txn_ts`, `merchant_id`, `merchant_name`, `category`, `amount` decimal(18,2), `txn_status`, `account_id` (group B, no dimension), `created_at` |
| `dim_date` | `date_sk` int, `calendar_date`, `year`, `quarter`, `month`, `month_name`, `week_of_year`, `day_of_month`, `day_of_week` (1 = Monday), `day_name`, `is_weekend`, `is_month_end`, `month_start_date` |
| `pii.dim_party_identity` | `customer_id`, `full_name_enc`, `full_name_masked`, `date_of_birth_enc` (decrypt with `as_type='DATE'`); changes detected on keyed hashes of the plaintext |
| `pii.dim_contact` | `customer_id`, `phone_enc` / `phone_hash` / `phone_masked` (phone normalized to digits and `+`), the same for `email` (lower-cased), `prev_*` of all six, `prev_changed_dt` |

Notes:
- SCD2 history starts at the first silver load. Bronze snapshots for past dates (backfill) hold the state at run time,
  not the real state on that date (section 7, item 11).
- Codes are trimmed and upper-cased but not yet checked against the allowed values.
- SCD3: re-running a date with different data overwrites `prev_*` with the value of the first run of that date.

## 6. Gold layer

_Not designed yet._ Planned: transaction analytics marts.

---

## 7. Known quirks & data-quality notes

Keep in mind when designing silver / gold:

1. **Groups A and B are unrelated.** `core_account.account_id` (`ACC001`) does not match `account.account_num`
   (`1900…`), and `cif` / `customer_name` do not link to `customer`. Names in `core_account` are unaccented.
2. **Dates are relative to the seed time.** `account.open_date`, `transaction.transaction_date` and
   `merchant_transaction.txn_ts` are computed from `CURRENT_DATE` / `NOW()` when the Postgres volume was first
   initialised, so they shift each time the stack is reset.
3. **Seed values are random** (`random()`): account types, balances, statuses, transaction amounts differ after every reset.
   Row counts and key distribution (10 accounts/customer, 50 transactions/account) are deterministic.
4. **`last_updated` / `updated_at` are not maintained automatically** — there is no trigger; they only hold the
   insert time unless the writer sets them. They are not reliable watermarks for incremental loads yet.
5. **`amount` is always positive** in `transaction`; direction must be derived from `transaction_type`
   (`DEPOSIT` = inflow; `WITHDRAWAL`, `TRANSFER`, `PAYMENT` = outflow).
6. **Transactions on non-active accounts:** seed data assigns transactions to `CLOSED` / `FROZEN` accounts too.
7. **`balance` is not derived from transactions** — balances and transaction amounts are generated independently.
8. **Group B has no CHECK constraints** on `account_type`, `status`, `category`, `txn_status` — silver must validate them.
9. **`is_active` is SMALLINT 0/1**, not boolean — cast in silver.
10. **PII:** `full_name`, `date_of_birth`, `phone`, `email`, `customer_name` are personal data (synthetic here).
    Sample emails use real domains (`@gmail.com`, `@yahoo.com`).
11. **Snapshot backfill is not historical.** A snapshot task run for an old `cob_dt` stores the source as it is *now*,
    not as it was on that date (Postgres keeps no history). Only snapshots taken on the day are true point-in-time copies.
12. **Timestamps are local time without time zone** in Postgres. Bronze keeps them as `timestamp` (no zone); never read
    them as `timestamp with time zone` in Spark, or late-evening rows shift to the next date (+7h).
13. **Incremental dates follow the seed.** `transaction` only has rows on the 5 days before the seed date and
    `merchant_transaction` only on the seed date, so most other `cob_dt` values load 0 rows for these tables.

---

## 8. Change log

| Date | Change |
|---|---|
| 2026-11-24 | Document created from `init-sql/postgres/01–05` and `jobs/ingest_data/ingest_minio.py`. Row counts taken from seed scripts (Docker was not running; not yet checked against the live DB). |
| 2026-11-24 | Added Mermaid diagrams: data flow through layers (1.1) and full table relationship diagram (1.2). |
| 2026-11-24 | **Source split into domain schemas**: `crm` (customer), `core_banking` (account, transaction, core_account), `payment` (merchant_transaction); `public` keeps only the Iceberg catalog tables. New `init-sql/postgres/00_create_schemas.sql`; seeds use qualified names. Live DB migrated with `ALTER TABLE … SET SCHEMA` (no data loss, row counts 20/200/10000/7/10 verified). **Bronze split by domain**: Parquet `s3a://lab/bronze/<domain>/<table>/`, Iceberg `lakehouse.bronze_<domain>.<table>`; `_source` = `postgres.<schema>.<table>`. Old `bronze/<table>` Parquet folders and `lakehouse.bronze.*` Iceberg tables were deleted. |
| 2026-11-24 | **Iceberg catalog moved to its own database** `iceberg_catalog` (`ICEBERG_CATALOG_DB` in `.env`), separate from the banking source DB. `iceberg_tables` / `iceberg_namespace_properties` copied with `pg_dump` (5 tables, 6 namespace properties, identical md5); `iceberg-rest` now points at the new DB. New init script `init-sql/postgres/00_create_iceberg_catalog_db.sh`. Old copies in the source DB's `public` schema dropped after verification — `public` is now empty. |
| 2026-11-24 | **Bronze rebuilt with `cob_dt`**: new job `jobs/bronze/ingest_bronze.py` + task registry `jobs/bronze/bronze_tables.py`, Iceberg only, partition `cob_dt` (default D-1, UTC+7), overwrite-by-partition. Snapshot load for `customer`, `account`, `core_account`; incremental by event date for `transaction`, `merchant_transaction`. New columns `cob_dt`, `_load_type`, `_batch_id`, `_row_hash` (kept `_source`, `_ingested_at`); timestamps read as TIMESTAMP_NTZ (fixes a +7h date shift). Plain-Parquet bronze (`s3a://lab/bronze/`) and `partition_date` dropped. |
| 2026-11-24 | Cut-over: old bronze (Iceberg tables without `cob_dt`, Parquet `s3a://lab/bronze/`), test tables and `jobs/ingest_data/ingest_minio.py` deleted. Lakehouse is empty; no backfill run yet. |
| 2026-11-24 | Docs cleanup: section 1 no longer mentions Parquet bronze folders; section 5 records that silver reads bronze Iceberg and notes that backfilled snapshot partitions hold run-time state. No data change. |
| 2026-11-24 | **One bronze job per table**: `jobs/bronze/<domain>/ingest_<table>.py` (5 jobs) + shared `jobs/bronze/bronze_lib.py`; new option `--namespace-prefix` for test runs. Same load strategy, partitioning and columns as before. The all-tables job moved to `jobs/bronze/auto/` (parked). Tested on the cluster in `test_bronze_*` namespaces (row counts 20/200/7, transaction 2,000/day for 11-16..11-20, merchant_transaction 10 on 11-21, idempotent re-run), test namespaces dropped. |
| 2026-11-24 | **Bronze jobs made fully standalone** (user request): each `jobs/bronze/<domain>/ingest_<table>.py` has its own source query, write, checks and command line; the only shared code is `add_technical_columns()` in the new `jobs/bronze/bronze_columns.py`. `jobs/bronze/bronze_lib.py` deleted. Snapshot jobs no longer accept `--from-dt/--to-dt`. Same load strategy, partitioning and columns; tested in `test_bronze_*` (20/200/7 rows, transaction 2,000/day 11-16..11-20, merchant_transaction 10 on 11-21, idempotent re-run, `_row_hash` equal to the live bronze rows), test namespaces dropped. |
| 2026-11-24 | Bronze jobs simplified (user request): duplicate-key check, `cache()` and source-vs-bronze row-count check removed — one JDBC read, one Iceberg write per `cob_dt`; `PRIMARY_KEY` constant dropped from the jobs (keys stay documented in 4.2). Validation will be designed separately. Tested in `test_bronze_*` (counts checked from Trino outside the jobs: 20/200/7, transaction 2,000/day, merchant_transaction 10), dropped afterwards. |
| 2026-11-24 | **CSV-driven bronze jobs** in `jobs/bronze/auto/`: table list `bronze_jobs.csv` (`domain,table,load_type,event_col,active`, validated by `job_config.py`); approach B `ingest_table.py --domain --table`; approach A `generate_jobs.py` → `auto/generated/<domain>/ingest_<table>.py` from `auto/templates/` (folder wiped on every run); `run_domain.py` runs a domain table by table, one Spark app each, failures to `logs/bronze/errors.log`. Old `auto/ingest_bronze.py` + `auto/bronze_tables.py` replaced. Same tables, columns, load strategy. Tested in `test_bronze_*` with both approaches (all 5 tables, backfill skips snapshot tables, failing table logged and the next table still ran), test data and logs removed. |
| 2026-11-24 | **Column / row subsets and partial reload** for all bronze jobs (hand-written, auto/param, auto/generate): `COLUMNS` / CSV `columns` (subset of columns), `SOURCE_FILTER` / CSV `source_filter` (fixed WHERE, case 1), `--where` partial reload (case 2: replace only matching rows of a `cob_dt` partition, one atomic commit). New technical column `_source_filter` (NULL = no filter). CSV is now `domain,source_table,target_table,load_type,event_col,columns,source_filter,active` (target name may differ from the source); hand-written job constant `TABLE` renamed `TARGET_TABLE`. Real CSV rows unchanged in meaning (no filter, all columns, same names). Tested in `test_bronze_*` incl. renamed targets, column subsets, filters (counts equal to Postgres: 19 / 170 / 9) and partial reloads; test tables dropped. |
| 2026-11-24 | **Explicit StructType schema** in every bronze job: `COLUMNS` replaced by `SCHEMA` (one `StructField` per column, cast after the JDBC read); CSV `columns` is now `name:type|...` (quoted cell). The 5 jobs and the CSV declare all columns with the types bronze already had (int, string, date, decimal(15,2)/(18,2), timestamp_ntz), so existing tables are unchanged — verified by comparing DESCRIBE of test tables (all 3 job kinds) with the live bronze tables. A custom schema test (subset + casts to double / string) worked; test tables dropped. |
| 2026-11-25 | Job naming `<layer>__<table>.py` (e.g. `jobs/bronze/crm/bronze__customer.py`); CSV tooling moved to `jobs/auto/` (`generate/generate_bronze_job/`, `generate/generate_sql/` reserved). The bronze job files are now generated straight into `jobs/bronze/<domain>/` from `bronze_jobs.csv`. No change to tables, columns or load strategy (all 3 ways tested in test namespaces, 20 / 2,000 / 10 rows). |
| 2026-11-25 | Job configuration split into two files: `jobs/auto/bronze_tables.csv` (one row per job) and `jobs/auto/bronze_columns.csv` (one row per column, `column_name,data_type`, file order = load order) — replaces `bronze_jobs.csv` and its `columns` cell. Generated jobs identical to before the split; no change to tables or columns. |
| 2026-11-25 | `bronze_tables.csv`: `domain` split into `source_domain` (Postgres schema) and `target_domain` (bronze namespace / job folder); `bronze_columns.csv` keyed by `target_domain`. New `jobs/auto/validate/` checks both files and the source in Postgres (known_issues KI-018..KI-030). Current values unchanged (source = target domain); generated jobs identical. |
| 2026-11-25 | **One Iceberg catalog per layer**: bronze tables are now `bronze.<domain>.<table>` in Spark and Trino (was `lakehouse.bronze_<domain>.<table>`). New REST service `iceberg-rest-bronze-dungafk` + registry database `iceberg_catalog_bronze`, warehouse `s3://lakehouse/bronze/`. The 5 existing tables were registered into the new catalog with `register_table` (data and snapshot history kept, files stay under `s3://lakehouse/bronze_<domain>/`); row counts unchanged (20 / 400 / 7 / 10,000 / 10). Test runs use `--test` → `bronze.test_<domain>`. |
| 2026-11-25 | Old Iceberg registry database `iceberg_catalog` (catalog `lakehouse`) dropped (user OK); only `iceberg_catalog_bronze` remains. No data change: table metadata and files are on MinIO. |
| 2026-11-25 | Technical column `_source_filter` removed (user decision: the filter is not stored) — dropped from `bronze.core_banking.transaction` and `bronze.payment.merchant_transaction`. Incremental jobs: `source_filter` is now **required** and is the date window, with `{cob_dt}` / `{next_dt}` (transaction: `transaction_date >= DATE '{cob_dt}' AND transaction_date < DATE '{next_dt}'`, merchant_transaction: same on `txn_ts`; default cob_dt = T-1). **Init**: the first run of an incremental job (table missing) loads the whole source table, `cob_dt` = date of `event_col`, `_load_type` = `init`. New validations KI-031 / KI-032. Tested in `bronze.test_*` (init 10,000 rows over 5 days, T-1, backfill, partial reload), dropped afterwards. |
| 2026-11-26 | **Silver layer built**: catalog `silver` (namespaces `base`, `pii`), shared load patterns `jobs/common/load_patterns.py` (merge_upsert, overwrite_where, scd1, scd2, scd3; auto-increment surrogate keys) and PII functions `jobs/common/pii.py` (AES-256-GCM, keyed hash, masks; keys in `.env`). 8 Spark SQL jobs `jobs/silver/<namespace>/silver__<table>.py`. Loaded: dim_party 20, dim_account 200, dim_date 7,670, fact_account_balance 400 (2026-11-20 / 23), fact_transaction 10,000 (2026-11-16..20, no missing sk), fact_merchant_transaction 10, pii.dim_party_identity 20, pii.dim_contact 20. Tested first in `silver.test_*` (SCD1/2/3 change cases, same-date re-run, out-of-order date refused, idempotent re-run writes 0 rows), test namespaces dropped. |

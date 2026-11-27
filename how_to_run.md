# How to run — LakeHouse_Banking

Run every command from the project folder `E:\Study\LakeHouseProject\LakeHouse_Banking` (PowerShell).
MinIO and Postgres usernames / passwords are in `.env`.

---

## 1. Start / stop the stack

```powershell
# First time only: create .env from the template, then set real credentials in it
Copy-Item .env.example .env

# Build images (first time, or after changing a Dockerfile / startup script)
docker compose build spark-master-dungafk      # lakehouse-spark:3.5.0 — build this FIRST
docker compose build jupyter-dungafk           # lakehouse-jupyter (also used by spark-client)

# Start the whole stack
docker compose up -d

# Recreate containers after changing .env or docker-compose.yml
docker compose up -d --force-recreate

# Status
docker ps --format "table {{.Names}}\t{{.Status}}"

# Stop (keep data)
docker compose down

# Stop and DELETE all data (MinIO + Postgres + Iceberg catalog)
docker compose down -v
```

> `minio-init-dungafk` showing `Exited (0)` is **normal** — it runs once to create the `lakehouse` and `lab` buckets, then exits.

---

## 2. Web UIs / ports

| Service | URL | Notes |
|---|---|---|
| Spark Master UI | http://localhost:8080 | Check worker / core count |
| Spark Worker UI | http://localhost:8081 | |
| Spark App UI (spark-client) | http://localhost:4040 | Only while a job is running |
| Spark App UI (Jupyter) | http://localhost:4041 | Only while a notebook is running Spark |
| MinIO Console | http://localhost:9001 | Log in with `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD` |
| MinIO S3 API | http://localhost:9000 | |
| JupyterLab | http://localhost:8888 | No token required |
| Iceberg REST (bronze) | http://localhost:8181 | `/v1/config` = health check |
| Iceberg REST (silver) | http://localhost:8182 | `/v1/config` = health check |
| Trino (Web UI) | http://localhost:8086 | No authentication: type any user name, no password |
| Trino (JDBC, DBeaver) | `jdbc:trino://localhost:8086/bronze` (or `/silver`) | Any user name (e.g. `lab`), leave the password empty |
| Postgres | `localhost:5432` | Database / user / password = `POSTGRES_DB` / `POSTGRES_USER` / `POSTGRES_PASSWORD` in `.env` |
| MySQL (LOS) | `localhost:3306` | Database `los_db`, user `dungnn_mysql`, password in `.env` (`MYSQL_PASSWORD`) — DBeaver: enable `allowPublicKeyRetrieval=true` if prompted |

All current logins are also kept in `E:\Study\LakeHouseProject\LakeHouse_Banking_Credentials.xlsx` (outside the repo).

---

## 3. Bronze ingestion Postgres → Iceberg (`jobs/bronze/`)

Jobs run inside `spark-client-dungafk` (`./jobs` is mounted at `/home/spark/jobs`). **One job per
table** (generated from the templates; shared code in `jobs/common/` — `jdbc.py`, `load_patterns.py`, `dates.py`,
`parsers.parse_job_args` — and `jobs/bronze/bronze_columns.py`); each run loads a
business date `cob_dt` (optional, no date = **full load** (snapshot: T-1; incremental: whole source table, `cob_dt` = date of the event column, every partition present replaced; silver: every `cob_dt` of the bronze source)) into partition `cob_dt` of `bronze.<domain>.<table>`.
Re-running a date replaces that partition only.

| Job (under `/home/spark/jobs/bronze/`) | Table | Load type |
|---|---|---|
| `crm/bronze__crm__customer.py` | `crm.customer` | snapshot |
| `t24/bronze__t24__account.py` | `t24.account` | snapshot |
| `t24/bronze__t24__core_account.py` | `t24.core_account` | snapshot |
| `t24/bronze__t24__transaction.py` | `t24.transaction` | incremental by `transaction_date` |
| `payment/bronze__payment__merchant_transaction.py` | `payment.merchant_transaction` | incremental by `txn_ts` |
| `los/bronze__los__loan_product.py` | MySQL `los_db.loan_product` | snapshot |
| `los/bronze__los__loan_application.py` | MySQL `los_db.loan_application` | snapshot |
| `los/bronze__los__loan_contract.py` | MySQL `los_db.loan_contract` | snapshot |
| `los/bronze__los__repayment.py` | MySQL `los_db.repayment` | incremental by `payment_date` |

Options:

| Option | Jobs | Meaning |
|---|---|---|
| *(none)* | all | **Full load**: snapshot jobs load T-1 (yesterday, UTC+7); incremental jobs reload the whole source table (`_load_type = full`, `cob_dt` = date of `EVENT_COL`; only the partitions present are replaced) |
| `--cob-dt YYYY-MM-DD` | all | Load one business date |
| `--from-dt YYYY-MM-DD --to-dt YYYY-MM-DD` | incremental only | Backfill a date range (inclusive), one partition per date |
| `--where "<predicate>"` | all | **Partial reload**: read only the source rows matching the predicate and replace exactly those rows in the `cob_dt` partition; other rows are kept (one atomic commit) |

Snapshot jobs have no `--from-dt/--to-dt`: a snapshot always copies the table as it is **now**, so loading past dates
in a loop would only repeat today's state under old dates.

**What each job reads** — constants at the top of the job file:

| Constant | Meaning |
|---|---|
| `SOURCE_TABLE` | Postgres `<schema>.<table>` |
| `DOMAIN` / `TARGET_TABLE` | Bronze table `bronze.<DOMAIN>.<TARGET_TABLE>` (target name may differ from the source) |
| `SCHEMA` | `StructType([StructField("<column>", <Type>()), ...])`: the columns to load **and their bronze types**, declared up front so they can be adjusted to the business rules — remove a field to stop loading it, change its type to cast it (e.g. `DecimalType(15, 2)` → `DoubleType()`). Only these columns are selected in Postgres. `None` = all source columns with their source types |
| `SOURCE_FILTER` | Snapshot: optional fixed WHERE (e.g. `"status <> 'CLOSED'"`). Incremental: **required** window of one date with `{cob_dt}` / `{next_dt}`, e.g. `"transaction_date >= DATE '{cob_dt}' AND transaction_date < DATE '{next_dt}'"` (default cob_dt = T-1) |
| `EVENT_COL` | Incremental jobs: date / timestamp column of the event; gives the `cob_dt` of each row in the **init** load |

**First run of an incremental job** (bronze table does not exist yet) = **init**: the whole source table is loaded and
each row goes to the partition of the date of its `EVENT_COL` (`_load_type = init`). Later runs load one date each, or
the whole table again when no date is given (`_load_type = full`). `--where` needs a date on incremental jobs.

`SOURCE_FILTER` and `--where` run in Postgres (`SELECT <SCHEMA columns> FROM <SOURCE_TABLE> WHERE ...`); they are not
stored in bronze. Write predicates **without** `WHERE`,
keep them simple and portable (comparisons, `IN`, `BETWEEN`, `IS NULL`, `AND`/`OR`/`NOT`): `--where` is also evaluated by
Spark on the bronze table, so its columns must be loaded columns. `;`, `--` and `/* */` are refused.

```powershell
# One snapshot table for a given date (without --cob-dt: T-1)
docker exec -it spark-client-dungafk spark-submit /home/spark/jobs/bronze/crm/bronze__crm__customer.py --cob-dt 2026-11-23

# One table for a given business date
docker exec -it spark-client-dungafk spark-submit /home/spark/jobs/bronze/t24/bronze__t24__transaction.py --cob-dt 2026-11-20

# Backfill the seeded transaction days
docker exec -it spark-client-dungafk spark-submit /home/spark/jobs/bronze/t24/bronze__t24__transaction.py --from-dt 2026-11-16 --to-dt 2026-11-20

# Partial reload: re-read and replace only some rows of one business date
docker exec -it spark-client-dungafk spark-submit /home/spark/jobs/bronze/t24/bronze__t24__transaction.py --cob-dt 2026-11-20 --where "transaction_id IN (1, 2, 3)"

# Help
docker exec -it spark-client-dungafk spark-submit /home/spark/jobs/bronze/crm/bronze__crm__customer.py --help
```

Run all five jobs one after another (PowerShell), here for one business date:
```powershell
$jobs = "crm/bronze__crm__customer.py", "t24/bronze__t24__account.py", "t24/bronze__t24__core_account.py",
        "t24/bronze__t24__transaction.py", "payment/bronze__payment__merchant_transaction.py"
foreach ($j in $jobs) { docker exec spark-client-dungafk spark-submit "/home/spark/jobs/bronze/$j" --cob-dt 2026-11-20 }
```

Hide INFO logs for readability:
```powershell
docker exec spark-client-dungafk spark-submit /home/spark/jobs/bronze/crm/bronze__crm__customer.py --cob-dt 2026-11-23 2>&1 | Where-Object { $_ -notmatch ' INFO |^\s+at ' }
```

Jobs always write to the real tables (there is no test mode, removed 2026-11-26): re-running a date replaces it.

The job files in `jobs/bronze/<domain>/` are **generated** from `jobs/auto/bronze_tables.csv` + `bronze_columns.csv` (approach A, section 3.1):
they start with an `AUTO-GENERATED` line and are overwritten on every generator run — **do not edit them by hand**.
To add or change a table: edit the CSV, run the generator, then document the table in `data_dictionary.md`.
To change the job code itself: edit the template in `jobs/auto/generate/generate_bronze_job/templates/` and regenerate.
A truly hand-written job (no `AUTO-GENERATED` line, name not in the CSV) can also live there; the generator never
deletes it.

### 3.1 CSV-driven jobs (`jobs/auto/`)

Job files are named `<layer>__<domain>__<table>.py` (e.g. `bronze__crm__customer.py`), hand-written or generated.
`jobs/auto/generate/` has one folder per generator: `generate_bronze_job/` (below) and `generate_sql/` (reserved, empty).

The jobs are defined by **two CSV files** in `jobs/auto/` (edit them in Excel or a text editor):

**`bronze_tables.csv`** — one row per job / bronze table:

| Column | Meaning |
|---|---|
| `source_domain` | Postgres schema of the source table (e.g. `t24`) |
| `source_table` | Postgres table name |
| `target_domain` | Bronze domain → namespace `bronze.<target_domain>`, folder `jobs/bronze/<target_domain>/` (usually = `source_domain`). `--domain` of the tools means this name |
| `target_table` | Bronze table name (may differ from the source; unique per target domain). `--table` of the tools means this name |
| `load_type` | `snapshot` or `incremental` |
| `event_col` | Incremental only: date / timestamp column of the event; gives the `cob_dt` of each row in the init load (empty for snapshot) |
| `source_filter` | WHERE predicate without `WHERE`. **Incremental: required**, the window of one date using `{cob_dt}` and `{next_dt}` on `event_col`, e.g. `transaction_date >= DATE '{cob_dt}' AND transaction_date < DATE '{next_dt}'`. Snapshot: optional fixed filter, e.g. `status <> 'CLOSED'` (no placeholders). Quote the cell if it contains a comma |
| `active` | 1 = run, 0 = skip |

**`bronze_columns.csv`** — one row per column to load, in load order (becomes the job's StructType `SCHEMA`):

| Column | Meaning |
|---|---|
| `target_domain`, `target_table` | The job (must exist in `bronze_tables.csv`) |
| `column_name` | Source column |
| `data_type` | Bronze type: `string`, `int`, `bigint`, `smallint`, `double`, `float`, `boolean`, `date`, `timestamp`, `timestamp_ntz`, `decimal(p,s)` — quote decimals: `"decimal(15,2)"` |

A job with **no rows** in `bronze_columns.csv` loads all source columns with their source types.

Example (not in the real files) — a light copy of `account` with 3 columns and only open accounts:
```
bronze_tables.csv:   t24,account,t24,account_open,snapshot,,"status IN ('ACTIVE', 'FROZEN')",1
bronze_columns.csv:  t24,account_open,account_num,string
                     t24,account_open,balance,"decimal(15,2)"
                     t24,account_open,status,string
```
**Validation** — `jobs/auto/validate/` checks both files, and the source in Postgres:
```powershell
docker exec spark-client-dungafk python3 /home/spark/jobs/auto/validate/validate_config.py            # all checks
docker exec spark-client-dungafk python3 /home/spark/jobs/auto/validate/validate_config.py --offline  # files only
```
It reports `ERROR` (must be fixed), `FIXED` (an exact duplicate column row was removed from `bronze_columns.csv`) and
`WARNING`. Every message starts with a known-issue ID — see [known_issues → Validate](known_issues/README.md#validate).
Checked: data_type, load_type, source domain / table exist, event_col exists and is a date / timestamp, columns exist
in the source, duplicated columns (same type → removed, other type → error), source_filter runs, incremental jobs have a
`{cob_dt}` window on `event_col` and load `event_col`, duplicated target
tables, columns of unknown tables, CSV header / quoting, names, `active`.
`generate_jobs.py`, `run_domain.py` and `ingest_table.py` run the same checks first and stop on an ERROR
(`ingest_table.py` only the file checks; `--offline` skips the Postgres checks in the other two).
To add a job: add a row to `bronze_tables.csv` and its columns to `bronze_columns.csv`, then regenerate (approach A).
Other files can be used with `--tables-config` / `--columns-config`. `--where` (partial reload) works as in section 3.

**Approach B — one parameterised job** (`auto/param/ingest_table.py`, same options as the hand-written jobs plus `--domain/--table`):
```powershell
docker exec -it spark-client-dungafk spark-submit /home/spark/jobs/auto/param/ingest_table.py --domain crm --table customer --cob-dt 2026-11-23
docker exec -it spark-client-dungafk spark-submit /home/spark/jobs/auto/param/ingest_table.py --domain t24 --table transaction --from-dt 2026-11-16 --to-dt 2026-11-20
docker exec -it spark-client-dungafk spark-submit /home/spark/jobs/auto/param/ingest_table.py --domain crm --table customer --cob-dt 2026-11-20 --where "customer_id IN (1, 2)"
```

**Approach A — generate one file per table** (for hand-over). Writes `jobs/bronze/<domain>/bronze__<domain>__<table>.py` for every
active CSV row from the templates in `jobs/auto/generate/generate_bronze_job/templates/`. Each run also **deletes the
generated files of tables no longer in the CSV** (or set to `active=0`) — only files with the `AUTO-GENERATED` line,
never hand-written files. A hand-written file with the same name as a CSV job is replaced (the output says so):
```powershell
docker exec spark-client-dungafk python3 /home/spark/jobs/auto/generate/generate_bronze_job/generate_jobs.py
docker exec -it spark-client-dungafk spark-submit /home/spark/jobs/bronze/crm/bronze__crm__customer.py --cob-dt 2026-11-20
```

**Run a whole domain** — active tables one after another, one Spark application per table:
```powershell
# approach B (default)
docker exec spark-client-dungafk python3 /home/spark/jobs/auto/run_domain.py --domain t24 --cob-dt 2026-11-23
docker exec spark-client-dungafk python3 /home/spark/jobs/auto/run_domain.py --domain t24 --cob-dt 2026-11-20
# backfill: incremental tables get the range, snapshot tables are SKIPPED
docker exec spark-client-dungafk python3 /home/spark/jobs/auto/run_domain.py --domain t24 --from-dt 2026-11-16 --to-dt 2026-11-20
# approach A (run generate/generate_bronze_job/generate_jobs.py first)
docker exec spark-client-dungafk python3 /home/spark/jobs/auto/run_domain.py --domain crm --mode generated --cob-dt 2026-11-23
# LOS domain (MySQL source)
docker exec spark-client-dungafk python3 /home/spark/jobs/auto/run_domain.py --domain los --cob-dt 2026-11-23
docker exec spark-client-dungafk spark-submit /home/spark/jobs/bronze/los/bronze__los__repayment.py --from-dt 2026-11-16 --to-dt 2026-11-23
```
`run_domain.py` exits with 1 if any table failed (2 for a bad CSV / unknown domain). A failed table does not stop the
next ones.

**Logs** (host folder `logs/`, mounted at `/home/spark/logs` in `spark-client-dungafk`, git-ignored):

| File | Content |
|---|---|
| `logs/bronze/errors.log` | One tab-separated line per failed table, all runs appended: time (UTC), run_id, mode, domain, table, dates, exit code, error (root cause, e.g. `PSQLException: relation ... does not exist`), path of the run log |
| `logs/bronze/runs/<domain>_<run_id>.log` | Full output of one `run_domain.py` run, including the Spark logs of every table |

```powershell
Get-Content logs\bronze\errors.log | Select-Object -Last 20
```

> Running from **Git Bash**: prefix the command with `MSYS_NO_PATHCONV=1`, otherwise Git Bash rewrites `/home/...` into a Windows path.

---

## 4. Silver modelling bronze → silver (`jobs/silver/`)

Silver tables are defined as **YAML configs** `jobs/silver/<namespace>/silver__<namespace>__<table>.yaml` (namespaces
`base`, `pii`) and executed by one generic runner **`jobs/silver/run_silver.py`**. Exception: `dim_date` is a static
calendar script (`jobs/silver/base/silver__base__dim_date.py`, no options). Jobs always write to the real tables:

| Option | Meaning |
|---|---|
| `--job <job_code>` or `--config <path>` | Required (choose one): locate YAML by `job_code` under `jobs/silver/` or pass an explicit path |
| *(no date)* | **full load**: every `cob_dt` of `source.main_table`, oldest first (SCD2 dims: from the latest version on) |
| `--cob-dt YYYY-MM-DD` | one business date |
| `--from-dt D1 --to-dt D2` | every date of the range, **in order** (dates without rows in `main_table` are skipped) |

**Run order:** dims → pii → facts (facts look up the dimension keys). SCD2 dims must be loaded date by date in
order — an older date than the last one loaded is refused.

```powershell
# once: static calendar & DPD debt-classification reference rules (Python scripts)
docker exec spark-client-dungafk spark-submit /home/spark/jobs/silver/base/silver__base__dim_date.py
docker exec spark-client-dungafk spark-submit /home/spark/jobs/silver/base/silver__base__dim_dpd_classification.py
```
```powershell
# daily for one business date (here 2026-11-23): dims → pii → facts
docker exec spark-client-dungafk spark-submit /home/spark/jobs/silver/run_silver.py --job silver__base__dim_party --cob-dt 2026-11-23
docker exec spark-client-dungafk spark-submit /home/spark/jobs/silver/run_silver.py --job silver__base__dim_account --cob-dt 2026-11-23
docker exec spark-client-dungafk spark-submit /home/spark/jobs/silver/run_silver.py --job silver__base__dim_loan_product --cob-dt 2026-11-23
docker exec spark-client-dungafk spark-submit /home/spark/jobs/silver/run_silver.py --job silver__base__dim_loan_contract --cob-dt 2026-11-23
docker exec spark-client-dungafk spark-submit /home/spark/jobs/silver/run_silver.py --job silver__pii__dim_party_identity --cob-dt 2026-11-23
docker exec spark-client-dungafk spark-submit /home/spark/jobs/silver/run_silver.py --job silver__pii__dim_contact --cob-dt 2026-11-23
docker exec spark-client-dungafk spark-submit /home/spark/jobs/silver/run_silver.py --job silver__base__fact_account_balance --cob-dt 2026-11-23
docker exec spark-client-dungafk spark-submit /home/spark/jobs/silver/run_silver.py --job silver__base__fact_transaction --cob-dt 2026-11-23
docker exec spark-client-dungafk spark-submit /home/spark/jobs/silver/run_silver.py --job silver__base__fact_merchant_transaction --cob-dt 2026-11-23
docker exec spark-client-dungafk spark-submit /home/spark/jobs/silver/run_silver.py --job silver__base__fact_loan_balance --cob-dt 2026-11-23
docker exec spark-client-dungafk spark-submit /home/spark/jobs/silver/run_silver.py --job silver__base__fact_loan_application --cob-dt 2026-11-23
docker exec spark-client-dungafk spark-submit /home/spark/jobs/silver/run_silver.py --job silver__base__fact_repayment --from-dt 2026-07-01 --to-dt 2026-11-23
```

**Updating DPD debt-classification rules (`jobs/silver/reference/dpd_classification.csv`):**
1. Edit `jobs/silver/reference/dpd_classification.csv` (non-overlapping ranges covering `0..∞` per validity period).
2. Re-run `spark-submit /home/spark/jobs/silver/base/silver__base__dim_dpd_classification.py`.
3. Re-run `silver__base__fact_loan_balance` for the affected `cob_dt` dates.
```powershell
# backfill the seeded transaction days
docker exec spark-client-dungafk spark-submit /home/spark/jobs/silver/run_silver.py --job silver__base__fact_transaction --from-dt 2026-11-16 --to-dt 2026-11-20

# full load (no date = every cob_dt present in source.main_table)
docker exec spark-client-dungafk spark-submit /home/spark/jobs/silver/run_silver.py --job silver__base__dim_party
```

Each run prints one line per date with the Iceberg commit (`added-records`, `deleted-records`, `total-records`);
a re-run with unchanged data shows `added-records: 0` for `scd1/2/3` and atomically replaces the partition for `overwrite_partitions`.

**How to add a new silver table:**
1. Copy an existing YAML in `jobs/silver/<namespace>/` and name it `silver__<namespace>__<table>.yaml`.
2. Fill `job_description` (`job_code` = file name without `.yaml`, `job_parents`, `job_param: [cob_dt]`),
   `source` (`type: spark`, `main_table`, optional `pipeline` temp views `step1`/`step2`/..., final `sql` returning `cob_dt` from `main_table` — placeholders `{cob_dt}`, `{next_dt}`, `{pii.<fn>(<col>)}`), and
   `target` (`type: iceberg`, `target_table`, `write_mode`: `overwrite_partitions` | `append` | `scd1` | `scd2` | `scd3` + its options).
3. Run `spark-submit /home/spark/jobs/silver/run_silver.py --job silver__<namespace>__<table> --cob-dt <date>` and document the table in `data_dictionary.md`.

---

## 4b. Gold analytics silver → gold (`jobs/gold/`)

Gold tables live in catalog **`gold`** (namespaces `common`, `c360`, `credit`; REST port `8183`), defined as YAML configs `jobs/gold/<namespace>/gold__<namespace>__<table>.yaml` and executed by **`jobs/gold/run_gold.py`** (reads `silver.*` and `gold.*` only — never `bronze.*`; `overwrite_partitions` only).

**Run order:** load `dim_ews_rule` first, then the 7 YAML jobs (monthly reports after the month's daily silver snapshots exist):
```powershell
# 1. EWS rules CSV → gold.credit.dim_ews_rule (full rebuild)
docker exec spark-client-dungafk spark-submit /home/spark/jobs/gold/credit/gold__credit__dim_ews_rule.py

# 2. Credit marts (daily + monthly)
docker exec spark-client-dungafk spark-submit /home/spark/jobs/gold/run_gold.py --job gold__credit__credit_portfolio_snapshot
docker exec spark-client-dungafk spark-submit /home/spark/jobs/gold/run_gold.py --job gold__credit__credit_ews_snapshot --cob-dt 2026-11-23
docker exec spark-client-dungafk spark-submit /home/spark/jobs/gold/run_gold.py --job gold__credit__rpt_debt_group_transition --cob-dt 2026-11-23
docker exec spark-client-dungafk spark-submit /home/spark/jobs/gold/run_gold.py --job gold__credit__rpt_loan_origination_funnel --cob-dt 2026-11-23

# 3. Customer 360 & common daily aggregates
docker exec spark-client-dungafk spark-submit /home/spark/jobs/gold/run_gold.py --job gold__c360__c360_customer_snapshot
docker exec spark-client-dungafk spark-submit /home/spark/jobs/gold/run_gold.py --job gold__common__agg_party_cashflow_daily --from-dt 2026-11-16 --to-dt 2026-11-20
docker exec spark-client-dungafk spark-submit /home/spark/jobs/gold/run_gold.py --job gold__common__agg_transaction_daily --from-dt 2026-11-16 --to-dt 2026-11-20
docker exec spark-client-dungafk spark-submit /home/spark/jobs/gold/run_gold.py --job gold__common__agg_merchant_revenue_daily --cob-dt 2026-11-21
```

**Updating EWS rules (`jobs/gold/reference/ews_rules.csv`):**
1. Never edit an active row in place — set `valid_to` on the old row and add a new row with the new threshold and `valid_from`.
2. Re-run `spark-submit /home/spark/jobs/gold/credit/gold__credit__dim_ews_rule.py`.
3. Re-run `gold__credit__credit_ews_snapshot` for the affected `cob_dt` date(s).

**How to add a new gold table:**
1. Create `jobs/gold/<namespace>/gold__<namespace>__<table>.yaml` with `source.main_table` (`silver.*` or `gold.*`), `source.period` (`daily` or `monthly`), `target.write_mode: overwrite_partitions`, and `target.partition_by` (`[cob_dt]` for daily, `[snapshot_month]` for monthly).
2. Run via `run_gold.py --job gold__<namespace>__<table>`, reconcile against Silver in Trino, and document in `data_dictionary.md`.

**Read encrypted PII** (Spark only — Trino has no key). In a Jupyter notebook:

```python
import sys; sys.path.insert(0, "/home/spark/jobs")
from common import pii
spark.sql(f"""SELECT customer_id, {pii.decrypt('full_name_enc')} AS full_name,
                     {pii.decrypt('date_of_birth_enc', 'DATE')} AS date_of_birth
              FROM silver.pii.dim_party_identity""").show()
```

Find a customer by phone without decrypting: compare `phone_hash` with `pii.keyed_hash("'0901234567'")`.
Do not print the SQL built with `pii.*`: it contains the key.

---

## 5. Verify the data

Files on MinIO: Console http://localhost:9001 → bucket `lakehouse` → `bronze/<domain>/<table>/data/cob_dt=YYYY-MM-DD/`
(the 5 tables created before 2026-11-25: `bronze_<domain>/<table>/...`).

### Query Iceberg tables with Trino
```powershell
# No password needed; --user is just a label shown in the query history
docker exec -it trino-dungafk trino --catalog bronze --user lab
```
```sql
SHOW SCHEMAS FROM bronze;                    -- crm, t24, payment
SHOW TABLES FROM bronze.t24;
SELECT * FROM bronze.crm.customer LIMIT 10;
-- rows per business date
SELECT cob_dt, count(*) FROM bronze.t24.transaction GROUP BY 1 ORDER BY 1;
-- partitions and Iceberg snapshots (commits)
SELECT * FROM bronze.t24."transaction$partitions";
SELECT committed_at, operation FROM bronze.t24."transaction$snapshots" ORDER BY 1;

SHOW SCHEMAS FROM silver;                    -- base, pii
-- current version of each customer (SCD2)
SELECT * FROM silver.base.dim_party WHERE is_current;
-- history of one account
SELECT account_sk, account_status, valid_from, valid_to, is_current FROM silver.base.dim_account
WHERE account_num = '190012393455' ORDER BY valid_from;   -- any account_num
-- net flow per day and channel
SELECT cob_dt, channel, sum(signed_amount) FROM silver.base.fact_transaction GROUP BY 1, 2 ORDER BY 1, 2;
-- PII is encrypted: Trino only sees ciphertext / masked values
SELECT customer_id, phone_masked, email_masked FROM silver.pii.dim_contact;
```

### Postgres (source data)
```powershell
docker exec -it postgres-dungafk sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB"'
```
```sql
\dn                                  -- schemas: crm, t24, payment (public is empty)
\dt crm.* t24.* payment.*
SELECT count(*) FROM t24.transaction;
```

### MySQL (LOS source data)
- **DBeaver:** New Connection → MySQL → Host `localhost`, Port `3306`, Database `los_db`, Username `dungnn_mysql`, Password from `.env` (`MYSQL_PASSWORD`). If DBeaver reports *Public Key Retrieval is not allowed*, set `allowPublicKeyRetrieval=true` in Driver Properties.
- **CLI:**
```powershell
docker exec -it mysql-dungafk sh -c 'mysql -u"$MYSQL_USER" -p"$MYSQL_PASSWORD" los_db'
```
```sql
SHOW TABLES;
SELECT COUNT(*) FROM loan_application;
SELECT payment_date, COUNT(*) FROM repayment GROUP BY 1 ORDER BY 1 DESC LIMIT 10;
```
> Note: `init-data/mysql/*.sql` runs **only on the first start of an empty `mysql_data` volume**. To reseed from scratch, remove the volume (`docker compose down` + `docker volume rm lakehouse_banking_mysql_data`, after user OK) and start `mysql-dungafk` again.

### SFTP (Card & Reference flat files)
- **Authentication:** key-based SSH (`secrets/sftp_id_ed25519`, git-ignored, mounted at `/home/spark/secrets:ro`; configured via `SFTP_HOST`, `SFTP_PORT`, `SFTP_USER`, `SFTP_KEY_PATH` in `.env`).
- **Run Bronze SFTP jobs:**
```powershell
docker exec spark-client-dungafk spark-submit /home/spark/jobs/bronze/card/bronze__card__card.py --cob-dt 2026-11-23
docker exec spark-client-dungafk spark-submit /home/spark/jobs/bronze/card/bronze__card__card_txn.py --cob-dt 2026-11-26
docker exec spark-client-dungafk spark-submit /home/spark/jobs/bronze/ref/bronze__ref__branch.py --cob-dt 2026-11-23
docker exec spark-client-dungafk spark-submit /home/spark/jobs/bronze/ref/bronze__ref__product.py --cob-dt 2026-11-23
docker exec spark-client-dungafk spark-submit /home/spark/jobs/bronze/ref/bronze__ref__gl_mapping.py --cob-dt 2026-11-23
```

### Postgres (Iceberg catalog registry — read only)
Separate database `ICEBERG_BRONZE_CATALOG_DB` (default `iceberg_catalog_bronze`) used by `iceberg-rest-bronze-dungafk`. Look, do not touch:
```powershell
docker exec -it postgres-dungafk sh -c 'psql -U "$POSTGRES_USER" -d "$ICEBERG_BRONZE_CATALOG_DB"'
```
```sql
SELECT table_namespace, table_name, metadata_location FROM iceberg_tables ORDER BY 1, 2;
SELECT * FROM iceberg_namespace_properties ORDER BY 2;
```

---

## 6. Logs & debugging

```powershell
docker logs --tail 50 spark-worker-dungafk
docker logs --tail 50 spark-master-dungafk
docker logs minio-init-dungafk
docker logs --tail 50 iceberg-rest-bronze-dungafk
docker logs --tail 50 iceberg-rest-silver-dungafk
docker logs --tail 50 trino-dungafk

# Open a shell in a container
docker exec -it spark-client-dungafk bash
```

---

## 7. Known errors & fixes

| Symptom | Cause | Fix |
|---|---|---|
| `not a directory: Are you trying to mount a directory onto a file` | The file to mount did not exist → Docker created a **directory** with that name | Delete that directory, put the real file at that path, `docker compose up -d` |
| `failed to read dockerfile: open Dockerfile: no such file` | `Dockerfile` missing in `docker/spark` or `docker/jupyter` | Add the Dockerfile |
| Spark UI shows **0 workers** | Wrong host in `SPARK_MASTER_URL` in `.env` (`UnknownHostException`) | Fix `.env`, `docker compose up -d --force-recreate` |
| `relation "crm.customer" does not exist` | `init-data/` scripts only run when the Postgres volume is **empty on first start** | Seed manually (section 8) |
| `initdb: superuser name "pg_..." is disallowed` | Postgres reserves the `pg_` prefix | Pick a user name not starting with `pg_` |
| Iceberg REST keeps restarting, `password authentication failed` | `.env` Postgres credentials changed but the volume still has the old ones | See section 7 |
| Trino (DBeaver): `Authentication failed` / SSL error | Old connection still set to port 8443, SSL or a password | Use `jdbc:trino://localhost:8086/bronze` (or `/silver`), any user, empty password, no SSL properties |
| Executor killed (OOM) | `SPARK_WORKER_MEM` > `SPARK_WORKER_MEM_LIMIT` in `.env` | Keep `MEM_LIMIT ≈ WORKER_MEM + 1g` |

---

## 8. Change credentials

| Account | Steps |
|---|---|
| **MinIO** | Edit `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD` in `.env` → `docker compose up -d --force-recreate` |
| **Postgres password** | Edit `POSTGRES_PASSWORD` in `.env`, then on the running DB: `docker exec postgres-dungafk psql -U <POSTGRES_USER> -d postgres -c "ALTER ROLE <POSTGRES_USER> WITH PASSWORD '<new>'"` → `docker compose up -d --force-recreate` |
| **Postgres user / database name** | Only applied on an empty volume: `docker compose down -v` + `up -d` (wipes all data). User names must not start with `pg_` |

Trino has **no authentication** (local lab only): it listens on plain HTTP, host port 8086, and accepts any user
name without a password. There are no Trino credentials to change.

After any credential change, update `E:\Study\LakeHouseProject\LakeHouse_Banking_Credentials.xlsx`.

---

## 9. Re-seed the Postgres source data

> ⚠️ Only run this when the tables **do not exist / are empty** — the INSERT statements duplicate data if run again.

```powershell
docker exec postgres-dungafk sh -c 'for f in /docker-entrypoint-initdb.d/*.sql; do echo "== $f"; psql -v ON_ERROR_STOP=1 -q -U "$POSTGRES_USER" -d "$POSTGRES_DB" -f "$f" || exit 1; done'
```

Clean alternative (wipes all data; Postgres re-runs `init-data/` on startup):
```powershell
docker compose down -v
docker compose up -d
```

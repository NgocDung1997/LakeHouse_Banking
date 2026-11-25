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
| Iceberg REST | http://localhost:8181 | |
| Trino (Web UI) | http://localhost:8086 | No authentication: type any user name, no password |
| Trino (JDBC, DBeaver) | `jdbc:trino://localhost:8086/lakehouse` | Any user name (e.g. `lab`), leave the password empty |
| Postgres | `localhost:5432` | Database / user / password = `POSTGRES_DB` / `POSTGRES_USER` / `POSTGRES_PASSWORD` in `.env` |

All current logins are also kept in `E:\Study\LakeHouseProject\LakeHouse_Banking_Credentials.xlsx` (outside the repo).

---

## 3. Bronze ingestion Postgres → Iceberg (`jobs/bronze/`)

Jobs run inside `spark-client-dungafk` (`./jobs` is mounted at `/home/spark/jobs`). **One self-contained job per
table** (the only shared code is `add_technical_columns()` in `jobs/bronze/bronze_columns.py` and the argument
parsers `parse_date` / `parse_prefix` in `jobs/common/parsers.py`); each run loads a
business date `cob_dt` (default: yesterday, UTC+7) into partition `cob_dt` of `lakehouse.bronze_<domain>.<table>`.
Re-running a date replaces that partition only.

| Job (under `/home/spark/jobs/bronze/`) | Table | Load type |
|---|---|---|
| `crm/bronze__customer.py` | `crm.customer` | snapshot |
| `core_banking/bronze__account.py` | `core_banking.account` | snapshot |
| `core_banking/bronze__core_account.py` | `core_banking.core_account` | snapshot |
| `core_banking/bronze__transaction.py` | `core_banking.transaction` | incremental by `transaction_date` |
| `payment/bronze__merchant_transaction.py` | `payment.merchant_transaction` | incremental by `txn_ts` |

Options:

| Option | Jobs | Meaning |
|---|---|---|
| *(none)* | all | `cob_dt` = yesterday (UTC+7) |
| `--cob-dt YYYY-MM-DD` | all | Load one business date |
| `--from-dt YYYY-MM-DD --to-dt YYYY-MM-DD` | incremental only | Backfill a date range (inclusive), one partition per date |
| `--where "<predicate>"` | all | **Partial reload**: read only the source rows matching the predicate and replace exactly those rows in the `cob_dt` partition; other rows are kept (one atomic commit) |
| `--namespace-prefix PREFIX` | all | Write to `lakehouse.<PREFIX>_<domain>` instead of `bronze_<domain>` — for test runs |

Snapshot jobs have no `--from-dt/--to-dt`: a snapshot always copies the table as it is **now**, so loading past dates
in a loop would only repeat today's state under old dates.

**What each job reads** — constants at the top of the job file:

| Constant | Meaning |
|---|---|
| `SOURCE_TABLE` | Postgres `<schema>.<table>` |
| `DOMAIN` / `TARGET_TABLE` | Bronze table `lakehouse.bronze_<DOMAIN>.<TARGET_TABLE>` (target name may differ from the source) |
| `SCHEMA` | `StructType([StructField("<column>", <Type>()), ...])`: the columns to load **and their bronze types**, declared up front so they can be adjusted to the business rules — remove a field to stop loading it, change its type to cast it (e.g. `DecimalType(15, 2)` → `DoubleType()`). Only these columns are selected in Postgres. `None` = all source columns with their source types |
| `SOURCE_FILTER` | Fixed WHERE predicate applied on every run (e.g. `"status <> 'CLOSED'"`); `None` = whole table / whole day |
| `EVENT_COL` | Incremental jobs: column that decides the `cob_dt` of a row |

`SOURCE_FILTER` and `--where` run in Postgres (`SELECT <SCHEMA columns> FROM <SOURCE_TABLE> WHERE ...`); the predicate(s) used
are stored per row in the bronze column `_source_filter` (NULL = no filter). Write predicates **without** `WHERE`,
keep them simple and portable (comparisons, `IN`, `BETWEEN`, `IS NULL`, `AND`/`OR`/`NOT`): `--where` is also evaluated by
Spark on the bronze table, so its columns must be loaded columns. `;`, `--` and `/* */` are refused.

```powershell
# One table, cob_dt = yesterday
docker exec -it spark-client-dungafk spark-submit /home/spark/jobs/bronze/crm/bronze__customer.py

# One table for a given business date
docker exec -it spark-client-dungafk spark-submit /home/spark/jobs/bronze/core_banking/bronze__transaction.py --cob-dt 2026-11-20

# Backfill the seeded transaction days
docker exec -it spark-client-dungafk spark-submit /home/spark/jobs/bronze/core_banking/bronze__transaction.py --from-dt 2026-11-16 --to-dt 2026-11-20

# Partial reload: re-read and replace only some rows of one business date
docker exec -it spark-client-dungafk spark-submit /home/spark/jobs/bronze/core_banking/bronze__transaction.py --cob-dt 2026-11-20 --where "transaction_id IN (1, 2, 3)"

# Test run into throw-away namespaces lakehouse.test_bronze_<domain> (drop them afterwards)
docker exec -it spark-client-dungafk spark-submit /home/spark/jobs/bronze/payment/bronze__merchant_transaction.py --cob-dt 2026-11-21 --namespace-prefix test_bronze

# Help
docker exec -it spark-client-dungafk spark-submit /home/spark/jobs/bronze/crm/bronze__customer.py --help
```

Run all five jobs one after another (PowerShell), here for one business date:
```powershell
$jobs = "crm/bronze__customer.py", "core_banking/bronze__account.py", "core_banking/bronze__core_account.py",
        "core_banking/bronze__transaction.py", "payment/bronze__merchant_transaction.py"
foreach ($j in $jobs) { docker exec spark-client-dungafk spark-submit "/home/spark/jobs/bronze/$j" --cob-dt 2026-11-20 }
```

Hide INFO logs for readability:
```powershell
docker exec spark-client-dungafk spark-submit /home/spark/jobs/bronze/crm/bronze__customer.py 2>&1 | Where-Object { $_ -notmatch ' INFO |^\s+at ' }
```

Drop test tables written with `--namespace-prefix` (example for `test_bronze`):
```powershell
docker exec spark-client-dungafk spark-sql -e "DROP TABLE lakehouse.test_bronze_crm.customer PURGE; DROP NAMESPACE lakehouse.test_bronze_crm;"
```

The job files in `jobs/bronze/<domain>/` are **generated** from `jobs/auto/bronze_jobs.csv` (approach A, section 3.1):
they start with an `AUTO-GENERATED` line and are overwritten on every generator run — **do not edit them by hand**.
To add or change a table: edit the CSV, run the generator, then document the table in `data_dictionary.md`.
To change the job code itself: edit the template in `jobs/auto/generate/generate_bronze_job/templates/` and regenerate.
A truly hand-written job (no `AUTO-GENERATED` line, name not in the CSV) can also live there; the generator never
deletes it.

### 3.1 CSV-driven jobs (`jobs/auto/`)

Job files are named `<layer>__<table>.py` (e.g. `bronze__customer.py`), hand-written or generated.
`jobs/auto/generate/` has one folder per generator: `generate_bronze_job/` (below) and `generate_sql/` (reserved, empty).

Job list: `jobs/auto/bronze_jobs.csv`, one row = one bronze job = one bronze table:

| Column | Meaning |
|---|---|
| `domain` | Bronze namespace `bronze_<domain>`; also the Postgres schema when `source_table` has none |
| `source_table` | Postgres `<table>` or `<schema>.<table>` |
| `target_table` | Bronze table name (may differ from the source; unique per domain). `--table` of the tools means this name |
| `load_type` | `snapshot` or `incremental` |
| `event_col` | Incremental only: column that decides `cob_dt` (empty for snapshot) |
| `columns` | Columns to load **with their type**, `name:type` separated by `\|`, e.g. `"txn_id:string\|amount:decimal(18,2)\|txn_ts:timestamp_ntz"` — becomes the job's StructType `SCHEMA` (approach A writes one `StructField` per column). Types: `string`, `int`, `bigint`, `smallint`, `double`, `float`, `boolean`, `date`, `timestamp`, `timestamp_ntz`, `decimal(p,s)`. **Quote the cell** (it contains commas when there is a decimal). Empty = all source columns with their source types |
| `source_filter` | Fixed WHERE predicate without `WHERE`, e.g. `status <> 'CLOSED'`; empty = none. Quote the cell if it contains a comma: `"status IN ('ACTIVE', 'FROZEN')"` |
| `active` | 1 = run, 0 = skip |

Example row (not in the real file): `core_banking,account,account_open,snapshot,,,"status IN ('ACTIVE', 'FROZEN')",1`.
Every tool validates the CSV first and stops with the list of problems (bad names, missing/extra `event_col`, duplicated
column, forbidden characters in `source_filter`, duplicated target table, ...).
To add a job: add a CSV row (and for approach A, regenerate). `--where` (partial reload) works the same as in the
hand-written jobs.

**Approach B — one parameterised job** (`auto/param/ingest_table.py`, same options as the hand-written jobs plus `--domain/--table`):
```powershell
docker exec -it spark-client-dungafk spark-submit /home/spark/jobs/auto/param/ingest_table.py --domain crm --table customer
docker exec -it spark-client-dungafk spark-submit /home/spark/jobs/auto/param/ingest_table.py --domain core_banking --table transaction --from-dt 2026-11-16 --to-dt 2026-11-20
docker exec -it spark-client-dungafk spark-submit /home/spark/jobs/auto/param/ingest_table.py --domain crm --table customer --cob-dt 2026-11-20 --where "customer_id IN (1, 2)"
```

**Approach A — generate one file per table** (for hand-over). Writes `jobs/bronze/<domain>/bronze__<table>.py` for every
active CSV row from the templates in `jobs/auto/generate/generate_bronze_job/templates/`. Each run also **deletes the
generated files of tables no longer in the CSV** (or set to `active=0`) — only files with the `AUTO-GENERATED` line,
never hand-written files. A hand-written file with the same name as a CSV job is replaced (the output says so):
```powershell
docker exec spark-client-dungafk python3 /home/spark/jobs/auto/generate/generate_bronze_job/generate_jobs.py
docker exec -it spark-client-dungafk spark-submit /home/spark/jobs/bronze/crm/bronze__customer.py --cob-dt 2026-11-20
```

**Run a whole domain** — active tables one after another, one Spark application per table:
```powershell
# approach B (default)
docker exec spark-client-dungafk python3 /home/spark/jobs/auto/run_domain.py --domain core_banking
docker exec spark-client-dungafk python3 /home/spark/jobs/auto/run_domain.py --domain core_banking --cob-dt 2026-11-20
# backfill: incremental tables get the range, snapshot tables are SKIPPED
docker exec spark-client-dungafk python3 /home/spark/jobs/auto/run_domain.py --domain core_banking --from-dt 2026-11-16 --to-dt 2026-11-20
# approach A (run generate/generate_bronze_job/generate_jobs.py first)
docker exec spark-client-dungafk python3 /home/spark/jobs/auto/run_domain.py --domain crm --mode generated
# test run into lakehouse.test_bronze_<domain>
docker exec spark-client-dungafk python3 /home/spark/jobs/auto/run_domain.py --domain payment --cob-dt 2026-11-21 --namespace-prefix test_bronze
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

## 4. Verify the data

Files on MinIO: Console http://localhost:9001 → bucket `lakehouse` → `bronze_<domain>/<table>/data/cob_dt=YYYY-MM-DD/`.

### Query Iceberg tables with Trino
```powershell
# No password needed; --user is just a label shown in the query history
docker exec -it trino-dungafk trino --catalog lakehouse --user lab
```
```sql
SHOW SCHEMAS FROM lakehouse;                 -- bronze_crm, bronze_core_banking, bronze_payment
SHOW TABLES FROM lakehouse.bronze_core_banking;
SELECT * FROM lakehouse.bronze_crm.customer LIMIT 10;
-- rows per business date
SELECT cob_dt, count(*) FROM lakehouse.bronze_core_banking.transaction GROUP BY 1 ORDER BY 1;
-- partitions and Iceberg snapshots (commits)
SELECT * FROM lakehouse.bronze_core_banking."transaction$partitions";
SELECT committed_at, operation FROM lakehouse.bronze_core_banking."transaction$snapshots" ORDER BY 1;
```

### Postgres (source data)
```powershell
docker exec -it postgres-dungafk sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB"'
```
```sql
\dn                                  -- schemas: crm, core_banking, payment (public is empty)
\dt crm.* core_banking.* payment.*
SELECT count(*) FROM core_banking.transaction;
```

### Postgres (Iceberg catalog registry — read only)
Separate database `ICEBERG_CATALOG_DB` (default `iceberg_catalog`) used by `iceberg-rest-dungafk`. Look, do not touch:
```powershell
docker exec -it postgres-dungafk sh -c 'psql -U "$POSTGRES_USER" -d "$ICEBERG_CATALOG_DB"'
```
```sql
SELECT table_namespace, table_name, metadata_location FROM iceberg_tables ORDER BY 1, 2;
SELECT * FROM iceberg_namespace_properties ORDER BY 2;
```

---

## 5. Logs & debugging

```powershell
docker logs --tail 50 spark-worker-dungafk
docker logs --tail 50 spark-master-dungafk
docker logs minio-init-dungafk
docker logs --tail 50 iceberg-rest-dungafk
docker logs --tail 50 trino-dungafk

# Open a shell in a container
docker exec -it spark-client-dungafk bash
```

---

## 6. Known errors & fixes

| Symptom | Cause | Fix |
|---|---|---|
| `not a directory: Are you trying to mount a directory onto a file` | The file to mount did not exist → Docker created a **directory** with that name | Delete that directory, put the real file at that path, `docker compose up -d` |
| `failed to read dockerfile: open Dockerfile: no such file` | `Dockerfile` missing in `docker/spark` or `docker/jupyter` | Add the Dockerfile |
| Spark UI shows **0 workers** | Wrong host in `SPARK_MASTER_URL` in `.env` (`UnknownHostException`) | Fix `.env`, `docker compose up -d --force-recreate` |
| `relation "crm.customer" does not exist` | `init-sql/` scripts only run when the Postgres volume is **empty on first start** | Seed manually (section 8) |
| `initdb: superuser name "pg_..." is disallowed` | Postgres reserves the `pg_` prefix | Pick a user name not starting with `pg_` |
| Iceberg REST keeps restarting, `password authentication failed` | `.env` Postgres credentials changed but the volume still has the old ones | See section 7 |
| Trino (DBeaver): `Authentication failed` / SSL error | Old connection still set to port 8443, SSL or a password | Use `jdbc:trino://localhost:8086/lakehouse`, any user, empty password, no SSL properties |
| Executor killed (OOM) | `SPARK_WORKER_MEM` > `SPARK_WORKER_MEM_LIMIT` in `.env` | Keep `MEM_LIMIT ≈ WORKER_MEM + 1g` |

---

## 7. Change credentials

| Account | Steps |
|---|---|
| **MinIO** | Edit `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD` in `.env` → `docker compose up -d --force-recreate` |
| **Postgres password** | Edit `POSTGRES_PASSWORD` in `.env`, then on the running DB: `docker exec postgres-dungafk psql -U <POSTGRES_USER> -d postgres -c "ALTER ROLE <POSTGRES_USER> WITH PASSWORD '<new>'"` → `docker compose up -d --force-recreate` |
| **Postgres user / database name** | Only applied on an empty volume: `docker compose down -v` + `up -d` (wipes all data). User names must not start with `pg_` |

Trino has **no authentication** (local lab only): it listens on plain HTTP, host port 8086, and accepts any user
name without a password. There are no Trino credentials to change.

After any credential change, update `E:\Study\LakeHouseProject\LakeHouse_Banking_Credentials.xlsx`.

---

## 8. Re-seed the Postgres source data

> ⚠️ Only run this when the tables **do not exist / are empty** — the INSERT statements duplicate data if run again.

```powershell
docker exec postgres-dungafk sh -c 'for f in /docker-entrypoint-initdb.d/*.sql; do echo "== $f"; psql -v ON_ERROR_STOP=1 -q -U "$POSTGRES_USER" -d "$POSTGRES_DB" -f "$f" || exit 1; done'
```

Clean alternative (wipes all data; Postgres re-runs `init-sql/` on startup):
```powershell
docker compose down -v
docker compose up -d
```

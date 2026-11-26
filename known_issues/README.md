# Known issues

Problems we already hit in this project and how to get past them.
**Something broke?** Find your error message in the table below, then open the entry.

## Quick list

Issues are grouped by **category**. Find your error message, then open the entry.

| Category | What belongs here |
|---|---|
| [Setup & configuration](#setup) | Installing and running the stack: Docker containers, Spark / Iceberg / MinIO settings. |
| [Coding (Python / PySpark)](#coding) | Writing or debugging job code. |
| [SQL & tables (Spark SQL / Iceberg)](#sql) | Filters, queries, table schemas. |
| [Resources (memory / CPU / disk limits)](#resources) | Something runs out of memory, CPU or disk. |
| [Validate (job configuration)](#validate) | Errors reported by `validate_config.py` for `bronze_tables.csv` / `bronze_columns.csv`. |
| [Data issues](#data-issues) | Wrong / late / corrected source data (`DI-xxx`). |

**Setup & configuration**

| ID | You see… | What to do |
|---|---|---|
| [KI-003](#ki-003) | Job runs but is not on the Spark UI | Nothing — already fixed in the config |
| [KI-009](#ki-009) | Spark UI shows **0 workers** | Wait 30 s, then check the worker log |
| [KI-011](#ki-011) | `Unable to load region` | Nothing — already fixed in the config |
| [KI-014](#ki-014) | Other containers restart too | Normal — wait, then check the workers |
| [KI-035](#ki-035) | `Container ... is paused, unpause the container before exec` | `docker unpause`, then restart the worker and catalogs |
| [KI-034](#ki-034) | `Failed to get table ... terminating connection due to administrator command` / job stays `WAITING` | Restart the Iceberg REST containers and the Spark worker |

**Coding (Python / PySpark)**

| ID | You see… | What to do |
|---|---|---|
| [KI-002](#ki-002) | Timestamps 7 hours too late | Nothing — already fixed in the jobs |
| [KI-004](#ki-004) | `An error occurred while calling o55.load` | Read the **next** line of the error |
| [KI-012](#ki-012) | `SyntaxError` / `TypeError` on `str \| None` | Write `Optional[str]` (Python 3.8) |

**SQL & tables (Spark SQL / Iceberg)**

| ID | You see… | What to do |
|---|---|---|
| [KI-001](#ki-001) | `INTERNAL_ERROR ... assertion failed` | Nothing — already fixed in the jobs |
| [KI-017](#ki-017) | `... is out of order` when writing | Nothing — already fixed in the config |
| [KI-036](#ki-036) | Trino `DROP COLUMN`: `Invalid last column ID: 16 < 17` | Drop the column with Spark SQL |
| [KI-033](#ki-033) | MERGE INTO: `UNRESOLVED_COLUMN` for a column that exists | Remove `write.spark.accept-any-schema` from the table |

**Resources (memory / CPU / disk limits)**

| ID | You see… | What to do |
|---|---|---|
| [KI-010](#ki-010) | Executors get killed | Give the worker container more memory |

**Validate (job configuration)** — messages of `jobs/auto/validate/validate_config.py`, they start with the ID

| ID | You see… | What to do |
|---|---|---|
| [KI-018](#ki-018) | `data_type 'varchar' is not allowed` | Use an allowed type, e.g. `string` |
| [KI-019](#ki-019) | `load_type 'Snapshot' is not valid` | Write `snapshot` or `incremental` |
| [KI-020](#ki-020) | `source_domain ... / source table ... does not exist` | Fix the name, or create the table first |
| [KI-021](#ki-021) | `incremental job needs an event_col` / `event_col ... is not a column` | Fill / fix `event_col` |
| [KI-022](#ki-022) | `duplicate of line N ... — removed` | Nothing — the duplicate row was removed |
| [KI-023](#ki-023) | `... is 'int' here but 'string' on line N` | Keep one of the two rows |
| [KI-024](#ki-024) | `column 'x' does not exist in source table` | Fix or remove the column row |
| [KI-025](#ki-025) | `event_col ... must be a date or timestamp column` | Choose a date / timestamp column |
| [KI-026](#ki-026) | `source_filter ...` error | Fix the WHERE predicate |
| [KI-027](#ki-027) | `target table ... is already defined on line N` | Remove or rename one row |
| [KI-028](#ki-028) | `columns for ..., which is not a target table` | Add the table, or fix its name |
| [KI-029](#ki-029) | `header must be ...` / `wrong number of fields` | Fix the header / quote the cell |
| [KI-030](#ki-030) | `... must be lower-case letters ...` / `active must be 1 or 0` | Fix the value |
| [KI-031](#ki-031) | `incremental job needs a source_filter for one date` / `must use {cob_dt}` | Write the date window with `{cob_dt}` |
| [KI-032](#ki-032) | `event_col ... is not in the columns of ...` | Add the event_col to `bronze_columns.csv` |

---

<a id="setup"></a>
## Setup & configuration

Installing and running the stack: Docker containers, Spark / Iceberg / MinIO settings.

<a id="ki-003"></a>
### KI-003 · Job runs, but not on the Spark cluster

- **You see:** the job finishes, but http://localhost:8080 → *Completed Applications* is empty.
- **Why:** without a master, `spark-submit` silently runs on one machine (`local[*]`).
- **Fix:** `spark.master spark://spark-master-dungafk:7077` in `conf/spark-defaults.conf`. Already set.

<a id="ki-009"></a>
### KI-009 · Spark UI shows 0 workers

- **You see:** http://localhost:8080 lists no worker.
- **Why:** the master was just restarted (the worker reconnects by itself after ~30 s), or `SPARK_MASTER_URL` in
  `.env` has a wrong host (`UnknownHostException` in the worker log).
- **Fix:** wait 30 s. Still 0? Run `docker logs spark-worker-dungafk`, fix `.env` if needed, then
  `docker compose up -d --force-recreate`.

<a id="ki-011"></a>
### KI-011 · Iceberg cannot reach MinIO: no region

- **You see:** `SdkClientException: Unable to load region`.
- **Why:** the AWS SDK used by Iceberg needs a region, even for MinIO.
- **Fix:** `AWS_REGION` (docker-compose) and `spark.sql.catalog.bronze.s3.region` (spark-defaults). Already set.

<a id="ki-014"></a>
### KI-014 · Restarting one container restarts others

- **You see:** after `docker compose up -d <service>`, other containers restart too (e.g. Trino restarts Postgres
  and MinIO; spark-client restarts spark-master).
- **Why:** Docker Compose also restarts the services listed in `depends_on`. Data is safe (it is in volumes).
- **Fix:** nothing to fix — wait until http://localhost:8080 shows the worker again before running a Spark job.

<a id="ki-035"></a>
### KI-035 · Containers are "Paused"

- **You see:** `Error response from daemon: Container spark-client-dungafk is paused, unpause the container before exec`;
  `docker ps` shows `(Paused)`, after unpausing some show `(unhealthy)` and http://localhost:8080 shows 0 workers.
- **Why:** Docker Desktop paused the containers (e.g. Resource Saver after a long idle time, or the PC went to sleep).
- **Fix:** `docker unpause $(docker ps --filter status=paused -q)`, then
  `docker restart spark-worker-dungafk iceberg-rest-bronze-dungafk iceberg-rest-silver-dungafk`; wait ~30 s until
  the worker is back on http://localhost:8080 and the health checks are `healthy`.

<a id="ki-034"></a>
### KI-034 · After editing .env, jobs fail on the catalog or never start

- **You see:** `ServiceFailureException: Server error: UncheckedSQLException: Failed to get table crm.customer`
  (REST log: `terminating connection due to administrator command`), or the job hangs and http://localhost:8080
  shows the app `WAITING` with **0 workers**.
- **Why:** every service reads `.env` (`env_file`), so after `.env` changes the next `docker compose up -d <service>`
  also **recreates** Postgres / MinIO. Containers that were not recreated (Iceberg REST, Spark worker) keep dead
  connections.
- **Fix:** `docker restart iceberg-rest-bronze-dungafk iceberg-rest-silver-dungafk spark-worker-dungafk`, wait until
  the worker is back on http://localhost:8080, run the job again.

---

<a id="coding"></a>
## Coding (Python / PySpark)

Writing or debugging job code.

<a id="ki-002"></a>
### KI-002 · Timestamps are 7 hours too late

- **You see:** times in bronze are +7h compared to Postgres; evening rows land on the next `cob_dt`.
- **Why:** Postgres `TIMESTAMP` has no time zone and Spark converts it with the session zone (UTC+7).
- **Fix:** JDBC option `preferTimestampNTZ=true` (keeps the value as is). Already in every bronze job.

<a id="ki-004"></a>
### KI-004 · The error message says nothing useful

- **You see:** `✘ An error occurred while calling o55.load.`
- **Why:** this is only the first line of a Java error; the real reason is on the next line.
- **Fix:** read the line after it, e.g. `PSQLException: ERROR: relation "core_banking.x" does not exist`.
  `run_domain.py` does this for you in `logs/bronze/errors.log`.

<a id="ki-012"></a>
### KI-012 · Python syntax error inside the Spark containers

- **You see:** `SyntaxError` or `TypeError` on a type hint like `str | None` or `list[str]`.
- **Why:** the Spark images run Python **3.8**, which does not know this syntax.
- **Fix:** use `Optional[str]`, `List[str]`, `Tuple[...]` from `typing`.

---

<a id="sql"></a>
## SQL & tables (Spark SQL / Iceberg)

Filters, queries, table schemas.

<a id="ki-001"></a>
### KI-001 · Spark crashes on a filter with `coalesce`

- **You see:** `[INTERNAL_ERROR] ... Caused by: java.lang.AssertionError: assertion failed`
- **Why:** a Spark 3.5 bug when a filter with `coalesce(...)` is pushed down to an Iceberg table.
- **Fix:** write `(<condition>) IS NOT TRUE` instead of `NOT coalesce(<condition>, false)` — same result.
  Already done in `overwrite_partition()` (`jobs/common/load_patterns.py`), used by every bronze job.

<a id="ki-017"></a>
### KI-017 · "Column is out of order" when writing to Iceberg

- **You see:** `Cannot write incompatible dataset ... * <new column> is out of order, before _row_hash`
- **Why:** a new column is always added at the **end** of an Iceberg table, while the job puts it in another place;
  Iceberg checks the column order by default.
- **Fix:** `spark.sql.iceberg.check-ordering false` in `conf/spark-defaults.conf` (match columns by name).
  Already set — do not remove it.

<a id="ki-036"></a>
### KI-036 · Trino cannot drop a column

- **You see:** `ALTER TABLE ... DROP COLUMN x` in Trino fails with
  `Failed to drop column: Invalid last column ID: 16 < 17 (previous last column ID)`.
- **Why:** the table already had a column dropped before (here `_source_filter`, dropped with Spark); Trino computes
  the new "last column ID" lower than the one stored in the Iceberg metadata and refuses the change.
- **Fix:** run the same statement in Spark SQL (`spark.sql("ALTER TABLE bronze.crm.customer DROP COLUMN x")`) —
  Spark keeps the stored ID. Trino reads the table normally afterwards.

<a id="ki-033"></a>
### KI-033 · MERGE INTO cannot find a column that exists

- **You see:** ``[UNRESOLVED_COLUMN.WITH_SUGGESTION] A column ... with name `t`.`customer_id` cannot be resolved.
  Did you mean one of the following? [`t`.`customer_id`, ...]`` on a `MERGE INTO` (Spark 3.5).
- **Why:** the target table has the property `write.spark.accept-any-schema=true` (used by bronze); Spark 3.5 then
  cannot resolve the target columns of a MERGE.
- **Fix:** silver tables are created without it (`jobs/common/load_patterns.py`, `TABLE_PROPERTIES`). For an existing
  table: `ALTER TABLE <table> UNSET TBLPROPERTIES ('write.spark.accept-any-schema')`. New columns in silver are added
  with `ALTER TABLE ... ADD COLUMN`.

---

<a id="resources"></a>
## Resources (memory / CPU / disk limits)

Something runs out of memory, CPU or disk.

<a id="ki-010"></a>
### KI-010 · Executors get killed (out of memory)

- **You see:** Spark tasks fail with executor lost / killed under load.
- **Why:** the worker promises more memory (`SPARK_WORKER_MEM`) than its container is allowed to use.
- **Fix:** keep `SPARK_WORKER_MEM_LIMIT` about 1 GB above `SPARK_WORKER_MEM` in `.env`
  (the limit is currently switched off in `docker-compose.yml`).

---

<a id="validate"></a>
## Validate (job configuration)

Checks run by `jobs/auto/validate/validate_config.py` on `jobs/auto/bronze_tables.csv` and `bronze_columns.csv`.
`generate_jobs.py`, `run_domain.py` and `ingest_table.py` run the same checks first and stop on an ERROR.
Run them alone with `docker exec spark-client-dungafk python3 /home/spark/jobs/auto/validate/validate_config.py`
(`--offline` = skip the Postgres checks).

<a id="ki-018"></a>
### KI-018 · Wrong data_type

- **You see:** `ERROR [KI-018] bronze_columns.csv line 3: data_type 'varchar' is not allowed — did you mean 'string'?`
- **Why:** only Spark type names are allowed: `string`, `int`, `bigint`, `smallint`, `double`, `float`, `boolean`,
  `date`, `timestamp`, `timestamp_ntz`, `decimal(p,s)` (p ≤ 38). Database names like `varchar`, `integer`, `numeric` are refused.
- **Fix:** write the suggested type. Upper / lower case does not matter (`DATE` = `date`).

<a id="ki-019"></a>
### KI-019 · Wrong load_type

- **You see:** `ERROR [KI-019] ... load_type 'Snapshot' is not valid — use snapshot or incremental`
- **Why:** `load_type` must be exactly `snapshot` or `incremental` (lower case).
- **Fix:** correct the value in `bronze_tables.csv`.

<a id="ki-020"></a>
### KI-020 · Source domain or table does not exist

- **You see:** `ERROR [KI-020] ... source_domain 'sales' does not exist in Postgres` or `source table crm.no_table does not exist`
  (or `cannot connect to check the sources`).
- **Why:** `source_domain` is the Postgres schema and `source_table` a table in it; both must exist.
- **Fix:** fix the spelling, or create the table in Postgres first. Not connected? Run inside `spark-client-dungafk`,
  or use `--offline` to skip this check.

<a id="ki-021"></a>
### KI-021 · event_col missing or wrong

- **You see:** `incremental job needs an event_col`, `snapshot job must not have an event_col`, or
  `event_col 'txn_date' is not a column of core_banking.transaction`.
- **Why:** an incremental job loads the rows whose `event_col` falls on `cob_dt`, so it needs a real column of the
  source table; a snapshot job copies the whole table and has no `event_col`.
- **Fix:** fill in / correct `event_col`, or empty it for a snapshot job.

<a id="ki-022"></a>
### KI-022 · Duplicate column row (removed automatically)

- **You see:** `FIXED [KI-022] bronze_columns.csv line 4: duplicate of line 2 (crm.customer.customer_id int) — removed`
- **Why:** the same column with the same type is listed twice for one target_domain + target_table.
- **Fix:** nothing — `validate_config.py` and `generate_jobs.py` delete the extra line from the file
  (`run_domain.py` / `ingest_table.py` only ignore it).

<a id="ki-023"></a>
### KI-023 · Same column, different data types

- **You see:** `ERROR [KI-023] ... crm.customer.city is 'int' here but 'string' on line 5 — keep one of them`
- **Why:** one column of one table (same target_domain + target_table) is listed twice with different types.
- **Fix:** delete the row with the wrong type.

<a id="ki-024"></a>
### KI-024 · Column does not exist in the source table

- **You see:** `ERROR [KI-024] ... column 'nope' does not exist in source table crm.customer`
- **Why:** every `column_name` in `bronze_columns.csv` must be a column of the job's source table.
- **Fix:** correct the spelling or remove the row.

<a id="ki-025"></a>
### KI-025 · event_col is not a date / timestamp

- **You see:** `ERROR [KI-025] ... event_col 'amount' is 'numeric'; it must be a date or timestamp column`
- **Why:** the job filters `event_col >= cob_dt AND event_col < cob_dt + 1`, which only makes sense on a date / time.
- **Fix:** choose the date / timestamp column of the event.

<a id="ki-026"></a>
### KI-026 · source_filter is not valid

- **You see:** `source_filter: WHERE predicate must not contain [';']`, or
  `source_filter does not run in Postgres: column "stat" does not exist`.
- **Why:** the filter is added to the source query as `WHERE (<source_filter>)`; it must be a valid Postgres
  predicate on the source table, without `WHERE`, `;` or comments.
- **Fix:** correct the predicate (e.g. `status <> 'CLOSED'`); quote the cell if it contains a comma.

<a id="ki-027"></a>
### KI-027 · Target table defined twice

- **You see:** `ERROR [KI-027] ... target table crm.customer is already defined on line 2`
- **Why:** each target_domain + target_table is one bronze table, so it can only have one row.
- **Fix:** remove the extra row or give it another `target_table`.

<a id="ki-028"></a>
### KI-028 · Columns for an unknown table

- **You see:** `ERROR [KI-028] ... columns for crm.ghost2, which is not a target table in bronze_tables.csv`
- **Why:** rows in `bronze_columns.csv` must belong to a job of `bronze_tables.csv` (same target_domain + target_table).
- **Fix:** add the job to `bronze_tables.csv`, or fix the names in `bronze_columns.csv`.

<a id="ki-029"></a>
### KI-029 · CSV format: header or number of fields

- **You see:** `header must be ...` or `wrong number of fields — quote cells that contain a comma`.
- **Why:** the header must match exactly; a value with a comma (e.g. `decimal(15,2)`, `status IN ('A', 'B')`)
  splits the row unless it is quoted.
- **Fix:** restore the header; write `"decimal(15,2)"` with quotes (Excel does this when saving as CSV).

<a id="ki-030"></a>
### KI-030 · Invalid name or active value

- **You see:** `... must be lower-case letters, digits and _` or `active must be 1 or 0`.
- **Why:** domains, tables and columns become Iceberg / folder / file names; `active` is a switch.
- **Fix:** use lower-case names like `core_banking`, and `1` / `0` for `active`.

<a id="ki-031"></a>
### KI-031 · Incremental job without a date window

- **You see:** `incremental job needs a source_filter for one date`, `incremental source_filter must use {cob_dt}`,
  `... must filter on event_col`, `unknown placeholder(s)`, or `{cob_dt} / {next_dt} are only for incremental jobs`.
- **Why:** an incremental job loads ONE business date per run; `source_filter` is that window. `{cob_dt}` and
  `{next_dt}` (= cob_dt + 1 day) are filled in for every date loaded (without a date the job reloads the whole table). Snapshot jobs copy the whole table, so they
  have no window.
- **Fix:** e.g. `transaction_date >= DATE '{cob_dt}' AND transaction_date < DATE '{next_dt}'` (on the job's `event_col`).
  Only `{cob_dt}` and `{next_dt}` exist.

<a id="ki-032"></a>
### KI-032 · event_col is not loaded

- **You see:** `event_col 'transaction_date' is not in the columns of core_banking.t_cols in bronze_columns.csv — add it`
- **Why:** the first run of an incremental job (init) puts each row in the partition of the date of its `event_col`,
  so that column must be one of the loaded columns.
- **Fix:** add the `event_col` row to `bronze_columns.csv` (or list no columns at all = load every column).

---

## Data issues

None yet. Use `DI-xxx` entries for source data problems (late corrections, wrong values, missing rows) and write down
the partial reload (`--where`) that fixed them.

---

## Add a new entry

1. Take the next free ID: **KI-037** (technical) or **DI-001** (data). IDs are never reused or renumbered
   (removed: KI-005, KI-006, KI-007, KI-008, KI-013, KI-015, KI-016), so references elsewhere stay valid.
2. Pick its category (Setup & configuration, Coding, SQL & tables, Resources, Validate — or Data issues) and add one line to
   that category's table in the Quick list. Put the entry in the matching section. If none fits, add a new category
   (section + Quick-list table + line in the category table).
3. Copy the block below.

Do not add: issues about git or Git Bash, a port already used by another program, or a file (e.g. Excel)
that is open / locked by someone else — those are obvious and handled by hand.

```markdown
<a id="ki-0xx"></a>
### KI-0xx · <short title in plain words>

- **You see:** <the error message or what looks wrong>
- **Why:** <one sentence>
- **Fix:** <what to do, or "Already fixed in <file>">
```

For a data issue, also write down the reload:

```markdown
<a id="di-0xx"></a>
### DI-0xx · <short title>

- **You see:** e.g. bronze amounts differ from the core report for cob_dt 2026-11-20
- **Why:** e.g. the core system corrected `status` after close of business
- **Fix:** the exact command, e.g.
  `spark-submit /home/spark/jobs/bronze/core_banking/bronze__transaction.py --cob-dt 2026-11-20 --where "transaction_id IN (8001, 8002)"`
  → batch `transaction_20261120_...`
```

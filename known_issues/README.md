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
| [Data issues](#data-issues) | Wrong / late / corrected source data (`DI-xxx`). |

**Setup & configuration**

| ID | You see… | What to do |
|---|---|---|
| [KI-003](#ki-003) | Job runs but is not on the Spark UI | Nothing — already fixed in the config |
| [KI-009](#ki-009) | Spark UI shows **0 workers** | Wait 30 s, then check the worker log |
| [KI-011](#ki-011) | `Unable to load region` | Nothing — already fixed in the config |
| [KI-014](#ki-014) | Other containers restart too | Normal — wait, then check the workers |

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

**Resources (memory / CPU / disk limits)**

| ID | You see… | What to do |
|---|---|---|
| [KI-010](#ki-010) | Executors get killed | Give the worker container more memory |

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
- **Fix:** `AWS_REGION` (docker-compose) and `spark.sql.catalog.lakehouse.s3.region` (spark-defaults). Already set.

<a id="ki-014"></a>
### KI-014 · Restarting one container restarts others

- **You see:** after `docker compose up -d <service>`, other containers restart too (e.g. Trino restarts Postgres
  and MinIO; spark-client restarts spark-master).
- **Why:** Docker Compose also restarts the services listed in `depends_on`. Data is safe (it is in volumes).
- **Fix:** nothing to fix — wait until http://localhost:8080 shows the worker again before running a Spark job.

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
  All bronze jobs already do this (`write_partition()`).

<a id="ki-017"></a>
### KI-017 · "Column is out of order" when writing to Iceberg

- **You see:** `Cannot write incompatible dataset ... * _source_filter is out of order, before _row_hash`
- **Why:** a new column is always added at the **end** of an Iceberg table, while the job puts it in another place;
  Iceberg checks the column order by default.
- **Fix:** `spark.sql.iceberg.check-ordering false` in `conf/spark-defaults.conf` (match columns by name).
  Already set — do not remove it.

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

## Data issues

None yet. Use `DI-xxx` entries for source data problems (late corrections, wrong values, missing rows) and write down
the partial reload (`--where`) that fixed them.

---

## Add a new entry

1. Take the next free ID: **KI-018** (technical) or **DI-001** (data). IDs are never reused or renumbered
   (removed: KI-005, KI-006, KI-007, KI-008, KI-013, KI-015, KI-016), so references elsewhere stay valid.
2. Pick its category (Setup & configuration, Coding, SQL & tables, Resources — or Data issues) and add one line to
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
